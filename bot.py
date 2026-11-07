import copy
import json
import logging
import os
import queue
import re
import shutil
import signal
import sqlite3
import subprocess
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import requests


# ---------- Configuration ----------

TOKEN = "8758704422:AAGTexKCiwc1ASoolDYb39NFBTEgWMbo9ZM".strip()

if not TOKEN:
    raise SystemExit("Bot token is required.")


DATA = Path(os.environ.get("DATA_DIR", "./data")).resolve()
WORK = DATA / "work"
DATA.mkdir(parents=True, exist_ok=True)
WORK.mkdir(parents=True, exist_ok=True)

ALLOWED = {
    int(x.strip())
    for x in os.environ.get("ALLOWED_USERS", "").split(",")
    if x.strip()
}

THREADS = max(1, min(int(os.environ.get("FFMPEG_THREADS", "2")), 8))
MAX_SECONDS = max(60, int(os.environ.get("MAX_VIDEO_SECONDS", "1800")))
ENCODE_TIMEOUT = max(60, int(os.environ.get("ENCODE_TIMEOUT", "3600")))

DOWNLOAD_LIMIT = 19_000_000
UPLOAD_LIMIT = 49_000_000
MAX_ACTIVE_PER_USER = 10
MAX_ACTIVE_TOTAL = 30
MAX_PIXELS = 3840 * 2160

STOP = threading.Event()
DB_LOCK = threading.RLock()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
LOG = logging.getLogger("compressor")

DEFAULTS = {
    "codec": "h264",
    "quality": "balanced",
    "height": 720,
    "fps": 0,
    "audio": 96,
    "delivery": "document",
    "auto": False,
    "strip_metadata": True,
}

CODECS = {
    "h264": {
        "encoder": "libx264",
        "title": "H.264",
        "crf": {"quality": 21, "balanced": 25, "small": 29},
    },
    "hevc": {
        "encoder": "libx265",
        "title": "H.265 / HEVC",
        "crf": {"quality": 24, "balanced": 28, "small": 32},
    },
    "av1": {
        "encoder": "libsvtav1",
        "title": "AV1",
        "crf": {"quality": 28, "balanced": 33, "small": 38},
    },
}

QUALITY_LABELS = {
    "quality": "کیفیت بیشتر",
    "balanced": "متعادل",
    "small": "حجم کمتر",
}

CHOICES = {
    "codec": [("H.264", "h264"), ("H.265", "hevc"), ("AV1", "av1")],
    "quality": [
        ("کیفیت بیشتر", "quality"),
        ("متعادل", "balanced"),
        ("حجم کمتر", "small"),
    ],
    "height": [("اصلی", 0), ("1080", 1080), ("720", 720), ("480", 480)],
    "fps": [("اصلی", 0), ("30", 30), ("24", 24)],
    "audio": [("128 kbps", 128), ("96 kbps", 96), ("بی‌صدا", 0)],
    "delivery": [("فایل", "document"), ("ویدیوی قابل پخش", "video")],
    "auto": [("تأیید هر فایل", False), ("صف خودکار", True)],
    "strip_metadata": [("حذف متادیتای قابل حذف", True), ("حفظ نسبی", False)],
}


class UserError(Exception):
    pass


class Cancelled(Exception):
    pass


class Requeue(Exception):
    pass


# ---------- Database ----------

DB = sqlite3.connect(DATA / "state.sqlite3", check_same_thread=False)
DB.row_factory = sqlite3.Row
DB.execute("PRAGMA journal_mode=WAL")
DB.execute("PRAGMA busy_timeout=5000")
DB.executescript("""
CREATE TABLE IF NOT EXISTS kv (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS users (
    uid INTEGER PRIMARY KEY,
    settings TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    uid INTEGER NOT NULL,
    chat INTEGER NOT NULL,
    file_id TEXT NOT NULL,
    name TEXT NOT NULL,
    size INTEGER NOT NULL,
    settings TEXT NOT NULL,
    status TEXT NOT NULL,
    message INTEGER,
    created REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS jobs_status_created
ON jobs(status, created);
""")
DB.commit()


def execute(sql, args=()):
    with DB_LOCK:
        cur = DB.execute(sql, args)
        DB.commit()
        return cur.rowcount


def one(sql, args=()):
    with DB_LOCK:
        row = DB.execute(sql, args).fetchone()
        return dict(row) if row else None


def many(sql, args=()):
    with DB_LOCK:
        return [dict(x) for x in DB.execute(sql, args).fetchall()]


def get_kv(key, default=None):
    row = one("SELECT value FROM kv WHERE key=?", (key,))
    return row["value"] if row else default


def put_kv(key, value):
    execute(
        "INSERT OR REPLACE INTO kv(key,value) VALUES(?,?)",
        (key, str(value)),
    )


def get_settings(uid):
    row = one("SELECT settings FROM users WHERE uid=?", (uid,))
    settings = copy.deepcopy(DEFAULTS)
    if row:
        settings.update(json.loads(row["settings"]))
    return settings


def save_settings(uid, settings):
    execute(
        "INSERT OR REPLACE INTO users(uid,settings) VALUES(?,?)",
        (uid, json.dumps(settings)),
    )


def authorized(uid, claim=False):
    if ALLOWED:
        return uid in ALLOWED

    with DB_LOCK:
        owner = get_kv("owner")
        if owner is None and claim:
            put_kv("owner", uid)
            owner = str(uid)
        return owner == str(uid)


def job_by_id(job_id):
    return one("SELECT * FROM jobs WHERE id=?", (job_id,))


# ---------- Telegram API ----------

def api(method, data=None, files=None, read_timeout=90):
    payload = {}
    for key, value in (data or {}).items():
        if value is None:
            continue
        if isinstance(value, (dict, list)):
            payload[key] = json.dumps(value, ensure_ascii=False)
        else:
            payload[key] = value

    # A shared requests.Session is intentionally not used across threads.
    for attempt in range(2):
        if files:
            for item in files.values():
                item[1].seek(0)

        try:
            response = requests.post(
                f"https://api.telegram.org/bot{TOKEN}/{method}",
                data=payload,
                files=files,
                timeout=(10, read_timeout),
            )
            body = response.json()
        except (requests.RequestException, ValueError):
            # Do not expose exception URLs: they may contain BOT_TOKEN.
            raise UserError("ارتباط با Telegram برقرار نشد.") from None

        if body.get("ok"):
            return body["result"]

        if body.get("error_code") == 429 and attempt == 0:
            delay = int(body.get("parameters", {}).get("retry_after", 3))
            if delay <= 30:
                if STOP.wait(max(1, delay)):
                    raise Requeue()
                continue

        description = str(body.get("description", "API error"))
        description = description.replace(TOKEN, "[TOKEN]")
        raise UserError(f"Telegram: {description[:180]}")

    raise UserError("درخواست Telegram ناموفق بود.")


def button(text, callback):
    return {"text": text, "callback_data": callback}


def keyboard(rows):
    return {"inline_keyboard": rows}


def send(chat, text, markup=None):
    return api(
        "sendMessage",
        {"chat_id": chat, "text": text, "reply_markup": markup},
    )


def safe_send(chat, text, markup=None):
    try:
        return send(chat, text, markup)
    except Exception:
        LOG.warning("Could not send notification.")
        return None


def edit(chat, message_id, text, markup=None):
    if not message_id:
        return
    try:
        api(
            "editMessageText",
            {
                "chat_id": chat,
                "message_id": message_id,
                "text": text,
                "reply_markup": markup or keyboard([]),
            },
            read_timeout=15,
        )
    except Exception:
        # An unchanged message or temporary API failure must not stop encoding.
        pass


def answer(callback_id, text=""):
    try:
        api("answerCallbackQuery", {
            "callback_query_id": callback_id,
            "text": text,
        }, read_timeout=15)
    except Exception:
        pass


# ---------- UI ----------

def mb(size):
    return f"{size / 1_000_000:.2f} MB"


def clean_name(name):
    name = Path(name or "video").name
    return "".join(c for c in name if c.isprintable())[:80] or "video"


def summary(settings):
    codec = CODECS[settings["codec"]]
    crf = codec["crf"][settings["quality"]]
    height = settings["height"] or "اصلی"
    fps = settings["fps"] or "اصلی"
    audio = f'{settings["audio"]} kbps' if settings["audio"] else "حذف صدا"
    delivery = (
        "ویدیو" if settings["delivery"] == "video"
        and settings["codec"] == "h264"
        else "فایل"
    )
    return (
        f'کدک: {codec["title"]}\n'
        f'کیفیت: {QUALITY_LABELS[settings["quality"]]} | CRF={crf}\n'
        f"سقف ارتفاع: {height} | FPS: {fps}\n"
        f"صدا: {audio} | ارسال: {delivery}\n"
        f'صف خودکار: {"روشن" if settings["auto"] else "خاموش"}\n'
        f'حذف متادیتا: {"روشن" if settings["strip_metadata"] else "خاموش"}'
    )


def home(chat):
    send(
        chat,
        "🎬 فشرده‌ساز ویدیو\n\n"
        "ویدیو را به شکل Video یا Document بفرست.\n"
        "می‌توانی چند ویدیو پشت‌سرهم ارسال کنی.\n"
        f"سقف ورودی این نسخه: {mb(DOWNLOAD_LIMIT)}",
        keyboard([
            [button("⚙️ تنظیمات", "menu:settings"),
             button("📋 صف", "menu:queue")],
            [button("❓ راهنما", "menu:help")],
        ]),
    )


def settings_menu(chat, uid, message_id=None):
    text = (
        "⚙️ تنظیمات فایل‌های بعدی\n\n"
        + summary(get_settings(uid))
        + "\n\nتغییرات روی کارهای قبلاً ثبت‌شده اثر ندارد."
    )
    markup = keyboard([
        [button("کدک", "pick:codec"), button("کیفیت", "pick:quality")],
        [button("رزولوشن", "pick:height"), button("FPS", "pick:fps")],
        [button("صدا", "pick:audio"), button("نوع ارسال", "pick:delivery")],
        [button("صف خودکار", "pick:auto"),
         button("متادیتا", "pick:strip_metadata")],
        [button("بازنشانی تنظیمات", "reset")],
    ])
    if message_id:
        edit(chat, message_id, text, markup)
    else:
        send(chat, text, markup)


def show_queue(chat, uid):
    rows = many(
        "SELECT * FROM jobs WHERE uid=? ORDER BY created DESC LIMIT 10",
        (uid,),
    )
    labels = {
        "awaiting": "منتظر تأیید",
        "queued": "در صف",
        "running": "در حال پردازش",
        "done": "ارسال‌شده",
        "failed": "ناموفق",
        "cancelled": "لغوشده",
    }

    if not rows:
        send(chat, "صف خالی است.")
        return

    lines = ["📋 آخرین کارها:"]
    buttons = []
    for row in rows:
        lines.append(
            f'{row["id"]} | {labels.get(row["status"], row["status"])}'
            f'\n{row["name"]}'
        )
        if row["status"] == "awaiting":
            buttons.append([
                button(f'تأیید {row["id"]}', f'go:{row["id"]}'),
                button("لغو", f'cancel:{row["id"]}'),
            ])
        elif row["status"] in ("queued", "running"):
            buttons.append([
                button(f'لغو {row["id"]}', f'cancel:{row["id"]}')
            ])

    send(chat, "\n\n".join(lines), keyboard(buttons))


def help_text():
    return (
        "راهنما\n\n"
        "۱. تنظیمات را انتخاب کن.\n"
        "۲. ویدیو یا فایل ویدیویی بفرست.\n"
        "۳. کارت مشخصات را تأیید کن.\n"
        "۴. نتیجه و گزارش نهایی را دریافت کن.\n\n"
        "H.264: سریع‌تر و سازگارتر.\n"
        "H.265: معمولاً فشرده‌تر، سازگاری کمتر.\n"
        "AV1: پردازش CPU سنگین‌تر؛ مناسب کارهای غیرعجله‌ای.\n\n"
        "کیفیت بیشتر یعنی حجم معمولاً بیشتر.\n"
        "حجم نهایی قبل از انکود قابل اعلام دقیق نیست.\n"
        "درصد پیشرفت مربوط به انکود است، نه آپلود.\n"
        "زیرنویس، فصل‌ها و ترک‌های اضافه منتقل نمی‌شوند.\n"
        "HDR در این نسخه پذیرفته نمی‌شود.\n\n"
        "/settings تنظیمات\n"
        "/queue صف و لغو تکی\n"
        "/status همان صف\n"
        "/cancel لغو همه کارهای فعال خودت\n"
        "/privacy توضیح نگهداری داده‌ها"
    )


# ---------- Receiving videos ----------

def receive_video(message, uid, chat):
    media = message.get("video") or message.get("document")
    if not media:
        send(chat, "لطفاً یک ویدیو یا فایل ویدیویی ارسال کن.")
        return

    name = clean_name(media.get("file_name") or "video.mp4")
    mime = media.get("mime_type", "")
    extensions = (".mp4", ".mkv", ".mov", ".webm", ".avi", ".m4v", ".ts")

    if (
        "video" not in message
        and not mime.startswith("video/")
        and not name.lower().endswith(extensions)
    ):
        send(chat, "این فایل ظاهراً ویدیویی نیست.")
        return

    size = int(media.get("file_size", 0))
    if size > DOWNLOAD_LIMIT:
        send(
            chat,
            f"فایل بزرگ‌تر از سقف این نسخه است: {mb(DOWNLOAD_LIMIT)}.\n"
            "برای فایل‌های بزرگ، Local Bot API Server لازم است.",
        )
        return

    settings = get_settings(uid)
    job_id = uuid.uuid4().hex[:12]
    status = "queued" if settings["auto"] else "awaiting"

    with DB_LOCK:
        active = one(
            "SELECT COUNT(*) AS n FROM jobs "
            "WHERE status IN ('awaiting','queued','running')"
        )["n"]
        personal = one(
            "SELECT COUNT(*) AS n FROM jobs WHERE uid=? "
            "AND status IN ('awaiting','queued','running')",
            (uid,),
        )["n"]

        if active >= MAX_ACTIVE_TOTAL or personal >= MAX_ACTIVE_PER_USER:
            send(chat, "صف پر است؛ کمی صبر کن یا کارهای قبلی را لغو کن.")
            return

        execute(
            "INSERT INTO jobs "
            "(id,uid,chat,file_id,name,size,settings,status,message,created) "
            "VALUES(?,?,?,?,?,?,?,?,?,?)",
            (
                job_id, uid, chat, media["file_id"], name, size,
                json.dumps(settings), status, None, time.time(),
            ),
        )

    actions = [[button("لغو", f"cancel:{job_id}")]]
    if status == "awaiting":
        actions[0].insert(0, button("✅ شروع فشرده‌سازی", f"go:{job_id}"))

    text = (
        f"🎞 کار: {job_id}\n"
        f"فایل: {name}\n"
        f'حجم ورودی: {mb(size) if size else "بعد از دانلود مشخص می‌شود"}\n\n'
        f"{summary(settings)}\n\n"
        "خروجی: MP4، ویدیوی 8-bit SDR.\n"
        "حجم دقیق پس از پردازش مشخص می‌شود.\n"
        "تنها اولین ترک ویدیو و اولین ترک صدا استفاده می‌شود.\n\n"
        + ("در صف قرار گرفت." if status == "queued" else "شروع را تأیید کن.")
    )

    result = safe_send(chat, text, keyboard(actions))
    if result:
        execute(
            "UPDATE jobs SET message=? WHERE id=?",
            (result["message_id"], job_id),
        )


# ---------- Update handling ----------

def handle_callback(callback):
    message = callback.get("message", {})
    if message.get("chat", {}).get("type") != "private":
        answer(callback["id"], "فقط گفت‌وگوی خصوصی.")
        return

    uid = callback["from"]["id"]
    chat = message["chat"]["id"]
    mid = message["message_id"]

    if not authorized(uid):
        answer(callback["id"], "اجازه دسترسی نداری.")
        return

    answer(callback["id"])
    data = callback.get("data", "")
    parts = data.split(":")

    if data == "menu:settings":
        settings_menu(chat, uid, mid)
    elif data == "menu:queue":
        show_queue(chat, uid)
    elif data == "menu:help":
        send(chat, help_text())
    elif data == "reset":
        save_settings(uid, copy.deepcopy(DEFAULTS))
        settings_menu(chat, uid, mid)
    elif parts[0] == "pick" and len(parts) == 2 and parts[1] in CHOICES:
        key = parts[1]
        rows = [
            [button(label, f"set:{key}:{index}")]
            for index, (label, _) in enumerate(CHOICES[key])
        ]
        rows.append([button("بازگشت", "menu:settings")])
        edit(chat, mid, "یک گزینه انتخاب کن:", keyboard(rows))
    elif parts[0] == "set" and len(parts) == 3 and parts[1] in CHOICES:
        key = parts[1]
        try:
            index = int(parts[2])
            if index < 0:
                return
            value = CHOICES[key][index][1]
        except (ValueError, IndexError):
            return
        settings = get_settings(uid)
        settings[key] = value
        save_settings(uid, settings)
        settings_menu(chat, uid, mid)
    elif parts[0] in ("go", "cancel") and len(parts) == 2:
        job = job_by_id(parts[1])
        if not job or job["uid"] != uid:
            return

        if parts[0] == "go":
            changed = execute(
                "UPDATE jobs SET status='queued' WHERE id=? AND status='awaiting'",
                (job["id"],),
            )
            if changed:
                edit(
                    chat, mid, f'✅ کار {job["id"]} در صف قرار گرفت.',
                    keyboard([[button("لغو", f'cancel:{job["id"]}')]]),
                )
        else:
            changed = execute(
                "UPDATE jobs SET status='cancelled' WHERE id=? "
                "AND status IN ('awaiting','queued','running')",
                (job["id"],),
            )
            if changed:
                edit(chat, mid, f'لغو کار {job["id"]} ثبت شد.')


def handle_message(message):
    if message.get("chat", {}).get("type") != "private":
        return

    uid = message.get("from", {}).get("id")
    if not uid:
        return

    chat = message["chat"]["id"]
    text = message.get("text", "").strip()
    command = text.split()[0].split("@")[0] if text.startswith("/") else ""

    if not authorized(uid, claim=command == "/start"):
        send(chat, "این ربات خصوصی است و دسترسی شما فعال نیست.")
        return

    if command == "/start":
        home(chat)
    elif command == "/settings":
        settings_menu(chat, uid)
    elif command in ("/queue", "/status"):
        show_queue(chat, uid)
    elif command == "/help":
        send(chat, help_text())
    elif command == "/privacy":
        send(
            chat,
            "فایل‌های موقت بعد از پایان هر کار حذف می‌شوند.\n"
            "تنظیمات، شناسه تلگرام، file_id، نام فایل و وضعیت کارها "
            "در دیتابیس سرور ذخیره می‌شوند.\n"
            "حذف فایل محلی، به معنی حذف نسخه موجود در Telegram نیست.\n"
            "حذف متادیتا تضمین ناشناس‌سازی محتوا نیست.",
        )
    elif command == "/cancel":
        count = execute(
            "UPDATE jobs SET status='cancelled' WHERE uid=? "
            "AND status IN ('awaiting','queued','running')",
            (uid,),
        )
        send(
            chat,
            f"لغو {count} کار ثبت شد.\n"
            "ارسالی که قبلاً وارد مرحله آپلود شده ممکن است قابل توقف نباشد.",
        )
    elif message.get("video") or message.get("document"):
        receive_video(message, uid, chat)
    else:
        home(chat)


def handle_update(update):
    if "callback_query" in update:
        handle_callback(update["callback_query"])
    elif "message" in update:
        handle_message(update["message"])


# ---------- Download / media inspection ----------

def check_job(job_id):
    if STOP.is_set():
        raise Requeue()
    row = job_by_id(job_id)
    if not row or row["status"] == "cancelled":
        raise Cancelled()


def download(job, target):
    info = api("getFile", {"file_id": job["file_id"]})
    if int(info.get("file_size", 0)) > DOWNLOAD_LIMIT:
        raise UserError("حجم فایل از سقف دانلود این نسخه بیشتر است.")

    path = info.get("file_path")
    if not path:
        raise UserError("مسیر دانلود فایل دریافت نشد.")

    total = 0
    try:
        with requests.get(
            f"https://api.telegram.org/file/bot{TOKEN}/{path}",
            stream=True,
            timeout=(10, 30),
        ) as response:
            response.raise_for_status()
            with target.open("wb") as output:
                for chunk in response.iter_content(256 * 1024):
                    check_job(job["id"])
                    if not chunk:
                        continue
                    total += len(chunk)
                    if total > DOWNLOAD_LIMIT:
                        raise UserError("فایل از سقف دانلود مجاز بزرگ‌تر است.")
                    output.write(chunk)
    except requests.RequestException:
        raise UserError("دانلود فایل از Telegram ناموفق بود.") from None

    if total == 0:
        raise UserError("فایل دانلودشده خالی است.")

    return total


def probe(path):
    try:
        result = subprocess.run(
            [
                "ffprobe", "-v", "error",
                "-protocol_whitelist", "file,pipe",
                "-show_format", "-show_streams",
                "-of", "json", str(path),
            ],
            capture_output=True,
            text=True,
            timeout=30
        )
    except (subprocess.TimeoutExpired, OSError):
        raise UserError("بررسی مشخصات فایل ناموفق بود.") from None

    if result.returncode:
        raise UserError("فایل خراب است یا فرمت قابل پردازشی ندارد.")

    try:
        data = json.loads(result.stdout)
        streams = data.get("streams", [])
        video = next(
            s for s in streams
            if s.get("codec_type") == "video"
            and not s.get("disposition", {}).get("attached_pic")
        )

        duration = float(
            video.get("duration")
            or data.get("format", {}).get("duration")
            or 0
        )
        width = int(video.get("width", 0))
        height = int(video.get("height", 0))

        fraction = video.get("avg_frame_rate", "0/1")
        numerator, denominator = fraction.split("/")
        fps = float(numerator) / max(float(denominator), 1)

        rotation = 0
        for side in video.get("side_data_list", []):
            if "rotation" in side:
                rotation = int(float(side["rotation"]))
        if abs(rotation) % 180 == 90:
            width, height = height, width

        audio = next(
            (s for s in streams if s.get("codec_type") == "audio"),
            None,
        )

        return {
            "duration": duration,
            "width": width,
            "height": height,
            "fps": fps,
            "codec": video.get("codec_name", "?"),
            "pixel_format": video.get("pix_fmt", "?"),
            "transfer": video.get("color_transfer", ""),
            "video_index": int(video["index"]),
            "audio_index": int(audio["index"]) if audio else None,
        }
    except (ValueError, TypeError, KeyError, StopIteration, ZeroDivisionError):
        raise UserError("مشخصات ویدیو قابل تشخیص نیست.") from None


def validate_media(info):
    import math

    if not math.isfinite(info["duration"]) or info["duration"] <= 0:
        raise UserError("مدت ویدیو معتبر نیست.")
    if info["duration"] > MAX_SECONDS:
        raise UserError(
            f"مدت ویدیو بیشتر از سقف {MAX_SECONDS // 60} دقیقه است."
        )
    if info["width"] < 2 or info["height"] < 2:
        raise UserError("ابعاد ویدیو نامعتبر است.")
    if info["width"] * info["height"] > MAX_PIXELS:
        raise UserError("رزولوشن ورودی از سقف مجاز بیشتر است.")
    if info["transfer"] in ("smpte2084", "arib-std-b67"):
        raise UserError(
            "ویدیوی HDR شناسایی شد. این نسخه تبدیل HDR به SDR ندارد."
        )


# ---------- Encoding ----------

def build_command(source, target, settings, info):
    selected = CODECS[settings["codec"]]
    crf = selected["crf"][settings["quality"]]

    target_height = min(
        info["height"],
        settings["height"] or info["height"],
    )
    target_height = max(2, (target_height // 2) * 2)

    filters = [f"scale=-2:{target_height}:flags=lanczos"]

    # FPS settings act as a ceiling; low-FPS video is not upsampled.
    if settings["fps"] and (
        info["fps"] <= 0 or info["fps"] > settings["fps"]
    ):
        filters.append(f'fps={settings["fps"]}')

    command = [
        "ffmpeg",
        "-hide_banner",
        "-nostdin",
        "-y",
        "-loglevel", "error",
        "-progress", "pipe:1",
        "-stats_period", "1",
        "-filter_threads", str(THREADS),
        "-threads", str(THREADS),
        "-protocol_whitelist", "file,pipe",
        "-i", str(source),
        "-map", f'0:{info["video_index"]}',
    ]

    if settings["audio"] and info["audio_index"] is not None:
        command += [
            "-map", f'0:{info["audio_index"]}',
            "-c:a", "aac",
            "-b:a", f'{settings["audio"]}k',
            "-ac", "2",
        ]
    else:
        command += ["-an"]

    command += [
        "-sn", "-dn",
        "-vf", ",".join(filters),
        "-c:v", selected["encoder"],
        "-crf", str(crf),
        "-pix_fmt", "yuv420p",
        "-threads", str(THREADS),
    ]

    if settings["codec"] == "h264":
        command += ["-preset", "medium"]
    elif settings["codec"] == "hevc":
        command += [
            "-preset", "medium",
            "-tag:v", "hvc1",
            "-x265-params",
            f"pools={THREADS}:frame-threads=1:log-level=error",
        ]
    else:
        command += [
            "-preset", "8",
            "-svtav1-params", f"lp={THREADS}",
        ]

    if settings["strip_metadata"]:
        command += ["-map_metadata", "-1"]

    command += [
        "-map_chapters", "-1",
        "-t", str(MAX_SECONDS),
        "-movflags", "+faststart",
        str(target),
    ]
    return command


def stop_process(process):
    if process.poll() is not None:
        return
    try:
        process.terminate()
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()
    except ProcessLookupError:
        pass


def encode(job, source, target, settings, info, folder):
    command = build_command(source, target, settings, info)
    progress = queue.Queue()
    started = time.monotonic()
    last_edit = 0.0
    media_seconds = 0.0
    speed = "—"

    # stderr goes to a file rather than a pipe to prevent a pipe deadlock.
    error_path = folder / "ffmpeg.log"

    with error_path.open("wb") as errors:
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=errors,
            text=True,
            bufsize=1,
        )

        def read_progress():
            try:
                for line in process.stdout:
                    progress.put(line.strip())
            finally:
                process.stdout.close()

        reader = threading.Thread(target=read_progress, daemon=True)
        reader.start()

        try:
            while process.poll() is None:
                check_job(job["id"])

                elapsed = time.monotonic() - started
                if elapsed > ENCODE_TIMEOUT:
                    raise UserError("زمان مجاز فشرده‌سازی تمام شد.")

                if target.exists() and target.stat().st_size > UPLOAD_LIMIT:
                    raise UserError(
                        "خروجی از سقف ارسال بیشتر شد؛ "
                        "کیفیت یا رزولوشن را کاهش بده."
                    )

                while True:
                    try:
                        line = progress.get_nowait()
                    except queue.Empty:
                        break

                    key, _, value = line.partition("=")
                    if key == "out_time_us":
                        try:
                            media_seconds = max(
                                media_seconds, int(value) / 1_000_000
                            )
                        except ValueError:
                            pass
                    elif key == "speed":
                        speed = value

                if elapsed - last_edit >= 6:
                    last_edit = elapsed
                    percent = min(
                        99.5, media_seconds / info["duration"] * 100
                    )
                    remaining = "در حال محاسبه"
                    if media_seconds > 0:
                        estimate = (
                            (info["duration"] - media_seconds)
                            * elapsed / media_seconds
                        )
                        remaining = f"{max(0, int(estimate))} ثانیه"

                    filled = min(10, int(percent // 10))
                    bar = "█" * filled + "░" * (10 - filled)

                    edit(
                        job["chat"],
                        job["message"],
                        f'🎬 کار {job["id"]}\n'
                        f'{job["name"]}\n\n'
                        f"{bar} {percent:.1f}%\n"
                        f"سرعت: {speed}\n"
                        f"زمان سپری‌شده: {int(elapsed)} ثانیه\n"
                        f"زمان تقریبی باقی‌مانده: {remaining}\n\n"
                        "این درصد مربوط به انکود است؛ "
                        "نهایی‌سازی MP4 و آپلود جداست.",
                        keyboard([[
                            button("لغو", f'cancel:{job["id"]}')
                        ]]),
                    )

                time.sleep(0.3)

            reader.join(timeout=2)
            check_job(job["id"])

            if process.returncode != 0:
                # FFmpeg log remains local and is removed with the job folder.
                raise UserError(
                    "FFmpeg نتوانست این فایل را پردازش کند. "
                    "کدک H.264 یا تنظیمات ساده‌تر را امتحان کن."
                )

            if not target.exists() or target.stat().st_size == 0:
                raise UserError("خروجی معتبری تولید نشد.")

            if target.stat().st_size > UPLOAD_LIMIT:
                raise UserError(
                    "حجم خروجی از سقف ارسال بیشتر است؛ "
                    "رزولوشن یا کیفیت را کاهش بده."
                )

            return time.monotonic() - started
        finally:
            stop_process(process)
            reader.join(timeout=2)


# ---------- Job execution ----------

def upload_result(job, target, settings, info, caption):
    filename = f'compressed_{settings["codec"]}_{job["id"]}.mp4'
    as_video = (
        settings["delivery"] == "video"
        and settings["codec"] == "h264"
    )
    method = "sendVideo" if as_video else "sendDocument"
    field = "video" if as_video else "document"

    data = {
        "chat_id": job["chat"],
        "caption": caption,
    }
    if as_video:
        data.update({
            "duration": max(1, int(info["duration"])),
            "width": info["width"],
            "height": info["height"],
            "supports_streaming": "true",
        })

    with target.open("rb") as file:
        api(
            method,
            data,
            files={field: (filename, file, "video/mp4")},
            read_timeout=300,
        )


def process_job(job):
    folder = WORK / job["id"]
    folder.mkdir(parents=True, exist_ok=True)
    source = folder / "input"
    target = folder / "output.mp4"
    started = time.monotonic()

    try:
        check_job(job["id"])

        if shutil.disk_usage(WORK).free < 512_000_000:
            raise UserError("فضای آزاد دیسک کمتر از 512 MB است.")

        if not job["message"]:
            message = send(
                job["chat"], f'شروع کار {job["id"]}…'
            )
            job["message"] = message["message_id"]
            execute(
                "UPDATE jobs SET message=? WHERE id=?",
                (job["message"], job["id"]),
            )

        edit(
            job["chat"], job["message"],
            f'📥 در حال دانلود کار {job["id"]}…',
            keyboard([[button("لغو", f'cancel:{job["id"]}')]]),
        )

        input_size = download(job, source)
        check_job(job["id"])
        info = probe(source)
        validate_media(info)
        settings = json.loads(job["settings"])

        # Actual source characteristics are available only after download.
        edit(
            job["chat"], job["message"],
            f'🔎 ورودی: {info["width"]}×{info["height"]}\n'
            f'مدت: {info["duration"]:.1f} ثانیه\n'
            f'کدک ورودی: {info["codec"]}\n\n'
            "فشرده‌سازی در حال شروع است…",
            keyboard([[button("لغو", f'cancel:{job["id"]}')]]),
        )

        encoding_seconds = encode(
            job, source, target, settings, info, folder
        )
        output_info = probe(target)
        output_size = target.stat().st_size
        reduction = (1 - output_size / input_size) * 100

        if reduction >= 0:
            change = f"کاهش حجم: {reduction:.1f}%"
        else:
            change = f"⚠️ افزایش حجم: {abs(reduction):.1f}%"

        caption = (
            f'✅ نتیجه کار {job["id"]}\n'
            f'نام: {job["name"]}\n\n'
            f"ورودی: {mb(input_size)}\n"
            f"خروجی: {mb(output_size)}\n"
            f"{change}\n"
            f"نسبت حجم ورودی به خروجی: {input_size / output_size:.2f}×\n"
            f'کدک: {CODECS[settings["codec"]]["title"]}\n'
            f'ابعاد: {output_info["width"]}×{output_info["height"]}\n'
            f'FPS: {output_info["fps"]:.2f}\n'
            f'مدت: {output_info["duration"]:.1f} ثانیه\n'
            f"زمان انکود: {encoding_seconds:.1f} ثانیه"
        )

        check_job(job["id"])
        edit(
            job["chat"], job["message"],
            f'📤 انکود کار {job["id"]} تمام شد.\n'
            f"حجم خروجی: {mb(output_size)}\n"
            "در حال ارسال؛ درصد آپلود در این نسخه نمایش داده نمی‌شود.",
        )

        upload_result(job, target, settings, output_info, caption)

        # If cancellation arrived during upload, the completed delivery wins.
        execute(
            "UPDATE jobs SET status='done' WHERE id=?",
            (job["id"],),
        )
        edit(
            job["chat"], job["message"],
            f'✅ کار {job["id"]} ارسال شد.\n'
            f"زمان کل: {time.monotonic() - started:.1f} ثانیه",
        )

    except Cancelled:
        execute(
            "UPDATE jobs SET status='cancelled' WHERE id=?",
            (job["id"],),
        )
        edit(
            job["chat"], job["message"],
            f'⛔ کار {job["id"]} لغو شد.',
        )

    except Requeue:
        execute(
            "UPDATE jobs SET status='queued' "
            "WHERE id=? AND status='running'",
            (job["id"],),
        )

    except Exception as exc:
        # On shutdown, retain unfinished jobs for the next startup.
        if STOP.is_set():
            execute(
                "UPDATE jobs SET status='queued' "
                "WHERE id=? AND status='running'",
                (job["id"],),
            )
        else:
            current = job_by_id(job["id"])
            if current and current["status"] == "cancelled":
                return

            execute(
                "UPDATE jobs SET status='failed' WHERE id=?",
                (job["id"],),
            )
            reason = (
                str(exc) if isinstance(exc, UserError)
                else "خطای داخلی پردازش."
            )
            LOG.warning(
                "Job %s failed (%s).",
                job["id"], type(exc).__name__,
            )
            safe_send(
                job["chat"],
                f'❌ کار {job["id"]} ناموفق بود.\n{reason}',
            )
            edit(
                job["chat"], job["message"],
                f'❌ کار {job["id"]} ناموفق بود.',
            )
    finally:
        shutil.rmtree(folder, ignore_errors=True)


def worker():
    while not STOP.is_set():
        with DB_LOCK:
            job = one(
                "SELECT * FROM jobs WHERE status='queued' "
                "ORDER BY created LIMIT 1"
            )
            if job:
                execute(
                    "UPDATE jobs SET status='running' WHERE id=?",
                    (job["id"],),
                )

        if not job:
            STOP.wait(1)
            continue

        process_job(job)


# ---------- Health endpoint and startup ----------

def start_health_server():
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path not in ("/", "/health"):
                self.send_error(404)
                return
            content = b"stopping" if STOP.is_set() else b"ok"
            self.send_response(503 if STOP.is_set() else 200)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", str(len(content)))
            self.end_headers()
            self.wfile.write(content)

        def log_message(self, *args):
            pass

    port = int(os.environ.get("PORT", "8080"))
    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


def check_encoders():
    if not shutil.which("ffmpeg") or not shutil.which("ffprobe"):
        raise SystemExit("ffmpeg and ffprobe must be installed.")

    result = subprocess.run(
        ["ffmpeg", "-hide_banner", "-encoders"],
        capture_output=True,
        text=True,
        timeout=20,
    )
    missing = [
        item["encoder"] for item in CODECS.values()
        if not re.search(
            rf'\b{re.escape(item["encoder"])}\b',
            result.stdout,
        )
    ]
    if missing:
        raise SystemExit(
            "Required FFmpeg encoders are missing: " + ", ".join(missing)
        )


def on_signal(signum, frame):
    STOP.set()


def main():
    signal.signal(signal.SIGTERM, on_signal)
    signal.signal(signal.SIGINT, on_signal)
    check_encoders()

    # Only one instance may use this data directory.
    import fcntl
    lock_file = (DATA / "instance.lock").open("a")
    try:
        fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        raise SystemExit("Another bot instance is using DATA_DIR.")

    # Remove leftovers from interrupted jobs.
    for path in WORK.iterdir():
        if path.is_dir():
            shutil.rmtree(path, ignore_errors=True)
        else:
            path.unlink(missing_ok=True)

    execute(
        "UPDATE jobs SET status='queued' WHERE status='running'"
    )

    identity = api("getMe")
    LOG.info("Starting @%s", identity.get("username", "bot"))

    # This bot uses long polling, not Telegram webhooks.
    api("deleteWebhook", {"drop_pending_updates": "false"})
    try:
        api("setMyCommands", {"commands": [
            {"command": "start", "description": "منوی اصلی"},
            {"command": "settings", "description": "تنظیمات فشرده‌سازی"},
            {"command": "queue", "description": "صف و وضعیت کارها"},
            {"command": "cancel", "description": "لغو تمام کارهای فعال"},
            {"command": "help", "description": "راهنما"},
            {"command": "privacy", "description": "نگهداری اطلاعات"},
        ]})
    except UserError:
        LOG.warning("Could not update bot commands.")

    health = start_health_server()
    thread = threading.Thread(target=worker, daemon=True)
    thread.start()
    offset = int(get_kv("update_offset", "0"))

    try:
        while not STOP.is_set():
            try:
                updates = api(
                    "getUpdates",
                    {
                        "offset": offset,
                        "timeout": 25,
                        "allowed_updates": ["message", "callback_query"],
                    },
                    read_timeout=40,
                )

                for update in updates:
                    if STOP.is_set():
                        break
                    try:
                        handle_update(update)
                    except Exception as exc:
                        LOG.warning(
                            "Update handling failed (%s).",
                            type(exc).__name__,
                        )
                    finally:
                        offset = update["update_id"] + 1
                        put_kv("update_offset", offset)

            except Requeue:
                break
            except Exception as exc:
                LOG.warning(
                    "Polling failed (%s); retrying.",
                    type(exc).__name__,
                )
                STOP.wait(3)
    finally:
        STOP.set()
        thread.join(timeout=15)
        health.shutdown()
        # Keep lock_file alive until the process exits.


if __name__ == "__main__":
    main()
