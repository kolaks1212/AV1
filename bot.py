import copy
import json
import logging
import lzma
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

try:
    import brotli
    import zstandard as zstd
except ImportError as exc:
    raise SystemExit(f"Missing compression library: {exc}")


# ---------- Configuration ----------

TOKEN = os.environ.get("BOT_TOKEN", "8758704422:AAGTexKCiwc1ASoolDYb39NFBTEgWMbo9ZM").strip()
API_BASE = "http://127.0.0.1:8081"
API_ID = os.environ.get("TELEGRAM_API_ID", "").strip()
API_HASH = os.environ.get("TELEGRAM_API_HASH", "").strip()


if not TOKEN:
    raise SystemExit("Bot token is required.")

USE_LOCAL_API = True
DOWNLOAD_LIMIT = 1_900_000_000
UPLOAD_LIMIT = 1_900_000_000

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
FILE_TIMEOUT = max(60, int(os.environ.get("FILE_TIMEOUT", "3600")))
MAX_CONCURRENT_DOWNLOADS = max(1, min(int(os.environ.get("MAX_CONCURRENT_DOWNLOADS", "3")), 10))

MAX_ACTIVE_PER_USER = 20
MAX_ACTIVE_TOTAL = 60
MAX_PIXELS = 3840 * 2160

STOP = threading.Event()
DB_LOCK = threading.RLock()
DOWNLOAD_SEM = threading.Semaphore(MAX_CONCURRENT_DOWNLOADS)
PENDING_PRESET_NAME = {}  # uid -> kind

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
LOG = logging.getLogger("compressor")


# ---------- Codecs ----------

VIDEO_CODECS = {
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

FILE_CODECS = {
    "zstd": {
        "title": "Zstandard",
        "ext": ".zst",
        "levels": {"fast": 1, "balanced": 6, "max": 19},
    },
    "brotli": {
        "title": "Brotli",
        "ext": ".br",
        "levels": {"fast": 1, "balanced": 6, "max": 11},
    },
    "lzma2": {
        "title": "LZMA2 (XZ)",
        "ext": ".xz",
        "levels": {"fast": 1, "balanced": 6, "max": 9},
    },
}

QUALITY_LABELS = {
    "quality": "کیفیت بیشتر",
    "balanced": "متعادل",
    "small": "حجم کمتر",
}

LEVEL_LABELS = {
    "fast": "سریع",
    "balanced": "متعادل",
    "max": "حداکثر فشرده‌سازی",
}

DEFAULTS = {
    # video
    "codec": "h264",
    "quality": "balanced",
    "height": 720,
    "fps": 0,
    "audio": 96,
    "delivery": "document",
    "auto": False,
    "strip_metadata": True,
    # file
    "file_codec": "zstd",
    "file_level": "balanced",
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
    "strip_metadata": [("حذف", True), ("حفظ نسبی", False)],
    "file_codec": [
        ("Zstandard", "zstd"),
        ("Brotli", "brotli"),
        ("LZMA2", "lzma2"),
    ],
    "file_level": [
        ("سریع", "fast"),
        ("متعادل", "balanced"),
        ("حداکثری", "max"),
    ],
}

INFO_TEXTS = {
    "codec": (
        "H.264: سازگاری بالا با همه دستگاه‌ها و برنامه‌ها.\n"
        "H.265: حجم کمتر با همان کیفیت، ولی برخی دستگاه‌های قدیمی باز نمی‌کنند.\n"
        "AV1: جدیدترین کدک، کم‌ترین حجم، پردازش کندتر و سازگاری محدود."
    ),
    "quality": (
        "کیفیت بیشتر: CRF پایین‌تر، حجم بیشتر، جزئیات بیشتر.\n"
        "متعادل: نقطه تعادل بین حجم و کیفیت.\n"
        "حجم کمتر: CRF بالاتر، حجم کم‌تر، افت جزئیات."
    ),
    "height": "سقف ارتفاع خروجی. اگر ویدیو کوتاه‌تر باشد، تغییری نمی‌کند.",
    "fps": "سقف نرخ فریم. ویدیوهای کم‌فریم بالا نمی‌روند.",
    "audio": "بی‌صدا: صدا کاملاً حذف می‌شود.",
    "delivery": (
        "فایل: ارسال به شکل Document، بدون فشرده‌سازی مجدد تلگرام.\n"
        "ویدیوی قابل پخش: فقط برای H.264 و پیش‌نمایش در تلگرام."
    ),
    "auto": (
        "تأیید هر فایل: قبل از شروع هر کار باید تأیید کنی.\n"
        "صف خودکار: بلافاصله پس از دریافت، در صف قرار می‌گیرد."
    ),
    "strip_metadata": "حذف متادیتا ممکن است برخی اطلاعات مثل GPS را پاک کند.",
    "file_codec": (
        "Zstandard: سریع و متعادل، مناسب اکثر فایل‌ها.\n"
        "Brotli: فشرده‌سازی بهتر برای متن و وب، کمی کندتر.\n"
        "LZMA2 (XZ): بالاترین نسبت فشرده‌سازی، کندترین."
    ),
    "file_level": (
        "سریع: کمترین زمان، فشرده‌سازی کمتر.\n"
        "متعادل: تعادل بین سرعت و نسبت.\n"
        "حداکثری: بیشترین نسبت، کندترین."
    ),
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
    created REAL NOT NULL,
    kind TEXT NOT NULL DEFAULT 'video'
);

CREATE INDEX IF NOT EXISTS jobs_status_created
ON jobs(status, created);

CREATE TABLE IF NOT EXISTS presets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    uid INTEGER NOT NULL,
    name TEXT NOT NULL,
    kind TEXT NOT NULL,
    settings TEXT NOT NULL,
    created REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS presets_uid_kind
ON presets(uid, kind);
""")

# Migration: add 'kind' if the DB is older
cols = [row[1] for row in DB.execute("PRAGMA table_info(jobs)").fetchall()]
if "kind" not in cols:
    DB.execute("ALTER TABLE jobs ADD COLUMN kind TEXT NOT NULL DEFAULT 'video'")
    DB.commit()

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
        try:
            settings.update(json.loads(row["settings"]))
        except (ValueError, TypeError):
            pass
    return settings


def save_settings(uid, settings):
    execute(
        "INSERT OR REPLACE INTO users(uid,settings) VALUES(?,?)",
        (uid, json.dumps(settings)),
    )


def save_preset(uid, name, kind, settings):
    if kind == "video":
        keys = ["codec", "quality", "height", "fps",
                "audio", "delivery", "strip_metadata"]
    else:
        keys = ["file_codec", "file_level"]
    subset = {k: settings[k] for k in keys if k in settings}
    execute(
        "INSERT INTO presets(uid,name,kind,settings,created) VALUES(?,?,?,?,?)",
        (uid, name[:32], kind, json.dumps(subset), time.time()),
    )


def list_presets(uid, kind):
    return many(
        "SELECT * FROM presets WHERE uid=? AND kind=? ORDER BY created DESC",
        (uid, kind),
    )


def get_preset(uid, preset_id):
    row = one(
        "SELECT * FROM presets WHERE id=? AND uid=?",
        (preset_id, uid),
    )
    return row


def delete_preset(uid, preset_id):
    return execute(
        "DELETE FROM presets WHERE id=? AND uid=?",
        (preset_id, uid),
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

    for attempt in range(2):
        if files:
            for item in files.values():
                item[1].seek(0)

        try:
            response = requests.post(
                f"{API_BASE}/bot{TOKEN}/{method}",
                data=payload,
                files=files,
                timeout=(10, read_timeout),
            )
            body = response.json()
        except (requests.RequestException, ValueError):
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
        pass


def answer(callback_id, text="", show_alert=False):
    try:
        data = {"callback_query_id": callback_id, "text": text[:200]}
        if show_alert:
            data["show_alert"] = "true"
        api("answerCallbackQuery", data, read_timeout=15)
    except Exception:
        pass


# ---------- UI ----------

def mb(size):
    return f"{size / 1_000_000:.2f} MB"


def clean_name(name):
    name = Path(name or "file").name
    return "".join(c for c in name if c.isprintable())[:80] or "file"


def video_summary(settings):
    codec = VIDEO_CODECS[settings["codec"]]
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
        f'کدک: {codec["title"]} | CRF={crf}\n'
        f'کیفیت: {QUALITY_LABELS[settings["quality"]]}\n'
        f"سقف ارتفاع: {height} | FPS: {fps}\n"
        f"صدا: {audio} | ارسال: {delivery}\n"
        f'صف خودکار: {"روشن" if settings["auto"] else "خاموش"}\n'
        f'حذف متادیتا: {"روشن" if settings["strip_metadata"] else "خاموش"}'
    )


def file_summary(settings):
    codec = FILE_CODECS[settings["file_codec"]]
    level = codec["levels"][settings["file_level"]]
    return (
        f'الگوریتم: {codec["title"]}\n'
        f'سطح: {LEVEL_LABELS[settings["file_level"]]} (level={level})'
    )


def home(chat):
    send(
        chat,
        "🎬 فشرده‌ساز چندمنظوره\n\n"
        "یکی از دو حالت را انتخاب کن:\n"
        "🎥 ویدیو — با FFmpeg به H.264 / H.265 / AV1\n"
        "📦 فایل — با Zstd / Brotli / LZMA2\n\n"
        f"سقف ورودی: {mb(DOWNLOAD_LIMIT)}"
        + (" (Local API فعال)" if USE_LOCAL_API else ""),
        keyboard([
            [button("🎥 فشرده‌سازی ویدیو", "menu:video")],
            [button("📦 فشرده‌سازی فایل", "menu:file")],
            [button("⚙️ تنظیمات ویدیو", "menu:settings"),
             button("📦 تنظیمات فایل", "menu:file_settings")],
            [button("📋 صف کارها", "menu:queue"),
             button("📂 پریست‌ها", "menu:presets")],
            [button("❓ راهنما", "menu:help")],
        ]),
    )


def settings_menu(chat, uid, message_id=None):
    text = (
        "🎥 تنظیمات فشرده‌سازی ویدیو\n"
        "(برای فایل‌های ویدیویی بعدی)\n\n"
        + video_summary(get_settings(uid))
        + "\n\nتغییرات روی کارهای ثبت‌شده اثر ندارد."
    )
    markup = keyboard([
        [button("کدک", "pick:codec"), button("کیفیت", "pick:quality")],
        [button("رزولوشن", "pick:height"), button("FPS", "pick:fps")],
        [button("صدا", "pick:audio"), button("نوع ارسال", "pick:delivery")],
        [button("صف خودکار", "pick:auto"),
         button("متادیتا", "pick:strip_metadata")],
        [button("💾 ذخیره پریست", "preset:save:video"),
         button("📂 پریست‌ها", "menu:presets")],
        [button("♻️ بازنشانی", "reset:video"),
         button("🔙 بازگشت", "menu:home")],
    ])
    if message_id:
        edit(chat, message_id, text, markup)
    else:
        send(chat, text, markup)


def file_settings_menu(chat, uid, message_id=None):
    text = (
        "📦 تنظیمات فشرده‌سازی فایل\n"
        "(برای فایل‌های غیر ویدیویی)\n\n"
        + file_summary(get_settings(uid))
        + "\n\nتغییرات روی کارهای ثبت‌شده اثر ندارد."
    )
    markup = keyboard([
        [button("الگوریتم", "pick:file_codec")],
        [button("سطح فشرده‌سازی", "pick:file_level")],
        [button("💾 ذخیره پریست", "preset:save:file"),
         button("📂 پریست‌ها", "menu:presets")],
        [button("♻️ بازنشانی", "reset:file"),
         button("🔙 بازگشت", "menu:home")],
    ])
    if message_id:
        edit(chat, message_id, text, markup)
    else:
        send(chat, text, markup)


def show_presets(chat, uid, message_id=None):
    parts = ["📂 پریست‌های ذخیره‌شده:"]
    buttons = []

    for kind, title in (("video", "🎥"), ("file", "📦")):
        rows = list_presets(uid, kind)
        parts.append(f"\n{title} {'ویدیو' if kind=='video' else 'فایل'}:")
        if not rows:
            parts.append("— خالی —")
            continue
        for row in rows:
            parts.append(f'• {row["name"]}')
            buttons.append([
                button(f'{title} {row["name"]}', f'preset:load:{row["id"]}'),
                button("🗑", f'preset:del:{row["id"]}'),
            ])

    markup = keyboard(buttons + [[button("🔙 بازگشت", "menu:home")]])
    text = "\n".join(parts)
    if message_id:
        edit(chat, message_id, text, markup)
    else:
        send(chat, text, markup)


def show_queue(chat, uid):
    rows = many(
        "SELECT * FROM jobs WHERE uid=? ORDER BY created DESC LIMIT 15",
        (uid,),
    )
    labels = {
        "downloading": "در حال دانلود",
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
        kind_icon = "🎥" if row.get("kind") == "video" else "📦"
        lines.append(
            f'{kind_icon} {row["id"]} | '
            f'{labels.get(row["status"], row["status"])}\n'
            f'{row["name"]}'
        )
        if row["status"] == "awaiting":
            buttons.append([
                button(f'▶️ {row["id"]}', f'go:{row["id"]}'),
                button("لغو", f'cancel:{row["id"]}'),
            ])
        elif row["status"] in ("queued", "running", "downloading"):
            buttons.append([
                button(f'لغو {row["id"]}', f'cancel:{row["id"]}'),
            ])

    send(chat, "\n\n".join(lines), keyboard(buttons))


def help_text():
    return (
        "📖 راهنما\n\n"
        "🎥 فشرده‌سازی ویدیو\n"
        "۱. از منو وارد «فشرده‌سازی ویدیو» شو\n"
        "۲. تنظیمات دلخواه را انتخاب کن\n"
        "۳. ویدیو یا فایل ویدیویی بفرست\n"
        "۴. اگر «تأیید هر فایل» روشن باشد، روی شروع بزن\n"
        "۵. خروجی MP4 (H.264/H.265/AV1) دریافت می‌کنی\n\n"
        "📦 فشرده‌سازی فایل\n"
        "۱. از منو وارد «فشرده‌سازی فایل» شو\n"
        "۲. الگوریتم و سطح را انتخاب کن\n"
        "۳. هر فایلی (PDF، ZIP، متن، ...) بفرست\n"
        "۴. نسخه فشرده با پسوند مناسب دریافت می‌کنی\n\n"
        "💾 پریست‌ها\n"
        "تنظیمات فعلی را با نام ذخیره کن و بعداً با یک دکمه بارگذاری کن.\n\n"
        "⚡ دانلود قبل از صف\n"
        "فایل‌ها ابتدا روی سرور دانلود می‌شوند و سپس در صف قرار می‌گیرند.\n\n"
        "📊 درباره معیارها\n"
        "• CRF پایین‌تر = کیفیت بالاتر = حجم بیشتر\n"
        "• درصد پیشرفت مربوط به انکود/فشرده‌سازی است\n"
        "• آپلود نهایی جداست\n\n"
        "⚠️ محدودیت‌ها\n"
        "• HDR پذیرفته نمی‌شود\n"
        "• زیرنویس/چپتر حذف می‌شوند\n"
        "• سقف ورودی بسته به Local API متفاوت است\n\n"
        "دستورات: /start /settings /queue /cancel /help /privacy"
    )


# ---------- Background download (upload-before-queue) ----------

def start_background_download(job_id, auto):
    def runner():
        acquired = DOWNLOAD_SEM.acquire(blocking=False)
        job = job_by_id(job_id)
        if not job or job["status"] != "downloading":
            if acquired:
                DOWNLOAD_SEM.release()
            return

        if not acquired:
            # Can't download now: demote to queued/awaiting for the worker.
            new_status = "queued" if auto else "awaiting"
            execute(
                "UPDATE jobs SET status=? WHERE id=? AND status='downloading'",
                (new_status, job_id),
            )
            _render_ready_message(job_id)
            return

        try:
            folder = WORK / job_id
            folder.mkdir(parents=True, exist_ok=True)
            source = folder / "input"

            if shutil.disk_usage(WORK).free < 512_000_000:
                raise UserError("فضای دیسک کم است.")

            download(job, source)

            new_status = "queued" if auto else "awaiting"
            changed = execute(
                "UPDATE jobs SET status=? WHERE id=? AND status='downloading'",
                (new_status, job_id),
            )
            if changed:
                _render_ready_message(job_id)
        except Cancelled:
            execute(
                "UPDATE jobs SET status='cancelled' WHERE id=? AND status='downloading'",
                (job_id,),
            )
        except Exception as exc:
            if STOP.is_set():
                execute(
                    "UPDATE jobs SET status='queued' WHERE id=? AND status='downloading'",
                    (job_id,),
                )
                return
            execute(
                "UPDATE jobs SET status='failed' WHERE id=? AND status='downloading'",
                (job_id,),
            )
            reason = (
                str(exc) if isinstance(exc, UserError)
                else "خطای داخلی دانلود."
            )
            LOG.warning("Background download failed for %s: %s", job_id, type(exc).__name__)
            safe_send(
                job["chat"],
                f'❌ دانلود کار {job_id} ناموفق بود.\n{reason}',
            )
            edit(
                job["chat"], job.get("message"),
                f'❌ دانلود کار {job_id} ناموفق بود.',
            )
        finally:
            DOWNLOAD_SEM.release()

    threading.Thread(target=runner, daemon=True).start()


def _render_ready_message(job_id):
    job = job_by_id(job_id)
    if not job:
        return
    settings = json.loads(job["settings"])
    kind = job.get("kind", "video")

    if kind == "video":
        summary = video_summary(settings)
    else:
        summary = file_summary(settings)

    actions = [[button("لغو", f"cancel:{job_id}")]]
    if job["status"] == "awaiting":
        actions[0].insert(0, button("✅ شروع", f"go:{job_id}"))

    tail = "در صف قرار گرفت." if job["status"] == "queued" else "شروع را تأیید کن."
    text = (
        f'{"🎞" if kind=="video" else "📦"} کار {job_id}\n'
        f'فایل: {job["name"]}\n'
        f'حجم ورودی: {mb(job["size"]) if job["size"] else "—"}\n\n'
        f"{summary}\n\n{tail}"
    )
    edit(job["chat"], job.get("message"), text, keyboard(actions))


# ---------- Receiving files ----------

def _create_job(chat, uid, media, name, size, kind):
    settings = get_settings(uid)
    job_id = uuid.uuid4().hex[:12]
    auto = settings["auto"]

    with DB_LOCK:
        active = one(
            "SELECT COUNT(*) AS n FROM jobs "
            "WHERE status IN ('downloading','awaiting','queued','running')"
        )["n"]
        personal = one(
            "SELECT COUNT(*) AS n FROM jobs WHERE uid=? "
            "AND status IN ('downloading','awaiting','queued','running')",
            (uid,),
        )["n"]

        if active >= MAX_ACTIVE_TOTAL or personal >= MAX_ACTIVE_PER_USER:
            send(chat, "صف پر است؛ کمی صبر کن یا کارهای قبلی را لغو کن.")
            return

        execute(
            "INSERT INTO jobs "
            "(id,uid,chat,file_id,name,size,settings,status,message,created,kind) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            (
                job_id, uid, chat, media["file_id"], name, size,
                json.dumps(settings), "downloading", None,
                time.time(), kind,
            ),
        )

    preview = video_summary(settings) if kind == "video" else file_summary(settings)
    text = (
        f'{"🎞" if kind=="video" else "📦"} کار {job_id}\n'
        f"فایل: {name}\n"
        f"حجم: {mb(size) if size else '—'}\n\n"
        f"{preview}\n\n"
        "📥 در حال دانلود به سرور…\n"
        "(برای لغو دکمه زیر)"
    )
    result = safe_send(
        chat, text,
        keyboard([[button("لغو", f"cancel:{job_id}")]]),
    )
    if result:
        execute(
            "UPDATE jobs SET message=? WHERE id=?",
            (result["message_id"], job_id),
        )

    start_background_download(job_id, auto)


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
            "برای فایل‌های بزرگ‌تر، Local Bot API Server لازم است.",
        )
        return

    _create_job(chat, uid, media, name, size, "video")


def receive_file(message, uid, chat):
    media = message.get("document")
    if not media:
        send(chat, "لطفاً یک فایل ارسال کن.")
        return

    name = clean_name(media.get("file_name") or "file.bin")
    mime = media.get("mime_type", "")

    if "video" in message or mime.startswith("video/"):
        send(chat, "این فایل ویدیویی است؛ از بخش «فشرده‌سازی ویدیو» استفاده کن.")
        return

    size = int(media.get("file_size", 0))
    if size > DOWNLOAD_LIMIT:
        send(
            chat,
            f"فایل بزرگ‌تر از سقف این نسخه است: {mb(DOWNLOAD_LIMIT)}.\n"
            "برای فایل‌های بزرگ‌تر، Local Bot API Server لازم است.",
        )
        return

    _create_job(chat, uid, media, name, size, "file")


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

    data = callback.get("data", "")
    parts = data.split(":")

    if data == "menu:home":
        answer(callback["id"])
        home(chat)
    elif data == "menu:video":
        answer(callback["id"])
        send(
            chat,
            "🎥 فشرده‌سازی ویدیو\n\n"
            "تنظیمات را بررسی کن، سپس ویدیو را بفرست.\n"
            "می‌توانی چند فایل پشت‌سرهم ارسال کنی.",
            keyboard([
                [button("⚙️ تنظیمات", "menu:settings")],
                [button("📋 صف", "menu:queue"),
                 button("🔙 بازگشت", "menu:home")],
            ]),
        )
    elif data == "menu:file":
        answer(callback["id"])
        send(
            chat,
            "📦 فشرده‌سازی فایل\n\n"
            "الگوریتم و سطح را تنظیم کن، سپس فایل بفرست.\n"
            "خروجی با پسوند مناسب ارسال می‌شود.",
            keyboard([
                [button("⚙️ تنظیمات فایل", "menu:file_settings")],
                [button("📋 صف", "menu:queue"),
                 button("🔙 بازگشت", "menu:home")],
            ]),
        )
    elif data == "menu:settings":
        answer(callback["id"])
        settings_menu(chat, uid, mid)
    elif data == "menu:file_settings":
        answer(callback["id"])
        file_settings_menu(chat, uid, mid)
    elif data == "menu:queue":
        answer(callback["id"])
        show_queue(chat, uid)
    elif data == "menu:help":
        answer(callback["id"])
        send(chat, help_text())
    elif data == "menu:presets":
        answer(callback["id"])
        show_presets(chat, uid, mid)
    elif parts[0] == "reset" and len(parts) == 2:
        answer(callback["id"])
        settings = get_settings(uid)
        if parts[1] == "video":
            for k in ("codec", "quality", "height", "fps", "audio",
                      "delivery", "strip_metadata"):
                settings[k] = copy.deepcopy(DEFAULTS[k])
        else:
            settings["file_codec"] = DEFAULTS["file_codec"]
            settings["file_level"] = DEFAULTS["file_level"]
        save_settings(uid, settings)
        if parts[1] == "video":
            settings_menu(chat, uid, mid)
        else:
            file_settings_menu(chat, uid, mid)
    elif parts[0] == "pick" and len(parts) == 2 and parts[1] in CHOICES:
        answer(callback["id"])
        key = parts[1]
        rows = [
            [button(label, f"set:{key}:{index}")]
            for index, (label, _) in enumerate(CHOICES[key])
        ]
        rows.append([button("ℹ️ توضیحات", f"info:{key}")])
        rows.append([button("🔙 بازگشت", "menu:settings"
                            if key not in ("file_codec", "file_level")
                            else "menu:file_settings")])
        edit(chat, mid, f"یک گزینه برای «{key}» انتخاب کن:", keyboard(rows))
    elif data.startswith("info:") and len(parts) == 2:
        key = parts[1]
        answer(callback["id"], INFO_TEXTS.get(key, ""), show_alert=True)
    elif parts[0] == "set" and len(parts) == 3 and parts[1] in CHOICES:
        answer(callback["id"])
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
        if key in ("file_codec", "file_level"):
            file_settings_menu(chat, uid, mid)
        else:
            settings_menu(chat, uid, mid)
    elif parts[0] == "preset" and len(parts) >= 2:
        action = parts[1]
        if action == "save" and len(parts) == 3 and parts[2] in ("video", "file"):
            answer(callback["id"])
            PENDING_PRESET_NAME[uid] = parts[2]
            send(
                chat,
                f"یک نام برای پریست {'ویدیو' if parts[2]=='video' else 'فایل'} بفرست "
                "(حداکثر ۳۲ نویسه):",
            )
        elif action == "load" and len(parts) == 3:
            preset = get_preset(uid, int(parts[2]))
            if not preset:
                answer(callback["id"], "پریست یافت نشد.")
                return
            try:
                subset = json.loads(preset["settings"])
            except (ValueError, TypeError):
                answer(callback["id"], "پریست معتبر نیست.")
                return
            settings = get_settings(uid)
            settings.update(subset)
            save_settings(uid, settings)
            answer(callback["id"], f'پریست «{preset["name"]}» بارگذاری شد.')
            if preset["kind"] == "video":
                settings_menu(chat, uid, mid)
            else:
                file_settings_menu(chat, uid, mid)
        elif action == "del" and len(parts) == 3:
            preset = get_preset(uid, int(parts[2]))
            if not preset:
                answer(callback["id"], "پریست یافت نشد.")
                return
            delete_preset(uid, int(parts[2]))
            answer(callback["id"], "پریست حذف شد.")
            show_presets(chat, uid, mid)
        else:
            answer(callback["id"], "پارامتر نامعتبر.")
    elif parts[0] in ("go", "cancel") and len(parts) == 2:
        job = job_by_id(parts[1])
        if not job or job["uid"] != uid:
            answer(callback["id"], "کار یافت نشد.")
            return

        if parts[0] == "go":
            changed = execute(
                "UPDATE jobs SET status='queued' WHERE id=? AND status='awaiting'",
                (job["id"],),
            )
            answer(callback["id"], "در صف قرار گرفت." if changed else "امکان‌پذیر نیست.")
            if changed:
                edit(
                    chat, mid,
                    f'✅ کار {job["id"]} در صف قرار گرفت.',
                    keyboard([[button("لغو", f'cancel:{job["id"]}')]]),
                )
        else:
            changed = execute(
                "UPDATE jobs SET status='cancelled' WHERE id=? "
                "AND status IN ('downloading','awaiting','queued','running')",
                (job["id"],),
            )
            answer(callback["id"], "لغو ثبت شد." if changed else "امکان‌پذیر نیست.")
            if changed:
                edit(chat, mid, f'⛔ کار {job["id"]} لغو شد.')
    else:
        answer(callback["id"])


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

    # Preset name pending?
    if uid in PENDING_PRESET_NAME and text and not text.startswith("/"):
        kind = PENDING_PRESET_NAME.pop(uid)
        name = text.strip()[:32] or "بدون‌نام"
        save_preset(uid, name, kind, get_settings(uid))
        send(
            chat,
            f'✅ پریست «{name}» برای '
            f'{"ویدیو" if kind=="video" else "فایل"} ذخیره شد.',
        )
        return

    if command == "/start":
        home(chat)
    elif command == "/settings":
        settings_menu(chat, uid)
    elif command == "/filesettings":
        file_settings_menu(chat, uid)
    elif command in ("/queue", "/status"):
        show_queue(chat, uid)
    elif command == "/presets":
        show_presets(chat, uid)
    elif command == "/help":
        send(chat, help_text())
    elif command == "/privacy":
        send(
            chat,
            "فایل‌های موقت بعد از پایان هر کار حذف می‌شوند.\n"
            "تنظیمات، پریست‌ها، شناسه تلگرام، file_id، نام فایل و وضعیت "
            "کارها در دیتابیس سرور ذخیره می‌شوند.\n"
            "حذف فایل محلی = حذف نسخه در Telegram نیست.\n"
            "حذف متادیتا تضمین ناشناس‌سازی محتوا نیست.",
        )
    elif command == "/cancel":
        count = execute(
            "UPDATE jobs SET status='cancelled' WHERE uid=? "
            "AND status IN ('downloading','awaiting','queued','running')",
            (uid,),
        )
        send(
            chat,
            f"لغو {count} کار ثبت شد.\n"
            "آپلود درحال‌انجام ممکن است متوقف نشود.",
        )
    elif message.get("video"):
        receive_video(message, uid, chat)
    elif message.get("document"):
        # Route based on mime/extension: video goes to video pipeline
        media = message["document"]
        mime = media.get("mime_type", "")
        name = (media.get("file_name") or "").lower()
        video_exts = (".mp4", ".mkv", ".mov", ".webm", ".avi", ".m4v", ".ts")
        if mime.startswith("video/") or name.endswith(video_exts):
            receive_video(message, uid, chat)
        else:
            receive_file(message, uid, chat)
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
    file_size = int(info.get("file_size", 0))
    if file_size > DOWNLOAD_LIMIT:
        raise UserError("حجم فایل از سقف دانلود بیشتر است.")

    path = info.get("file_path")
    if not path:
        raise UserError("مسیر دانلود دریافت نشد.")

    # ---------- حالت Local: کپی مستقیم از دیسک ----------
    if path.startswith("/") and Path(path).is_file():
        LOG.info("Local file copy: %s", path)
        try:
            shutil.copyfile(path, target)
        except OSError as e:
            raise UserError("کپی فایل لوکال ناموفق بود.") from e

        size = target.stat().st_size
        if size == 0:
            raise UserError("فایل دانلودشده خالی است.")
        if size > DOWNLOAD_LIMIT:
            raise UserError("فایل از سقف دانلود بزرگ‌تر است.")
        return size

    # ---------- حالت عادی: دانلود از HTTP ----------
    if path.startswith("/"):
        work_dir = "/var/lib/telegram-bot-api/"
        if path.startswith(work_dir):
            path = path[len(work_dir):]
        else:
            path = path.lstrip("/")

    url = f"{API_BASE}/file/bot{TOKEN}/{path}"
    total = 0
    try:
        with requests.get(url, stream=True, timeout=(10, 120)) as response:
            response.raise_for_status()
            with target.open("wb") as output:
                for chunk in response.iter_content(256 * 1024):
                    check_job(job["id"])
                    if not chunk:
                        continue
                    total += len(chunk)
                    if total > DOWNLOAD_LIMIT:
                        raise UserError("فایل از سقف دانلود بزرگ‌تر است.")
                    output.write(chunk)
    except requests.RequestException as e:
        LOG.warning("Download failed: %s | url=%s", e, url)
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
        raise UserError("فایل خراب است یا فرمت قابل پردازش ندارد.")

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


# ---------- Video encoding ----------

def build_command(source, target, settings, info):
    selected = VIDEO_CODECS[settings["codec"]]
    crf = selected["crf"][settings["quality"]]

    target_height = min(
        info["height"],
        settings["height"] or info["height"],
    )
    target_height = max(2, (target_height // 2) * 2)

    filters = [f"scale=-2:{target_height}:flags=lanczos"]

    if settings["fps"] and (
        info["fps"] <= 0 or info["fps"] > settings["fps"]
    ):
        filters.append(f'fps={settings["fps"]}')

    command = [
        "ffmpeg",
        "-hide_banner", "-nostdin", "-y",
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
                        "کیفیت یا رزولوشن را کم کن."
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
                    percent = min(99.5, media_seconds / info["duration"] * 100)
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
                        job["chat"], job["message"],
                        f'🎬 کار {job["id"]}\n{job["name"]}\n\n'
                        f"{bar} {percent:.1f}%\n"
                        f"سرعت: {speed}\n"
                        f"سپری‌شده: {int(elapsed)} ثانیه\n"
                        f"باقی‌مانده: {remaining}",
                        keyboard([[button("لغو", f'cancel:{job["id"]}')]]),
                    )
                time.sleep(0.3)

            reader.join(timeout=2)
            check_job(job["id"])

            if process.returncode != 0:
                raise UserError(
                    "FFmpeg نتوانست فایل را پردازش کند. "
                    "کدک H.264 یا تنظیمات ساده‌تر را امتحان کن."
                )
            if not target.exists() or target.stat().st_size == 0:
                raise UserError("خروجی معتبری تولید نشد.")
            if target.stat().st_size > UPLOAD_LIMIT:
                raise UserError(
                    "حجم خروجی از سقف ارسال بیشتر است؛ "
                    "رزولوشن یا کیفیت را کم کن."
                )
            return time.monotonic() - started
        finally:
            stop_process(process)
            reader.join(timeout=2)


# ---------- File compression ----------

def compress_file(job, source, target, settings, input_size, folder):
    codec = settings["file_codec"]
    level = FILE_CODECS[codec]["levels"][settings["file_level"]]
    started = time.monotonic()
    last_edit = 0.0
    processed = [0]

    def on_progress(processed_bytes, total):
        nonlocal last_edit
        now = time.monotonic()
        if now - last_edit < 4:
            return
        last_edit = now
        elapsed = now - started
        percent = min(99.5, processed_bytes / total * 100) if total else 0
        filled = min(10, int(percent // 10))
        bar = "█" * filled + "░" * (10 - filled)
        edit(
            job["chat"], job["message"],
            f'📦 کار {job["id"]}\n{job["name"]}\n\n'
            f"{bar} {percent:.1f}%\n"
            f'الگوریتم: {FILE_CODECS[codec]["title"]} (level={level})\n'
            f"سپری‌شده: {int(elapsed)} ثانیه",
            keyboard([[button("لغو", f'cancel:{job["id"]}')]]),
        )

    if codec == "lzma2":
        with lzma.open(target, "wb", preset=level) as out, source.open("rb") as inp:
            while True:
                check_job(job["id"])
                chunk = inp.read(4 * 1024 * 1024)
                if not chunk:
                    break
                out.write(chunk)
                processed[0] += len(chunk)
                on_progress(processed[0], input_size)
    elif codec == "zstd":
        cctx = zstd.ZstdCompressor(level=level, threads=THREADS)
        with source.open("rb") as inp, target.open("wb") as out:
            with cctx.stream_writer(out, closefd=False) as writer:
                while True:
                    check_job(job["id"])
                    chunk = inp.read(4 * 1024 * 1024)
                    if not chunk:
                        break
                    writer.write(chunk)
                    processed[0] += len(chunk)
                    on_progress(processed[0], input_size)
    elif codec == "brotli":
        with source.open("rb") as inp, target.open("wb") as out:
            comp = brotli.Compressor(quality=level)
            while True:
                check_job(job["id"])
                chunk = inp.read(4 * 1024 * 1024)
                if not chunk:
                    break
                out.write(comp.process(chunk))
                processed[0] += len(chunk)
                on_progress(processed[0], input_size)
            out.write(comp.finish())
    else:
        raise UserError("الگوریتم ناشناخته.")

    check_job(job["id"])
    if not target.exists() or target.stat().st_size == 0:
        raise UserError("خروجی معتبری تولید نشد.")
    if target.stat().st_size > UPLOAD_LIMIT:
        raise UserError("حجم خروجی از سقف ارسال بیشتر است.")
    return time.monotonic() - started


# ---------- Job execution ----------

def upload_result(job, target, settings, info, caption, kind):
    if kind == "video":
        filename = f'compressed_{settings["codec"]}_{job["id"]}.mp4'
        as_video = (
            settings["delivery"] == "video"
            and settings["codec"] == "h264"
        )
        method = "sendVideo" if as_video else "sendDocument"
        field = "video" if as_video else "document"
        data = {"chat_id": job["chat"], "caption": caption}
        if as_video:
            data.update({
                "duration": max(1, int(info["duration"])),
                "width": info["width"],
                "height": info["height"],
                "supports_streaming": "true",
            })
        mime = "video/mp4"
    else:
        ext = FILE_CODECS[settings["file_codec"]]["ext"]
        filename = job["name"] + ext
        method = "sendDocument"
        field = "document"
        data = {"chat_id": job["chat"], "caption": caption}
        mime = "application/octet-stream"

    with target.open("rb") as file:
        api(
            method, data,
            files={field: (filename, file, mime)},
            read_timeout=600,
        )


def process_job(job):
    job_id = job["id"]
    kind = job.get("kind", "video")
    folder = WORK / job_id
    folder.mkdir(parents=True, exist_ok=True)
    source = folder / "input"
    target = folder / ("output.mp4" if kind == "video" else "output.bin")
    started = time.monotonic()

    try:
        check_job(job_id)

        if shutil.disk_usage(WORK).free < 512_000_000:
            raise UserError("فضای آزاد دیسک کمتر از 512 MB است.")

        if not job.get("message"):
            message = send(job["chat"], f'شروع کار {job_id}…')
            execute(
                "UPDATE jobs SET message=? WHERE id=?",
                (message["message_id"], job_id),
            )
            job["message"] = message["message_id"]

        # Download if background didn't already
        if not source.exists() or source.stat().st_size == 0:
            edit(
                job["chat"], job["message"],
                f'📥 در حال دانلود کار {job_id}…',
                keyboard([[button("لغو", f'cancel:{job_id}')]]),
            )
            input_size = download(job, source)
        else:
            input_size = source.stat().st_size

        check_job(job_id)
        settings = json.loads(job["settings"])

        if kind == "video":
            info = probe(source)
            validate_media(info)
            edit(
                job["chat"], job["message"],
                f'🔎 ورودی: {info["width"]}×{info["height"]}\n'
                f'مدت: {info["duration"]:.1f} ثانیه\n'
                f'کدک: {info["codec"]}\n\n'
                "فشرده‌سازی شروع شد…",
                keyboard([[button("لغو", f'cancel:{job_id}')]]),
            )
            elapsed_encoding = encode(job, source, target, settings, info, folder)
            output_info = probe(target)
            output_size = target.stat().st_size
            reduction = (1 - output_size / input_size) * 100
            change = (
                f"کاهش حجم: {reduction:.1f}%"
                if reduction >= 0
                else f"⚠️ افزایش حجم: {abs(reduction):.1f}%"
            )
            caption = (
                f'✅ نتیجه کار {job_id}\n'
                f'نام: {job["name"]}\n\n'
                f"ورودی: {mb(input_size)}\n"
                f"خروجی: {mb(output_size)}\n"
                f"{change}\n"
                f"نسبت: {input_size / output_size:.2f}×\n"
                f'کدک: {VIDEO_CODECS[settings["codec"]]["title"]}\n'
                f'ابعاد: {output_info["width"]}×{output_info["height"]}\n'
                f'FPS: {output_info["fps"]:.2f}\n'
                f'مدت: {output_info["duration"]:.1f} ثانیه\n'
                f"زمان انکود: {elapsed_encoding:.1f} ثانیه"
            )
        else:
            edit(
                job["chat"], job["message"],
                f'📦 فشرده‌سازی فایل {job_id}\n'
                f'{job["name"]}\n\n'
                f"{file_summary(settings)}\n\n"
                "در حال فشرده‌سازی…",
                keyboard([[button("لغو", f'cancel:{job_id}')]]),
            )
            elapsed_encoding = compress_file(
                job, source, target, settings, input_size, folder
            )
            output_size = target.stat().st_size
            reduction = (1 - output_size / input_size) * 100
            change = (
                f"کاهش حجم: {reduction:.1f}%"
                if reduction >= 0
                else f"⚠️ افزایش حجم: {abs(reduction):.1f}%"
            )
            caption = (
                f'✅ نتیجه کار {job_id}\n'
                f'نام: {job["name"]}\n\n'
                f"ورودی: {mb(input_size)}\n"
                f"خروجی: {mb(output_size)}\n"
                f"{change}\n"
                f"نسبت: {input_size / output_size:.2f}×\n"
                f'الگوریتم: {FILE_CODECS[settings["file_codec"]]["title"]}\n'
                f'سطح: {LEVEL_LABELS[settings["file_level"]]}\n'
                f"زمان: {elapsed_encoding:.1f} ثانیه"
            )
            output_info = None

        check_job(job_id)
        edit(
            job["chat"], job["message"],
            f'📤 کار {job_id} آماده ارسال است.\n'
            f"حجم خروجی: {mb(output_size)}",
        )

        upload_result(job, target, settings, output_info, caption, kind)

        execute("UPDATE jobs SET status='done' WHERE id=?", (job_id,))
        edit(
            job["chat"], job["message"],
            f'✅ کار {job_id} ارسال شد.\n'
            f"زمان کل: {time.monotonic() - started:.1f} ثانیه",
        )

    except Cancelled:
        execute("UPDATE jobs SET status='cancelled' WHERE id=?", (job_id,))
        edit(job["chat"], job["message"], f'⛔ کار {job_id} لغو شد.')
    except Requeue:
        execute(
            "UPDATE jobs SET status='queued' WHERE id=? AND status='running'",
            (job_id,),
        )
    except Exception as exc:
        if STOP.is_set():
            execute(
                "UPDATE jobs SET status='queued' WHERE id=? AND status='running'",
                (job_id,),
            )
        else:
            current = job_by_id(job_id)
            if current and current["status"] == "cancelled":
                return
            execute("UPDATE jobs SET status='failed' WHERE id=?", (job_id,))
            reason = (
                str(exc) if isinstance(exc, UserError)
                else "خطای داخلی پردازش."
            )
            LOG.warning(
                "Job %s failed (%s).", job_id, type(exc).__name__
            )
            safe_send(
                job["chat"],
                f'❌ کار {job_id} ناموفق بود.\n{reason}',
            )
            edit(job["chat"], job["message"], f'❌ کار {job_id} ناموفق بود.')
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
                execute("UPDATE jobs SET status='running' WHERE id=?", (job["id"],))
        if not job:
            STOP.wait(1)
            continue
        process_job(job)


# ---------- Health & startup ----------

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
        capture_output=True, text=True, timeout=20,
    )
    missing = [
        item["encoder"] for item in VIDEO_CODECS.values()
        if not re.search(rf'\b{re.escape(item["encoder"])}\b', result.stdout)
    ]
    if missing:
        raise SystemExit("Missing FFmpeg encoders: " + ", ".join(missing))


def on_signal(signum, frame):
    STOP.set()


def main():
    signal.signal(signal.SIGTERM, on_signal)
    signal.signal(signal.SIGINT, on_signal)
    check_encoders()

    import fcntl
    lock_file = (DATA / "instance.lock").open("a")
    try:
        fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        raise SystemExit("Another bot instance is using DATA_DIR.")

    # Clean leftovers
    for path in WORK.iterdir():
        if path.is_dir():
            shutil.rmtree(path, ignore_errors=True)
        else:
            path.unlink(missing_ok=True)

    # Reset interrupted states
    execute("UPDATE jobs SET status='queued' WHERE status='running'")
    execute("UPDATE jobs SET status='queued' WHERE status='downloading'")

    identity = api("getMe")
    LOG.info("Starting @%s (local_api=%s)", identity.get("username", "bot"), USE_LOCAL_API)

    api("deleteWebhook", {"drop_pending_updates": "false"})
    try:
        api("setMyCommands", {"commands": [
            {"command": "start", "description": "منوی اصلی"},
            {"command": "settings", "description": "تنظیمات ویدیو"},
            {"command": "filesettings", "description": "تنظیمات فایل"},
            {"command": "queue", "description": "صف کارها"},
            {"command": "presets", "description": "پریست‌ها"},
            {"command": "cancel", "description": "لغو همه کارهای فعال"},
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
                LOG.warning("Polling failed (%s); retrying.", type(exc).__name__)
                STOP.wait(3)
    finally:
        STOP.set()
        thread.join(timeout=15)
        health.shutdown()


if __name__ == "__main__":
    main()
