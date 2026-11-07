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

import brotli
import requests
import zstandard as zstd

# ---------- Configuration ----------

# توکن را از متغیر محیطی می‌خواند (BOT_TOKEN یا TOKEN).
# اگر داخل کد هم گذاشته باشی، همان استفاده می‌شود.
TOKEN = (
    os.environ.get("BOT_TOKEN")
    or os.environ.get("TOKEN")
    or "8758704422:AAGTexKCiwc1ASoolDYb39NFBTEgWMbo9ZM"
).strip()

# فقط وقتی هنوز placeholder است خطا می‌دهد
if not TOKEN or TOKEN in ("توکن_واقعی_ربات_را_اینجا_قرار_بده", "YOUR_BOT_TOKEN") or TOKEN.startswith("توکن_"):
    raise SystemExit(
        "BOT_TOKEN is required.\n"
        "Either set environment variable BOT_TOKEN (or TOKEN) in Railway,\n"
        "or replace the placeholder string inside bot.py with your real token."
    )

# Optional: for Local Bot API Server (supports much larger files)
# Example: BOT_API_BASE=http://localhost:8081
# You still need API_ID + API_HASH when you *run* the local server itself
# (get them from https://my.telegram.org → API development tools)
BOT_API_BASE = os.environ.get("BOT_API_BASE", "https://api.telegram.org").rstrip("/")

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
FILE_COMPRESS_TIMEOUT = max(60, int(os.environ.get("FILE_COMPRESS_TIMEOUT", "1800")))

# Standard Bot API limits (raised automatically when using Local Bot API)
DOWNLOAD_LIMIT = int(os.environ.get("DOWNLOAD_LIMIT", "19_000_000"))
UPLOAD_LIMIT = int(os.environ.get("UPLOAD_LIMIT", "49_000_000"))
# When using Local Bot API these can be much higher (up to ~2000 MB upload)
if "localhost" in BOT_API_BASE or "127.0.0.1" in BOT_API_BASE or os.environ.get("LOCAL_BOT_API"):
    DOWNLOAD_LIMIT = int(os.environ.get("DOWNLOAD_LIMIT", "1_900_000_000"))
    UPLOAD_LIMIT = int(os.environ.get("UPLOAD_LIMIT", "1_900_000_000"))

MAX_ACTIVE_PER_USER = 15
MAX_ACTIVE_TOTAL = 40
MAX_PIXELS = 3840 * 2160
MAX_FILE_SIZE = DOWNLOAD_LIMIT  # for non-video files

STOP = threading.Event()
DB_LOCK = threading.RLock()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
LOG = logging.getLogger("compressor")

# ---------- Defaults & choices ----------

DEFAULTS_VIDEO = {
    "mode": "video",
    "codec": "h264",
    "quality": "balanced",
    "height": 720,
    "fps": 0,
    "audio": 96,
    "delivery": "document",
    "auto": False,
    "strip_metadata": True,
}

DEFAULTS_FILE = {
    "mode": "file",
    "algo": "zstd",          # zstd | brotli | lzma2
    "level": "balanced",     # fast | balanced | max
    "auto": False,
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

FILE_ALGOS = {
    "zstd": {
        "title": "Zstandard (zstd)",
        "ext": ".zst",
        "levels": {"fast": 3, "balanced": 9, "max": 19},
        "desc": "سریع + نسبت خوب — بهترین انتخاب عمومی ۲۰۲۶",
    },
    "brotli": {
        "title": "Brotli",
        "ext": ".br",
        "levels": {"fast": 4, "balanced": 6, "max": 11},
        "desc": "نسبت عالی برای متن و اسناد",
    },
    "lzma2": {
        "title": "LZMA2 (xz)",
        "ext": ".xz",
        "levels": {"fast": 3, "balanced": 6, "max": 9},
        "desc": "بیشترین فشرده‌سازی — کندتر",
    },
}

LEVEL_LABELS = {
    "fast": "سریع",
    "balanced": "متعادل",
    "max": "حداکثر فشرده‌سازی",
}

CHOICES_VIDEO = {
    "codec": [("H.264 (سازگار)", "h264"), ("H.265 / HEVC", "hevc"), ("AV1", "av1")],
    "quality": [
        ("کیفیت بیشتر", "quality"),
        ("متعادل (پیشنهادی)", "balanced"),
        ("حجم کمتر", "small"),
    ],
    "height": [("اصلی", 0), ("1080p", 1080), ("720p", 720), ("480p", 480)],
    "fps": [("اصلی", 0), ("30 fps", 30), ("24 fps", 24)],
    "audio": [("128 kbps", 128), ("96 kbps", 96), ("بی‌صدا", 0)],
    "delivery": [("فایل (Document)", "document"), ("ویدیوی قابل پخش", "video")],
    "auto": [("تأیید هر فایل", False), ("صف خودکار", True)],
    "strip_metadata": [("حذف متادیتای قابل حذف", True), ("حفظ نسبی", False)],
}

CHOICES_FILE = {
    "algo": [
        ("Zstandard — سریع و قوی (پیشنهادی)", "zstd"),
        ("Brotli — عالی برای متن", "brotli"),
        ("LZMA2 — حداکثر فشرده‌سازی", "lzma2"),
    ],
    "level": [
        ("سریع", "fast"),
        ("متعادل (پیشنهادی)", "balanced"),
        ("حداکثر فشرده‌سازی", "max"),
    ],
    "auto": [("تأیید هر فایل", False), ("صف خودکار", True)],
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
    settings TEXT NOT NULL,
    mode TEXT NOT NULL DEFAULT 'video'
);

CREATE TABLE IF NOT EXISTS presets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    uid INTEGER NOT NULL,
    name TEXT NOT NULL,
    mode TEXT NOT NULL,
    settings TEXT NOT NULL,
    created REAL NOT NULL,
    UNIQUE(uid, name)
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
    job_type TEXT NOT NULL DEFAULT 'video',
    local_path TEXT
);

CREATE INDEX IF NOT EXISTS jobs_status_created ON jobs(status, created);
CREATE INDEX IF NOT EXISTS presets_uid ON presets(uid);
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


def get_user_mode(uid):
    row = one("SELECT mode FROM users WHERE uid=?", (uid,))
    return row["mode"] if row else "video"


def set_user_mode(uid, mode):
    # ensure user row exists
    settings = get_settings(uid, mode)
    execute(
        "INSERT INTO users(uid,settings,mode) VALUES(?,?,?) "
        "ON CONFLICT(uid) DO UPDATE SET mode=excluded.mode",
        (uid, json.dumps(settings), mode),
    )


def get_settings(uid, mode=None):
    if mode is None:
        mode = get_user_mode(uid)
    defaults = copy.deepcopy(DEFAULTS_VIDEO if mode == "video" else DEFAULTS_FILE)
    row = one("SELECT settings, mode FROM users WHERE uid=?", (uid,))
    if row:
        stored = json.loads(row["settings"])
        # only apply settings that belong to current mode
        if stored.get("mode") == mode:
            defaults.update({k: v for k, v in stored.items() if k in defaults})
    defaults["mode"] = mode
    return defaults


def save_settings(uid, settings):
    mode = settings.get("mode", "video")
    execute(
        "INSERT INTO users(uid,settings,mode) VALUES(?,?,?) "
        "ON CONFLICT(uid) DO UPDATE SET settings=excluded.settings, mode=excluded.mode",
        (uid, json.dumps(settings), mode),
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


def list_presets(uid, mode=None):
    if mode:
        return many(
            "SELECT * FROM presets WHERE uid=? AND mode=? ORDER BY created DESC",
            (uid, mode),
        )
    return many("SELECT * FROM presets WHERE uid=? ORDER BY created DESC", (uid,))


def save_preset(uid, name, settings):
    name = name.strip()[:40]
    if not name:
        raise UserError("نام پریست خالی است.")
    mode = settings.get("mode", "video")
    execute(
        "INSERT INTO presets(uid,name,mode,settings,created) VALUES(?,?,?,?,?) "
        "ON CONFLICT(uid, name) DO UPDATE SET settings=excluded.settings, mode=excluded.mode, created=excluded.created",
        (uid, name, mode, json.dumps(settings), time.time()),
    )


def delete_preset(uid, preset_id):
    execute("DELETE FROM presets WHERE id=? AND uid=?", (preset_id, uid))


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

    url = f"{BOT_API_BASE}/bot{TOKEN}/{method}"

    for attempt in range(2):
        if files:
            for item in files.values():
                item[1].seek(0)

        try:
            response = requests.post(
                url,
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
        {"chat_id": chat, "text": text, "reply_markup": markup, "parse_mode": "HTML"},
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
                "parse_mode": "HTML",
            },
            read_timeout=15,
        )
    except Exception:
        pass


def answer(callback_id, text=""):
    try:
        api("answerCallbackQuery", {
            "callback_query_id": callback_id,
            "text": text,
        }, read_timeout=15)
    except Exception:
        pass


# ---------- UI helpers ----------

def mb(size):
    if size >= 1_000_000_000:
        return f"{size / 1_000_000_000:.2f} GB"
    return f"{size / 1_000_000:.2f} MB"


def clean_name(name):
    name = Path(name or "file").name
    return "".join(c for c in name if c.isprintable())[:80] or "file"


def summary_video(settings):
    codec = CODECS[settings["codec"]]
    crf = codec["crf"][settings["quality"]]
    height = settings["height"] or "اصلی"
    fps = settings["fps"] or "اصلی"
    audio = f'{settings["audio"]} kbps' if settings["audio"] else "حذف صدا"
    delivery = (
        "ویدیو قابل پخش" if settings["delivery"] == "video" and settings["codec"] == "h264"
        else "فایل (Document)"
    )
    return (
        f'🎞 <b>حالت: فشرده‌سازی ویدیو</b>\n'
        f'کدک: <b>{codec["title"]}</b>\n'
        f'کیفیت: {QUALITY_LABELS[settings["quality"]]} | CRF={crf}\n'
        f"سقف ارتفاع: {height} | FPS: {fps}\n"
        f"صدا: {audio} | ارسال: {delivery}\n"
        f'صف خودکار: {"✅ روشن" if settings["auto"] else "❌ خاموش"}\n'
        f'حذف متادیتا: {"✅ روشن" if settings["strip_metadata"] else "❌ خاموش"}'
    )


def summary_file(settings):
    algo = FILE_ALGOS[settings["algo"]]
    level_val = algo["levels"][settings["level"]]
    return (
        f'📦 <b>حالت: فشرده‌سازی فایل/سند</b>\n'
        f'الگوریتم: <b>{algo["title"]}</b>\n'
        f'سطح: {LEVEL_LABELS[settings["level"]]} (level={level_val})\n'
        f'{algo["desc"]}\n'
        f'صف خودکار: {"✅ روشن" if settings["auto"] else "❌ خاموش"}\n'
        f'پسوند خروجی: <code>{algo["ext"]}</code>'
    )


def summary(settings):
    if settings.get("mode") == "file":
        return summary_file(settings)
    return summary_video(settings)


def home(chat, uid):
    mode = get_user_mode(uid)
    mode_label = "ویدیو 🎬" if mode == "video" else "فایل/سند 📦"
    limit_text = mb(DOWNLOAD_LIMIT)
    send(
        chat,
        f"🎬 <b>فشرده‌ساز پیشرفته</b>\n\n"
        f"حالت فعلی: <b>{mode_label}</b>\n"
        f"سقف ورودی این نسخه: <b>{limit_text}</b>\n\n"
        "• ویدیو را به شکل Video یا Document بفرست\n"
        "• فایل و سند را به شکل Document بفرست\n"
        "• می‌توانی چند فایل پشت‌سرهم ارسال کنی\n\n"
        "از دکمه‌های زیر برای تنظیمات و راهنما استفاده کن.",
        keyboard([
            [
                button("🎬 حالت ویدیو", "mode:video"),
                button("📦 حالت فایل", "mode:file"),
            ],
            [
                button("⚙️ تنظیمات", "menu:settings"),
                button("💾 پریست‌ها", "menu:presets"),
            ],
            [
                button("📋 صف کارها", "menu:queue"),
                button("❓ راهنما", "menu:help"),
            ],
        ]),
    )


def settings_menu(chat, uid, message_id=None):
    settings = get_settings(uid)
    mode = settings.get("mode", "video")
    text = (
        "⚙️ <b>تنظیمات فایل‌های بعدی</b>\n\n"
        + summary(settings)
        + "\n\n"
        "<i>تغییرات روی کارهای قبلاً ثبت‌شده اثر ندارد.</i>"
    )

    if mode == "video":
        markup = keyboard([
            [button("کدک", "pick:codec"), button("کیفیت", "pick:quality")],
            [button("رزولوشن", "pick:height"), button("FPS", "pick:fps")],
            [button("صدا", "pick:audio"), button("نوع ارسال", "pick:delivery")],
            [button("صف خودکار", "pick:auto"), button("متادیتا", "pick:strip_metadata")],
            [button("💾 ذخیره به عنوان پریست", "preset:save")],
            [button("بازنشانی تنظیمات ویدیو", "reset")],
            [button("🔙 بازگشت", "menu:home")],
        ])
    else:
        markup = keyboard([
            [button("الگوریتم", "pick:algo"), button("سطح فشرده‌سازی", "pick:level")],
            [button("صف خودکار", "pick:auto")],
            [button("💾 ذخیره به عنوان پریست", "preset:save")],
            [button("بازنشانی تنظیمات فایل", "reset")],
            [button("🔙 بازگشت", "menu:home")],
        ])

    if message_id:
        edit(chat, message_id, text, markup)
    else:
        send(chat, text, markup)


def presets_menu(chat, uid, message_id=None):
    mode = get_user_mode(uid)
    rows = list_presets(uid, mode)
    text = (
        f"💾 <b>پریست‌های ذخیره‌شده</b> (حالت: {'ویدیو' if mode == 'video' else 'فایل'})\n\n"
        "با پریست می‌توانی تنظیمات پرکاربرد را با یک کلیک اعمال کنی."
    )
    buttons = []
    if not rows:
        text += "\n\nهنوز پریستی ذخیره نکرده‌ای.\nاز منوی تنظیمات «ذخیره به عنوان پریست» را بزن."
    else:
        for p in rows[:12]:
            buttons.append([
                button(f"📌 {p['name']}", f"preset:load:{p['id']}"),
                button("🗑", f"preset:del:{p['id']}"),
            ])
    buttons.append([button("🔙 بازگشت", "menu:home")])
    markup = keyboard(buttons)
    if message_id:
        edit(chat, message_id, text, markup)
    else:
        send(chat, text, markup)


def show_queue(chat, uid):
    rows = many(
        "SELECT * FROM jobs WHERE uid=? ORDER BY created DESC LIMIT 12",
        (uid,),
    )
    labels = {
        "awaiting": "⏳ منتظر تأیید",
        "queued": "📥 در صف",
        "running": "🔄 در حال پردازش",
        "done": "✅ ارسال‌شده",
        "failed": "❌ ناموفق",
        "cancelled": "⛔ لغوشده",
    }

    if not rows:
        send(chat, "📋 صف خالی است.")
        return

    lines = ["📋 <b>آخرین کارها:</b>"]
    buttons = []
    for row in rows:
        jtype = "🎬" if row.get("job_type") == "video" else "📦"
        lines.append(
            f'{jtype} <code>{row["id"]}</code> | {labels.get(row["status"], row["status"])}\n'
            f'{row["name"]}'
        )
        if row["status"] == "awaiting":
            buttons.append([
                button(f'✅ تأیید {row["id"]}', f'go:{row["id"]}'),
                button("لغو", f'cancel:{row["id"]}'),
            ])
        elif row["status"] in ("queued", "running"):
            buttons.append([
                button(f'لغو {row["id"]}', f'cancel:{row["id"]}')
            ])

    send(chat, "\n\n".join(lines), keyboard(buttons))


def help_text():
    return (
        "❓ <b>راهنمای کامل</b>\n\n"
        "<b>۱. انتخاب حالت</b>\n"
        "• 🎬 ویدیو → فشرده‌سازی با H.264 / H.265 / AV1\n"
        "• 📦 فایل → فشرده‌سازی با Zstd / Brotli / LZMA2\n\n"
        "<b>۲. تنظیمات</b>\n"
        "تنظیمات دلخواه را انتخاب کن و در صورت نیاز به عنوان پریست ذخیره کن.\n\n"
        "<b>۳. ارسال فایل</b>\n"
        "ویدیو یا سند را بفرست. چند فایل پشت‌سرهم هم می‌توانی بفرستی.\n"
        "اگر «صف خودکار» خاموش باشد باید هر کار را تأیید کنی.\n\n"
        "<b>۴. نتیجه</b>\n"
        "پس از پردازش، فایل فشرده‌شده + گزارش حجم و زمان دریافت می‌کنی.\n\n"
        "────────────\n"
        "<b>کدک‌های ویدیو</b>\n"
        "• H.264: سریع و سازگار با همه دستگاه‌ها\n"
        "• H.265: فشرده‌تر، سازگاری کمی کمتر\n"
        "• AV1: بهترین فشرده‌سازی، سنگین‌تر برای CPU\n\n"
        "<b>الگوریتم‌های فایل (۲۰۲۶)</b>\n"
        "• <b>Zstandard</b>: بهترین تعادل سرعت و نسبت — انتخاب پیشنهادی\n"
        "• <b>Brotli</b>: عالی برای متن، HTML، JSON، اسناد\n"
        "• <b>LZMA2</b>: حداکثر فشرده‌سازی (کندتر)\n\n"
        "<b>حجم فایل</b>\n"
        f"سقف فعلی: {mb(DOWNLOAD_LIMIT)} دانلود / {mb(UPLOAD_LIMIT)} آپلود\n"
        "برای حجم‌های خیلی بزرگ (تا حدود ۲ گیگ) باید Local Bot API Server راه‌اندازی کنی "
        "(نیاز به API_ID و API_HASH از my.telegram.org).\n\n"
        "<b>دستورات</b>\n"
        "/start — منوی اصلی\n"
        "/settings — تنظیمات\n"
        "/presets — پریست‌ها\n"
        "/queue یا /status — صف و لغو تکی\n"
        "/cancel — لغو همه کارهای فعال\n"
        "/mode — تعویض سریع حالت ویدیو/فایل\n"
        "/privacy — نگهداری داده‌ها\n"
        "/help — همین راهنما"
    )


# ---------- Receiving ----------

def receive_media(message, uid, chat):
    media = message.get("video") or message.get("document")
    if not media:
        send(chat, "لطفاً یک ویدیو یا فایل ارسال کن.")
        return

    name = clean_name(media.get("file_name") or ("video.mp4" if "video" in message else "file"))
    mime = media.get("mime_type", "")
    size = int(media.get("file_size", 0))
    mode = get_user_mode(uid)
    settings = get_settings(uid, mode)

    # Decide job type
    video_exts = (".mp4", ".mkv", ".mov", ".webm", ".avi", ".m4v", ".ts")
    is_video_like = (
        "video" in message
        or mime.startswith("video/")
        or name.lower().endswith(video_exts)
    )

    if mode == "video":
        if not is_video_like and not mime.startswith("video/"):
            send(
                chat,
                "حالت فعلی «ویدیو» است اما فایل ویدیویی به نظر نمی‌رسد.\n"
                "برای فشرده‌سازی سند، ابتدا حالت را به «فایل» تغییر بده.",
            )
            return
        job_type = "video"
    else:
        job_type = "file"

    if size > DOWNLOAD_LIMIT:
        send(
            chat,
            f"فایل بزرگ‌تر از سقف این نسخه است: {mb(DOWNLOAD_LIMIT)}.\n"
            "برای فایل‌های بزرگ‌تر Local Bot API Server لازم است "
            "(API_ID و API_HASH از my.telegram.org + اجرای سرور محلی).",
        )
        return

    job_id = uuid.uuid4().hex[:12]
    status = "queued" if settings.get("auto") else "awaiting"

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
            "(id,uid,chat,file_id,name,size,settings,status,message,created,job_type,local_path) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                job_id, uid, chat, media["file_id"], name, size,
                json.dumps(settings), status, None, time.time(),
                job_type, None,
            ),
        )

    actions = [[button("لغو", f"cancel:{job_id}")]]
    if status == "awaiting":
        actions[0].insert(0, button("✅ شروع", f"go:{job_id}"))

    type_icon = "🎬" if job_type == "video" else "📦"
    text = (
        f"{type_icon} <b>کار:</b> <code>{job_id}</code>\n"
        f"فایل: {name}\n"
        f'حجم ورودی: {mb(size) if size else "بعد از دانلود مشخص می‌شود"}\n\n'
        f"{summary(settings)}\n\n"
        + ("در صف قرار گرفت و به‌زودی پردازش می‌شود." if status == "queued"
           else "برای شروع، دکمه ✅ را بزن.")
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

    if data == "menu:home":
        home(chat, uid)
    elif data == "menu:settings":
        settings_menu(chat, uid, mid)
    elif data == "menu:queue":
        show_queue(chat, uid)
    elif data == "menu:help":
        send(chat, help_text())
    elif data == "menu:presets":
        presets_menu(chat, uid, mid)
    elif data.startswith("mode:"):
        new_mode = parts[1]
        if new_mode in ("video", "file"):
            set_user_mode(uid, new_mode)
            answer(callback["id"], f"حالت به {'ویدیو' if new_mode == 'video' else 'فایل'} تغییر کرد")
            home(chat, uid)
    elif data == "reset":
        mode = get_user_mode(uid)
        defaults = copy.deepcopy(DEFAULTS_VIDEO if mode == "video" else DEFAULTS_FILE)
        save_settings(uid, defaults)
        settings_menu(chat, uid, mid)
    elif parts[0] == "pick" and len(parts) == 2:
        key = parts[1]
        mode = get_user_mode(uid)
        choices = CHOICES_VIDEO if mode == "video" else CHOICES_FILE
        if key not in choices:
            return
        rows = [
            [button(label, f"set:{key}:{index}")]
            for index, (label, _) in enumerate(choices[key])
        ]
        rows.append([button("بازگشت", "menu:settings")])
        edit(chat, mid, "یک گزینه انتخاب کن:", keyboard(rows))
    elif parts[0] == "set" and len(parts) == 3:
        key = parts[1]
        mode = get_user_mode(uid)
        choices = CHOICES_VIDEO if mode == "video" else CHOICES_FILE
        if key not in choices:
            return
        try:
            index = int(parts[2])
            if index < 0:
                return
            value = choices[key][index][1]
        except (ValueError, IndexError):
            return
        settings = get_settings(uid, mode)
        settings[key] = value
        save_settings(uid, settings)
        settings_menu(chat, uid, mid)
    elif data == "preset:save":
        # Ask for name via a simple prompt (we use a temporary state)
        put_kv(f"await_preset_name:{uid}", "1")
        edit(
            chat, mid,
            "نام پریست را بنویس و ارسال کن (مثلاً: «ویدیو موبایل» یا «اسناد سریع»):\n"
            "برای لغو /cancel بفرست.",
        )
    elif parts[0] == "preset" and parts[1] == "load" and len(parts) == 3:
        pid = int(parts[2])
        row = one("SELECT * FROM presets WHERE id=? AND uid=?", (pid, uid))
        if row:
            settings = json.loads(row["settings"])
            save_settings(uid, settings)
            set_user_mode(uid, row["mode"])
            send(chat, f"✅ پریست «{row['name']}» اعمال شد.")
            settings_menu(chat, uid)
    elif parts[0] == "preset" and parts[1] == "del" and len(parts) == 3:
        pid = int(parts[2])
        delete_preset(uid, pid)
        presets_menu(chat, uid, mid)
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
                    chat, mid, f'✅ کار <code>{job["id"]}</code> در صف قرار گرفت.',
                    keyboard([[button("لغو", f'cancel:{job["id"]}')]]),
                )
        else:
            changed = execute(
                "UPDATE jobs SET status='cancelled' WHERE id=? "
                "AND status IN ('awaiting','queued','running')",
                (job["id"],),
            )
            if changed:
                edit(chat, mid, f'لغو کار <code>{job["id"]}</code> ثبت شد.')


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

    # Handle pending preset name
    if get_kv(f"await_preset_name:{uid}"):
        put_kv(f"await_preset_name:{uid}", "")  # clear
        if command == "/cancel" or text.lower() in ("cancel", "لغو"):
            send(chat, "ذخیره پریست لغو شد.")
            return
        try:
            settings = get_settings(uid)
            save_preset(uid, text, settings)
            send(chat, f"✅ پریست «{text.strip()[:40]}» ذخیره شد.")
            presets_menu(chat, uid)
        except UserError as e:
            send(chat, str(e))
        return

    if command == "/start":
        home(chat, uid)
    elif command == "/settings":
        settings_menu(chat, uid)
    elif command == "/presets":
        presets_menu(chat, uid)
    elif command in ("/queue", "/status"):
        show_queue(chat, uid)
    elif command == "/help":
        send(chat, help_text())
    elif command == "/mode":
        current = get_user_mode(uid)
        new_mode = "file" if current == "video" else "video"
        set_user_mode(uid, new_mode)
        send(chat, f"حالت به <b>{'ویدیو 🎬' if new_mode == 'video' else 'فایل 📦'}</b> تغییر کرد.")
        home(chat, uid)
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
        receive_media(message, uid, chat)
    else:
        home(chat, uid)


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
        raise UserError("حجم فایل از سقف دانلود این نسخه بیشتر است.")

    path = info.get("file_path")
    if not path:
        raise UserError("مسیر دانلود فایل دریافت نشد.")

    # Local Bot API often returns absolute path
    if path.startswith("/") and Path(path).exists():
        shutil.copy2(path, target)
        return Path(path).stat().st_size

    total = 0
    download_url = f"{BOT_API_BASE}/file/bot{TOKEN}/{path}"
    try:
        with requests.get(
            download_url,
            stream=True,
            timeout=(10, 60),
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
            timeout=30,
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


# ---------- Video encoding (unchanged core) ----------

def build_command(source, target, settings, info):
    selected = CODECS[settings["codec"]]
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


def encode_video(job, source, target, settings, info, folder):
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
                        f'🎬 کار <code>{job["id"]}</code>\n'
                        f'{job["name"]}\n\n'
                        f"{bar} {percent:.1f}%\n"
                        f"سرعت: {speed}\n"
                        f"زمان سپری‌شده: {int(elapsed)} ثانیه\n"
                        f"زمان تقریبی باقی‌مانده: {remaining}\n\n"
                        "این درصد مربوط به انکود است؛ "
                        "نهایی‌سازی و آپلود جداست.",
                        keyboard([[
                            button("لغو", f'cancel:{job["id"]}')
                        ]]),
                    )

                time.sleep(0.3)

            reader.join(timeout=2)
            check_job(job["id"])

            if process.returncode != 0:
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


# ---------- File compression ----------

def compress_file(job, source, target, settings, folder):
    algo = settings["algo"]
    level_name = settings["level"]
    level = FILE_ALGOS[algo]["levels"][level_name]
    started = time.monotonic()

    edit(
        job["chat"], job["message"],
        f'📦 در حال فشرده‌سازی با {FILE_ALGOS[algo]["title"]}…\n'
        f'سطح: {LEVEL_LABELS[level_name]}',
        keyboard([[button("لغو", f'cancel:{job["id"]}')]]),
    )

    try:
        if algo == "zstd":
            cctx = zstd.ZstdCompressor(level=level, threads=THREADS)
            with source.open("rb") as src, target.open("wb") as dst:
                # stream in chunks to keep memory reasonable
                cctx.copy_stream(src, dst, read_size=1 << 20, write_size=1 << 20)
        elif algo == "brotli":
            # Brotli is single-threaded; quality 0-11
            quality = level
            with source.open("rb") as src, target.open("wb") as dst:
                compressor = brotli.Compressor(quality=quality)
                while True:
                    check_job(job["id"])
                    chunk = src.read(1 << 20)
                    if not chunk:
                        break
                    dst.write(compressor.process(chunk))
                dst.write(compressor.finish())
        elif algo == "lzma2":
            import lzma
            # preset 0-9
            with source.open("rb") as src, lzma.open(
                target, "wb", format=lzma.FORMAT_XZ, preset=level
            ) as dst:
                while True:
                    check_job(job["id"])
                    chunk = src.read(1 << 20)
                    if not chunk:
                        break
                    dst.write(chunk)
                    if time.monotonic() - started > FILE_COMPRESS_TIMEOUT:
                        raise UserError("زمان مجاز فشرده‌سازی فایل تمام شد.")
        else:
            raise UserError("الگوریتم ناشناخته.")
    except Cancelled:
        raise
    except Exception as exc:
        if isinstance(exc, UserError):
            raise
        LOG.warning("File compress failed: %s", type(exc).__name__)
        raise UserError("فشرده‌سازی فایل ناموفق بود.") from None

    if not target.exists() or target.stat().st_size == 0:
        raise UserError("خروجی معتبری تولید نشد.")

    if target.stat().st_size > UPLOAD_LIMIT:
        raise UserError(
            "حجم خروجی از سقف ارسال بیشتر است. "
            "الگوریتم یا سطح دیگری را امتحان کن."
        )

    return time.monotonic() - started


# ---------- Job execution ----------

def upload_result(job, target, settings, caption, is_video=False, video_info=None):
    if is_video:
        filename = f'compressed_{settings["codec"]}_{job["id"]}.mp4'
        as_video = (
            settings.get("delivery") == "video"
            and settings.get("codec") == "h264"
        )
        method = "sendVideo" if as_video else "sendDocument"
        field = "video" if as_video else "document"
        data = {"chat_id": job["chat"], "caption": caption, "parse_mode": "HTML"}
        if as_video and video_info:
            data.update({
                "duration": max(1, int(video_info["duration"])),
                "width": video_info["width"],
                "height": video_info["height"],
                "supports_streaming": "true",
            })
    else:
        algo = settings["algo"]
        ext = FILE_ALGOS[algo]["ext"]
        base = Path(job["name"]).stem
        filename = f"{base}_{algo}{ext}"
        method = "sendDocument"
        field = "document"
        data = {"chat_id": job["chat"], "caption": caption, "parse_mode": "HTML"}

    with target.open("rb") as file:
        api(
            method,
            data,
            files={field: (filename, file, "application/octet-stream")},
            read_timeout=300,
        )


def process_job(job):
    folder = WORK / job["id"]
    folder.mkdir(parents=True, exist_ok=True)
    source = folder / "input"
    job_type = job.get("job_type") or "video"
    started = time.monotonic()

    try:
        check_job(job["id"])

        if shutil.disk_usage(WORK).free < 512_000_000:
            raise UserError("فضای آزاد دیسک کمتر از 512 MB است.")

        if not job["message"]:
            message = send(job["chat"], f'شروع کار <code>{job["id"]}</code>…')
            job["message"] = message["message_id"]
            execute(
                "UPDATE jobs SET message=? WHERE id=?",
                (job["message"], job["id"]),
            )

        edit(
            job["chat"], job["message"],
            f'📥 در حال دانلود کار <code>{job["id"]}</code>…',
            keyboard([[button("لغو", f'cancel:{job["id"]}')]]),
        )

        input_size = download(job, source)
        check_job(job["id"])
        settings = json.loads(job["settings"])

        if job_type == "video":
            info = probe(source)
            validate_media(info)

            edit(
                job["chat"], job["message"],
                f'🔎 ورودی: {info["width"]}×{info["height"]}\n'
                f'مدت: {info["duration"]:.1f} ثانیه\n'
                f'کدک ورودی: {info["codec"]}\n\n'
                "فشرده‌سازی ویدیو در حال شروع است…",
                keyboard([[button("لغو", f'cancel:{job["id"]}')]]),
            )

            target = folder / "output.mp4"
            encoding_seconds = encode_video(
                job, source, target, settings, info, folder
            )
            output_info = probe(target)
            output_size = target.stat().st_size
            reduction = (1 - output_size / input_size) * 100 if input_size else 0

            if reduction >= 0:
                change = f"کاهش حجم: {reduction:.1f}%"
            else:
                change = f"⚠️ افزایش حجم: {abs(reduction):.1f}%"

            caption = (
                f'✅ <b>نتیجه کار</b> <code>{job["id"]}</code>\n'
                f'نام: {job["name"]}\n\n'
                f"ورودی: {mb(input_size)}\n"
                f"خروجی: {mb(output_size)}\n"
                f"{change}\n"
                f"نسبت: {input_size / output_size:.2f}×\n"
                f'کدک: {CODECS[settings["codec"]]["title"]}\n'
                f'ابعاد: {output_info["width"]}×{output_info["height"]}\n'
                f'FPS: {output_info["fps"]:.2f}\n'
                f'مدت: {output_info["duration"]:.1f} ثانیه\n'
                f"زمان انکود: {encoding_seconds:.1f} ثانیه"
            )

            check_job(job["id"])
            edit(
                job["chat"], job["message"],
                f'📤 انکود تمام شد. حجم خروجی: {mb(output_size)}\n'
                "در حال ارسال…",
            )
            upload_result(job, target, settings, caption, is_video=True, video_info=output_info)

        else:  # file
            algo = settings["algo"]
            target = folder / f"output{FILE_ALGOS[algo]['ext']}"
            compress_seconds = compress_file(
                job, source, target, settings, folder
            )
            output_size = target.stat().st_size
            reduction = (1 - output_size / input_size) * 100 if input_size else 0

            if reduction >= 0:
                change = f"کاهش حجم: {reduction:.1f}%"
            else:
                change = f"⚠️ افزایش حجم: {abs(reduction):.1f}%"

            caption = (
                f'✅ <b>نتیجه کار</b> <code>{job["id"]}</code>\n'
                f'نام: {job["name"]}\n\n'
                f"ورودی: {mb(input_size)}\n"
                f"خروجی: {mb(output_size)}\n"
                f"{change}\n"
                f"نسبت: {input_size / max(output_size, 1):.2f}×\n"
                f'الگوریتم: {FILE_ALGOS[algo]["title"]}\n'
                f'سطح: {LEVEL_LABELS[settings["level"]]}\n'
                f"زمان فشرده‌سازی: {compress_seconds:.1f} ثانیه"
            )

            check_job(job["id"])
            edit(
                job["chat"], job["message"],
                f'📤 فشرده‌سازی تمام شد. حجم خروجی: {mb(output_size)}\n'
                "در حال ارسال…",
            )
            upload_result(job, target, settings, caption, is_video=False)

        execute("UPDATE jobs SET status='done' WHERE id=?", (job["id"],))
        edit(
            job["chat"], job["message"],
            f'✅ کار <code>{job["id"]}</code> ارسال شد.\n'
            f"زمان کل: {time.monotonic() - started:.1f} ثانیه",
        )

    except Cancelled:
        execute("UPDATE jobs SET status='cancelled' WHERE id=?", (job["id"],))
        edit(job["chat"], job["message"], f'⛔ کار <code>{job["id"]}</code> لغو شد.')

    except Requeue:
        execute(
            "UPDATE jobs SET status='queued' WHERE id=? AND status='running'",
            (job["id"],),
        )

    except Exception as exc:
        if STOP.is_set():
            execute(
                "UPDATE jobs SET status='queued' WHERE id=? AND status='running'",
                (job["id"],),
            )
        else:
            current = job_by_id(job["id"])
            if current and current["status"] == "cancelled":
                return

            execute("UPDATE jobs SET status='failed' WHERE id=?", (job["id"],))
            reason = (
                str(exc) if isinstance(exc, UserError)
                else "خطای داخلی پردازش."
            )
            LOG.warning("Job %s failed (%s).", job["id"], type(exc).__name__)
            safe_send(
                job["chat"],
                f'❌ کار <code>{job["id"]}</code> ناموفق بود.\n{reason}',
            )
            edit(job["chat"], job["message"], f'❌ کار <code>{job["id"]}</code> ناموفق بود.')
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

    import fcntl
    lock_file = (DATA / "instance.lock").open("a")
    try:
        fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        raise SystemExit("Another bot instance is using DATA_DIR.")

    for path in WORK.iterdir():
        if path.is_dir():
            shutil.rmtree(path, ignore_errors=True)
        else:
            path.unlink(missing_ok=True)

    execute("UPDATE jobs SET status='queued' WHERE status='running'")

    identity = api("getMe")
    LOG.info("Starting @%s (API base: %s)", identity.get("username", "bot"), BOT_API_BASE)

    api("deleteWebhook", {"drop_pending_updates": "false"})
    try:
        api("setMyCommands", {"commands": [
            {"command": "start", "description": "منوی اصلی"},
            {"command": "settings", "description": "تنظیمات فشرده‌سازی"},
            {"command": "presets", "description": "پریست‌های ذخیره‌شده"},
            {"command": "mode", "description": "تعویض حالت ویدیو / فایل"},
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


if __name__ == "__main__":
    main()
