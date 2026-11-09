
import copy
import io
import json
import logging
import lzma
import os
import re
import shutil
import signal
import sqlite3
import subprocess
import tarfile
import threading
import time
import uuid
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import requests

try:
    import brotli
    import zstandard as zstd
except ImportError as exc:
    raise SystemExit(f"Missing compression library: {exc}")


# ---------- Configuration ----------
START_TIME = time.time()
TOKEN = os.environ.get("BOT_TOKEN", "").strip()
API_BASE = "http://127.0.0.1:8081"
if not TOKEN:
    raise SystemExit("Bot token is required.")

USE_LOCAL_API = True
DOWNLOAD_LIMIT = 1_900_000_000
UPLOAD_LIMIT = 1_900_000_000

DATA = Path(os.environ.get("DATA_DIR", "./data")).resolve()
WORK = DATA / "work"
DATA.mkdir(parents=True, exist_ok=True)
WORK.mkdir(parents=True, exist_ok=True)

ALLOWED = {int(x.strip()) for x in os.environ.get("ALLOWED_USERS", "").split(",") if x.strip()}

THREADS = max(1, min(int(os.environ.get("FFMPEG_THREADS", "2")), 8))
MAX_SECONDS = max(60, int(os.environ.get("MAX_VIDEO_SECONDS", "1800")))
ENCODE_TIMEOUT = max(60, int(os.environ.get("ENCODE_TIMEOUT", "3600")))

MAX_ACTIVE_PER_USER = 20
MAX_ACTIVE_TOTAL = 60
MAX_PIXELS = 3840 * 2160
MAX_CONCURRENT_DOWNLOADS = 3

STOP = threading.Event()
DB_LOCK = threading.RLock()
DOWNLOAD_SEM = threading.Semaphore(MAX_CONCURRENT_DOWNLOADS)
SESSION = {}
COLLECT = {}

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOG = logging.getLogger("compressor")


# ---------- Codecs ----------
VIDEO_CODECS = {
    "h264": {"encoder": "libx264", "title": "H.264", "crf": {"quality": 21, "balanced": 25, "small": 29}},
    "hevc": {"encoder": "libx265", "title": "H.265 / HEVC", "crf": {"quality": 24, "balanced": 28, "small": 32}},
    "av1": {"encoder": "libsvtav1", "title": "AV1", "crf": {"quality": 28, "balanced": 33, "small": 38}},
}

FILE_CODECS = {
    "zstd": {"title": "Zstandard", "ext": ".zst", "levels": {"fast": 1, "balanced": 6, "max": 19}},
    "brotli": {"title": "Brotli", "ext": ".br", "levels": {"fast": 1, "balanced": 6, "max": 11}},
    "lzma2": {"title": "LZMA2 (XZ)", "ext": ".xz", "levels": {"fast": 1, "balanced": 6, "max": 9}},
}

QUALITY_LABELS = {"quality": "کیفیت بیشتر", "balanced": "متعادل", "small": "حجم کمتر", "target": "🎯 حجم هدف"}
LEVEL_LABELS = {"fast": "سریع", "balanced": "متعادل", "max": "حداکثر"}

KEY_TITLES = {
    "codec": "🎛 کدک", "quality": "⚡ کیفیت", "height": "📐 رزولوشن",
    "fps": "🎞 نرخ فریم", "audio": "🔊 صدا", "delivery": "📤 ارسال",
    "auto": "🤖 خودکار", "strip_metadata": "🧹 متادیتا",
    "keep_subs": "💬 زیرنویس", "hdr_to_sdr": "🌈 HDR→SDR",
    "preview_only": "🧪 نمونه", "file_codec": "🗜 الگوریتم",
    "file_level": "🎚 سطح",
}

DEFAULTS = {
    "codec": "h264", "quality": "balanced", "target_mb": 0, "height": 720,
    "fps": 0, "audio": 96, "delivery": "document", "auto": False,
    "strip_metadata": True, "keep_subs": False, "hdr_to_sdr": True,
    "preview_only": False, "file_codec": "zstd", "file_level": "balanced",
}

CHOICES = {
    "codec": [("H.264 — سازگارترین", "h264"), ("H.265 — حجم کمتر", "hevc"), ("AV1 — کم‌ترین حجم", "av1")],
    "quality": [("کیفیت بیشتر", "quality"), ("متعادل", "balanced"), ("حجم کمتر", "small"), ("🎯 حجم هدف دلخواه", "target")],
    "height": [("اصلی", 0), ("1080p", 1080), ("720p", 720), ("480p", 480)],
    "fps": [("اصلی", 0), ("30", 30), ("24", 24)],
    "audio": [("128 kbps", 128), ("96 kbps", 96), ("64 kbps", 64), ("بی‌صدا", 0)],
    "delivery": [("📎 فایل", "document"), ("▶️ ویدیوی قابل پخش", "video")],
    "auto": [("✋ تأیید هر فایل", False), ("🤖 صف خودکار", True)],
    "strip_metadata": [("حذف", True), ("حفظ", False)],
    "keep_subs": [("حفظ زیرنویس", True), ("حذف زیرنویس", False)],
    "hdr_to_sdr": [("تبدیل HDR→SDR", True), ("رد کردن HDR", False)],
    "preview_only": [("فقط نمونه ۱۰ ثانیه", True), ("کامل", False)],
    "file_codec": [("Zstandard", "zstd"), ("Brotli", "brotli"), ("LZMA2 (XZ)", "lzma2")],
    "file_level": [("سریع", "fast"), ("متعادل", "balanced"), ("حداکثری", "max")],
}

BUILTIN_PRESETS = {
    "archive": {"title": "🎬 کیفیت بالا برای آرشیو",
                "settings": {"codec": "h264", "quality": "quality", "height": 0, "fps": 0, "audio": 128, "strip_metadata": False, "keep_subs": True}},
    "social": {"title": "📱 اشتراک در شبکه‌های اجتماعی",
               "settings": {"codec": "h264", "quality": "balanced", "height": 1080, "fps": 30, "audio": 96, "strip_metadata": True, "keep_subs": False}},
    "compact": {"title": "📦 کم‌حجم برای ذخیره",
                "settings": {"codec": "hevc", "quality": "small", "height": 720, "fps": 24, "audio": 64, "strip_metadata": True, "keep_subs": False}},
    "fast": {"title": "⚡ سریع‌ترین",
             "settings": {"codec": "h264", "quality": "balanced", "height": 720, "fps": 0, "audio": 96, "strip_metadata": True, "keep_subs": False}},
}

INFO_TEXTS = {
    "codec": "🎛 **کدک ویدیو**\n\n• **H.264**: همه‌جا پخش می‌شود. امن‌ترین.\n• **H.265**: ~۳۰–۵۰٪ کم‌تر، اما در دستگاه‌های قدیمی ممکن است پخش نشود.\n• **AV1**: جدیدترین، کم‌ترین حجم، پردازش کندتر.",
    "quality": "⚡ **کیفیت**\n\n• **کیفیت بیشتر**: جزئیات بالا، حجم بیشتر.\n• **متعادل**: تعادل خوب.\n• **حجم کمتر**: افت جزئیات در ازای حجم.\n• **🎯 حجم هدف**: خودت حجم خروجی را تعیین می‌کنی.",
    "height": "📐 سقف ارتفاع. اگر ویدیو کوتاه‌تر باشد، تغییر نمی‌کند.",
    "fps": "🎞 سقف نرخ فریم. ویدیوهای کم‌فریم بالا نمی‌روند.",
    "audio": "🔊 بی‌صدا: صدا کاملاً حذف می‌شود.",
    "delivery": "📤 فایل: بدون فشرده‌سازی مجدد تلگرام.\n▶️ ویدیو: فقط H.264، پیش‌نمایش در چت.",
    "auto": "🤖 صف خودکار: بلافاصله شروع می‌شود.\n✋ تأیید: قبل از شروع، مشخصات را می‌بینی و تأیید می‌کنی.",
    "strip_metadata": "🧹 حذف GPS و اطلاعات دوربین.",
    "keep_subs": "💬 زیرنویس‌های داخلی فایل در خروجی حفظ می‌شوند.",
    "hdr_to_sdr": "🌈 HDR را به SDR تبدیل می‌کند (کیفیت رنگ کاهش می‌یابد اما سازگاری بالا می‌رود).",
    "preview_only": "🧪 فقط ۱۰ ثانیه از وسط ویدیو را انکود می‌کند تا کیفیت را ببینی.",
    "file_codec": "🗜 **الگوریتم**\n\n• **Zstd**: سریع و متعادل.\n• **Brotli**: بهتر برای متن/وب.\n• **LZMA2**: بالاترین نسبت، کندترین.",
    "file_level": "🎚 سریع‌تر = حجم بیشتر. حداکثری = کندتر اما کم‌حجم‌تر.",
}


class UserError(Exception): pass
class Cancelled(Exception): pass
class Requeue(Exception): pass


# ---------- Utils ----------
def human_size(size):
    if size is None: return "—"
    if size < 1024: return f"{int(size)} بایت"
    if size < 1024**2: return f"{size/1024:.1f} KB"
    if size < 1024**3: return f"{size/1024**2:.1f} MB"
    return f"{size/1024**3:.2f} GB"


def human_time(seconds):
    seconds = max(0, int(seconds))
    if seconds < 60: return f"{seconds} ثانیه"
    m, s = divmod(seconds, 60)
    if m < 60: return f"{m} دقیقه" if s == 0 else f"{m} دقیقه و {s} ثانیه"
    h, m = divmod(m, 60)
    if h < 24: return f"{h} ساعت و {m} دقیقه"
    d, h = divmod(h, 24)
    return f"{d} روز و {h} ساعت"


def progress_bar(percent, width=10):
    p = max(0.0, min(100.0, percent))
    filled = int(p / 100 * width)
    return "█" * filled + "░" * (width - filled) + f" {p:.1f}%"


class UploadProgress:
    def __init__(self, f, total, callback):
        self._f = f; self._total = total; self._cb = callback
        self._read = 0; self._last = 0
    def read(self, n=-1):
        data = self._f.read(n)
        if data:
            self._read += len(data)
            now = time.monotonic()
            if now - self._last >= 2:
                self._last = now
                try: self._cb(self._read, self._total)
                except Exception: pass
        return data
    def seek(self, pos, whence=0):
        r = self._f.seek(pos, whence)
        if pos == 0 and whence == 0: self._read = 0
        return r
    def tell(self): return self._f.tell()
    def __getattr__(self, name): return getattr(self._f, name)


def _has_filter(name):
    try:
        r = subprocess.run(["ffmpeg", "-hide_banner", "-filters"], capture_output=True, text=True, timeout=10)
        return bool(re.search(rf"\s{name}\s", r.stdout))
    except Exception: return False

HAS_ZSCALE = _has_filter("zscale")
HAS_TONEMAP = _has_filter("tonemap")


# ---------- DB ----------
DB = sqlite3.connect(DATA / "state.sqlite3", check_same_thread=False)
DB.row_factory = sqlite3.Row
DB.execute("PRAGMA journal_mode=WAL")
DB.execute("PRAGMA busy_timeout=5000")
DB.executescript("""
CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS users (uid INTEGER PRIMARY KEY, settings TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY, uid INTEGER NOT NULL, chat INTEGER NOT NULL,
    file_id TEXT NOT NULL, name TEXT NOT NULL, size INTEGER NOT NULL,
    settings TEXT NOT NULL, status TEXT NOT NULL, message INTEGER,
    created REAL NOT NULL, kind TEXT NOT NULL DEFAULT 'video'
);
CREATE INDEX IF NOT EXISTS jobs_status_created ON jobs(status, created);
CREATE TABLE IF NOT EXISTS presets (
    id INTEGER PRIMARY KEY AUTOINCREMENT, uid INTEGER NOT NULL, name TEXT NOT NULL,
    kind TEXT NOT NULL, settings TEXT NOT NULL, created REAL NOT NULL
);
""")

cols = [r[1] for r in DB.execute("PRAGMA table_info(jobs)").fetchall()]
for c, d in [
    ("kind", "TEXT NOT NULL DEFAULT 'video'"),
    ("server_path", "TEXT DEFAULT NULL"),
    ("output_size", "INTEGER DEFAULT NULL"),
    ("finished_at", "REAL DEFAULT NULL"),
    ("media_info", "TEXT DEFAULT NULL"),
    ("error_msg", "TEXT DEFAULT NULL"),
]:
    if c not in cols:
        DB.execute(f"ALTER TABLE jobs ADD COLUMN {c} {d}")
DB.commit()


def execute(sql, args=()):
    with DB_LOCK:
        cur = DB.execute(sql, args); DB.commit(); return cur.rowcount

def one(sql, args=()):
    with DB_LOCK:
        r = DB.execute(sql, args).fetchone()
        return dict(r) if r else None

def many(sql, args=()):
    with DB_LOCK:
        return [dict(x) for x in DB.execute(sql, args).fetchall()]

def get_kv(k, d=None):
    r = one("SELECT value FROM kv WHERE key=?", (k,))
    return r["value"] if r else d

def put_kv(k, v):
    execute("INSERT OR REPLACE INTO kv(key,value) VALUES(?,?)", (k, str(v)))

def get_settings(uid):
    r = one("SELECT settings FROM users WHERE uid=?", (uid,))
    s = copy.deepcopy(DEFAULTS)
    if r:
        try: s.update(json.loads(r["settings"]))
        except (ValueError, TypeError): pass
    return s

def save_settings(uid, s):
    execute("INSERT OR REPLACE INTO users(uid,settings) VALUES(?,?)", (uid, json.dumps(s)))

def save_preset(uid, name, kind, settings):
    keys = (["codec", "quality", "height", "fps", "audio", "delivery", "strip_metadata", "keep_subs", "hdr_to_sdr", "preview_only", "target_mb"] if kind == "video" else ["file_codec", "file_level"])
    subset = {k: settings[k] for k in keys if k in settings}
    execute("INSERT INTO presets(uid,name,kind,settings,created) VALUES(?,?,?,?,?)",
            (uid, name[:32], kind, json.dumps(subset), time.time()))

def list_presets(uid, kind):
    return many("SELECT * FROM presets WHERE uid=? AND kind=? ORDER BY created DESC", (uid, kind))

def get_preset(uid, pid):
    return one("SELECT * FROM presets WHERE id=? AND uid=?", (pid, uid))

def delete_preset(uid, pid):
    return execute("DELETE FROM presets WHERE id=? AND uid=?", (pid, uid))

def authorized(uid, claim=False):
    if ALLOWED: return uid in ALLOWED
    with DB_LOCK:
        owner = get_kv("owner")
        if owner is None and claim:
            put_kv("owner", uid); owner = str(uid)
        return owner == str(uid)

def job_by_id(jid): return one("SELECT * FROM jobs WHERE id=?", (jid,))


# ---------- API ----------
def api(method, data=None, files=None, read_timeout=90):
    payload = {}
    for k, v in (data or {}).items():
        if v is None: continue
        payload[k] = json.dumps(v, ensure_ascii=False) if isinstance(v, (dict, list)) else v
    for attempt in range(2):
        if files:
            for item in files.values():
                try: item[1].seek(0)
                except Exception: pass
        try:
            r = requests.post(f"{API_BASE}/bot{TOKEN}/{method}", data=payload, files=files, timeout=(10, read_timeout))
            body = r.json()
        except (requests.RequestException, ValueError):
            raise UserError("ارتباط با Telegram برقرار نشد.") from None
        if body.get("ok"): return body["result"]
        if body.get("error_code") == 429 and attempt == 0:
            d = int(body.get("parameters", {}).get("retry_after", 3))
            if d <= 30:
                if STOP.wait(max(1, d)): raise Requeue()
                continue
        desc = str(body.get("description", "API error")).replace(TOKEN, "[TOKEN]")
        raise UserError(f"Telegram: {desc[:180]}")
    raise UserError("درخواست Telegram ناموفق بود.")


def button(text, callback): return {"text": text, "callback_data": callback}
def keyboard(rows): return {"inline_keyboard": rows}
def send(chat, text, markup=None): return api("sendMessage", {"chat_id": chat, "text": text, "reply_markup": markup})

def safe_send(chat, text, markup=None):
    try: return send(chat, text, markup)
    except Exception:
        LOG.warning("Could not send notification."); return None

def edit(chat, mid, text, markup=None):
    if not mid: return
    try:
        api("editMessageText", {"chat_id": chat, "message_id": mid, "text": text, "reply_markup": markup or keyboard([])}, read_timeout=15)
    except Exception: pass

def answer(cid, text="", show_alert=False):
    try:
        data = {"callback_query_id": cid, "text": text[:200]}
        if show_alert: data["show_alert"] = "true"
        api("answerCallbackQuery", data, read_timeout=15)
    except Exception: pass


# ---------- Menus ----------
def clean_name(name):
    name = Path(name or "file").name
    return "".join(c for c in name if c.isprintable())[:80] or "file"

def video_summary(settings):
    c = VIDEO_CODECS[settings["codec"]]
    if settings["quality"] == "target":
        q = f"🎯 هدف: {settings.get('target_mb', 0)} MB"
    else:
        q = f'{QUALITY_LABELS[settings["quality"]]} (CRF={c["crf"][settings["quality"]]})'
    h = settings["height"] or "اصلی"
    a = f'{settings["audio"]} kbps' if settings["audio"] else "بی‌صدا"
    return (
        f'کدک: **{c["title"]}** | کیفیت: **{q}**\n'
        f'📐 {h} | 🎞 {settings["fps"] or "اصلی"} FPS | 🔊 {a}\n'
        f'💬 زیرنویس: {"حفظ" if settings["keep_subs"] else "حذف"} | '
        f'🌈 HDR: {"SDR" if settings["hdr_to_sdr"] else "رد"}\n'
        f'🧪 {"فقط نمونه ۱۰s" if settings["preview_only"] else "کامل"} | '
        f'🤖 {"خودکار" if settings["auto"] else "تأیید"}'
    )

def file_summary(settings):
    c = FILE_CODECS[settings["file_codec"]]
    lv = c["levels"][settings["file_level"]]
    return f'الگوریتم: **{c["title"]}** | سطح: {LEVEL_LABELS[settings["file_level"]]} (level={lv})'

def home(chat, message_id=None):
    text = (
        "🎬 **فشرده‌ساز چندمنظوره**\n\n"
        "📥 ویدیو یا فایل بفرست تا فشرده شود.\n"
        "می‌توانی چند فایل پشت‌سرهم ارسال کنی.\n\n"
        f"📏 سقف ورودی: **{human_size(DOWNLOAD_LIMIT)}** (Local API فعال)\n"
        f"⏱ مدت مجاز ویدیو: **{human_time(MAX_SECONDS)}**"
    )
    markup = keyboard([
        [button("⚙️ تنظیمات ویدیو", "menu:settings"), button("📦 تنظیمات فایل", "menu:file_settings")],
        [button("💾 پریست‌های آماده", "menu:builtin"), button("📂 پریست‌های من", "menu:presets")],
        [button("📋 صف کارها", "menu:queue"), button("📊 آمار ربات", "menu:stats")],
        [button("❓ راهنما", "menu:help")],
    ])
    if message_id: edit(chat, message_id, text, markup)
    else: send(chat, text, markup)

def settings_menu(chat, uid, mid=None):
    s = get_settings(uid)
    text = "⚙️ **تنظیمات ویدیو**\n(برای ویدیوهای بعدی)\n\n" + video_summary(s) + "\n\nتغییرات روی کارهای ثبت‌شده اثر ندارد."
    markup = keyboard([
        [button("🎛 کدک", "pick:codec"), button("⚡ کیفیت", "pick:quality")],
        [button("📐 رزولوشن", "pick:height"), button("🎞 FPS", "pick:fps")],
        [button("🔊 صدا", "pick:audio"), button("📤 ارسال", "pick:delivery")],
        [button("🤖 خودکار", "pick:auto"), button("🧹 متادیتا", "pick:strip_metadata")],
        [button("💬 زیرنویس", "pick:keep_subs"), button("🌈 HDR", "pick:hdr_to_sdr")],
        [button("🧪 نمونه", "pick:preview_only"), button("💾 ذخیره پریست", "preset:save:video")],
        [button("♻️ بازنشانی", "reset:video"), button("🔙 بازگشت", "menu:home")],
    ])
    if mid: edit(chat, mid, text, markup)
    else: send(chat, text, markup)

def file_settings_menu(chat, uid, mid=None):
    s = get_settings(uid)
    text = "📦 **تنظیمات فایل**\n(برای فایل‌های غیر ویدیویی)\n\n" + file_summary(s) + "\n\nتغییرات روی کارهای ثبت‌شده اثر ندارد."
    markup = keyboard([
        [button("🗜 الگوریتم", "pick:file_codec")],
        [button("🎚 سطح", "pick:file_level")],
        [button("💾 ذخیره پریست", "preset:save:file"), button("📂 پریست‌های من", "menu:presets")],
        [button("📦 حالت جمع‌آوری", "menu:collect")],
        [button("♻️ بازنشانی", "reset:file"), button("🔙 بازگشت", "menu:home")],
    ])
    if mid: edit(chat, mid, text, markup)
    else: send(chat, text, markup)

def builtin_presets_menu(chat, mid=None):
    text = "💾 **پریست‌های آماده**\n\nیکی را انتخاب کن تا تنظیماتت اعمال شود:"
    rows = [[button(p["title"], f"builtin:{k}")] for k, p in BUILTIN_PRESETS.items()]
    rows.append([button("🔙 بازگشت", "menu:home")])
    if mid: edit(chat, mid, text, keyboard(rows))
    else: send(chat, text, keyboard(rows))

def show_presets(chat, uid, mid=None):
    parts = ["📂 **پریست‌های من**"]
    buttons = []
    for kind, title in (("video", "🎥"), ("file", "📦")):
        rows = list_presets(uid, kind)
        parts.append(f"\n{title} {'ویدیو' if kind=='video' else 'فایل'}:")
        if not rows:
            parts.append("— خالی —"); continue
        for r in rows:
            parts.append(f'• {r["name"]}')
            buttons.append([button(f'{title} {r["name"]}', f'preset:load:{r["id"]}'), button("🗑", f'preset:del:{r["id"]}')])
    buttons.append([button("🔙 بازگشت", "menu:home")])
    if mid: edit(chat, mid, "\n".join(parts), keyboard(buttons))
    else: send(chat, "\n".join(parts), keyboard(buttons))

def show_queue(chat, uid):
    rows = many("SELECT * FROM jobs WHERE uid=? ORDER BY created DESC LIMIT 15", (uid,))
    icons = {"downloading": "📥", "awaiting": "⏳", "queued": "📋", "running": "🔄",
             "done": "✅", "failed": "❌", "cancelled": "⛔"}
    labels = {"downloading": "در حال دانلود", "awaiting": "منتظر تأیید", "queued": "در صف",
              "running": "در حال پردازش", "done": "ارسال‌شده", "failed": "ناموفق", "cancelled": "لغوشده"}
    if not rows:
        send(chat, "📋 صف خالی است."); return
    lines = ["📋 **صف و وضعیت کارها**\n"]
    buttons = []
    for r in rows:
        k = "🎥" if r.get("kind") == "video" else "📦"
        icon = icons.get(r["status"], "•")
        lines.append(f'{icon} {k} `{r["id"]}` — {labels.get(r["status"], r["status"])}\n   {r["name"]}')
        if r["status"] == "awaiting":
            buttons.append([button(f'▶️ شروع {r["id"]}', f'go:{r["id"]}'), button("❌ لغو", f'cancel:{r["id"]}')])
        elif r["status"] in ("queued", "running", "downloading"):
            buttons.append([button(f'❌ لغو {r["id"]}', f'cancel:{r["id"]}')])
    send(chat, "\n\n".join(lines), keyboard(buttons))

def show_stats(chat, mid=None):
    users = one("SELECT COUNT(*) n FROM users")["n"]
    total_jobs = one("SELECT COUNT(*) n FROM jobs")["n"]
    done = one("SELECT COUNT(*) n FROM jobs WHERE status='done'")["n"]
    failed = one("SELECT COUNT(*) n FROM jobs WHERE status='failed'")["n"]
    cancelled = one("SELECT COUNT(*) n FROM jobs WHERE status='cancelled'")["n"]
    active = one("SELECT COUNT(*) n FROM jobs WHERE status IN ('downloading','awaiting','queued','running')")["n"]
    video = one("SELECT COUNT(*) n FROM jobs WHERE kind='video'")["n"]
    filej = one("SELECT COUNT(*) n FROM jobs WHERE kind='file'")["n"]
    sin = one("SELECT COALESCE(SUM(size),0) s FROM jobs WHERE status='done'")["s"]
    sout = one("SELECT COALESCE(SUM(output_size),0) s FROM jobs WHERE status='done'")["s"]
    avg_time = one("SELECT AVG(finished_at - created) a FROM jobs WHERE status='done' AND finished_at IS NOT NULL")["a"] or 0
    avg_dur = one("SELECT AVG(json_extract(media_info,'$.duration')) a FROM jobs WHERE media_info IS NOT NULL")["a"] or 0
    today_start = time.time() - 86400
    today_jobs = one("SELECT COUNT(*) n FROM jobs WHERE created > ?", (today_start,))["n"]
    week_start = time.time() - 7*86400
    week_jobs = one("SELECT COUNT(*) n FROM jobs WHERE created > ?", (week_start,))["n"]
    du = shutil.disk_usage(DATA)
    uptime = time.time() - START_TIME
    ratio = (sout/sin*100) if sin else 0
    text = (
        "📊 **آمار ربات و سرور**\n\n"
        f"⏱ آپتایم: {human_time(uptime)}\n"
        f"👥 کاربران: **{users}**\n"
        f"🎬 کل کارها: **{total_jobs}**\n"
        f"   ✅ موفق: {done} | ❌ ناموفق: {failed} | ⛔ لغو: {cancelled}\n"
        f"   🔄 فعال: {active}\n"
        f"   🎥 ویدیو: {video} | 📦 فایل: {filej}\n\n"
        f"📥 ورودی کل: {human_size(sin)}\n"
        f"📤 خروجی کل: {human_size(sout)}\n"
        f"📉 نسبت فشرده‌سازی: {ratio:.1f}%\n"
        f"⏱ میانگین زمان پردازش: {human_time(avg_time)}\n"
        f"🎞 میانگین مدت ویدیو: {human_time(avg_dur)}\n\n"
        f"📅 امروز: {today_jobs} | 📆 این هفته: {week_jobs}\n\n"
        f"💾 دیسک کل: {human_size(du.total)}\n"
        f"   آزاد: {human_size(du.free)}\n"
        f"   استفاده‌شده: {human_size(du.used)}"
    )
    if mid: edit(chat, mid, text, keyboard([[button("🔙 بازگشت", "menu:home")]]))
    else: send(chat, text, keyboard([[button("🔙 بازگشت", "menu:home")]]))

def help_text():
    return (
        "📖 **راهنمای کامل**\n\n"
        "🎥 **ویدیو**\n"
        "1. تنظیمات را انتخاب کن\n"
        "2. ویدیو بفرست\n"
        "3. در کادر بازشده، اگر خواستی روی «نمونه ۱۰ ثانیه‌ای» بزن تا کیفیت را ببینی\n"
        "4. سپس روی «شروع» بزن\n\n"
        "📦 **فایل**\n"
        "1. تنظیمات الگوریتم را انتخاب کن\n"
        "2. فایل بفرست\n"
        "3. خروجی فشرده دریافت کن\n\n"
        "⚡ **ویژگی‌ها**\n"
        "• 🧪 نمونه ۱۰ ثانیه‌ای: کیفیت را قبل از پردازش کامل ببین\n"
        "• 🌈 HDR→SDR: ویدیوهای HDR پذیرفته می‌شوند\n"
        "• 💬 حفظ زیرنویس داخلی\n"
        "• 📦 حالت جمع‌آوری: چند فایل با هم در یک آرشیو\n"
        "• 🗑 حذف خودکار فایل پس از ارسال\n\n"
        "⚠️ **محدودیت‌ها**\n"
        f"• مدت مجاز ویدیو: {human_time(MAX_SECONDS)}\n"
        f'• سقف حجم: {human_size(DOWNLOAD_LIMIT)}\n\n'
        "📜 دستورات:\n"
        "/start — منوی اصلی\n"
        "/settings — تنظیمات ویدیو\n"
        "/filesettings — تنظیمات فایل\n"
        "/queue — صف کارها\n"
        "/stats — آمار ربات\n"
        "/presets — پریست‌های من\n"
        "/cancel — لغو همه کارها\n"
        "/privacy — نگهداری داده‌ها"
    )


# ---------- Receive ----------
def _create_job(chat, uid, media, name, size, kind):
    s = get_settings(uid)
    jid = uuid.uuid4().hex[:12]
    with DB_LOCK:
        active = one("SELECT COUNT(*) n FROM jobs WHERE status IN ('downloading','awaiting','queued','running')")["n"]
        personal = one("SELECT COUNT(*) n FROM jobs WHERE uid=? AND status IN ('downloading','awaiting','queued','running')", (uid,))["n"]
        if active >= MAX_ACTIVE_TOTAL or personal >= MAX_ACTIVE_PER_USER:
            send(chat, "❌ صف پر است؛ کمی صبر کن یا کارهای قبلی را لغو کن."); return
        execute("INSERT INTO jobs (id,uid,chat,file_id,name,size,settings,status,message,created,kind) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                (jid, uid, chat, media["file_id"], name, size, json.dumps(s), "downloading", None, time.time(), kind))
    preview = video_summary(s) if kind == "video" else file_summary(s)
    text = (f'{"🎞" if kind=="video" else "📦"} کار `{jid}`\n'
            f'📄 {name}\n💾 {human_size(size)}\n\n{preview}\n\n📥 در حال دانلود به سرور...')
    result = safe_send(chat, text, keyboard([[button("❌ لغو", f"cancel:{jid}")]]))
    if result:
        execute("UPDATE jobs SET message=? WHERE id=?", (result["message_id"], jid))
    start_background_download(jid, s["auto"])

def receive_video(message, uid, chat):
    media = message.get("video") or message.get("document")
    if not media:
        send(chat, "لطفاً ویدیو یا فایل ویدیویی بفرست."); return
    name = clean_name(media.get("file_name") or "video.mp4")
    mime = media.get("mime_type", "")
    exts = (".mp4", ".mkv", ".mov", ".webm", ".avi", ".m4v", ".ts")
    if "video" not in message and not mime.startswith("video/") and not name.lower().endswith(exts):
        send(chat, "این فایل ویدیویی نیست."); return
    size = int(media.get("file_size", 0))
    if size > DOWNLOAD_LIMIT:
        send(chat, f"فایل از سقف ({human_size(DOWNLOAD_LIMIT)}) بزرگ‌تر است."); return
    _create_job(chat, uid, media, name, size, "video")

def receive_file(message, uid, chat):
    media = message.get("document")
    if not media:
        send(chat, "لطفاً فایل ارسال کن."); return
    name = clean_name(media.get("file_name") or "file.bin")
    mime = media.get("mime_type", "")
    if "video" in message or mime.startswith("video/"):
        send(chat, "این فایل ویدیویی است؛ از بخش ویدیو استفاده کن."); return
    size = int(media.get("file_size", 0))
    if size > DOWNLOAD_LIMIT:
        send(chat, f"فایل از سقف ({human_size(DOWNLOAD_LIMIT)}) بزرگ‌تر است."); return
    _create_job(chat, uid, media, name, size, "file")


# ---------- Background download ----------
def start_background_download(job_id, auto):
    def runner():
        if not DOWNLOAD_SEM.acquire(blocking=False):
            new = "queued" if auto else "awaiting"
            execute("UPDATE jobs SET status=? WHERE id=? AND status='downloading'", (new, job_id))
            _render_ready(job_id); return
        job = job_by_id(job_id)
        if not job or job["status"] != "downloading":
            DOWNLOAD_SEM.release(); return
        try:
            folder = WORK / job_id; folder.mkdir(parents=True, exist_ok=True)
            source = folder / "input"
            if shutil.disk_usage(WORK).free < 512_000_000: raise UserError("فضای دیسک کم است.")
            download(job, source)
            new = "queued" if auto else "awaiting"
            changed = execute("UPDATE jobs SET status=? WHERE id=? AND status='downloading'", (new, job_id))
            if changed: _render_ready(job_id)
        except Cancelled:
            execute("UPDATE jobs SET status='cancelled' WHERE id=? AND status='downloading'", (job_id,))
        except Exception as exc:
            if STOP.is_set():
                execute("UPDATE jobs SET status='queued' WHERE id=? AND status='downloading'", (job_id,)); return
            execute("UPDATE jobs SET status='failed', error_msg=? WHERE id=? AND status='downloading'",
                    (str(exc)[:200], job_id))
            reason = str(exc) if isinstance(exc, UserError) else "خطای دانلود."
            safe_send(job["chat"], f'❌ دانلود `{job_id}` ناموفق بود.\n{reason}')
            edit(job["chat"], job.get("message"), f'❌ دانلود `{job_id}` ناموفق بود.')
        finally:
            DOWNLOAD_SEM.release()
    threading.Thread(target=runner, daemon=True).start()


def _render_ready(job_id):
    job = job_by_id(job_id)
    if not job: return
    s = json.loads(job["settings"])
    kind = job.get("kind", "video")
    info = json.loads(job["media_info"]) if job.get("media_info") else None
    if kind == "video" and info is None:
        try:
            folder = WORK / job_id
            source = folder / "input"
            if source.exists():
                info = probe(source)
                execute("UPDATE jobs SET media_info=? WHERE id=?", (json.dumps(info), job_id))
        except Exception: pass
    
    lines = [f'🎞 کار: `{job_id}`']
    lines.append(f'📄 {job["name"]}')
    lines.append(f'💾 {human_size(job["size"])}')
    
    if info and kind == "video":
        lines.append(f'\n🔍 مشخصات ورودی:')
        lines.append(f'📐 {info["width"]}×{info["height"]} | ⏱ {human_time(info["duration"])} | 🎞 {info["fps"]:.1f} FPS')
        if info.get("transfer") in ("smpte2084", "arib-std-b67"):
            lines.append("🌈 **HDR** شناسایی شد (تبدیل به SDR)")
        est = estimate_output_size(info, s)
        if est: lines.append(f"📊 حجم تخمینی: **~{human_size(est)}**")
        
    lines.append("\n⚙️ تنظیمات اعمال‌شده:")
    if kind == "video":
        c = VIDEO_CODECS[s["codec"]]
        q_label = QUALITY_LABELS[s["quality"]] if s["quality"] != "target" else f"🎯 {s.get('target_mb', 0)} MB"
        lines.append(f'🎛 کدک: {c["title"]} | ⚡ کیفیت: {q_label}')
        lines.append(f'📐 {s["height"] or "اصلی"}p | 🔊 {s["audio"]} kbps | 🎞 {s["fps"] or "اصلی"} FPS')
        lines.append(f'💬 زیرنویس: {"حفظ" if s["keep_subs"] else "حذف"} | 🌈 HDR: {"SDR" if s["hdr_to_sdr"] else "رد"}')
    else:
        lines.append(file_summary(s))
        
    lines.append("\n👋 تنظیمات را می‌توانی با پریست تغییر بدهی. برای تست، روی «نمونه» بزن.")
    
    actions = []
    if job["status"] == "queued":
        lines.append("📋 در صف قرار گرفت.")
        actions = [[button("❌ لغو", f"cancel:{job_id}")]]
    else:
        if kind == "video":
            actions.append([button("🧪 نمونه ۱۰ ثانیه‌ای", f"preview:{job_id}")])
        actions.append([button("✅ شروع", f"go:{job_id}"), button("❌ لغو", f"cancel:{job_id}")])
        actions.append([button("📂 اعمال پریست", f"applypreset:{job_id}")])
        
    edit(job["chat"], job.get("message"), "\n".join(lines), keyboard(actions))


def estimate_output_size(info, settings):
    if not info: return 0
    base = {"h264": 2200, "hevc": 1400, "av1": 1000}.get(settings.get("codec", "h264"), 2000)
    if settings.get("quality") == "quality": base *= 1.5
    elif settings.get("quality") == "small": base *= 0.6
    elif settings.get("quality") == "target":
        return (settings.get("target_mb", 0) or 0) * 1_000_000
    h = settings.get("height") or info.get("height", 720)
    if info.get("height", 0) > 0:
        base *= max(0.3, (h / info["height"]) ** 1.4)
    audio = settings.get("audio", 0) or 0
    total = (base + audio) * info["duration"] / 8
    return int(total)


# ---------- Preview Generation ----------
def send_preview(job_id):
    job = job_by_id(job_id)
    if not job: return
    folder = WORK / job_id
    source = folder / "input"
    if not source.exists():
        safe_send(job["chat"], "فایل ورودی یافت نشد."); return
        
    s = json.loads(job["settings"])
    preview_path = folder / "preview.mp4"
    info = probe(source)
    
    # Calculate middle of video (subtract 5s to get 10s sample)
    mid = max(0, info["duration"] / 2 - 5)
    
    cmd = [
        "ffmpeg", "-hide_banner", "-y",
        "-ss", str(mid), "-t", "10",
        "-i", str(source),
        "-c:v", "libx264", "-preset", "ultrafast", "-crf", "28",
        "-c:a", "aac", "-b:a", "64k",
        str(preview_path)
    ]
    try:
        subprocess.run(cmd, capture_output=True, check=True)
        with preview_path.open("rb") as f:
            api("sendVideo", {"chat_id": job["chat"], "caption": "🧪 این یک نمونه ۱۰ ثانیه‌ای است. اگر کیفیت مناسب است، دکمه شروع را در پیام قبلی بزن."}, files={"video": ("preview.mp4", f, "video/mp4")})
    except Exception as e:
        safe_send(job["chat"], f"خطا در ساخت نمونه: {e}")
    finally:
        if preview_path.exists(): preview_path.unlink()


# ---------- Download ----------
def download(job, target):
    info = api("getFile", {"file_id": job["file_id"]})
    size_hint = int(info.get("file_size", 0))
    if size_hint > DOWNLOAD_LIMIT: raise UserError("فایل از سقف بزرگ‌تر است.")
    file_path = info.get("file_path")
    if not file_path: raise UserError("مسیر دانلود دریافت نشد.")
    execute("UPDATE jobs SET server_path=? WHERE id=?", (file_path, job["id"]))
    total = 0
    if USE_LOCAL_API:
        src = Path(file_path)
        if not src.exists(): raise UserError(f"فایل روی سرور نیست: {file_path}")
        file_total = src.stat().st_size
        last = 0.0; started = time.monotonic()
        try:
            with src.open("rb") as fsrc, target.open("wb") as fdst:
                while True:
                    check_job(job["id"])
                    chunk = fsrc.read(512 * 1024)
                    if not chunk: break
                    total += len(chunk)
                    if total > DOWNLOAD_LIMIT: raise UserError("فایل بزرگ‌تر از سقف.")
                    fdst.write(chunk)
                    now = time.monotonic()
                    if now - last >= 2.5 and file_total:
                        last = now
                        pct = total / file_total * 100
                        elapsed = now - started
                        eta = ((file_total - total) * elapsed / total) if total > 0 else 0
                        edit(job["chat"], job["message"],
                             f'📥 **دانلود کار** `{job["id"]}`\n'
                             f'📄 {job["name"]}\n'
                             f'💾 {human_size(total)} / {human_size(file_total)}\n\n'
                             f'{progress_bar(pct)}\n'
                             f'⏱ سپری: {human_time(elapsed)} | باقی: {human_time(eta)}',
                             keyboard([[button("❌ لغو", f"cancel:{job['id']}")]]))
        except OSError as exc:
            raise UserError(f"کپی ناموفق بود: {type(exc).__name__}") from None
    else:
        rel = file_path.lstrip("/")
        url = f"{API_BASE}/file/bot{TOKEN}/{rel}"
        try:
            with requests.get(url, stream=True, timeout=(30, 300)) as r:
                r.raise_for_status()
                file_total = int(r.headers.get("Content-Length", 0))
                last = 0.0; started = time.monotonic()
                with target.open("wb") as f:
                    for chunk in r.iter_content(512 * 1024):
                        check_job(job["id"])
                        if not chunk: continue
                        total += len(chunk)
                        if total > DOWNLOAD_LIMIT: raise UserError("بزرگ‌تر از سقف.")
                        f.write(chunk)
                        now = time.monotonic()
                        if now - last >= 2.5 and file_total:
                            last = now
                            pct = total / file_total * 100
                            elapsed = now - started
                            eta = ((file_total - total) * elapsed / total) if total > 0 else 0
                            edit(job["chat"], job["message"],
                                 f'📥 **دانلود** `{job["id"]}`\n{human_size(total)} / {human_size(file_total)}\n\n'
                                 f'{progress_bar(pct)}\n⏱ {human_time(elapsed)} | باقی: {human_time(eta)}',
                                 keyboard([[button("❌ لغو", f"cancel:{job['id']}")]]))
        except requests.RequestException as exc:
            LOG.warning(f"DL {job['id']}: {exc}")
            raise UserError(f"دانلود ناموفق ({type(exc).__name__})") from None
    if total == 0: raise UserError("فایل خالی است.")
    return total


# ---------- Probe / validate ----------
def probe(path):
    try:
        r = subprocess.run(["ffprobe", "-v", "error", "-show_format", "-show_streams", "-of", "json", str(path)],
                           capture_output=True, text=True, timeout=30)
    except (subprocess.TimeoutExpired, OSError):
        raise UserError("بررسی فایل ناموفق بود.") from None
    if r.returncode: raise UserError("فایل خراب یا فرمت نامعتبر.")
    try:
        data = json.loads(r.stdout)
        streams = data.get("streams", [])
        v = next(s for s in streams if s.get("codec_type") == "video" and not s.get("disposition", {}).get("attached_pic"))
        dur = float(v.get("duration") or data.get("format", {}).get("duration") or 0)
        w = int(v.get("width", 0)); h = int(v.get("height", 0))
        num, den = (v.get("avg_frame_rate", "0/1")).split("/")
        fps = float(num) / max(float(den), 1)
        rot = 0
        for side in v.get("side_data_list", []):
            if "rotation" in side: rot = int(float(side["rotation"]))
        if abs(rot) % 180 == 90: w, h = h, w
        a = next((s for s in streams if s.get("codec_type") == "audio"), None)
        sub = any(s.get("codec_type") == "subtitle" for s in streams)
        return {
            "duration": dur, "width": w, "height": h, "fps": fps,
            "codec": v.get("codec_name", "?"), "transfer": v.get("color_transfer", ""),
            "video_index": int(v["index"]),
            "audio_index": int(a["index"]) if a else None,
            "has_subs": sub,
        }
    except (ValueError, TypeError, KeyError, StopIteration, ZeroDivisionError):
        raise UserError("مشخصات ویدیو قابل تشخیص نیست.") from None


def validate_media(info, settings):
    import math
    if not math.isfinite(info["duration"]) or info["duration"] <= 0: raise UserError("مدت ویدیو معتبر نیست.")
    if info["duration"] > MAX_SECONDS: raise UserError(f"مدت بیشتر از {human_time(MAX_SECONDS)} است.")
    if info["width"] < 2 or info["height"] < 2: raise UserError("ابعاد نامعتبر.")
    if info["width"] * info["height"] > MAX_PIXELS: raise UserError("رزولوشن بیش از حد مجاز.")
    if info["transfer"] in ("smpte2084", "arib-std-b67") and not settings.get("hdr_to_sdr"):
        raise UserError("HDR رد شد (در تنظیمات می‌توانی تبدیل را فعال کنی).")


# ---------- Encode ----------
def build_command(source, target, settings, info):
    c = VIDEO_CODECS[settings["codec"]]
    quality = settings["quality"]
    target_height = min(info["height"], settings["height"] or info["height"])
    target_height = max(2, (target_height // 2) * 2)
    filters = [f"scale=-2:{target_height}:flags=lanczos"]
    if settings["fps"] and (info["fps"] <= 0 or info["fps"] > settings["fps"]):
        filters.append(f'fps={settings["fps"]}')
    hdr = info["transfer"] in ("smpte2084", "arib-std-b67")
    if hdr and settings.get("hdr_to_sdr"):
        if HAS_ZSCALE and HAS_TONEMAP:
            filters.append("zscale=t=linear:npl=100,format=gbrpf32le,zscale=p=bt709,tonemap=hable:desat=0,zscale=t=bt709:m=bt709:r=tv,format=yuv420p")
    cmd = ["ffmpeg", "-hide_banner", "-nostdin", "-y", "-loglevel", "error",
           "-progress", "pipe:1", "-stats_period", "1",
           "-filter_threads", str(THREADS), "-threads", str(THREADS),
           "-protocol_whitelist", "file,pipe"]
    if settings.get("preview_only"):
        mid = max(0, info["duration"] / 2 - 5)
        cmd += ["-ss", f"{mid:.2f}", "-t", "10"]
    cmd += ["-i", str(source), "-map", f'0:{info["video_index"]}']
    if settings["audio"] and info["audio_index"] is not None:
        cmd += ["-map", f'0:{info["audio_index"]}', "-c:a", "aac", "-b:a", f'{settings["audio"]}k', "-ac", "2"]
    else:
        cmd += ["-an"]
    if settings["keep_subs"] and info.get("has_subs"):
        cmd += ["-map", "0:s?", "-c:s", "mov_text"]
    else:
        cmd += ["-sn"]
    cmd += ["-dn", "-vf", ",".join(filters), "-c:v", c["encoder"], "-pix_fmt", "yuv420p", "-threads", str(THREADS)]
    if quality == "target" and settings.get("target_mb") and info["duration"] > 0:
        total_kbps = (settings["target_mb"] * 1_000_000 * 8) / info["duration"] / 1000
        video_kbps = int(total_kbps - (settings["audio"] or 0))
        if video_kbps < 100: raise UserError("حجم هدف برای این مدت خیلی کم است.")
        cmd += ["-b:v", f"{video_kbps}k", "-maxrate", f"{int(video_kbps*1.5)}k", "-bufsize", f"{int(video_kbps*2)}k"]
    else:
        cmd += ["-crf", str(c["crf"][quality])]
    if settings["codec"] == "h264": cmd += ["-preset", "medium"]
    elif settings["codec"] == "hevc":
        cmd += ["-preset", "medium", "-tag:v", "hvc1", "-x265-params", f"pools={THREADS}:frame-threads=1:log-level=error"]
    else:
        cmd += ["-preset", "8", "-svtav1-params", f"lp={THREADS}"]
    if settings["strip_metadata"]: cmd += ["-map_metadata", "-1"]
    cmd += ["-map_chapters", "-1", "-t", str(MAX_SECONDS), "-movflags", "+faststart", str(target)]
    return cmd


def stop_process(p):
    if p.poll() is not None: return
    try:
        p.terminate(); p.wait(timeout=5)
    except subprocess.TimeoutExpired:
        p.kill(); p.wait()
    except ProcessLookupError: pass


def encode(job, source, target, settings, info, folder):
    cmd = build_command(source, target, settings, info)
    progress = __import__("queue").Queue()
    started = time.monotonic()
    last_edit = 0.0
    media_seconds = 0.0
    speed = "—"
    effective_duration = 10 if settings.get("preview_only") else info["duration"]
    err_path = folder / "ffmpeg.log"
    with err_path.open("wb") as errors:
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=errors, text=True, bufsize=1)
        def reader():
            try:
                for line in p.stdout: progress.put(line.strip())
            finally: p.stdout.close()
        rt = threading.Thread(target=reader, daemon=True); rt.start()
        try:
            while p.poll() is None:
                check_job(job["id"])
                elapsed = time.monotonic() - started
                if elapsed > ENCODE_TIMEOUT: raise UserError("زمان مجاز تمام شد.")
                if target.exists() and target.stat().st_size > UPLOAD_LIMIT:
                    raise UserError("خروجی از سقف بزرگ‌تر است.")
                while True:
                    try: line = progress.get_nowait()
                    except __import__("queue").Empty: break
                    k, _, v = line.partition("=")
                    if k == "out_time_us":
                        try: media_seconds = max(media_seconds, int(v) / 1_000_000)
                        except ValueError: pass
                    elif k == "speed": speed = v
                if elapsed - last_edit >= 5:
                    last_edit = elapsed
                    pct = min(99.5, media_seconds / effective_duration * 100) if effective_duration else 0
                    eta = ((effective_duration - media_seconds) * elapsed / media_seconds) if media_seconds > 0 else 0
                    edit(job["chat"], job["message"],
                         f'🎬 **فشرده‌سازی** `{job["id"]}`\n📄 {job["name"]}\n\n'
                         f'{progress_bar(pct)}\n'
                         f'⚙️ سرعت: {speed}\n'
                         f'⏱ سپری: {human_time(elapsed)} | باقی: {human_time(eta)}',
                         keyboard([[button("❌ لغو", f"cancel:{job['id']}")]]))
                time.sleep(0.3)
            rt.join(timeout=2)
            check_job(job["id"])
            if p.returncode != 0:
                raise UserError("FFmpeg نتوانست پردازش کند. کدک H.264 را امتحان کن.")
            if not target.exists() or target.stat().st_size == 0: raise UserError("خروجی معتبر نبود.")
            if target.stat().st_size > UPLOAD_LIMIT: raise UserError("حجم خروجی از سقف بیشتر است.")
            return time.monotonic() - started
        finally:
            stop_process(p); rt.join(timeout=2)


# ---------- File compress ----------
def compress_file(job, source, target, settings, input_size, folder):
    codec = settings["file_codec"]
    level = FILE_CODECS[codec]["levels"][settings["file_level"]]
    started = time.monotonic()
    last_edit = 0.0
    processed = [0]
    def on_progress():
        nonlocal last_edit
        now = time.monotonic()
        if now - last_edit < 3: return
        last_edit = now
        elapsed = now - started
        pct = min(99.5, processed[0] / input_size * 100) if input_size else 0
        eta = ((input_size - processed[0]) * elapsed / processed[0]) if processed[0] > 0 else 0
        edit(job["chat"], job["message"],
             f'📦 **فشرده‌سازی** `{job["id"]}`\n📄 {job["name"]}\n\n'
             f'{progress_bar(pct)}\n'
             f'🗜 {FILE_CODECS[codec]["title"]} (level={level})\n'
             f'⏱ سپری: {human_time(elapsed)} | باقی: {human_time(eta)}',
             keyboard([[button("❌ لغو", f"cancel:{job['id']}")]]))
    if codec == "lzma2":
        with lzma.open(target, "wb", preset=level) as out, source.open("rb") as inp:
            while True:
                check_job(job["id"])
                chunk = inp.read(4 * 1024 * 1024)
                if not chunk: break
                out.write(chunk); processed[0] += len(chunk); on_progress()
    elif codec == "zstd":
        cctx = zstd.ZstdCompressor(level=level, threads=THREADS)
        with source.open("rb") as inp, target.open("wb") as out:
            with cctx.stream_writer(out, closefd=False) as w:
                while True:
                    check_job(job["id"])
                    chunk = inp.read(4 * 1024 * 1024)
                    if not chunk: break
                    w.write(chunk); processed[0] += len(chunk); on_progress()
    elif codec == "brotli":
        with source.open("rb") as inp, target.open("wb") as out:
            comp = brotli.Compressor(quality=level)
            while True:
                check_job(job["id"])
                chunk = inp.read(4 * 1024 * 1024)
                if not chunk: break
                out.write(comp.process(chunk)); processed[0] += len(chunk); on_progress()
            out.write(comp.finish())
    else:
        raise UserError("الگوریتم ناشناخته.")
    check_job(job["id"])
    if not target.exists() or target.stat().st_size == 0: raise UserError("خروجی معتبر نبود.")
    if target.stat().st_size > UPLOAD_LIMIT: raise UserError("خروجی از سقف بیشتر است.")
    return time.monotonic() - started


# ---------- Upload ----------
def upload_result(job, target, settings, info, caption, kind):
    total_size = target.stat().st_size
    def on_progress(cur, tot):
        pct = cur / tot * 100 if tot else 0
        edit(job["chat"], job["message"],
             f'📤 **در حال آپلود** `{job["id"]}`\n'
             f'📄 {job["name"]}\n\n'
             f'{progress_bar(pct)}\n'
             f'💾 {human_size(cur)} / {human_size(tot)}\n\n'
             "_در تلگرام، پیشرفت دقیق ممکن است کمی متفاوت باشد._")
    if kind == "video":
        filename = f'compressed_{settings["codec"]}_{job["id"]}.mp4'
        as_video = settings["delivery"] == "video" and settings["codec"] == "h264" and not settings.get("preview_only")
        method = "sendVideo" if as_video else "sendDocument"
        field = "video" if as_video else "document"
        data = {"chat_id": job["chat"], "caption": caption}
        if as_video:
            data.update({"duration": max(1, int(info["duration"])), "width": info["width"],
                         "height": info["height"], "supports_streaming": "true"})
        mime = "video/mp4"
    else:
        ext = FILE_CODECS[settings["file_codec"]]["ext"]
        filename = job["name"] + ext
        method = "sendDocument"; field = "document"
        data = {"chat_id": job["chat"], "caption": caption}
        mime = "application/octet-stream"
    with target.open("rb") as f:
        wrapped = UploadProgress(f, total_size, on_progress)
        api(method, data, files={field: (filename, wrapped, mime)}, read_timeout=1800)


# ---------- Process ----------
def process_job(job):
    jid = job["id"]; kind = job.get("kind", "video")
    folder = WORK / jid; folder.mkdir(parents=True, exist_ok=True)
    source = folder / "input"
    target = folder / ("output.mp4" if kind == "video" else "output.bin")
    started = time.monotonic()
    try:
        check_job(jid)
        if shutil.disk_usage(WORK).free < 512_000_000: raise UserError("فضای دیسک کم است.")
        if not job.get("message"):
            m = send(job["chat"], f'شروع کار `{jid}`...')
            execute("UPDATE jobs SET message=? WHERE id=?", (m["message_id"], jid))
            job["message"] = m["message_id"]
        if not source.exists() or source.stat().st_size == 0:
            edit(job["chat"], job["message"], f'📥 در حال دانلود `{jid}`...',
                 keyboard([[button("❌ لغو", f"cancel:{jid}")]]))
            input_size = download(job, source)
        else:
            input_size = source.stat().st_size
        check_job(jid)
        settings = json.loads(job["settings"])
        if kind == "video":
            info = probe(source)
            execute("UPDATE jobs SET media_info=? WHERE id=?", (json.dumps(info), jid))
            validate_media(info, settings)
            elapsed_enc = encode(job, source, target, settings, info, folder)
            out_info = probe(target)
            output_size = target.stat().st_size
            reduction = (1 - output_size / input_size) * 100 if input_size else 0
            ch = f"📉 کاهش: {reduction:.1f}%" if reduction >= 0 else f"⚠️ افزایش: {abs(reduction):.1f}%"
            caption = (f'✅ **نتیجه** `{jid}`\n📄 {job["name"]}\n\n'
                       f'📥 ورودی: {human_size(input_size)}\n📤 خروجی: {human_size(output_size)}\n{ch}\n'
                       f'📊 نسبت: {input_size/output_size:.2f}×\n'
                       f'🎛 کدک: {VIDEO_CODECS[settings["codec"]]["title"]}\n'
                       f'📐 {out_info["width"]}×{out_info["height"]} | ⏱ {human_time(out_info["duration"])}\n'
                       f'⚙️ زمان انکود: {human_time(elapsed_enc)}')
        else:
            edit(job["chat"], job["message"],
                 f'📦 فشرده‌سازی `{jid}`\n{job["name"]}\n\n{file_summary(settings)}\n\nدر حال انجام...',
                 keyboard([[button("❌ لغو", f"cancel:{jid}")]]))
            elapsed_enc = compress_file(job, source, target, settings, input_size, folder)
            output_size = target.stat().st_size
            reduction = (1 - output_size / input_size) * 100 if input_size else 0
            ch = f"📉 کاهش: {reduction:.1f}%" if reduction >= 0 else f"⚠️ افزایش: {abs(reduction):.1f}%"
            caption = (f'✅ **نتیجه** `{jid}`\n📄 {job["name"]}\n\n'
                       f'📥 ورودی: {human_size(input_size)}\n📤 خروجی: {human_size(output_size)}\n{ch}\n'
                       f'📊 نسبت: {input_size/output_size:.2f}×\n'
                       f'🗜 {FILE_CODECS[settings["file_codec"]]["title"]}\n'
                       f'⚙️ زمان: {human_time(elapsed_enc)}')
            out_info = None
        check_job(jid)
        upload_result(job, target, settings, out_info, caption, kind)
        execute("UPDATE jobs SET status='done', output_size=?, finished_at=? WHERE id=?",
                (output_size, time.time(), jid))
        # auto-delete source from Local API storage
        if USE_LOCAL_API and job.get("server_path"):
            try:
                sp = Path(job["server_path"])
                if sp.exists() and sp.is_file(): sp.unlink()
                execute("UPDATE jobs SET server_path=NULL WHERE id=?", (jid,))
            except Exception as e:
                LOG.warning(f"Auto-delete failed for {jid}: {e}")
        edit(job["chat"], job["message"],
             f'✅ کار `{jid}` با موفقیت ارسال شد.\n'
             f'⏱ زمان کل: {human_time(time.monotonic() - started)}\n\n'
             "🗑 نسخه اصلی از سرور حذف شد (صرفه‌جویی دیسک).",
             keyboard([]))
    except Cancelled:
        execute("UPDATE jobs SET status='cancelled' WHERE id=?", (jid,))
        edit(job["chat"], job["message"], f'⛔ کار `{jid}` لغو شد.')
    except Requeue:
        execute("UPDATE jobs SET status='queued' WHERE id=? AND status='running'", (jid,))
    except Exception as exc:
        if STOP.is_set():
            execute("UPDATE jobs SET status='queued' WHERE id=? AND status='running'", (jid,))
        else:
            cur = job_by_id(jid)
            if cur and cur["status"] == "cancelled": return
            reason = str(exc) if isinstance(exc, UserError) else "خطای داخلی."
            execute("UPDATE jobs SET status='failed', error_msg=? WHERE id=?", (reason[:200], jid))
            LOG.warning(f"Job {jid} failed ({type(exc).__name__}): {reason}")
            safe_send(job["chat"], f'❌ کار `{jid}` ناموفق بود.\n{reason}')
            edit(job["chat"], job["message"], f'❌ کار `{jid}` ناموفق بود.')
    finally:
        shutil.rmtree(folder, ignore_errors=True)


def worker():
    while not STOP.is_set():
        with DB_LOCK:
            job = one("SELECT * FROM jobs WHERE status='queued' ORDER BY created LIMIT 1")
            if job: execute("UPDATE jobs SET status='running' WHERE id=?", (job["id"],))
        if not job:
            STOP.wait(1); continue
        process_job(job)


# ---------- Callbacks ----------
def handle_callback(cb):
    msg = cb.get("message", {})
    if msg.get("chat", {}).get("type") != "private":
        answer(cb["id"], "فقط گفت‌وگوی خصوصی."); return
    uid = cb["from"]["id"]; chat = msg["chat"]["id"]; mid = msg["message_id"]
    if not authorized(uid):
        answer(cb["id"], "دسترسی ندارید."); return
    data = cb.get("data", ""); parts = data.split(":")

    if data == "menu:home":
        answer(cb["id"]); home(chat, mid)
    elif data == "menu:settings":
        answer(cb["id"]); settings_menu(chat, uid, mid)
    elif data == "menu:file_settings":
        answer(cb["id"]); file_settings_menu(chat, uid, mid)
    elif data == "menu:queue":
        answer(cb["id"]); show_queue(chat, uid)
    elif data == "menu:help":
        answer(cb["id"]); edit(chat, mid, help_text(), keyboard([[button("🔙 بازگشت", "menu:home")]]))
    elif data == "menu:stats":
        answer(cb["id"]); show_stats(chat, mid)
    elif data == "menu:presets":
        answer(cb["id"]); show_presets(chat, uid, mid)
    elif data == "menu:builtin":
        answer(cb["id"]); builtin_presets_menu(chat, mid)
    elif parts[0] == "preview" and len(parts) == 2:
        answer(cb["id"], "در حال ساخت نمونه...")
        threading.Thread(target=send_preview, args=(parts[1],), daemon=True).start()
    elif parts[0] == "reset" and len(parts) == 2:
        answer(cb["id"]); s = get_settings(uid)
        if parts[1] == "video":
            for k in ("codec", "quality", "height", "fps", "audio", "delivery", "strip_metadata", "keep_subs", "hdr_to_sdr", "preview_only", "target_mb"):
                s[k] = copy.deepcopy(DEFAULTS[k])
            save_settings(uid, s); settings_menu(chat, uid, mid)
        else:
            s["file_codec"] = DEFAULTS["file_codec"]; s["file_level"] = DEFAULTS["file_level"]
            save_settings(uid, s); file_settings_menu(chat, uid, mid)
    elif parts[0] == "pick" and len(parts) == 2 and parts[1] in CHOICES:
        answer(cb["id"]); key = parts[1]
        s = get_settings(uid); cur = s.get(key)
        rows = []
        for i, (label, val) in enumerate(CHOICES[key]):
            mark = "✅ " if val == cur else ""
            rows.append([button(mark + label, f"set:{key}:{i}")])
        rows.append([button("ℹ️ توضیحات", f"info:{key}")])
        back = "menu:file_settings" if key in ("file_codec", "file_level") else "menu:settings"
        rows.append([button("❌ انصراف", back)])
        title = KEY_TITLES.get(key, key)
        edit(chat, mid, f'{title} را انتخاب کن:', keyboard(rows))
    elif data.startswith("info:") and len(parts) == 2:
        answer(cb["id"], INFO_TEXTS.get(parts[1], ""), show_alert=True)
    elif parts[0] == "set" and len(parts) == 3 and parts[1] in CHOICES:
        answer(cb["id"]); key = parts[1]
        try:
            i = int(parts[2]); val = CHOICES[key][i][1]
        except (ValueError, IndexError): return
        s = get_settings(uid)
        if key == "quality" and val == "target":
            SESSION[uid] = {"action": "target_mb", "msg_id": mid}
            edit(chat, mid, "🎯 حجم هدف را به مگابایت بفرست (مثلاً 50):",
                 keyboard([[button("❌ انصراف", "cancel_input")]]))
            return
        s[key] = val; save_settings(uid, s)
        if key in ("file_codec", "file_level"): file_settings_menu(chat, uid, mid)
        else: settings_menu(chat, uid, mid)
    elif parts[0] == "back" and len(parts) == 2:
        answer(cb["id"]); _render_ready(parts[1])
    elif parts[0] == "applypreset" and len(parts) == 2:
        job = job_by_id(parts[1])
        if not job or job["uid"] != uid: answer(cb["id"]); return
        answer(cb["id"])
        rows = []
        for k, p in BUILTIN_PRESETS.items():
            rows.append([button(p["title"], f"applyp:{parts[1]}:{k}")])
        for pr in list_presets(uid, "video"):
            rows.append([button(f'📂 {pr["name"]}', f"applyp:{parts[1]}:{pr['id']}")])
        rows.append([button("❌ انصراف", f"back:{parts[1]}")])
        edit(chat, mid, "پریست را انتخاب کن:", keyboard(rows))
    elif parts[0] == "applyp" and len(parts) == 3:
        job = job_by_id(parts[1])
        if not job or job["uid"] != uid: answer(cb["id"]); return
        target = parts[2]
        if target in BUILTIN_PRESETS:
            subset = BUILTIN_PRESETS[target]["settings"]
        else:
            pr = get_preset(uid, int(target))
            if not pr: answer(cb["id"], "یافت نشد."); return
            subset = json.loads(pr["settings"])
        s = json.loads(job["settings"]); s.update(subset)
        execute("UPDATE jobs SET settings=? WHERE id=?", (json.dumps(s), job["id"]))
        answer(cb["id"], "پریست اعمال شد."); _render_ready(job["id"])
    elif parts[0] == "preset":
        if parts[1] == "save" and len(parts) == 3 and parts[2] in ("video", "file"):
            answer(cb["id"])
            SESSION[uid] = {"action": "preset_name", "kind": parts[2], "msg_id": mid}
            edit(chat, mid, f"نام پریست {'ویدیو' if parts[2]=='video' else 'فایل'} را بفرست (حداکثر ۳۲ نویسه):",
                 keyboard([[button("❌ انصراف", "cancel_input")]]))
        elif parts[1] == "load" and len(parts) == 3:
            pr = get_preset(uid, int(parts[2]))
            if not pr: answer(cb["id"], "یافت نشد."); return
            s = get_settings(uid); s.update(json.loads(pr["settings"])); save_settings(uid, s)
            answer(cb["id"], f'پریست «{pr["name"]}» اعمال شد.')
            if pr["kind"] == "video": settings_menu(chat, uid, mid)
            else: file_settings_menu(chat, uid, mid)
        elif parts[1] == "del" and len(parts) == 3:
            pr = get_preset(uid, int(parts[2]))
            if not pr: answer(cb["id"]); return
            delete_preset(uid, int(parts[2]))
            answer(cb["id"], "حذف شد."); show_presets(chat, uid, mid)
    elif parts[0] == "builtin" and len(parts) == 2:
        p = BUILTIN_PRESETS.get(parts[1])
        if not p: answer(cb["id"]); return
        s = get_settings(uid); s.update(p["settings"]); save_settings(uid, s)
        answer(cb["id"], f'پریست «{p["title"]}» اعمال شد.')
        home(chat, mid)
    elif parts[0] == "cancel_input":
        answer(cb["id"]); SESSION.pop(uid, None)
        if len(parts) > 1:
            if parts[1] == "video": settings_menu(chat, uid, mid)
            else: file_settings_menu(chat, uid, mid)
        else: home(chat, mid)
    elif parts[0] in ("go", "cancel") and len(parts) == 2:
        job = job_by_id(parts[1])
        if not job or job["uid"] != uid: answer(cb["id"], "یافت نشد."); return
        if parts[0] == "go":
            changed = execute("UPDATE jobs SET status='queued' WHERE id=? AND status='awaiting'", (job["id"],))
            answer(cb["id"], "در صف قرار گرفت." if changed else "امکان‌پذیر نیست.")
            if changed: edit(chat, mid, f'✅ کار `{job["id"]}` در صف قرار گرفت.', keyboard([[button("❌ لغو", f"cancel:{job['id']}")]]))
        else:
            changed = execute("UPDATE jobs SET status='cancelled' WHERE id=? AND status IN ('downloading','awaiting','queued','running')", (job["id"],))
            answer(cb["id"], "لغو شد." if changed else "امکان‌پذیر نیست.")
            if changed: edit(chat, mid, f'⛔ کار `{job["id"]}` لغو شد.')
    else:
        answer(cb["id"])


# ---------- Message ----------
def handle_message(message):
    if message.get("chat", {}).get("type") != "private": return
    uid = message.get("from", {}).get("id")
    if not uid: return
    chat = message["chat"]["id"]
    text = message.get("text", "").strip()
    command = text.split()[0].split("@")[0] if text.startswith("/") else ""

    if not authorized(uid, claim=command == "/start"):
        send(chat, "این ربات خصوصی است."); return

    # Session input
    if uid in SESSION and text and not text.startswith("/"):
        sess = SESSION.pop(uid); mid = sess.get("msg_id")
        if sess["action"] == "preset_name":
            kind = sess["kind"]; name = text[:32] or "بدون‌نام"
            save_preset(uid, name, kind, get_settings(uid))
            send(chat, f'✅ پریست «{name}» ذخیره شد.')
            if kind == "video": settings_menu(chat, uid)
            else: file_settings_menu(chat, uid)
        elif sess["action"] == "target_mb":
            try: mbv = float(text.replace(",", "").replace("MB", "").replace("mb", ""))
            except ValueError:
                send(chat, "عدد معتبر بفرست (مثلاً 50)."); return
            if mbv <= 0: send(chat, "باید مثبت باشد."); return
            s = get_settings(uid); s["quality"] = "target"; s["target_mb"] = mbv
            save_settings(uid, s)
            send(chat, f'✅ هدف: {mbv} MB')
            settings_menu(chat, uid)
        return

    # Collect mode
    if uid in COLLECT and (message.get("document") or message.get("video")):
        handle_collect_file(message, uid, chat); return

    if command == "/start":
        home(chat)
    elif command == "/settings": settings_menu(chat, uid)
    elif command == "/filesettings": file_settings_menu(chat, uid)
    elif command in ("/queue", "/status"): show_queue(chat, uid)
    elif command == "/stats": show_stats(chat)
    elif command == "/presets": show_presets(chat, uid)
    elif command == "/help": send(chat, help_text())
    elif command == "/privacy":
        send(chat, "📜 **سیاست داده‌ها**\n\n"
                   "• فایل‌های اصلی پس از ارسال موفق، به‌طور خودکار از سرور حذف می‌شوند.\n"
                   "• فایل‌های موقت کاری پس از پایان پردازش پاک می‌شوند.\n"
                   "• در دیتابیس: شناسه تلگرام، تنظیمات، نام فایل، وضعیت کارها.\n"
                   "• حذف محلی ≠ حذف نسخه در سرور تلگرام.")
    elif command == "/cancel":
        cnt = execute("UPDATE jobs SET status='cancelled' WHERE uid=? AND status IN ('downloading','awaiting','queued','running')", (uid,))
        send(chat, f"✅ لغو {cnt} کار ثبت شد.")
    elif message.get("video"):
        receive_video(message, uid, chat)
    elif message.get("document"):
        media = message["document"]; mime = media.get("mime_type", "")
        name = (media.get("file_name") or "").lower()
        exts = (".mp4", ".mkv", ".mov", ".webm", ".avi", ".m4v", ".ts")
        if mime.startswith("video/") or name.endswith(exts): receive_video(message, uid, chat)
        else: receive_file(message, uid, chat)
    elif message.get("audio"):
        send(chat, "🎵 این یک فایل صوتی است. لطفاً ویدیو یا فایل بفرست.\nبرای فایل صوتی، ابتدا آن را به‌عنوان Document بفرست.")
    elif message.get("voice"):
        send(chat, "🎙 پیام صوتی قابل پردازش نیست. ابتدا آن را به‌عنوان فایل (Document) بفرست.")
    elif message.get("video_note"):
        send(chat, "⭕ پیام ویدیویی گرد قابل پردازش نیست. از فایل معمولی استفاده کن.")
    elif message.get("animation"):
        send(chat, "🎞 گیف قابل پردازش نیست. به‌عنوان فایل ویدیویی بفرست.")
    elif message.get("photo"):
        send(chat, "🖼 عکس پذیرفته نمی‌شود. این ربات روی ویدیو و فایل کار می‌کند.")
    elif message.get("sticker"):
        send(chat, "🙂 استیکر قابل پردازش نیست.")
    else:
        home(chat)


def handle_update(u):
    if "callback_query" in u: handle_callback(u["callback_query"])
    elif "message" in u: handle_message(u["message"])


# ---------- Startup ----------
def check_job(jid):
    if STOP.is_set(): raise Requeue()
    r = job_by_id(jid)
    if not r or r["status"] == "cancelled": raise Cancelled()


def start_health_server():
    class H(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path not in ("/", "/health"): self.send_error(404); return
            c = b"stopping" if STOP.is_set() else b"ok"
            self.send_response(503 if STOP.is_set() else 200)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", str(len(c)))
            self.end_headers(); self.wfile.write(c)
        def log_message(self, *a): pass
    port = int(os.environ.get("PORT", "8080"))
    s = ThreadingHTTPServer(("0.0.0.0", port), H)
    threading.Thread(target=s.serve_forever, daemon=True).start()
    return s


def check_encoders():
    if not shutil.which("ffmpeg") or not shutil.which("ffprobe"):
        raise SystemExit("ffmpeg/ffprobe required.")
    r = subprocess.run(["ffmpeg", "-hide_banner", "-encoders"], capture_output=True, text=True, timeout=20)
    missing = [i["encoder"] for i in VIDEO_CODECS.values() if not re.search(rf'\b{re.escape(i["encoder"])}\b', r.stdout)]
    if missing: raise SystemExit("Missing encoders: " + ", ".join(missing))


def on_signal(signum, frame): STOP.set()


def main():
    signal.signal(signal.SIGTERM, on_signal)
    signal.signal(signal.SIGINT, on_signal)
    check_encoders()
    import fcntl
    lf = (DATA / "instance.lock").open("a")
    try: fcntl.flock(lf, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError: raise SystemExit("Another instance running.")
    for p in WORK.iterdir():
        if p.is_dir(): shutil.rmtree(p, ignore_errors=True)
        else:
            try: p.unlink()
            except Exception: pass
    execute("UPDATE jobs SET status='queued' WHERE status='running'")
    execute("UPDATE jobs SET status='queued' WHERE status='downloading'")
    identity = api("getMe")
    LOG.info(f"Starting @{identity.get('username', 'bot')} (local_api={USE_LOCAL_API})")
    api("deleteWebhook", {"drop_pending_updates": "false"})
    try:
        api("setMyCommands", {"commands": [
            {"command": "start", "description": "منوی اصلی"},
            {"command": "settings", "description": "تنظیمات ویدیو"},
            {"command": "filesettings", "description": "تنظیمات فایل"},
            {"command": "queue", "description": "صف کارها"},
            {"command": "stats", "description": "آمار ربات"},
            {"command": "presets", "description": "پریست‌ها"},
            {"command": "cancel", "description": "لغو همه کارها"},
            {"command": "help", "description": "راهنما"},
            {"command": "privacy", "description": "سیاست داده"},
        ]})
    except UserError: LOG.warning("setMyCommands failed.")
    health = start_health_server()
    th = threading.Thread(target=worker, daemon=True); th.start()
    offset = int(get_kv("update_offset", "0"))
    try:
        while not STOP.is_set():
            try:
                updates = api("getUpdates", {"offset": offset, "timeout": 25, "allowed_updates": ["message", "callback_query"]}, read_timeout=40)
                for u in updates:
                    if STOP.is_set(): break
                    try: handle_update(u)
                    except Exception as exc: LOG.warning(f"Update failed ({type(exc).__name__})")
                    finally:
                        offset = u["update_id"] + 1
                        put_kv("update_offset", offset)
            except Requeue: break
            except Exception as exc:
                LOG.warning(f"Polling failed ({type(exc).__name__}); retrying.")
                STOP.wait(3)
    finally:
        STOP.set(); th.join(timeout=15); health.shutdown()


if __name__ == "__main__":
    main()
