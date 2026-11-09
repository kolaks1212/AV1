import copy
import json
import logging
import lzma
import os
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
from urllib.parse import urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

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
BATCH_MSG = {}          # uid -> {"chat","message_id","job_ids":[],"created","last","last_render"}
DOWNLOAD_PROGRESS = {}  # jid -> {"bytes","total","speed"}
ENCODE_PROGRESS = {}    # jid -> {"seconds","duration","speed"}

VIDEO_URL_EXTS = (".mp4", ".mkv", ".mov", ".webm", ".avi", ".m4v", ".ts",
                  ".flv", ".wmv", ".m3u8", ".mpeg", ".mpg", ".3gp", ".ogv")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOG = logging.getLogger("compressor")


# ---------- HTTP session for URL downloads ----------
def _build_session():
    s = requests.Session()
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504],
                    allowed_methods=["GET"])
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.mount("http://", HTTPAdapter(max_retries=retries))
    s.headers.update({
        "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/124.0.0.0 Safari/537.36"),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": "https://www.google.com/",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "cross-site",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
        "Connection": "keep-alive",
    })
    return s

URL_SESSION = _build_session()


# ---------- Codecs ----------
VIDEO_CODECS = {
    "h264": {"encoder": "libx264", "title": "H.264", "crf": {"quality": 21, "balanced": 25, "small": 29}},
    "hevc": {"encoder": "libx265", "title": "H.265 / HEVC", "crf": {"quality": 24, "balanced": 28, "small": 32}},
    "av1":  {"encoder": "libsvtav1", "title": "AV1", "crf": {"quality": 28, "balanced": 33, "small": 38}},
}

FILE_CODECS = {
    "zstd":  {"title": "Zstandard", "ext": ".zst",
              "levels": {"fast": 1, "quick": 3, "balanced": 6, "high": 12, "max": 19, "ultra": 22}},
    "brotli": {"title": "Brotli", "ext": ".br",
               "levels": {"fast": 1, "quick": 3, "balanced": 6, "high": 9, "max": 11}},
    "lzma2": {"title": "LZMA2 (XZ)", "ext": ".xz",
              "levels": {"fast": 1, "quick": 3, "balanced": 6, "high": 8, "max": 9,
                         "ultra": 9 | lzma.PRESET_EXTREME}},
}

QUALITY_LABELS = {"quality": "کیفیت بیشتر", "balanced": "متعادل", "small": "حجم کمتر", "target": "حجم هدف"}
LEVEL_LABELS = {"fast": "سریع", "quick": "عادی", "balanced": "متعادل", "high": "بالا", "max": "حداکثر", "ultra": "اولترا"}

KEY_TITLES = {
    "codec": "🎛 کدک", "quality": "⚡ کیفیت", "height": "📐 رزولوشن",
    "fps": "🎞 نرخ فریم", "audio": "🔊 صدا", "delivery": "📤 نوع ارسال",
    "auto": "🤖 حالت صف", "strip_metadata": "🧹 متادیتا",
    "keep_subs": "💬 زیرنویس", "hdr_to_sdr": "🌈 تبدیل HDR",
    "file_codec": "🗜 الگوریتم", "file_level": "🎚 سطح فشرده‌سازی", "file_threads": "⚙️ تعداد رشته‌ها",
}

DEFAULTS = {
    "codec": "h264", "quality": "balanced", "target_mb": 0, "height": 720,
    "fps": 0, "audio": 96, "delivery": "document", "auto": False,
    "strip_metadata": True, "keep_subs": False, "hdr_to_sdr": True,
    "preview_only": False, "file_codec": "zstd", "file_level": "balanced",
    "file_threads": 0,
}

CHOICES = {
    "codec":  [("H.264 — سازگارترین", "h264"), ("H.265 — حجم کمتر", "hevc"), ("AV1 — کم‌ترین حجم", "av1")],
    "quality": [("کیفیت بیشتر", "quality"), ("متعادل", "balanced"), ("حجم کمتر", "small"), ("حجم هدف دلخواه", "target")],
    "height": [("اصلی", 0), ("1080p", 1080), ("720p", 720), ("480p", 480)],
    "fps":    [("اصلی", 0), ("30 فریم", 30), ("24 فریم", 24)],
    "audio":  [("128 kbps", 128), ("96 kbps", 96), ("64 kbps", 64), ("بی‌صدا", 0)],
    "delivery": [("📎 فایل", "document"), ("▶️ ویدیوی قابل پخش", "video")],
    "auto":   [("✋ تأیید دستی", False), ("🤖 صف خودکار", True)],
    "strip_metadata": [("حذف", True), ("حفظ", False)],
    "keep_subs": [("حفظ زیرنویس", True), ("حذف زیرنویس", False)],
    "hdr_to_sdr": [("تبدیل HDR به SDR", True), ("رد کردن HDR", False)],
    "file_codec": [("Zstandard", "zstd"), ("Brotli", "brotli"), ("LZMA2 (XZ)", "lzma2")],
    "file_level": [("سریع", "fast"), ("عادی", "quick"), ("متعادل", "balanced"),
                   ("بالا", "high"), ("حداکثر", "max"), ("اولترا", "ultra")],
    "file_threads": [("خودکار", 0), ("1", 1), ("2", 2), ("4", 4), ("8", 8)],
}

INFO_TEXTS = {
    "codec": (
        "<b>کدک ویدیو</b>\n\n"
        "• <b>H.264</b>: پخش تقریباً روی همه‌ی دستگاه‌ها و مرورگرها. امن‌ترین انتخاب.\n"
        "• <b>H.265 / HEVC</b>: حدود ۳۰ تا ۵۰ درصد کم‌حجم‌تر با همان کیفیت، اما برخی دستگاه‌های قدیمی پخش نمی‌کنند.\n"
        "• <b>AV1</b>: جدیدترین کدک، کم‌ترین حجم، اما پردازش کندتر و نیاز به پخش‌کننده‌ی سازگار.\n\n"
        "اگر مطمئن نیستید، H.264 انتخاب کنید."
    ),
    "quality": (
        "<b>کیفیت خروجی</b>\n\n"
        "• <b>کیفیت بیشتر</b>: جزئیات بالا، حجم بیشتر. مناسب آرشیو.\n"
        "• <b>متعادل</b>: تعادل بین حجم و کیفیت. مناسب اکثر کاربردها.\n"
        "• <b>حجم کمتر</b>: افت جزئیات در ازای حجم کم. مناسب ارسال سریع.\n"
        "• <b>حجم هدف</b>: خودتان حجم نهایی را تعیین می‌کنید (ربات بیت‌ریت را طوری تنظیم می‌کند تا به آن حجم برسد)."
    ),
    "height": ("<b>رزولوشن</b>\n\nسقف ارتفاع خروجی. اگر ویدیوی اصلی کوتاه‌تر باشد، تغییری نمی‌کند.\n"
               "مثلاً ۷۲۰p برای اکثر گوشی‌ها کافی است و حجم را کاهش می‌دهد."),
    "fps":    ("<b>نرخ فریم (FPS)</b>\n\nسقف نرخ فریم. ویدیوی کم‌فریم هرگز به‌طور مصنوعی بالا نمی‌رود.\n"
               "۳۰ یا ۲۴ برای اکثر محتواها کافی است."),
    "audio":  ("<b>صدای خروجی</b>\n\nبیت‌ریت صدا. ۹۶ یا ۱۲۸ برای اکثر ویدیوها کافی است.\n"
               "بی‌صدا: صدا کاملاً حذف می‌شود و حجم کم می‌شود."),
    "delivery": ("<b>نوع ارسال</b>\n\n"
                 "• <b>فایل</b>: بدون فشرده‌سازی مجدد تلگرام. بهترین کیفیت.\n"
                 "• <b>ویدیوی قابل پخش</b>: فقط برای H.264. پیش‌نمایش در چت دارد اما تلگرام ممکن است دوباره فشرده کند."),
    "auto":   ("<b>حالت صف</b>\n\n"
               "• <b>صف خودکار</b>: فایل بلافاصله پس از دانلود وارد صف پردازش می‌شود.\n"
               "• <b>تأیید دستی</b>: ابتدا مشخصات و نمونه را می‌بینید، سپس تأیید می‌کنید."),
    "strip_metadata": ("<b>متادیتا</b>\n\nحذف اطلاعات مکانی (GPS)، مدل دوربین، تاریخ و نرم‌افزار.\n"
                       "برای حریم خصوصی توصیه می‌شود."),
    "keep_subs": ("<b>زیرنویس داخلی</b>\n\nاگر فایل ورودی زیرنویس داخلی داشته باشد، در خروجی نگه می‌دارد.\n"
                  "زیرنویس سخت همیشه بخشی از تصویر است."),
    "hdr_to_sdr": ("<b>HDR به SDR</b>\n\nویدیوهای HDR برای نمایشگرهای معمولی به SDR تبدیل می‌شوند.\n"
                   "اگر رد کنید، ویدیوهای HDR اصلاً پردازش نمی‌شوند."),
    "file_codec": ("<b>الگوریتم فشرده‌سازی</b>\n\n"
                   "• <b>Zstandard</b>: بسیار سریع، نسبت خوب.\n"
                   "• <b>Brotli</b>: نسبت بهتر برای متن. کمی کندتر.\n"
                   "• <b>LZMA2 (XZ)</b>: بالاترین نسبت، اما کندترین.\n\n"
                   "<i>راهنمای باز کردن فایل‌ها در بخش «راهنما» موجود است.</i>"),
    "file_level": ("<b>سطح فشرده‌سازی</b>\n\nهرچه سطح بالاتر، حجم کم‌تر اما زمان بیشتر.\n"
                   "• سریع: موقت.\n• متعادل: پیش‌فرض.\n• اولترا: فقط LZMA2."),
    "file_threads": ("<b>تعداد رشته‌ها</b>\n\nبرای Zstd: تردهای موازی.\n"
                     "خودکار = بر اساس هسته‌های سرور.\nمقدار بالاتر = سرعت بیشتر، مصرف CPU بالاتر."),
}


class UserError(Exception): pass
class Cancelled(Exception): pass
class Requeue(Exception): pass


# ---------- Utils ----------
def escape_html(text):
    if text is None: return ""
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def human_size(size):
    if size is None: return "—"
    if size <= 0: return "0"
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

def parse_time_part(s):
    s = (s or "").strip()
    if not s: raise ValueError("مقدار زمان خالی است")
    if ":" in s:
        parts = s.split(":")
        try:
            if len(parts) == 2: return float(parts[0]) * 60 + float(parts[1])
            if len(parts) == 3: return float(parts[0]) * 3600 + float(parts[1]) * 60 + float(parts[2])
        except ValueError: raise ValueError("عدد نامعتبر") from None
        raise ValueError("فرمت نامعتبر")
    try: v = float(s)
    except ValueError: raise ValueError(f"زمان نامعتبر: {s}") from None
    if v < 0: raise ValueError("زمان منفی")
    return v

def parse_clip_range(text):
    text = (text or "").strip()
    text = re.sub(r"\b(ثانیه|ثانیه‌ای|دقیقه|دقیقه‌ای|ساعت)\b", "", text).strip()
    if "-" in text and not text.startswith("-"):
        left, _, right = text.partition("-")
        if left.strip() and right.strip():
            start = parse_time_part(left); end = parse_time_part(right)
            if end <= start: raise ValueError("پایان باید بعد از شروع باشد")
            return start, end - start
    parts = text.split()
    if len(parts) != 2: raise ValueError("فرمت: «1:30 2:00» یا «1:30-3:30» یا «90 120»")
    start = parse_time_part(parts[0]); dur = parse_time_part(parts[1])
    if dur <= 0: raise ValueError("مدت باید مثبت باشد")
    return start, dur

def is_url(text):
    if not text: return False
    text = text.strip()
    if not (text.startswith("http://") or text.startswith("https://")): return False
    return not bool(re.search(r"\s", text))

def url_to_name(url):
    try:
        path = urlparse(url).path
        name = Path(path).name
        if name and len(name) >= 2: return clean_name(name)
    except Exception: pass
    return "download"

def detect_url_kind(url):
    try:
        path = urlparse(url).path.lower()
        if path.endswith(VIDEO_URL_EXTS): return "video"
    except Exception: pass
    return "file"


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
        r = subprocess.run(["ffmpeg", "-hide_banner", "-filters"],
                           capture_output=True, text=True, timeout=10)
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
""")
DB.execute("DROP TABLE IF EXISTS presets")

cols = [r[1] for r in DB.execute("PRAGMA table_info(jobs)").fetchall()]
for c, d in [
    ("kind", "TEXT NOT NULL DEFAULT 'video'"),
    ("server_path", "TEXT DEFAULT NULL"),
    ("output_size", "INTEGER DEFAULT NULL"),
    ("finished_at", "REAL DEFAULT NULL"),
    ("media_info", "TEXT DEFAULT NULL"),
    ("error_msg", "TEXT DEFAULT NULL"),
    ("source_url", "TEXT DEFAULT NULL"),
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

def authorized(uid, claim=False):
    return True

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
            r = requests.post(f"{API_BASE}/bot{TOKEN}/{method}", data=payload, files=files,
                              timeout=(10, read_timeout))
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

def send(chat, text, markup=None):
    return api("sendMessage", {"chat_id": chat, "text": text,
                               "parse_mode": "HTML", "reply_markup": markup})

def safe_send(chat, text, markup=None):
    try: return send(chat, text, markup)
    except Exception:
        LOG.warning("Could not send notification."); return None

def edit(chat, mid, text, markup=None):
    if not mid: return
    try:
        api("editMessageText", {"chat_id": chat, "message_id": mid, "text": text,
                                "parse_mode": "HTML", "reply_markup": markup or keyboard([])},
            read_timeout=15)
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
        f'🎛 کدک: <b>{c["title"]}</b> | ⚡ کیفیت: <b>{q}</b>\n'
        f'📐 {h} | 🎞 {settings["fps"] or "اصلی"} FPS | 🔊 {a}\n'
        f'💬 زیرنویس: {"حفظ" if settings["keep_subs"] else "حذف"} | '
        f'🌈 HDR: {"SDR" if settings["hdr_to_sdr"] else "رد"}\n'
        f'🤖 صف: {"خودکار" if settings["auto"] else "تأیید دستی"}'
    )


def file_summary(settings):
    c = FILE_CODECS[settings["file_codec"]]
    lv = c["levels"].get(settings["file_level"], c["levels"]["max"])
    return (
        f'🗜 الگوریتم: <b>{c["title"]}</b>\n'
        f'🎚 سطح: <b>{LEVEL_LABELS[settings["file_level"]]}</b> (level={lv})\n'
        f'⚙️ رشته‌ها: {"خودکار (" + str(THREADS) + ")" if not settings.get("file_threads") else settings["file_threads"]}'
    )


def home(chat, message_id=None):
    text = (
        "🎬 <b>فشرده‌ساز چندمنظوره</b>\n\n"
        "ویدیو، فایل یا <b>لینک دانلود مستقیم</b> بفرستید.\n"
        "می‌توانید چند فایل پشت‌سرهم بفرستید — همه در یک پیام گروه می‌شوند.\n\n"
        "━━━━━━━━━━━━━━━\n"
        f'📥 سقف حجم: <b>{human_size(DOWNLOAD_LIMIT)}</b>\n'
        f'⏱ حداکثر مدت ویدیو: <b>{human_time(MAX_SECONDS)}</b>\n'
        "━━━━━━━━━━━━━━━"
    )
    markup = keyboard([
        [button("⚙️ تنظیمات ویدیو", "menu:settings"), button("📦 تنظیمات فایل", "menu:file_settings")],
        [button("📚 مجموعه من", "menu:batch")],
        [button("📋 صف کارها", "menu:queue"), button("📊 آمار ربات", "menu:stats")],
        [button("❓ راهنما", "menu:help")],
    ])
    if message_id: edit(chat, message_id, text, markup)
    else: send(chat, text, markup)


def settings_menu(chat, uid, mid=None):
    s = get_settings(uid)
    text = (
        "⚙️ <b>تنظیمات ویدیو</b>\n"
        "<i>برای ویدیوهای بعدی اعمال می‌شود.</i>\n\n"
        + video_summary(s) +
        "\n\n<i>نمی‌دانید چه تنظیماتی مناسب است؟ روی «💡 راهنما» بزنید.</i>"
    )
    markup = keyboard([
        [button("🎛 کدک", "pick:codec"), button("⚡ کیفیت", "pick:quality")],
        [button("📐 رزولوشن", "pick:height"), button("🎞 نرخ فریم", "pick:fps")],
        [button("🔊 صدا", "pick:audio"), button("📤 نوع ارسال", "pick:delivery")],
        [button("🤖 حالت صف", "pick:auto"), button("🧹 متادیتا", "pick:strip_metadata")],
        [button("💬 زیرنویس", "pick:keep_subs"), button("🌈 HDR", "pick:hdr_to_sdr")],
        [button("💡 راهنما: کدام تنظیمات برای چه کاری؟", "menu:tips")],
        [button("♻️ بازنشانی به پیش‌فرض", "reset:video"), button("🔙 بازگشت", "menu:home")],
    ])
    if mid: edit(chat, mid, text, markup)
    else: send(chat, text, markup)


def tips_menu(chat, mid=None):
    text = (
        "💡 <b>راهنمای انتخاب تنظیمات</b>\n"
        "<i>برای هر کاربرد، بهترین ترکیب را انتخاب کنید.</i>\n\n"
        "📊 <b>۱. اسلاید، آموزش، صفحه‌نمایش</b>\n"
        "H.264 · کیفیت بیشتر · 1080p · 30fps\n"
        "<i>H.264 همه‌جا پخش می‌شود، کیفیت بالاتر متن و نمودار را خوانا نگه می‌دارد، 1080p برای وضوح متون ضروری است.</i>\n\n"
        "🎬 <b>۲. فیلم و محتوای پرتحرک</b>\n"
        "H.265 · کیفیت بیشتر · رزولوشن اصلی · FPS اصلی\n"
        "<i>H.265 حجم را بدون افت کیفیت کم می‌کند، حفظ FPS اصلی جزئیات حرکت را نگه می‌دارد و صحنه‌های سریع تار نمی‌شوند.</i>\n\n"
        "📶 <b>۳. اینترنت ضعیف (آپلود سریع)</b>\n"
        "H.264 · حجم کمتر · 480p · 24fps · صدای 64\n"
        "<i>کمترین حجم، حتی روی اینترنت کند آپلود می‌شود. H.264 رایج‌ترین کدک است و همه‌جا پخش می‌شود.</i>\n\n"
        "📼 <b>۴. آرشیو طولانی‌مدت</b>\n"
        "H.265 · متعادل · 1080p · FPS اصلی · صدای 128\n"
        "<i>تعادل بین کیفیت و حجم برای نگهداری سال‌ها. H.265 فضا را تقریباً نصف می‌کند بدون افت محسوس کیفیت.</i>\n\n"
        "✈️ <b>۵. پیش‌نمایش در تلگرام</b>\n"
        "H.264 · متعادل · 720p · نوع ارسال: ویدیوی قابل پخش\n"
        "<i>فقط H.264 در چت پیش‌نمایش می‌گیرد. 720p تعادل خوبی بین کیفیت و حجم دارد تا تلگرام دوباره فشرده نکند.</i>"
    )
    markup = keyboard([[button("🔙 بازگشت", "menu:settings")]])
    if mid: edit(chat, mid, text, markup)
    else: send(chat, text, markup)


def file_settings_menu(chat, uid, mid=None):
    s = get_settings(uid)
    text = (
        "📦 <b>تنظیمات فایل</b>\n"
        "<i>برای فایل‌های غیر ویدیویی اعمال می‌شود.</i>\n\n"
        + file_summary(s)
    )
    markup = keyboard([
        [button("🗜 الگوریتم", "pick:file_codec"), button("🎚 سطح فشرده‌سازی", "pick:file_level")],
        [button("⚙️ تعداد رشته‌ها", "pick:file_threads")],
        [button("♻️ بازنشانی", "reset:file"), button("🔙 بازگشت", "menu:home")],
    ])
    if mid: edit(chat, mid, text, markup)
    else: send(chat, text, markup)


def show_queue(chat, uid):
    rows = many("SELECT * FROM jobs WHERE uid=? ORDER BY created DESC LIMIT 15", (uid,))
    icons = {"downloading": "📥", "awaiting": "⏳", "queued": "📋", "running": "🔄",
             "done": "✅", "failed": "❌", "cancelled": "⛔"}
    labels = {"downloading": "در حال دانلود", "awaiting": "منتظر تأیید", "queued": "در صف",
              "running": "در حال پردازش", "done": "ارسال‌شده", "failed": "ناموفق", "cancelled": "لغوشده"}
    if not rows:
        send(chat, "📋 <b>صف خالی است.</b>"); return
    lines = ["📋 <b>صف و وضعیت کارها</b>"]
    buttons = []
    for r in rows:
        k = "🎥" if r.get("kind") == "video" else "📦"
        icon = icons.get(r["status"], "•")
        lines.append(f'{icon} {k} <code>{r["id"]}</code> — {labels.get(r["status"], r["status"])}\n   {escape_html(r["name"])}')
        if r["status"] in ("queued", "running", "downloading"):
            buttons.append([button(f'❌ لغو {r["id"]}', f'cancel:{r["id"]}')])
    send(chat, "\n\n".join(lines), keyboard(buttons))


def show_batch(chat, uid, mid=None):
    # FIX: exclude failed & cancelled, include done
    jobs = many(
        "SELECT * FROM jobs WHERE uid=? AND status IN "
        "('downloading','awaiting','queued','running','done') "
        "ORDER BY created DESC LIMIT 25", (uid,))
    if not jobs:
        text = ("📚 <b>مجموعه شما خالی است</b>\n\n"
                "هر ویدیو، فایل یا لینکی بفرستید — همه در یک پیام گروه می‌شوند.")
        markup = keyboard([[button("🔙 بازگشت", "menu:home")]])
        if mid: edit(chat, mid, text, markup)
        else: send(chat, text, markup)
        return

    lines = [f"📚 <b>مجموعه شما ({len(jobs)} فایل)</b>", ""]
    buttons = []
    awaiting_video = 0
    awaiting_file = 0
    for r in jobs:
        st = r["status"]
        icon = {"downloading": "📥", "awaiting": "⏳", "queued": "📋", "running": "🔄",
                "done": "✅"}.get(st, "•")
        k = "🎥" if r.get("kind") == "video" else "📦"
        src = "🔗 " if r.get("source_url") else ""
        lines.append(f'{icon} {k} {src}<code>{r["id"]}</code> · {escape_html(r["name"][:40])}')
        buttons.append([button(f'⚙️ {r["id"]} · {r["name"][:16]}', f'view:{r["id"]}')])
        if st == "awaiting":
            if r.get("kind") == "video": awaiting_video += 1
            else: awaiting_file += 1

    buttons.append([button("🔙 بازگشت", "menu:home")])

    # FIX: two preset buttons for video & file
    if awaiting_video:
        buttons.insert(-1, [button(f"📹 اعمال پریست به ویدیوها ({awaiting_video})", "batch:preset:video")])
    if awaiting_file:
        buttons.insert(-1, [button(f"📦 اعمال پریست به فایل‌ها ({awaiting_file})", "batch:preset:file")])

    if awaiting_video + awaiting_file:
        buttons.insert(-1, [button(f"▶️ شروع همه ({awaiting_video + awaiting_file})", "batch:start")])
        buttons.insert(-1, [button(f"🗑 لغو همه ({awaiting_video + awaiting_file})", "batch:cancel")])

    if mid: edit(chat, mid, "\n".join(lines), keyboard(buttons))
    else: send(chat, "\n".join(lines), keyboard(buttons))


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
    du = shutil.disk_usage(DATA)
    uptime = time.time() - START_TIME
    ratio = (sout/sin*100) if sin else 0
    text = (
        "📊 <b>آمار ربات</b>\n\n"
        f'⏱ آپتایم: {human_time(uptime)}\n'
        f'👥 کاربران: {users}\n'
        f'🎬 کل: {total_jobs} | ✅ {done} | ❌ {failed} | ⛔ {cancelled}\n'
        f'🔄 فعال: {active} (🎥 {video} | 📦 {filej})\n\n'
        f'📥 ورودی: {human_size(sin)}\n'
        f'📤 خروجی: {human_size(sout)}\n'
        f'📉 نسبت: {ratio:.1f}%\n\n'
        f'💾 دیسک: {human_size(du.used)} / {human_size(du.total)}\n'
        f'   آزاد: {human_size(du.free)}'
    )
    markup = keyboard([
        [button("🧹 پاک‌سازی فایل‌های Volume", "clear_volume")],
        [button("🔙 بازگشت", "menu:home")],
    ])
    if mid: edit(chat, mid, text, markup)
    else: send(chat, text, markup)


def help_text():
    return (
        "❓ <b>راهنمای کامل</b>\n\n"
        "🎥 <b>فشرده‌سازی ویدیو</b>\n"
        "1. تنظیمات را انتخاب کنید (یا از «راهنما» کمک بگیرید)\n"
        "2. ویدیو، فایل یا لینک بفرستید\n"
        "3. فایل‌ها در یک پیام گروه می‌شوند\n"
        "4. از «مجموعه من» روی هر فایل [⚙️] بزنید تا تنظیم کنید\n"
        "   • ✂️ بازه زمانی — فقط بخشی از ویدیو\n"
        "   • 🧪 نمونه ۱۰ ثانیه‌ای\n"
        "   • 🎵 استخراج صدا\n"
        "   • ✅ شروع\n\n"
        "🔗 <b>لینک دانلود مستقیم</b>\n"
        "هر لینکی با <code>http://</code> یا <code>https://</code> پذیرفته می‌شود.\n"
        "نوع فایل از پسوند URL تشخیص داده می‌شود.\n\n"
        "📚 <b>مجموعه خودکار</b>\n"
        "هر فایل/لینکی که بفرستید خودکار در «مجموعه من» ثبت می‌شود.\n"
        "فایل تکراری در وضعیت فعال مسدود می‌شود.\n\n"
        "━━━━━━━━━━━━━━━\n"
        "🗜 <b>باز کردن فایل‌های فشرده</b>\n\n"
        "پسوندها: <code>.zst</code> · <code>.br</code> · <code>.xz</code>\n\n"
        "📱 <b>اندروید</b>\n"
        "• <b>ZArchiver</b> از Google Play (پیشنهاد)\n"
        "• یا <b>RAR</b> از Google Play\n\n"
        "📱 <b>iPhone / iPad</b>\n"
        "• <b>iZip</b> یا <b>Documents by Readdle</b> از App Store\n"
        "• فایل را Share → Open in iZip کنید\n\n"
        "💻 <b>ویندوز</b>\n"
        "• <b>7-Zip</b> از 7-zip.org (نسخه 22+ برای .zst)\n"
        "• کلیک راست → Extract Here\n\n"
        "💻 <b>macOS</b>\n"
        "• <b>Keka</b> از keka.io\n"
        "• یا ترمینال: <code>brew install zstd brotli xz</code>\n"
        "• <code>zstd -d file.zst</code> · <code>brotli -d file.br</code> · <code>unxz file.xz</code>\n\n"
        "💻 <b>لینوکس</b>\n"
        "• <code>sudo apt install zstd brotli xz-utils</code>\n"
        "• <code>unzstd file.zst</code> · <code>brotli -d file.br</code> · <code>unxz file.xz</code>\n"
        "━━━━━━━━━━━━━━━\n\n"
        f'⚠️ مدت مجاز: {human_time(MAX_SECONDS)} · سقف: {human_size(DOWNLOAD_LIMIT)}\n\n'
        "📜 <b>دستورات</b>\n"
        "/start · /settings · /filesettings · /batch · /queue · /stats · /cancel · /help · /privacy"
    )


# ---------- Batch message (single per upload session) ----------
def _get_batch(uid, chat):
    b = BATCH_MSG.get(uid)
    if b and b["chat"] == chat and b["job_ids"]:
        active = 0
        for jid in b["job_ids"]:
            j = job_by_id(jid)
            if j and j["status"] in ("downloading", "awaiting", "queued", "running"):
                active += 1; break
        if active:
            b["last"] = time.time()
            return b
    b = {"chat": chat, "message_id": None, "job_ids": [], "created": time.time(),
         "last": time.time(), "last_render": 0}
    BATCH_MSG[uid] = b
    return b


def render_batch(uid, force=False):
    b = BATCH_MSG.get(uid)
    if not b: return
    now = time.monotonic()
    if not force and (now - b.get("last_render", 0)) < 2:
        return
    b["last_render"] = now

    jobs = [job_by_id(jid) for jid in b["job_ids"]]
    jobs = [j for j in jobs if j]
    if not jobs: return

    shown = jobs[-10:]
    hidden = len(jobs) - len(shown)
    lines = [f"📚 <b>{len(jobs)} فایل</b>", ""]
    buttons = []
    for j in shown:
        k = "🎥" if j.get("kind") == "video" else "📦"
        st = j["status"]
        icon = {"downloading": "📥", "awaiting": "⏳", "queued": "📋",
                "running": "🔄", "done": "✅", "failed": "❌", "cancelled": "⛔"}.get(st, "•")
        lines.append(f'{icon} {k} <code>{j["id"]}</code> · {escape_html(j["name"][:40])}')

        if st == "downloading":
            p = DOWNLOAD_PROGRESS.get(j["id"], {})
            if p.get("total"):
                pct = p["bytes"] / p["total"] * 100
                lines.append(f'   {progress_bar(pct, 8)} · {human_size(p["bytes"])}/{human_size(p["total"])}')
                if p.get("speed"):
                    lines.append(f'   ⚡ {human_size(p["speed"])}/s')
            else:
                lines.append(f'   💾 {human_size(p.get("bytes", 0))} دانلود شده')
        elif st == "running":
            p = ENCODE_PROGRESS.get(j["id"], {})
            if p.get("duration"):
                pct = min(99, p.get("seconds", 0) / p["duration"] * 100)
                lines.append(f'   {progress_bar(pct, 8)} · سرعت {p.get("speed", "—")}')
            else:
                lines.append(f'   🔄 در حال پردازش')
        elif st == "awaiting":
            lines.append(f'   💾 {human_size(j["size"])} · آماده تنظیم')
            buttons.append([button(f'⚙️ {j["id"]} · {j["name"][:16]}', f'view:{j["id"]}')])
        elif st == "queued":
            lines.append(f'   📋 در صف')
        elif st == "done":
            lines.append(f'   ✅ تکمیل · {human_size(j.get("output_size") or 0)}')
        elif st == "failed":
            lines.append(f'   ❌ ناموفق')
        elif st == "cancelled":
            lines.append(f'   ⛔ لغو')
        lines.append("")

    if hidden > 0:
        lines.append(f"<i>... و {hidden} مورد دیگر</i>\n")

    markup = keyboard(buttons + [[button("📚 مجموعه من", "menu:batch")]])
    text = "\n".join(lines)

    if b["message_id"]:
        edit(b["chat"], b["message_id"], text, markup)
    else:
        r = safe_send(b["chat"], text, markup)
        if r: b["message_id"] = r["message_id"]


# ---------- Receive ----------
def _check_duplicate(uid, file_id=None, source_url=None):
    active_states = ("downloading", "awaiting", "queued", "running")
    if source_url:
        r = one("SELECT id FROM jobs WHERE uid=? AND source_url=? AND status IN (?,?,?,?)",
                (uid, source_url) + active_states)
    elif file_id:
        r = one("SELECT id FROM jobs WHERE uid=? AND file_id=? AND status IN (?,?,?,?)",
                (uid, file_id) + active_states)
    else:
        r = None
    return r["id"] if r else None


def _create_job(chat, uid, media, name, size, kind, source_url=None):
    s = get_settings(uid)
    jid = uuid.uuid4().hex[:12]

    dup = _check_duplicate(uid, file_id=media["file_id"] if media else None, source_url=source_url)
    if dup:
        send(chat, f'⚠️ <b>فایل تکراری</b>\n\nقبلاً ثبت شده: <code>{dup}</code>\n'
                   f'<i>ابتدا آن را در مجموعه لغو کنید.</i>')
        return

    with DB_LOCK:
        active = one("SELECT COUNT(*) n FROM jobs WHERE status IN ('downloading','awaiting','queued','running')")["n"]
        personal = one("SELECT COUNT(*) n FROM jobs WHERE uid=? AND status IN ('downloading','awaiting','queued','running')", (uid,))["n"]
        if active >= MAX_ACTIVE_TOTAL or personal >= MAX_ACTIVE_PER_USER:
            send(chat, "❌ صف پر است؛ کمی صبر کنید."); return
        execute(
            "INSERT INTO jobs (id,uid,chat,file_id,name,size,settings,status,message,created,kind,source_url) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
            (jid, uid, chat, media["file_id"] if media else "url",
             name, size, json.dumps(s), "downloading", None, time.time(), kind, source_url)
        )

    b = _get_batch(uid, chat)
    b["job_ids"].append(jid)
    b["last"] = time.time()
    render_batch(uid, force=True)

    start_background_download(jid, s["auto"])


def receive_video(message, uid, chat):
    media = message.get("video") or message.get("document")
    if not media:
        send(chat, "لطفاً ویدیو بفرستید."); return
    name = clean_name(media.get("file_name") or "video.mp4")
    mime = media.get("mime_type", "")
    exts = (".mp4", ".mkv", ".mov", ".webm", ".avi", ".m4v", ".ts")
    if "video" not in message and not mime.startswith("video/") and not name.lower().endswith(exts):
        send(chat, "فایل ویدیویی نیست."); return
    size = int(media.get("file_size", 0))
    if size > DOWNLOAD_LIMIT:
        send(chat, f"از سقف ({human_size(DOWNLOAD_LIMIT)}) بزرگ‌تر."); return
    _create_job(chat, uid, media, name, size, "video")


def receive_file(message, uid, chat):
    media = message.get("document")
    if not media:
        send(chat, "لطفاً فایل بفرستید."); return
    name = clean_name(media.get("file_name") or "file.bin")
    mime = media.get("mime_type", "")
    if "video" in message or mime.startswith("video/"):
        send(chat, "این ویدیویی است؛ از بخش ویدیو."); return
    size = int(media.get("file_size", 0))
    if size > DOWNLOAD_LIMIT:
        send(chat, f"از سقف ({human_size(DOWNLOAD_LIMIT)}) بزرگ‌تر."); return
    _create_job(chat, uid, media, name, size, "file")


def receive_url(chat, uid, url):
    if len(url) > 4000:
        send(chat, "❌ لینک خیلی طولانی است."); return
    kind = detect_url_kind(url)
    name = url_to_name(url)
    _create_job(chat, uid, None, name, 0, kind, source_url=url)


# ---------- Background download ----------
def start_background_download(job_id, auto):
    def runner():
        if not DOWNLOAD_SEM.acquire(blocking=False):
            new = "queued" if auto else "awaiting"
            execute("UPDATE jobs SET status=? WHERE id=? AND status='downloading'", (new, job_id))
            j = job_by_id(job_id)
            if j: render_batch(j["uid"], force=True)
            return
        job = job_by_id(job_id)
        if not job or job["status"] != "downloading":
            DOWNLOAD_SEM.release(); return
        try:
            folder = WORK / job_id; folder.mkdir(parents=True, exist_ok=True)
            source = folder / "input"
            if not (source.exists() and source.stat().st_size > 0):
                if shutil.disk_usage(WORK).free < 512_000_000: raise UserError("فضای دیسک کم است.")
                download(job, source)
            new = "queued" if auto else "awaiting"
            changed = execute("UPDATE jobs SET status=? WHERE id=? AND status='downloading'", (new, job_id))
            if changed:
                render_batch(job["uid"], force=True)
                cur = job_by_id(job_id)
                if cur and cur.get("message"):
                    _render_ready(job_id)
        except Cancelled:
            execute("UPDATE jobs SET status='cancelled' WHERE id=? AND status='downloading'", (job_id,))
            render_batch(job["uid"], force=True)
        except Exception as exc:
            if STOP.is_set():
                execute("UPDATE jobs SET status='queued' WHERE id=? AND status='downloading'", (job_id,)); return
            execute("UPDATE jobs SET status='failed', error_msg=? WHERE id=? AND status='downloading'",
                    (str(exc)[:200], job_id))
            render_batch(job["uid"], force=True)
            reason = str(exc) if isinstance(exc, UserError) else "خطای دانلود."
            safe_send(job["chat"], f'❌ دانلود <code>{job_id}</code> ناموفق.\n{reason}')
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

    lines = [f'{"🎥" if kind=="video" else "📦"} <b>کار <code>{job_id}</code></b>', ""]
    lines.append(f'📄 {escape_html(job["name"])}')
    lines.append(f'💾 {human_size(job["size"])}')
    if job.get("source_url"):
        lines.append(f'🔗 منبع: لینک')

    if job.get("output_size") and job.get("finished_at"):
        lines.append("")
        lines.append("✅ <b>آخرین نتیجه:</b>")
        lines.append(f'📤 {human_size(job["output_size"])}')
        if job["size"]:
            reduction = (1 - job["output_size"] / job["size"]) * 100
            lines.append(f'📉 کاهش: {reduction:.1f}%')

    if info and kind == "video":
        lines.append("")
        lines.append("🔍 <b>مشخصات ورودی:</b>")
        lines.append(f'📐 {info["width"]}×{info["height"]} | ⏱ {human_time(info["duration"])} | 🎞 {info["fps"]:.1f} FPS')
        if info.get("transfer") in ("smpte2084", "arib-std-b67"):
            lines.append("🌈 <b>HDR</b> (تبدیل به SDR)")

        clip_start = s.get("clip_start", 0) or 0
        clip_dur = s.get("clip_duration", 0) or 0
        if clip_dur:
            lines.append(f'✂️ بازه: <b>{human_time(clip_start)}</b> → <b>{human_time(clip_start + clip_dur)}</b>')
        est = estimate_output_size(info, s)
        if est:
            lines.append(f"📊 تخمین: <b>~{human_size(est)}</b>")

    lines.append("")
    lines.append("⚙️ <b>تنظیمات:</b>")
    if kind == "video":
        c = VIDEO_CODECS[s["codec"]]
        q_label = QUALITY_LABELS[s["quality"]] if s["quality"] != "target" else f'{s.get("target_mb", 0)} MB (هدف)'
        lines.append(f'🎛 {c["title"]} | ⚡ {q_label}')
        lines.append(f'📐 {s["height"] or "اصلی"}p | 🔊 {s["audio"] or "بدون صدا"} kbps | 🎞 {s["fps"] or "اصلی"} FPS')
        lines.append(f'💬 زیرنویس: {"حفظ" if s["keep_subs"] else "حذف"} | 🌈 HDR: {"SDR" if s["hdr_to_sdr"] else "رد"}')
    else:
        lines.append(file_summary(s))

    status = job["status"]
    actions = []
    if status == "queued":
        lines.append("\n📋 در صف")
        actions = [[button("❌ لغو", f"cancel:{job_id}")]]
    elif status == "running":
        lines.append("\n🔄 در حال پردازش...")
        actions = [[button("❌ لغو", f"cancel:{job_id}")]]
    elif status == "downloading":
        lines.append("\n📥 در حال دانلود...")
        actions = [[button("❌ لغو", f"cancel:{job_id}")]]
    else:
        if kind == "video":
            clip_label = "✂️ تغییر بازه" if (s.get("clip_duration") or 0) > 0 else "✂️ بازه زمانی"
            actions.append([button(clip_label, f"clip:{job_id}")])
            actions.append([
                button("🧪 نمونه ۱۰ ثانیه", f"preview:{job_id}"),
                button("🎵 استخراج صدا", f"extract_audio:{job_id}"),
            ])
        start_label = "🔄 فشرده‌سازی مجدد" if status == "done" else "✅ شروع"
        actions.append([button(start_label, f"go:{job_id}"), button("❌ لغو", f"cancel:{job_id}")])
        actions.append([button("📚 مجموعه من", "menu:batch")])

    mid = job.get("message")
    if not mid:
        r = safe_send(job["chat"], "\n".join(lines), keyboard(actions))
        if r:
            execute("UPDATE jobs SET message=? WHERE id=?", (r["message_id"], job_id))
    else:
        edit(job["chat"], mid, "\n".join(lines), keyboard(actions))


def estimate_output_size(info, settings):
    if not info: return 0
    base = {"h264": 2200, "hevc": 1400, "av1": 1000}.get(settings.get("codec", "h264"), 2000)
    if settings.get("quality") == "quality": base *= 1.5
    elif settings.get("quality") == "small": base *= 0.6
    elif settings.get("quality") == "target":
        return int((settings.get("target_mb", 0) or 0) * 1_000_000)
    h = settings.get("height") or info.get("height", 720)
    if info.get("height", 0) > 0:
        base *= max(0.3, (h / info["height"]) ** 1.4)
    audio = settings.get("audio", 0) or 0
    duration = settings.get("clip_duration", 0) or info["duration"]
    return int((base + audio) * duration / 8)


# ---------- Preview / audio extract ----------
def send_preview(job_id):
    job = job_by_id(job_id)
    if not job: return
    folder = WORK / job_id
    source = folder / "input"
    if not source.exists():
        safe_send(job["chat"], "❌ فایل ورودی یافت نشد."); return
    s = json.loads(job["settings"])
    try: info = probe(source)
    except Exception as e:
        safe_send(job["chat"], f"❌ خطا: {e}"); return

    clip_start = s.get("clip_start", 0) or 0
    clip_dur = s.get("clip_duration", 0) or 0
    clip_line = f'✂️ از: {human_time(clip_start)} → {human_time(clip_start + clip_dur)}\n' if clip_dur else ""

    status = safe_send(job["chat"],
        f'🧪 <b>ساخت نمونه ۱۰ ثانیه‌ای...</b>\n\n📄 {escape_html(job["name"])}\n'
        f'⚙️ {VIDEO_CODECS[s["codec"]]["title"]} · {QUALITY_LABELS.get(s["quality"], s["quality"])}\n'
        f'📐 {s["height"] or "اصلی"}p | 🔊 {s["audio"] or "بی‌صدا"} kbps\n{clip_line}')

    preview_path = folder / "preview.mp4"
    s_preview = dict(s); s_preview["preview_only"] = True
    try:
        cmd, _ = build_command(source, preview_path, s_preview, info)
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
        if result.returncode != 0 or not preview_path.exists():
            raise UserError("FFmpeg نتوانست نمونه را بسازد.")
        with preview_path.open("rb") as f:
            caption = (
                f'🧪 <b>نمونه ۱۰ ثانیه‌ای</b>\n\n📄 {escape_html(job["name"])}\n'
                f'⚙️ {VIDEO_CODECS[s["codec"]]["title"]} · {QUALITY_LABELS.get(s["quality"], s["quality"])}\n'
                f'📐 {s["height"] or "اصلی"}p | 🔊 {s["audio"] or "بی‌صدا"} kbps\n{clip_line}\n'
                f'<i>اگر مناسب است، از پیام قبلی «شروع» را بزنید.</i>'
            )
            api("sendVideo", {"chat_id": job["chat"], "caption": caption,
                              "parse_mode": "HTML", "supports_streaming": "true"},
                files={"video": ("preview.mp4", f, "video/mp4")})
    except subprocess.TimeoutExpired:
        safe_send(job["chat"], "❌ زمان تمام شد.")
    except Exception as e:
        LOG.warning(f"Preview failed: {e}")
        safe_send(job["chat"], f"❌ خطا: {e}")
    finally:
        if preview_path.exists():
            try: preview_path.unlink()
            except Exception: pass
        if status:
            try: api("deleteMessage", {"chat_id": job["chat"], "message_id": status["message_id"]}, read_timeout=10)
            except Exception: pass


def extract_audio_only(job_id):
    job = job_by_id(job_id)
    if not job: return
    folder = WORK / job_id
    source = folder / "input"
    if not source.exists():
        safe_send(job["chat"], "❌ فایل یافت نشد."); return
    s = json.loads(job["settings"])
    try: info = probe(source)
    except Exception as e:
        safe_send(job["chat"], f"❌ خطا: {e}"); return
    if info.get("audio_index") is None:
        safe_send(job["chat"], "❌ این ویدیو صدا ندارد."); return

    clip_start = float(s.get("clip_start", 0) or 0)
    clip_dur = float(s.get("clip_duration", 0) or 0)
    bitrate = s.get("audio", 128) or 128
    if bitrate == 0: bitrate = 128

    audio_path = folder / "audio.m4a"
    status = safe_send(job["chat"],
        f'🎵 <b>استخراج صدا...</b>\n📄 {escape_html(job["name"])}\n🔊 {bitrate} kbps'
        + (f'\n✂️ از: {human_time(clip_start)} → {human_time(clip_start + clip_dur)}' if clip_dur else ""))
    try:
        cmd = ["ffmpeg", "-hide_banner", "-nostdin", "-y", "-loglevel", "error"]
        if clip_start > 0: cmd += ["-ss", f"{clip_start:.3f}"]
        cmd += ["-i", str(source), "-map", f'0:{info["audio_index"]}', "-vn"]
        if clip_dur > 0: cmd += ["-t", f"{clip_dur:.3f}"]
        cmd += ["-c:a", "aac", "-b:a", f"{bitrate}k", "-ac", "2", str(audio_path)]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
        if result.returncode != 0 or not audio_path.exists():
            raise UserError("FFmpeg نتوانست صدا را استخراج کند.")
        output_size = audio_path.stat().st_size
        with audio_path.open("rb") as f:
            stem = Path(job["name"]).stem or "audio"
            clip_line = f'✂️ {human_time(clip_start)} → {human_time(clip_start + clip_dur)}\n' if clip_dur else ""
            caption = (f'🎵 <b>صدا استخراج شد</b>\n📄 {escape_html(job["name"])}\n'
                       f'💾 {human_size(output_size)} · 🔊 {bitrate} kbps\n{clip_line}')
            api("sendAudio", {"chat_id": job["chat"], "caption": caption, "parse_mode": "HTML",
                              "title": stem[:64], "performer": "Audio Extract"},
                files={"audio": (f"{stem}.m4a", f, "audio/mp4")})
    except subprocess.TimeoutExpired:
        safe_send(job["chat"], "❌ زمان تمام شد.")
    except Exception as e:
        LOG.warning(f"Audio extract failed: {e}")
        safe_send(job["chat"], f"❌ خطا: {e}")
    finally:
        if audio_path.exists():
            try: audio_path.unlink()
            except Exception: pass
        if status:
            try: api("deleteMessage", {"chat_id": job["chat"], "message_id": status["message_id"]}, read_timeout=10)
            except Exception: pass


# ---------- Download ----------
def download(job, target):
    if job.get("source_url"):
        return _download_url(job, target, job["source_url"])
    info = api("getFile", {"file_id": job["file_id"]})
    size_hint = int(info.get("file_size", 0))
    if size_hint > DOWNLOAD_LIMIT: raise UserError("از سقف بزرگ‌تر.")
    file_path = info.get("file_path")
    if not file_path: raise UserError("مسیر دانلود دریافت نشد.")
    execute("UPDATE jobs SET server_path=? WHERE id=?", (file_path, job["id"]))
    total = 0
    part = target.with_name(target.name + ".part")
    try:
        if USE_LOCAL_API:
            src = Path(file_path)
            if not src.exists(): raise UserError(f"فایل روی سرور نیست.")
            file_total = src.stat().st_size
            last = 0.0; started = time.monotonic()
            try:
                with src.open("rb") as fsrc, part.open("wb") as fdst:
                    while True:
                        check_job(job["id"])
                        chunk = fsrc.read(512 * 1024)
                        if not chunk: break
                        total += len(chunk)
                        if total > DOWNLOAD_LIMIT: raise UserError("بزرگ‌تر از سقف.")
                        fdst.write(chunk)
                        now = time.monotonic()
                        if now - last >= 1.5 and file_total:
                            last = now
                            speed = total / max(now - started, 0.01)
                            DOWNLOAD_PROGRESS[job["id"]] = {"bytes": total, "total": file_total, "speed": speed}
                            render_batch(job["uid"])
                            cur = job_by_id(job["id"])
                            if cur and cur.get("message"):
                                pct = total / file_total * 100
                                edit(cur["chat"], cur["message"],
                                     f'📥 <b>دانلود</b>\n💾 {human_size(total)} / {human_size(file_total)}\n'
                                     f'{progress_bar(pct)}\n⚡ {human_size(speed)}/s',
                                     keyboard([[button("❌ لغو", f"cancel:{job['id']}")]]))
            except OSError as exc:
                raise UserError(f"کپی ناموفق: {type(exc).__name__}") from None
        else:
            rel = file_path.lstrip("/")
            url = f"{API_BASE}/file/bot{TOKEN}/{rel}"
            with requests.get(url, stream=True, timeout=(30, 300)) as r:
                r.raise_for_status()
                file_total = int(r.headers.get("Content-Length", 0))
                last = 0.0; started = time.monotonic()
                with part.open("wb") as f:
                    for chunk in r.iter_content(512 * 1024):
                        check_job(job["id"])
                        if not chunk: continue
                        total += len(chunk)
                        if total > DOWNLOAD_LIMIT: raise UserError("بزرگ‌تر از سقف.")
                        f.write(chunk)
                        now = time.monotonic()
                        if now - last >= 1.5 and file_total:
                            last = now
                            speed = total / max(now - started, 0.01)
                            DOWNLOAD_PROGRESS[job["id"]] = {"bytes": total, "total": file_total, "speed": speed}
                            render_batch(job["uid"])
        if total == 0: raise UserError("فایل خالی.")
        part.rename(target)
        if job["size"] == 0:
            execute("UPDATE jobs SET size=? WHERE id=?", (total, job["id"]))
        DOWNLOAD_PROGRESS.pop(job["id"], None)
        return total
    except requests.RequestException as exc:
        raise UserError(f"دانلود ناموفق ({type(exc).__name__})") from None
    finally:
        if part.exists():
            try: part.unlink()
            except Exception: pass


def _download_url(job, target, url):
    """Download from a direct URL with browser-like headers and retries."""
    part = target.with_name(target.name + ".part")
    try:
        # Use the shared session with retries and full browser headers.
        r = URL_SESSION.get(url, stream=True, timeout=(30, 600), allow_redirects=True)
        r.raise_for_status()

        file_total = int(r.headers.get("Content-Length", 0) or 0)
        if file_total > DOWNLOAD_LIMIT:
            raise UserError(f"از سقف ({human_size(DOWNLOAD_LIMIT)}) بزرگ‌تر.")

        last = 0.0
        started = time.monotonic()
        total = 0
        with part.open("wb") as f:
            for chunk in r.iter_content(1024 * 1024):
                check_job(job["id"])
                if not chunk: continue
                total += len(chunk)
                if total > DOWNLOAD_LIMIT:
                    raise UserError("از سقف بیشتر شد.")
                f.write(chunk)
                now = time.monotonic()
                if now - last >= 1.5:
                    last = now
                    speed = total / max(now - started, 0.01)
                    DOWNLOAD_PROGRESS[job["id"]] = {"bytes": total, "total": file_total, "speed": speed}
                    render_batch(job["uid"])
                    cur = job_by_id(job["id"])
                    if cur and cur.get("message"):
                        if file_total > 0:
                            pct = total / file_total * 100
                            edit(cur["chat"], cur["message"],
                                 f'📥 <b>دانلود از لینک</b>\n💾 {human_size(total)} / {human_size(file_total)}\n'
                                 f'{progress_bar(pct)}\n⚡ {human_size(speed)}/s',
                                 keyboard([[button("❌ لغو", f"cancel:{job['id']}")]]))
                        else:
                            edit(cur["chat"], cur["message"],
                                 f'📥 <b>دانلود از لینک</b>\n💾 {human_size(total)}\n⚡ {human_size(speed)}/s',
                                 keyboard([[button("❌ لغو", f"cancel:{job['id']}")]]))
        if total == 0: raise UserError("فایل خالی.")
        part.rename(target)
        if job["size"] == 0:
            execute("UPDATE jobs SET size=? WHERE id=?", (total, job["id"]))
        DOWNLOAD_PROGRESS.pop(job["id"], None)
        return total
    except UserError:
        raise
    except requests.RequestException as exc:
        LOG.warning(f"URL DL {job['id']}: {exc}")
        raise UserError(f"دانلود ناموفق ({type(exc).__name__})") from None
    finally:
        if part.exists():
            try: part.unlink()
            except Exception: pass


# ---------- Probe / validate ----------
def probe(path):
    try:
        r = subprocess.run(["ffprobe", "-v", "error", "-show_format", "-show_streams", "-of", "json", str(path)],
                           capture_output=True, text=True, timeout=30)
    except (subprocess.TimeoutExpired, OSError):
        raise UserError("بررسی فایل ناموفق.") from None
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
        return {"duration": dur, "width": w, "height": h, "fps": fps,
                "codec": v.get("codec_name", "?"), "transfer": v.get("color_transfer", ""),
                "video_index": int(v["index"]),
                "audio_index": int(a["index"]) if a else None, "has_subs": sub}
    except (ValueError, TypeError, KeyError, StopIteration, ZeroDivisionError):
        raise UserError("مشخصات ویدیو قابل تشخیص نیست.") from None


def validate_media(info, settings):
    import math
    if not math.isfinite(info["duration"]) or info["duration"] <= 0: raise UserError("مدت نامعتبر.")
    if info["duration"] > MAX_SECONDS: raise UserError(f"مدت بیشتر از {human_time(MAX_SECONDS)}.")
    if info["width"] < 2 or info["height"] < 2: raise UserError("ابعاد نامعتبر.")
    if info["width"] * info["height"] > MAX_PIXELS: raise UserError("رزولوشن زیاد.")
    if info["transfer"] in ("smpte2084", "arib-std-b67") and not settings.get("hdr_to_sdr"):
        raise UserError("HDR رد شد.")


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

    clip_start = float(settings.get("clip_start", 0) or 0)
    clip_dur = float(settings.get("clip_duration", 0) or 0)
    preview = bool(settings.get("preview_only"))

    if preview:
        seek = clip_start if clip_dur > 0 else max(0.0, info["duration"] / 2 - 5)
        eff_dur = min(10.0, clip_dur if clip_dur > 0 else info["duration"])
    else:
        seek = clip_start
        eff_dur = min(clip_dur, info["duration"] - seek) if clip_dur > 0 else info["duration"] - seek
    if eff_dur <= 0: raise UserError("بازه نامعتبر.")

    cmd = ["ffmpeg", "-hide_banner", "-nostdin", "-y", "-loglevel", "error",
           "-progress", "pipe:1", "-stats_period", "1",
           "-filter_threads", str(THREADS), "-threads", str(THREADS),
           "-protocol_whitelist", "file,pipe"]
    if seek > 0: cmd += ["-ss", f"{seek:.3f}"]
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
    if quality == "target" and settings.get("target_mb") and eff_dur > 0:
        total_kbps = (settings["target_mb"] * 1_000_000 * 8) / eff_dur / 1000
        video_kbps = int(total_kbps - (settings["audio"] or 0))
        if video_kbps < 100: raise UserError("حجم هدف خیلی کم.")
        cmd += ["-b:v", f"{video_kbps}k", "-maxrate", f"{int(video_kbps*1.5)}k", "-bufsize", f"{int(video_kbps*2)}k"]
    else:
        cmd += ["-crf", str(c["crf"][quality])]
    if settings["codec"] == "h264":
        cmd += ["-preset", "medium"]
    elif settings["codec"] == "hevc":
        cmd += ["-preset", "medium", "-tag:v", "hvc1",
                "-x265-params", f"pools={THREADS}:frame-threads=1:log-level=error"]
    else:
        cmd += ["-preset", "8", "-svtav1-params", f"lp={THREADS}"]
    if settings["strip_metadata"]:
        cmd += ["-map_metadata", "-1"]
    cmd += ["-map_chapters", "-1", "-t", f"{eff_dur:.3f}", "-movflags", "+faststart", str(target)]
    return cmd, eff_dur


def stop_process(p):
    if p.poll() is not None: return
    try:
        p.terminate(); p.wait(timeout=5)
    except subprocess.TimeoutExpired:
        p.kill(); p.wait()
    except ProcessLookupError: pass


def encode(job, source, target, settings, info, folder):
    cmd, effective_duration = build_command(source, target, settings, info)
    progress = __import__("queue").Queue()
    started = time.monotonic()
    last_edit = 0.0; media_seconds = 0.0; speed = "—"
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
                if elapsed > ENCODE_TIMEOUT: raise UserError("زمان تمام شد.")
                if target.exists() and target.stat().st_size > UPLOAD_LIMIT:
                    raise UserError("خروجی از سقف بزرگ‌تر.")
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
                    ENCODE_PROGRESS[job["id"]] = {"seconds": media_seconds,
                                                   "duration": effective_duration, "speed": speed}
                    render_batch(job["uid"])
                    cur = job_by_id(job["id"])
                    if cur and cur.get("message"):
                        pct = min(99.5, media_seconds / effective_duration * 100) if effective_duration else 0
                        eta = ((effective_duration - media_seconds) * elapsed / media_seconds) if media_seconds > 0 else 0
                        edit(cur["chat"], cur["message"],
                             f'🎬 <b>فشرده‌سازی</b>\n{escape_html(job["name"])}\n\n'
                             f'{progress_bar(pct)}\n⚙️ سرعت: {speed}\n'
                             f'⏱ سپری: {human_time(elapsed)} | باقی: {human_time(eta)}',
                             keyboard([[button("❌ لغو", f"cancel:{job['id']}")]]))
                time.sleep(0.3)
            rt.join(timeout=2)
            check_job(job["id"])
            if p.returncode != 0:
                raise UserError("FFmpeg ناموفق. کدک H.264 را امتحان کنید.")
            if not target.exists() or target.stat().st_size == 0: raise UserError("خروجی نامعتبر.")
            if target.stat().st_size > UPLOAD_LIMIT: raise UserError("خروجی از سقف بزرگ‌تر.")
            return time.monotonic() - started
        finally:
            stop_process(p); rt.join(timeout=2)


# ---------- File compress ----------
def compress_file(job, source, target, settings, input_size, folder):
    codec = settings["file_codec"]
    c_def = FILE_CODECS[codec]
    level = c_def["levels"].get(settings["file_level"], c_def["levels"]["max"])
    threads = settings.get("file_threads", 0) or THREADS
    started = time.monotonic(); last_edit = 0.0; processed = [0]

    def on_progress():
        nonlocal last_edit
        now = time.monotonic()
        if now - last_edit < 3: return
        last_edit = now
        elapsed = now - started
        pct = min(99.5, processed[0] / input_size * 100) if input_size else 0
        eta = ((input_size - processed[0]) * elapsed / processed[0]) if processed[0] > 0 else 0
        ENCODE_PROGRESS[job["id"]] = {"seconds": processed[0], "duration": input_size,
                                       "speed": human_size(processed[0]/max(elapsed,0.01)) + "/s"}
        render_batch(job["uid"])
        cur = job_by_id(job["id"])
        if cur and cur.get("message"):
            edit(cur["chat"], cur["message"],
                 f'📦 <b>فشرده‌سازی</b>\n{escape_html(job["name"])}\n\n{progress_bar(pct)}\n'
                 f'🗜 {c_def["title"]} (level={level})\n'
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
        cctx = zstd.ZstdCompressor(level=level, threads=threads)
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
    if not target.exists() or target.stat().st_size == 0: raise UserError("خروجی نامعتبر.")
    return time.monotonic() - started


# ---------- Upload ----------
def upload_result(job, target, settings, info, caption, kind):
    total_size = target.stat().st_size
    def on_progress(cur, tot):
        pct = cur / tot * 100 if tot else 0
        edit(job["chat"], job.get("message"),
             f'📤 <b>ارسال</b>\n{escape_html(job["name"])}\n\n{progress_bar(pct)}\n'
             f'💾 {human_size(cur)} / {human_size(tot)}')
    if kind == "video":
        filename = f'compressed_{settings["codec"]}_{job["id"]}.mp4'
        as_video = settings["delivery"] == "video" and settings["codec"] == "h264" and not settings.get("preview_only")
        method = "sendVideo" if as_video else "sendDocument"
        field = "video" if as_video else "document"
        data = {"chat_id": job["chat"], "caption": caption, "parse_mode": "HTML"}
        if as_video and info:
            data.update({"duration": max(1, int(info["duration"])), "width": info["width"],
                         "height": info["height"], "supports_streaming": "true"})
        mime = "video/mp4"
    else:
        ext = FILE_CODECS[settings["file_codec"]]["ext"]
        filename = job["name"] + ext
        method = "sendDocument"; field = "document"
        data = {"chat_id": job["chat"], "caption": caption, "parse_mode": "HTML"}
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
        if shutil.disk_usage(WORK).free < 512_000_000: raise UserError("فضای دیسک کم.")
        if not source.exists() or source.stat().st_size == 0:
            input_size = download(job, source)
        else:
            input_size = source.stat().st_size
        check_job(jid)
        settings = json.loads(job["settings"])
        if kind == "video":
            info = probe(source)
            execute("UPDATE jobs SET media_info=? WHERE id=?", (json.dumps(info), jid))
            if settings.get("clip_duration"):
                start = float(settings.get("clip_start", 0) or 0)
                dur = float(settings["clip_duration"])
                if start >= info["duration"]:
                    settings["clip_start"] = 0
                    settings["clip_duration"] = min(10, info["duration"])
                    execute("UPDATE jobs SET settings=? WHERE id=?", (json.dumps(settings), jid))
                elif start + dur > info["duration"]:
                    settings["clip_duration"] = info["duration"] - start
                    execute("UPDATE jobs SET settings=? WHERE id=?", (json.dumps(settings), jid))
            validate_media(info, settings)
            elapsed_enc = encode(job, source, target, settings, info, folder)
            out_info = probe(target)
            output_size = target.stat().st_size
            reduction = (1 - output_size / input_size) * 100 if input_size else 0
            ch = f"📉 کاهش: <b>{reduction:.1f}%</b>" if reduction >= 0 else f"⚠️ افزایش: <b>{abs(reduction):.1f}%</b>"
            clip_line = ""
            if settings.get("clip_duration"):
                cs = settings.get("clip_start", 0)
                clip_line = f'✂️ بازه: {human_time(cs)} → {human_time(cs + settings["clip_duration"])}\n'
            caption = (f'✅ <b>نتیجه</b> <code>{jid}</code>\n'
                       f'📄 {escape_html(job["name"])}\n\n'
                       f'📥 {human_size(input_size)} → 📤 {human_size(output_size)}\n'
                       f'{ch} | 📊 {input_size/output_size:.2f}×\n'
                       f'{clip_line}\n'
                       f'🎛 {VIDEO_CODECS[settings["codec"]]["title"]}\n'
                       f'📐 {out_info["width"]}×{out_info["height"]} | ⏱ {human_time(out_info["duration"])}\n'
                       f'⚙️ {human_time(elapsed_enc)}')
        else:
            elapsed_enc = compress_file(job, source, target, settings, input_size, folder)
            output_size = target.stat().st_size
            reduction = (1 - output_size / input_size) * 100 if input_size else 0
            ch = f"📉 کاهش: <b>{reduction:.1f}%</b>" if reduction >= 0 else f"⚠️ افزایش: <b>{abs(reduction):.1f}%</b>"
            caption = (f'✅ <b>نتیجه</b> <code>{jid}</code>\n'
                       f'📄 {escape_html(job["name"])}\n\n'
                       f'📥 {human_size(input_size)} → 📤 {human_size(output_size)}\n'
                       f'{ch} | 📊 {input_size/output_size:.2f}×\n\n'
                       f'🗜 {FILE_CODECS[settings["file_codec"]]["title"]}\n'
                       f'⚙️ {human_time(elapsed_enc)}')
            out_info = None
        check_job(jid)
        upload_result(job, target, settings, out_info, caption, kind)
        execute("UPDATE jobs SET status='done', output_size=?, finished_at=? WHERE id=?",
                (output_size, time.time(), jid))
        ENCODE_PROGRESS.pop(jid, None)
        render_batch(job["uid"], force=True)
        cur = job_by_id(jid)
        if cur and cur.get("message"):
            _render_ready(jid)
    except Cancelled:
        execute("UPDATE jobs SET status='cancelled' WHERE id=?", (jid,))
        ENCODE_PROGRESS.pop(jid, None)
        render_batch(job["uid"], force=True)
        cur = job_by_id(jid)
        if cur and cur.get("message"):
            edit(cur["chat"], cur["message"], f'⛔ کار <code>{jid}</code> لغو شد.')
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
            ENCODE_PROGRESS.pop(jid, None)
            LOG.warning(f"Job {jid} failed: {reason}")
            safe_send(job["chat"], f'❌ <code>{jid}</code> ناموفق.\n{reason}')
            render_batch(job["uid"], force=True)
            cur = job_by_id(jid)
            if cur and cur.get("message"):
                edit(cur["chat"], cur["message"], f'❌ <code>{jid}</code> ناموفق.')
    finally:
        if folder.exists():
            for f in folder.iterdir():
                if f.name == "input": continue
                try:
                    if f.is_file(): f.unlink()
                    elif f.is_dir(): shutil.rmtree(f, ignore_errors=True)
                except Exception: pass


def worker():
    while not STOP.is_set():
        with DB_LOCK:
            job = one("SELECT * FROM jobs WHERE status='queued' ORDER BY created LIMIT 1")
            if job:
                execute("UPDATE jobs SET status='running', output_size=NULL, finished_at=NULL WHERE id=?",
                        (job["id"],))
        if not job:
            STOP.wait(1); continue
        render_batch(job["uid"], force=True)
        process_job(job)


# ---------- Clear volume ----------
def clear_volume_files(chat):
    deleted = 0; total_size = 0
    rows = many("SELECT id, server_path FROM jobs WHERE server_path IS NOT NULL")
    for r in rows:
        try:
            p = Path(r["server_path"])
            if p.exists() and p.is_file():
                total_size += p.stat().st_size; p.unlink(); deleted += 1
        except Exception: pass
        execute("UPDATE jobs SET server_path=NULL WHERE id=?", (r["id"],))
    for p in [os.environ.get("TELEGRAM_WORK_DIR", ""), "/var/lib/telegram-bot-api",
              "/data/telegram-bot-api", str(DATA / "telegram-bot-api")]:
        if not p: continue
        root = Path(p)
        if not root.exists(): continue
        for f in root.rglob("*"):
            if not f.is_file(): continue
            try:
                total_size += f.stat().st_size; f.unlink(); deleted += 1
            except Exception: pass
    active_ids = {r["id"] for r in many(
        "SELECT id FROM jobs WHERE status IN ('downloading','awaiting','queued','running')")}
    for p in WORK.iterdir():
        if not p.is_dir() or p.name in active_ids: continue
        try:
            for f in p.rglob("*"):
                if f.is_file():
                    try: total_size += f.stat().st_size
                    except Exception: pass
            shutil.rmtree(p, ignore_errors=True); deleted += 1
        except Exception: pass
    send(chat, f'🧹 <b>پاک‌سازی</b>\n\n🗑 {deleted} مورد\n💾 {human_size(total_size)} آزاد شد.')


# ---------- Callbacks ----------
def handle_callback(cb):
    msg = cb.get("message", {})
    if msg.get("chat", {}).get("type") != "private":
        answer(cb["id"], "فقط گفت‌وگوی خصوصی."); return
    uid = cb["from"]["id"]; chat = msg["chat"]["id"]; mid = msg["message_id"]
    if not authorized(uid): answer(cb["id"], "دسترسی ندارید."); return
    data = cb.get("data", ""); parts = data.split(":")

    if data == "menu:home":
        answer(cb["id"]); home(chat, mid)
    elif data == "menu:settings":
        answer(cb["id"]); settings_menu(chat, uid, mid)
    elif data == "menu:tips":
        answer(cb["id"]); tips_menu(chat, mid)
    elif data == "menu:file_settings":
        answer(cb["id"]); file_settings_menu(chat, uid, mid)
    elif data == "menu:queue":
        answer(cb["id"]); show_queue(chat, uid)
    elif data == "menu:help":
        answer(cb["id"]); edit(chat, mid, help_text(), keyboard([[button("🔙 بازگشت", "menu:home")]]))
    elif data == "menu:stats":
        answer(cb["id"]); show_stats(chat, mid)
    elif data == "menu:batch":
        answer(cb["id"]); show_batch(chat, uid, mid)
    elif data == "clear_volume":
        answer(cb["id"], "در حال پاک‌سازی...")
        threading.Thread(target=clear_volume_files, args=(chat,), daemon=True).start()
    elif parts[0] == "batch" and len(parts) == 2:
        if parts[1] == "start":
            count = execute(
                "UPDATE jobs SET status='queued', output_size=NULL, finished_at=NULL "
                "WHERE uid=? AND status='awaiting'", (uid,))
            answer(cb["id"], f'✅ {count} کار در صف.')
            show_batch(chat, uid, mid)
        elif parts[1] == "cancel":
            count = execute(
                "UPDATE jobs SET status='cancelled' "
                "WHERE uid=? AND status IN ('downloading','awaiting','queued')", (uid,))
            answer(cb["id"], f'⛔ {count} کار لغو شد.')
            show_batch(chat, uid, mid)
    elif parts[0] == "batch" and len(parts) == 3 and parts[1] == "preset":
        kind_filter = parts[2]  # "video" or "file"
        answer(cb["id"])
        rows = []
        if kind_filter == "video":
            # Show video-preset choices (derived from DEFAULTS/CHOICES)
            rows.append([button("H.264 · متعادل · 720p", "batchapply:video:1")])
            rows.append([button("H.265 · کیفیت بالا · 1080p", "batchapply:video:2")])
            rows.append([button("H.264 · حجم کم · 480p", "batchapply:video:3")])
        else:
            rows.append([button("Zstandard · متعادل", "batchapply:file:1")])
            rows.append([button("LZMA2 · حداکثر", "batchapply:file:2")])
        rows.append([button("❌ انصراف", "menu:batch")])
        edit(chat, mid, f"📂 <b>پریست {kind_filter} را انتخاب کنید:</b>", keyboard(rows))
    elif parts[0] == "batchapply" and len(parts) == 3:
        kind_filter = parts[1]
        preset_id = int(parts[2])
        # Build a simple preset mapping
        if kind_filter == "video":
            presets = {
                1: {"codec": "h264", "quality": "balanced", "height": 720, "fps": 0, "audio": 96},
                2: {"codec": "hevc", "quality": "quality", "height": 1080, "fps": 0, "audio": 128},
                3: {"codec": "h264", "quality": "small", "height": 480, "fps": 24, "audio": 64},
            }
        else:
            presets = {
                1: {"file_codec": "zstd", "file_level": "balanced"},
                2: {"file_codec": "lzma2", "file_level": "max"},
            }
        subset = presets.get(preset_id, {})
        jobs = many("SELECT id, settings FROM jobs WHERE uid=? AND status='awaiting' AND kind=?",
                    (uid, kind_filter))
        count = 0
        for j in jobs:
            s = json.loads(j["settings"])
            s.update(subset)
            execute("UPDATE jobs SET settings=? WHERE id=?", (json.dumps(s), j["id"]))
            count += 1
        answer(cb["id"], f'✅ پریست به {count} فایل اعمال شد.')
        show_batch(chat, uid, mid)
    elif parts[0] == "view" and len(parts) == 2:
        job = job_by_id(parts[1])
        if not job or job["uid"] != uid: answer(cb["id"]); return
        answer(cb["id"])
        _render_ready(parts[1])
    elif parts[0] == "preview" and len(parts) == 2:
        answer(cb["id"], "در حال ساخت نمونه...")
        threading.Thread(target=send_preview, args=(parts[1],), daemon=True).start()
    elif parts[0] == "extract_audio" and len(parts) == 2:
        answer(cb["id"], "در حال استخراج صدا...")
        threading.Thread(target=extract_audio_only, args=(parts[1],), daemon=True).start()
    elif parts[0] == "clip" and len(parts) == 2:
        job = job_by_id(parts[1])
        if not job or job["uid"] != uid:
            answer(cb["id"], "یافت نشد."); return
        answer(cb["id"])
        SESSION[uid] = {"action": "clip_range", "jid": parts[1], "msg_id": mid}
        s = json.loads(job["settings"])
        cur = ""
        if s.get("clip_duration"):
            cur = (f'\n✂️ بازه فعلی: <b>{human_time(s.get("clip_start", 0))}</b> → '
                   f'<b>{human_time((s.get("clip_start", 0) or 0) + s["clip_duration"])}</b>\n')
        edit(chat, mid,
             "✂️ <b>بازه زمانی</b>\n" + cur + "\n"
             "فرمت‌ها:\n• <code>1:30 2:00</code>\n• <code>1:30-3:30</code>\n• <code>90 120</code>\n\n"
             "برای حذف بازه: <code>0</code>",
             keyboard([[button("❌ انصراف", f"cancel_input:clip:{parts[1]}")]]))
    elif parts[0] == "reset" and len(parts) == 2:
        answer(cb["id"]); s = get_settings(uid)
        if parts[1] == "video":
            for k in ("codec", "quality", "height", "fps", "audio", "delivery",
                      "strip_metadata", "keep_subs", "hdr_to_sdr", "target_mb"):
                s[k] = copy.deepcopy(DEFAULTS[k])
            save_settings(uid, s); settings_menu(chat, uid, mid)
        else:
            for k in ("file_codec", "file_level", "file_threads"):
                s[k] = DEFAULTS[k]
            save_settings(uid, s); file_settings_menu(chat, uid, mid)
    elif parts[0] == "pick" and len(parts) == 2 and parts[1] in CHOICES:
        answer(cb["id"]); key = parts[1]
        s = get_settings(uid); cur = s.get(key)
        rows = []
        for i, (label, val) in enumerate(CHOICES[key]):
            mark = "✅ " if val == cur else ""
            rows.append([button(mark + label, f"set:{key}:{i}")])
        rows.append([button("ℹ️ توضیحات", f"info:{key}")])
        back = "menu:file_settings" if key.startswith("file_") else "menu:settings"
        rows.append([button("❌ انصراف", back)])
        title = KEY_TITLES.get(key, key)
        edit(chat, mid, f'<b>{title} را انتخاب کنید:</b>', keyboard(rows))
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
            edit(chat, mid, "🎯 <b>حجم هدف (مگابایت):</b>",
                 keyboard([[button("❌ انصراف", "cancel_input")]]))
            return
        s[key] = val; save_settings(uid, s)
        if key.startswith("file_"): file_settings_menu(chat, uid, mid)
        else: settings_menu(chat, uid, mid)
    elif parts[0] == "back" and len(parts) == 2:
        answer(cb["id"]); _render_ready(parts[1])
    elif parts[0] == "cancel_input":
        answer(cb["id"]); SESSION.pop(uid, None)
        if len(parts) == 3 and parts[1] == "clip":
            _render_ready(parts[2])
        elif len(parts) > 1:
            if parts[1] == "video": settings_menu(chat, uid, mid)
            else: file_settings_menu(chat, uid, mid)
        else: home(chat, mid)
    elif parts[0] in ("go", "cancel") and len(parts) == 2:
        job = job_by_id(parts[1])
        if not job or job["uid"] != uid: answer(cb["id"], "یافت نشد."); return
        if parts[0] == "go":
            changed = execute(
                "UPDATE jobs SET status='queued', error_msg=NULL, output_size=NULL, finished_at=NULL "
                "WHERE id=? AND status IN ('awaiting','done','failed','cancelled')", (job["id"],))
            answer(cb["id"], "✅ در صف." if changed else "امکان‌پذیر نیست.")
            if changed:
                render_batch(job["uid"], force=True)
                _render_ready(job["id"])
        else:
            changed = execute(
                "UPDATE jobs SET status='cancelled' WHERE id=? "
                "AND status IN ('downloading','awaiting','queued','running','done','failed')", (job["id"],))
            answer(cb["id"], "لغو شد." if changed else "امکان‌پذیر نیست.")
            if changed:
                render_batch(job["uid"], force=True)
                cur = job_by_id(job["id"])
                if cur and cur.get("message"):
                    edit(cur["chat"], cur["message"], f'⛔ لغو شد.')
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

    if command: SESSION.pop(uid, None)

    if command == "/start":
        home(chat); return

    if uid in SESSION and text and not text.startswith("/"):
        sess = SESSION.pop(uid); mid = sess.get("msg_id")
        if sess["action"] == "target_mb":
            try: mbv = float(text.replace(",", "").replace("MB", "").replace("mb", ""))
            except ValueError:
                SESSION[uid] = sess
                send(chat, "عدد معتبر بفرستید."); return
            if mbv <= 0:
                SESSION[uid] = sess
                send(chat, "باید مثبت باشد."); return
            s = get_settings(uid); s["quality"] = "target"; s["target_mb"] = mbv
            save_settings(uid, s)
            send(chat, f'🎯 هدف: <b>{mbv} MB</b>')
            settings_menu(chat, uid)
        elif sess["action"] == "clip_range":
            jid = sess["jid"]
            job = job_by_id(jid)
            if not job or job["uid"] != uid:
                send(chat, "کار یافت نشد."); return
            if text.strip() in ("0", "clear", "پاک", "حذف"):
                s = json.loads(job["settings"])
                s.pop("clip_start", None); s.pop("clip_duration", None)
                execute("UPDATE jobs SET settings=? WHERE id=?", (json.dumps(s), jid))
                send(chat, "✂️ بازه پاک شد.")
                _render_ready(jid); return
            try:
                start, dur = parse_clip_range(text)
            except ValueError as e:
                SESSION[uid] = sess
                send(chat, f"❌ {e}\n\nفرمت: 1:30 2:00 یا 90 120")
                return
            info = json.loads(job["media_info"]) if job.get("media_info") else None
            if info and info.get("duration"):
                td = float(info["duration"])
                if start >= td:
                    SESSION[uid] = sess
                    send(chat, f"❌ شروع باید کمتر از {human_time(td)} باشد.")
                    return
                if start + dur > td:
                    dur = td - start
                    send(chat, f"⚠️ مدت به انتها محدود شد → {human_time(dur)}.")
            s = json.loads(job["settings"])
            s["clip_start"] = start; s["clip_duration"] = dur
            execute("UPDATE jobs SET settings=? WHERE id=?", (json.dumps(s), jid))
            send(chat, f'✂️ بازه: {human_time(start)} → {human_time(start+dur)}')
            _render_ready(jid)
        return

    if command == "/settings": settings_menu(chat, uid)
    elif command == "/filesettings": file_settings_menu(chat, uid)
    elif command == "/batch": show_batch(chat, uid)
    elif command in ("/queue", "/status"): show_queue(chat, uid)
    elif command == "/stats": show_stats(chat)
    elif command == "/help": send(chat, help_text())
    elif command == "/privacy":
        send(chat, "📜 <b>سیاست داده</b>\n\n"
                   "• فایل‌ها روی سرور تا زمانی که «پاک‌سازی» را نزنید باقی می‌مانند.\n"
                   "• در دیتابیس: شناسه تلگرام، تنظیمات، نام فایل، وضعیت.\n"
                   "• حذف محلی ≠ حذف در سرور تلگرام.")
    elif command == "/cancel":
        cnt = execute("UPDATE jobs SET status='cancelled' WHERE uid=? "
                      "AND status IN ('downloading','awaiting','queued','running')", (uid,))
        send(chat, f'⛔ لغو {cnt} کار.')
    elif message.get("video"):
        receive_video(message, uid, chat)
    elif message.get("document"):
        media = message["document"]; mime = media.get("mime_type", "")
        name = (media.get("file_name") or "").lower()
        exts = (".mp4", ".mkv", ".mov", ".webm", ".avi", ".m4v", ".ts")
        if mime.startswith("video/") or name.endswith(exts): receive_video(message, uid, chat)
        else: receive_file(message, uid, chat)
    elif is_url(text):
        receive_url(chat, uid, text)
    elif message.get("audio"):
        send(chat, "🎵 صوتی — ابتدا به‌عنوان Document بفرستید.")
    elif message.get("voice"):
        send(chat, "🎙 پیام صوتی — به‌عنوان Document بفرستید.")
    elif message.get("video_note"):
        send(chat, "⭕ ویدیوی گرد — از فایل معمولی استفاده کنید.")
    elif message.get("animation"):
        send(chat, "🎞 گیف — به‌عنوان فایل بفرستید.")
    elif message.get("photo"):
        send(chat, "🖼 عکس پذیرفته نمی‌شود.")
    elif message.get("sticker"):
        send(chat, "🙂 استیکر.")
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
    missing = [i["encoder"] for i in VIDEO_CODECS.values()
               if not re.search(rf'\b{re.escape(i["encoder"])}\b', r.stdout)]
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
        if p.is_file():
            try: p.unlink()
            except Exception: pass
    execute("UPDATE jobs SET status='queued' WHERE status='running'")
    execute("UPDATE jobs SET status='queued' WHERE status='downloading'")
    identity = api("getMe")
    LOG.info(f"Starting @{identity.get('username', 'bot')}")
    api("deleteWebhook", {"drop_pending_updates": "false"})
    try:
        api("setMyCommands", {"commands": [
            {"command": "start", "description": "منوی اصلی"},
            {"command": "settings", "description": "تنظیمات ویدیو"},
            {"command": "filesettings", "description": "تنظیمات فایل"},
            {"command": "batch", "description": "مجموعه من"},
            {"command": "queue", "description": "صف کارها"},
            {"command": "stats", "description": "آمار"},
            {"command": "cancel", "description": "لغو همه"},
            {"command": "help", "description": "راهنما"},
            {"command": "privacy", "description": "حریم خصوصی"},
        ]})
    except UserError: LOG.warning("setMyCommands failed.")
    health = start_health_server()
    th = threading.Thread(target=worker, daemon=True); th.start()
    offset = int(get_kv("update_offset", "0"))
    try:
        while not STOP.is_set():
            try:
                updates = api("getUpdates",
                              {"offset": offset, "timeout": 25, "allowed_updates": ["message", "callback_query"]},
                              read_timeout=40)
                for u in updates:
                    if STOP.is_set(): break
                    try: handle_update(u)
                    except Exception as exc: LOG.warning(f"Update failed: {type(exc).__name__}")
                    finally:
                        offset = u["update_id"] + 1
                        put_kv("update_offset", offset)
            except Requeue: break
            except Exception as exc:
                LOG.warning(f"Polling failed: {type(exc).__name__}")
                STOP.wait(3)
    finally:
        STOP.set(); th.join(timeout=15); health.shutdown()


if __name__ == "__main__":
    main()
