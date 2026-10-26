"""YouTube transcript search: search what was said in YouTube channels, and jump to the moment.

Downloads the captions of a channel's videos and livestreams (not Shorts) with yt-dlp and keeps
them in SQLite with a full-text index, so the page can search what was said and link to the moment.

A fetch walks the channel's Videos and Live tabs newest first. Videos already in the database are
skipped without asking YouTube, and the walk stops at the first video older than the cutoff, so
asking for 30 days a week after a 30-day fetch only downloads the new week, and asking for 90 days
later only downloads the older part. Livestreams still in progress are left for a later run;
videos whose captions aren't ready yet (YouTube takes hours after a stream ends) are "pending"
and retried every night for PENDING_DAYS.

Every night at NIGHTLY_AT it updates yt-dlp (YouTube changes often and old versions break), then
fetches each channel's new videos and retries the pending ones. Fetches run one at a time.

Captions: the creator's English captions when there are some, otherwise YouTube's auto-generated
ones. They're stored in passages of about 15 seconds; a search matches within one passage.

HTTP on :PORT. It serves the page (web/index.html, when present) at / and the API under /api/yt/,
so it can run on its own or behind another web server that passes /api/yt/ to it.
  GET    /api/yt/search?q=&mode=words|phrase&channel=&days=&sort=new|hits
  GET    /api/yt/channels                 channels, coverage, the running job and the nightly run
  POST   /api/yt/fetch                    body {"channel": "@handle or URL", "days": 30}
  POST   /api/yt/channels/<id>/update     fetch what's new since the last update
  DELETE /api/yt/channels/<id>            remove a channel and its transcripts

Settings come from the environment:
  DATA_DIR      database, yt-dlp updates and caches (default /data)
  PORT          web port (default 8002)
  NIGHTLY_AT    local time of the nightly run (default 03:00)
  PENDING_DAYS  how long to keep retrying videos without captions (default 7)
  VIDEO_PAUSE   seconds between videos, to stay polite to YouTube (default 5)
"""

import html
import json
import os
import queue
import re
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
from datetime import date, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

DATA_DIR = os.environ.get("DATA_DIR", "/data")
DB = os.path.join(DATA_DIR, "yt.db")
PYDEPS = os.path.join(DATA_DIR, "pydeps")   # yt-dlp updates; ahead of the image's copy on PYTHONPATH
NIGHTLY_AT = os.environ.get("NIGHTLY_AT", "03:00")
PENDING_DAYS = int(os.environ.get("PENDING_DAYS", "7"))
VIDEO_PAUSE = float(os.environ.get("VIDEO_PAUSE", "5"))
PORT = int(os.environ.get("PORT", "8002"))

CREATOR_LANGS = ["en", "en-US", "en-GB", "en-CA", "en-AU"]
AUTO_LANGS = ["en-orig", "en"]              # en-orig is the speech-to-text in the spoken language
PASSAGE_SECONDS = 15
BACKOFF = [60, 180, 600]                    # seconds to wait after each "too many requests"

PAGE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "web", "index.html")   # the page, when run on its own

YTDLP_ENV = {**os.environ, "PYTHONPATH": PYDEPS, "HOME": DATA_DIR, "XDG_CACHE_HOME": os.path.join(DATA_DIR, "cache")}


def log(msg):
    print(msg, flush=True)


# ----- database -----

def db():
    conn = sqlite3.connect(DB, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db():
    os.makedirs(PYDEPS, exist_ok=True)
    with db() as conn:
        conn.execute("PRAGMA journal_mode = WAL")
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS channels (
                id TEXT PRIMARY KEY,            -- UC... channel id
                name TEXT NOT NULL,
                handle TEXT,                    -- @handle
                covered_from TEXT,              -- every video from this date ...
                covered_to TEXT,                -- ... to this date has been looked at (YYYY-MM-DD)
                last_update TEXT,               -- ISO time of the last successful fetch
                last_error TEXT
            );
            CREATE TABLE IF NOT EXISTS videos (
                id TEXT PRIMARY KEY,
                channel_id TEXT NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
                title TEXT NOT NULL,
                upload_date TEXT,               -- YYYY-MM-DD
                duration INTEGER,
                live INTEGER NOT NULL DEFAULT 0,
                captions TEXT NOT NULL,         -- creator | auto | pending | none
                fetched_at TEXT NOT NULL,
                error TEXT
            );
            CREATE INDEX IF NOT EXISTS videos_channel ON videos(channel_id, upload_date);
            CREATE VIRTUAL TABLE IF NOT EXISTS passages USING fts5(
                text, video_id UNINDEXED, start UNINDEXED, tokenize = "unicode61 remove_diacritics 2");
            CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT);
        """)
        columns = [r[1] for r in conn.execute("PRAGMA table_info(channels)")]
        if "target_from" not in columns:   # how far back was asked for, so a stopped fetch can carry on
            conn.execute("ALTER TABLE channels ADD COLUMN target_from TEXT")


def get_meta(key, default=None):
    with db() as conn:
        row = conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
    return json.loads(row[0]) if row else default


def set_meta(key, value):
    with db() as conn:
        conn.execute("INSERT OR REPLACE INTO meta VALUES (?, ?)", (key, json.dumps(value)))


def delete_video_passages(conn, video_ids):
    conn.executemany("DELETE FROM passages WHERE video_id = ?", [(v,) for v in video_ids])


# ----- yt-dlp -----

class YouTubeBlocked(Exception):
    """YouTube refused us (rate limit / bot check): stop the whole job instead of marking videos."""


BLOCKED_SIGNS = ("Sign in to confirm", "HTTP Error 429", "Too Many Requests")


def ytdlp(*args, timeout=300):
    cmd = [sys.executable, "-m", "yt_dlp", "--ignore-config", "--no-progress", *args]
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, env=YTDLP_ENV)
    if any(s in r.stderr for s in BLOCKED_SIGNS):
        raise YouTubeBlocked(last_error_line(r.stderr))
    return r


def last_error_line(stderr):
    lines = [l for l in stderr.splitlines() if l.startswith("ERROR")] or stderr.strip().splitlines() or ["unknown error"]
    return lines[-1].removeprefix("ERROR: ")[:300]


def ytdlp_version():
    try:
        return ytdlp("--version", timeout=60).stdout.strip()
    except Exception:
        return None


def update_ytdlp():
    log("updating yt-dlp")
    r = subprocess.run(
        [sys.executable, "-m", "pip", "install", "--quiet", "--no-cache-dir", "--disable-pip-version-check",
         "--upgrade", "--target", PYDEPS, "yt-dlp[default,curl-cffi]"],
        capture_output=True, text=True, timeout=600, env={**os.environ, "HOME": DATA_DIR})
    if r.returncode:
        log(f"yt-dlp update failed: {r.stderr.strip()[-300:]}")
    version = ytdlp_version()
    set_meta("ytdlp_version", version)
    log(f"yt-dlp {version}")


def normalize_channel(text):
    """@handle or a youtube.com channel URL -> canonical channel URL, or None."""
    text = text.strip()
    m = re.fullmatch(r"@?([\w.-]{3,100})", text)
    if m:
        return f"https://www.youtube.com/@{m[1]}"
    m = re.fullmatch(r"(?:https?://)?(?:www\.|m\.)?youtube\.com/(@[\w.-]+|channel/UC[\w-]{22}|c/[\w.-]+|user/[\w.-]+)"
                     r"(?:[/?#].*)?", text)
    return f"https://www.youtube.com/{m[1]}" if m else None


def resolve_channel(url):
    r = ytdlp("--flat-playlist", "--playlist-end", "1", "-J", "--", url, timeout=120)
    if r.returncode:
        raise RuntimeError(f"couldn't find that channel: {last_error_line(r.stderr)}")
    info = json.loads(r.stdout)
    channel_id = info.get("channel_id") or info.get("id")
    if not channel_id or not channel_id.startswith("UC"):
        raise RuntimeError("that doesn't look like a YouTube channel")
    return {"id": channel_id, "name": info.get("channel") or info.get("title") or channel_id,
            "handle": info.get("uploader_id") if str(info.get("uploader_id", "")).startswith("@") else None}


def list_tab(channel_id, tab):
    """Yield (video id, live status, title) from a channel tab, newest first, as yt-dlp lists them."""
    cmd = [sys.executable, "-m", "yt_dlp", "--ignore-config", "--flat-playlist", "--lazy-playlist",
           "--print", "%(id)s\t%(live_status)s\t%(title)s", "--", f"https://www.youtube.com/channel/{channel_id}/{tab}"]
    errors = tempfile.TemporaryFile("w+")                  # a file, so a chatty stderr can't block yt-dlp
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=errors, text=True, env=YTDLP_ENV)
    try:
        for line in proc.stdout:
            vid, status, title = (line.rstrip("\n").split("\t", 2) + ["", ""])[:3]
            if re.fullmatch(r"[\w-]{11}", vid):
                yield vid, status, title
        proc.wait()
        errors.seek(0)
        stderr = errors.read()
        if proc.wait() and "does not have a" not in stderr:    # no Live tab is normal
            if any(s in stderr for s in BLOCKED_SIGNS):
                raise YouTubeBlocked(last_error_line(stderr))
            raise RuntimeError(f"listing {tab}: {last_error_line(stderr)}")
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait()
        errors.close()


def ytdlp_patient(*args):
    """ytdlp(), but when YouTube says to slow down, wait a while and try again before giving up."""
    for wait in BACKOFF + [None]:
        try:
            return ytdlp(*args)
        except YouTubeBlocked:
            if wait is None:
                raise
            log(f"YouTube asked us to slow down; waiting {wait // 60} min")
            with state_lock:
                if job:
                    job["waiting"] = f"YouTube asked us to slow down; trying again in {wait // 60} min"
            time.sleep(wait)
        finally:
            with state_lock:
                if job:
                    job.pop("waiting", None)


def fetch_video(vid):
    """Captions and details of one video: (info, captions kind, [(start second, text), ...]).

    Two steps, so only the one caption track we keep is downloaded (YouTube rate-limits captions):
    the video's details first, then that track from the saved details without asking for the page again.
    """
    with tempfile.TemporaryDirectory() as tmp:
        out = os.path.join(tmp, "%(id)s.%(ext)s")
        r = ytdlp_patient("--no-playlist", "--skip-download", "--ignore-no-formats-error", "--write-info-json",
                          "--sleep-requests", "1", "-o", out, "--", f"https://www.youtube.com/watch?v={vid}")
        info_path = os.path.join(tmp, f"{vid}.info.json")
        if not os.path.exists(info_path):
            raise RuntimeError(last_error_line(r.stderr))
        with open(info_path) as f:
            info = json.load(f)
        creator, auto = info.get("subtitles") or {}, info.get("automatic_captions") or {}
        choice = (next((("creator", l) for l in CREATOR_LANGS if l in creator), None)
                  or next((("auto", l) for l in AUTO_LANGS if l in auto), None))
        if not choice:
            return info, None, []
        kind, lang = choice
        r = ytdlp_patient("--load-info-json", info_path, "--skip-download",
                          "--write-subs" if kind == "creator" else "--write-auto-subs",
                          "--sub-langs", re.escape(lang), "--sub-format", "json3", "-o", out)
        path = os.path.join(tmp, f"{vid}.{lang}.json3")
        if not os.path.exists(path):
            raise RuntimeError(last_error_line(r.stderr))
        with open(path) as f:
            return info, kind, passages(json.load(f))


def passages(json3):
    """YouTube's json3 caption events -> passages of about PASSAGE_SECONDS: [(start second, text)]."""
    out, start, words, last = [], None, [], None
    for ev in json3.get("events", []):
        text = " ".join("".join(s.get("utf8", "") for s in ev.get("segs") or []).split())
        if not text or text == last:
            continue
        last = text
        t = ev.get("tStartMs", 0) // 1000
        if start is not None and (t - start >= PASSAGE_SECONDS or sum(map(len, words)) > 400):
            out.append((start, " ".join(words)))
            start, words = None, []
        if start is None:
            start = t
        words.append(text)
    if words:
        out.append((start, " ".join(words)))
    return out


# ----- fetch jobs (one at a time, in the worker thread) -----

jobs = queue.Queue()
queued = []                 # [{"channel": id or None, "label": ...}] waiting, for the page
job = None                  # the running job, for the page
state_lock = threading.Lock()


def queue_job(kind, label, channel=None, **args):
    with state_lock:
        if channel and (any(q["channel"] == channel for q in queued) or (job and job.get("channel") == channel)):
            return False
        queued.append({"kind": kind, "label": label, "channel": channel})
    jobs.put((kind, channel, args))
    return True


def store_video(conn, vid, channel_id, info, kind, parts, title=None, error=None):
    upload = info.get("upload_date") if info else None
    upload = f"{upload[:4]}-{upload[4:6]}-{upload[6:8]}" if upload else None
    age = (date.today() - date.fromisoformat(upload)).days if upload else 0
    captions = kind or ("pending" if error is None and age <= PENDING_DAYS else "none")
    delete_video_passages(conn, [vid])
    conn.execute("INSERT OR REPLACE INTO videos VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", (
        vid, channel_id, (info or {}).get("title") or title or vid, upload, (info or {}).get("duration"),
        int((info or {}).get("live_status") in ("was_live", "post_live")), captions,
        datetime.now().isoformat(timespec="seconds"), error))
    conn.executemany("INSERT INTO passages (text, video_id, start) VALUES (?, ?, ?)",
                     [(text, vid, start) for start, text in parts])
    return captions


def fetch_channel(channel_id, cutoff):
    """Look at every video from cutoff to today; download the ones not in the database yet."""
    added = 0
    for tab in ("videos", "streams"):
        for vid, status, title in list_tab(channel_id, tab):
            if status in ("is_live", "is_upcoming"):
                continue                                   # captions come after the stream
            with db() as conn:
                known = conn.execute("SELECT upload_date FROM videos WHERE id = ?", (vid,)).fetchone()
            if known:
                if known["upload_date"] and known["upload_date"] < cutoff:
                    break
                continue
            with state_lock:
                job.update(current=title, checked=job["checked"] + 1)
            try:
                info, kind, parts = fetch_video(vid)
            except YouTubeBlocked:
                raise
            except Exception as e:
                log(f"{vid}: {e}")
                with db() as conn:
                    store_video(conn, vid, channel_id, None, None, [], title=title, error=str(e)[:300])
                continue
            upload = info.get("upload_date") or ""
            if upload and f"{upload[:4]}-{upload[4:6]}-{upload[6:8]}" < cutoff:
                break                                      # older than asked for: done with this tab
            if info.get("live_status") in ("is_live", "is_upcoming"):
                continue
            with db() as conn:
                store_video(conn, vid, channel_id, info, kind, parts)
            added += 1
            with state_lock:
                job["added"] = added
            time.sleep(VIDEO_PAUSE)
    return added


def run_fetch(channel_id, args):
    """A fetch job: resolve a new channel if need be, fetch back to the cutoff, record coverage."""
    if channel_id is None:
        info = resolve_channel(args["url"])
        channel_id = info["id"]
        with db() as conn:
            conn.execute("INSERT OR IGNORE INTO channels (id, name, handle) VALUES (?, ?, ?)",
                         (channel_id, info["name"], info["handle"]))
            conn.execute("UPDATE channels SET name = ?, handle = COALESCE(?, handle) WHERE id = ?",
                         (info["name"], info["handle"], channel_id))
        with state_lock:
            job.update(channel=channel_id, label=info["name"])
    with db() as conn:
        ch = conn.execute("SELECT * FROM channels WHERE id = ?", (channel_id,)).fetchone()
    if ch is None:
        return 0                                           # removed while queued
    if args.get("days"):
        cutoff = (date.today() - timedelta(days=args["days"])).isoformat()
        with db() as conn:
            conn.execute("UPDATE channels SET target_from = MIN(COALESCE(target_from, ?), ?) WHERE id = ?",
                         (cutoff, cutoff, channel_id))
    elif ch["target_from"] and (ch["covered_from"] is None or ch["target_from"] < ch["covered_from"]):
        cutoff = ch["target_from"]                         # an earlier fetch stopped short: carry on
    else:                                                  # "what's new": a few days' overlap
        cutoff = (date.fromisoformat(ch["covered_to"] or date.today().isoformat()) - timedelta(days=3)).isoformat()
    try:
        added = fetch_channel(channel_id, cutoff)
    except Exception as e:
        message = str(e)[:300]
        if isinstance(e, YouTubeBlocked):
            message = f"YouTube is limiting downloads from this address for now; the nightly update will carry on. ({message})"
        with db() as conn:
            conn.execute("UPDATE channels SET last_error = ? WHERE id = ?", (message, channel_id))
        raise
    with db() as conn:
        conn.execute("""UPDATE channels SET covered_from = MIN(COALESCE(covered_from, ?), ?), covered_to = ?,
                        last_update = ?, last_error = NULL WHERE id = ?""",
                     (cutoff, cutoff, date.today().isoformat(), datetime.now().isoformat(timespec="seconds"), channel_id))
    log(f"{ch['name']}: {added} videos added (back to {cutoff})")
    return added


def retry_pending():
    """Videos without captions yet: try again, and give up after PENDING_DAYS."""
    with db() as conn:
        rows = conn.execute("SELECT id, channel_id, title FROM videos WHERE captions = 'pending'").fetchall()
    found = 0
    for row in rows:
        with state_lock:
            job.update(current=row["title"])
        try:
            info, kind, parts = fetch_video(row["id"])
        except YouTubeBlocked:
            raise
        except Exception as e:
            log(f"{row['id']}: {e}")
            continue
        with db() as conn:
            found += store_video(conn, row["id"], row["channel_id"], info, kind, parts) in ("creator", "auto")
        time.sleep(VIDEO_PAUSE)
    return found


def run_nightly():
    update_ytdlp()
    with db() as conn:
        channels = conn.execute("SELECT id, name FROM channels ORDER BY name").fetchall()
    added, errors = 0, []
    for ch in channels:
        with state_lock:
            job.update(channel=ch["id"], label=ch["name"], checked=0, current=None)
        try:
            added += run_fetch(ch["id"], {})
        except YouTubeBlocked as e:
            errors.append(f"{ch['name']}: {e}")
            break
        except Exception as e:
            errors.append(f"{ch['name']}: {e}")
    with state_lock:
        job.update(channel=None, label="Captions that weren't ready", current=None)
    try:
        retry_pending()
    except Exception as e:
        errors.append(f"pending: {e}")
    with db() as conn:
        pending = conn.execute("SELECT COUNT(*) FROM videos WHERE captions = 'pending'").fetchone()[0]
    set_meta("nightly", {"at": datetime.now().isoformat(timespec="seconds"), "added": added,
                         "pending": pending, "errors": errors})


def worker():
    global job
    while True:
        kind, channel, args = jobs.get()
        with state_lock:
            entry = next((q for q in queued if q["kind"] == kind and q["channel"] == channel), None)
            if entry:
                queued.remove(entry)
            job = {"kind": kind, "channel": channel, "label": entry["label"] if entry else kind,
                   "checked": 0, "added": 0, "current": None, "started": datetime.now().isoformat(timespec="seconds")}
        try:
            if kind == "fetch":
                run_fetch(channel, args)
            elif kind == "nightly":
                run_nightly()
            elif kind == "update-ytdlp":
                update_ytdlp()
        except Exception as e:
            log(f"{kind} {job.get('label')}: {e}")
            if job.get("channel") is None and kind == "fetch":
                set_meta("last_add_error", {"label": job["label"], "error": str(e)[:300],
                                            "at": datetime.now().isoformat(timespec="seconds")})
        finally:
            with state_lock:
                job = None


def scheduler():
    if get_meta("nightly_queued") is None:                 # first start: wait for tonight, not now
        set_meta("nightly_queued", date.today().isoformat())
    while True:
        now = datetime.now()
        today = now.date().isoformat()
        if now.strftime("%H:%M") >= NIGHTLY_AT and get_meta("nightly_queued") != today:
            set_meta("nightly_queued", today)
            queue_job("nightly", "Nightly update")
        time.sleep(30)


# ----- search -----

def search(params):
    q = params.get("q", "").strip()
    words = q.split()
    if not words:
        return {"total_hits": 0, "total_videos": 0, "results": []}
    quote = lambda s: '"' + s.replace('"', '""') + '"'
    match = quote(" ".join(words)) if params.get("mode") == "phrase" else " ".join(map(quote, words))

    where, args = ["passages MATCH ?"], [match]
    if params.get("channel"):
        where.append("v.channel_id = ?")
        args.append(params["channel"])
    if params.get("days", "").isdigit():
        where.append("v.upload_date >= ?")
        args.append((date.today() - timedelta(days=int(params["days"]))).isoformat())
    with db() as conn:
        rows = conn.execute(f"""
            SELECT p.video_id, CAST(p.start AS INTEGER) AS start, highlight(passages, 0, char(2), char(3)) AS hl,
                   v.title, v.upload_date, v.live, v.captions, c.name AS channel
            FROM passages p JOIN videos v ON v.id = p.video_id JOIN channels c ON c.id = v.channel_id
            WHERE {' AND '.join(where)} LIMIT 5000""", args).fetchall()
        searched = conn.execute("SELECT COUNT(*) FROM videos WHERE captions IN ('creator', 'auto')").fetchone()[0]

    videos = {}
    for r in rows:
        v = videos.setdefault(r["video_id"], {
            "id": r["video_id"], "title": r["title"], "channel": r["channel"], "date": r["upload_date"],
            "live": bool(r["live"]), "captions": r["captions"], "hits": []})
        v["hits"].append({"t": r["start"], "html": html.escape(r["hl"]).replace("\x02", "<mark>").replace("\x03", "</mark>")})
    results = list(videos.values())
    for v in results:
        v["hits"].sort(key=lambda h: h["t"])
    if params.get("sort") == "hits":
        results.sort(key=lambda v: (len(v["hits"]), v["date"] or ""), reverse=True)
    else:
        results.sort(key=lambda v: v["date"] or "", reverse=True)
    return {"total_hits": len(rows), "total_videos": len(results), "searched": searched,
            "capped": len(rows) == 5000, "results": results[:150]}


# ----- channels for the page -----

def channels_state():
    with db() as conn:
        rows = conn.execute("""
            SELECT c.*, COUNT(v.id) AS videos,
                   SUM(v.captions = 'pending') AS pending, SUM(v.captions = 'none') AS none
            FROM channels c LEFT JOIN videos v ON v.channel_id = c.id
            GROUP BY c.id ORDER BY c.name COLLATE NOCASE""").fetchall()
        text = dict(conn.execute("""SELECT v.channel_id, SUM(LENGTH(CAST(p.text AS BLOB)))
                                    FROM passages p JOIN videos v ON v.id = p.video_id GROUP BY v.channel_id""").fetchall())
    # Disk used by the database, shared out between channels by how much transcript text each has
    # (the search index grows with the text, so that's a fair split).
    db_bytes = sum(os.path.getsize(DB + ext) for ext in ("", "-wal", "-shm") if os.path.exists(DB + ext))
    total_text = sum(text.values()) or 1
    disk = os.statvfs(DATA_DIR)
    with state_lock:
        return {
            "today": date.today().isoformat(),
            "channels": [{**dict(r), "pending": r["pending"] or 0, "none": r["none"] or 0,
                          "bytes": round(db_bytes * (text.get(r["id"]) or 0) / total_text)} for r in rows],
            "storage": {"db_bytes": db_bytes, "free_bytes": disk.f_bavail * disk.f_frsize},
            "job": dict(job) if job else None,
            "queued": [dict(q) for q in queued],
            "nightly": get_meta("nightly"),
            "nightly_at": NIGHTLY_AT,
            "last_add_error": get_meta("last_add_error"),
            "ytdlp": get_meta("ytdlp_version"),
        }


def remove_channel(channel_id):
    with state_lock:
        if job and job.get("channel") == channel_id:
            return False
        queued[:] = [q for q in queued if q["channel"] != channel_id]
    with db() as conn:
        ids = [r[0] for r in conn.execute("SELECT id FROM videos WHERE channel_id = ?", (channel_id,))]
        delete_video_passages(conn, ids)
        conn.execute("DELETE FROM channels WHERE id = ?", (channel_id,))
    return True


# ----- web -----

class Handler(BaseHTTPRequestHandler):
    def send_json(self, data, status=200):
        body = json.dumps(data).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def route(self):
        url = urlsplit(self.path)
        return url.path.removeprefix("/api/yt").rstrip("/"), {k: v[0] for k, v in parse_qs(url.query).items()}

    def body(self):
        length = int(self.headers.get("Content-Length", 0))
        data = json.loads(self.rfile.read(length) or b"{}")
        if not isinstance(data, dict):
            raise ValueError
        return data

    def send_page(self):
        try:
            with open(PAGE, "rb") as f:
                body = f.read()
        except OSError:
            self.send_json({"error": "not found"}, 404)
            return
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        path, params = self.route()
        if urlsplit(self.path).path in ("/", "/index.html"):
            self.send_page()
        elif path == "/search":
            try:
                self.send_json(search(params))
            except sqlite3.OperationalError:
                self.send_json({"error": "Couldn't search for that; try plain words."}, 400)
        elif path == "/channels":
            self.send_json(channels_state())
        else:
            self.send_json({"error": "not found"}, 404)

    def do_POST(self):
        path, _ = self.route()
        if path == "/fetch":
            try:
                data = self.body()
                days = int(data.get("days", 30))
            except (ValueError, TypeError):
                self.send_json({"error": 'body must be {"channel": "@handle", "days": 30}'}, 400)
                return
            url = normalize_channel(str(data.get("channel", "")))
            if not url or not 1 <= days <= 3650:
                self.send_json({"error": "Enter a channel as @handle or a youtube.com channel link."}, 400)
                return
            # a channel we already have (by handle) keeps its row; anything else is resolved in the job
            handle = url.removeprefix("https://www.youtube.com/")
            with db() as conn:
                row = conn.execute("SELECT id, name FROM channels WHERE lower(handle) = lower(?) OR id = ?",
                                   (handle, handle.removeprefix("channel/"))).fetchone()
            if row:
                ok = queue_job("fetch", row["name"], row["id"], days=days)
            else:
                ok = queue_job("fetch", handle, None, url=url, days=days)
            self.send_json(channels_state() if ok else {"error": "That channel is already being fetched."}, 200 if ok else 409)
        elif m := re.fullmatch(r"/channels/(UC[\w-]{22})/update", path):
            with db() as conn:
                row = conn.execute("SELECT name FROM channels WHERE id = ?", (m[1],)).fetchone()
            if not row:
                self.send_json({"error": "not found"}, 404)
            elif queue_job("fetch", row["name"], m[1]):
                self.send_json(channels_state())
            else:
                self.send_json({"error": "That channel is already being fetched."}, 409)
        else:
            self.send_json({"error": "not found"}, 404)

    def do_DELETE(self):
        path, _ = self.route()
        m = re.fullmatch(r"/channels/(UC[\w-]{22})", path)
        if not m:
            self.send_json({"error": "not found"}, 404)
        elif remove_channel(m[1]):
            self.send_json(channels_state())
        else:
            self.send_json({"error": "That channel is being fetched right now; try again when it's done."}, 409)

    def log_message(self, fmt, *args):
        pass


if __name__ == "__main__":
    init_db()
    threading.Thread(target=worker, daemon=True).start()
    threading.Thread(target=scheduler, daemon=True).start()
    queue_job("update-ytdlp", "Updating yt-dlp")
    log(f"YouTube search: data in {DATA_DIR}, nightly at {NIGHTLY_AT}; web on :{PORT}")
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
