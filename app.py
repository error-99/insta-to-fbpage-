"""
FB Auto Publisher — production web service (Flask + gunicorn) for Render.

Public      GET  /health               liveness (always cheap)
Protected   GET  /                     dashboard            (basic auth)
            GET  /status               JSON status + logs   (basic auth)
            GET  /health?deep=1        API + FB token check (basic auth or RUN_KEY) -> 503 if degraded
            POST /run                  start a run          (basic auth or RUN_KEY, header X-Run-Key or ?key=)

Per run
  1. fetch hotbuddy_fb + soulofmemori_fb, merge, de-dupe by id
  2. sender 3997461173882085 -> schedule FB post, then POST {"id":X,"hotbuddy_fb":true}
     sender 1482699040386498 -> POST {"id":X,"soulofmemori_fb":true}   (no FB post)
"""
import os
import sys
import json
import time
import hmac
import logging
import tempfile
import threading
import collections
import datetime as dt

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from flask import Flask, jsonify, request, Response

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


# ═══════════════════════ CONFIG ═══════════════════════
def _int(name, default):
    try:
        return int(os.environ.get(name, default))
    except ValueError:
        raise RuntimeError(f"Env var {name} must be an integer")


def _flag(name, default="0"):
    return os.environ.get(name, default).strip().lower() in ("1", "true", "yes", "on")


API_URL          = os.environ.get("API_URL", "https://shabbir.serv00.net/Fbtools/memoriesbangla/api.php")
HTTP_TIMEOUT     = _int("HTTP_TIMEOUT", 30)
FB_API_VERSION   = os.environ.get("FB_API_VERSION", "v20.0")
GRAPH            = f"https://graph.facebook.com/{FB_API_VERSION}"

RUN_KEY          = os.environ.get("RUN_KEY", "")
DASH_USER        = os.environ.get("DASH_USER", "admin")
DASH_PASSWORD    = os.environ.get("DASH_PASSWORD", "")

ENABLE_SCHEDULER = _flag("ENABLE_SCHEDULER")
RUN_EVERY_MIN    = max(1, _int("RUN_EVERY_MIN", 120))
DELAY_SEC        = _int("SCHEDULE_DELAY_SEC", 3600)
STAGGER_SEC      = _int("SCHEDULE_STAGGER_SEC", 600)
MAX_ITEMS        = max(1, _int("MAX_ITEMS_PER_RUN", 20))
DRY_RUN          = _flag("DRY_RUN")
STATE_FILE       = os.environ.get("STATE_FILE", "/tmp/posted_ids.json")
LOG_LEVEL        = os.environ.get("LOG_LEVEL", "INFO").upper()

CATEGORIES = ["hotbuddy_fb", "soulofmemori_fb"]
FB_PAGES = {
    "hotbuddy_fb": {
        "name":    os.environ.get("FB_HOTBUDDY_PAGE_NAME", "HOT BADDY"),
        "page_id": os.environ.get("FB_HOTBUDDY_PAGE_ID", "1455823914281416"),
        "token":   os.environ.get("FB_HOTBUDDY_TOKEN", ""),
    },
    "soulofmemori_fb": {
        "name":    "Soul of memories",
        "page_id": os.environ.get("FB_SOULOFMEMORI_PAGE_ID", "1359221437281984"),
        "token":   os.environ.get("FB_SOULOFMEMORI_TOKEN", ""),
    },
}
MARK_ONLY_SENDERS    = {"1482699040386498"}
UPLOAD_SENDERS       = {"3997461173882085"}
MARK_FIELD_BY_SENDER = {"1482699040386498": "soulofmemori_fb", "3997461173882085": "hotbuddy_fb"}
SENDER_TO_CATEGORY   = {"3997461173882085": "hotbuddy_fb"}
FOLLOW_TEMPLATE      = "\n\nFollow my page: @{page_name}"

# ── fail fast on bad config ──
_missing = [k for k, v in (("FB_HOTBUDDY_TOKEN", FB_PAGES["hotbuddy_fb"]["token"]),
                           ("RUN_KEY", RUN_KEY), ("DASH_PASSWORD", DASH_PASSWORD)) if not v]
if _missing and not os.environ.get("ALLOW_MISSING_ENV"):
    raise RuntimeError("Missing required env vars: " + ", ".join(_missing))
if DELAY_SEC < 600:
    raise RuntimeError("SCHEDULE_DELAY_SEC must be >= 600 (Facebook minimum is 10 minutes)")
if len(RUN_KEY) < 16 and RUN_KEY:
    raise RuntimeError("RUN_KEY should be at least 16 characters")


# ═══════════════════════ LOGGING (secrets redacted) ═══════════════════════
SECRETS = [s for s in (RUN_KEY, DASH_PASSWORD, FB_PAGES["hotbuddy_fb"]["token"],
                       FB_PAGES["soulofmemori_fb"]["token"]) if s]
LOGS = collections.deque(maxlen=500)
logging.basicConfig(stream=sys.stdout, level=getattr(logging, LOG_LEVEL, logging.INFO),
                    format="%(asctime)s %(levelname)s %(message)s")
_logger = logging.getLogger("fbpub")


def _redact(msg: str) -> str:
    for s in SECRETS:
        msg = msg.replace(s, "***")
    return msg


def log(msg: str, level=logging.INFO) -> None:
    msg = _redact(str(msg))
    _logger.log(level, msg)
    LOGS.append(f"{now_iso()}  {msg}")


def now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ═══════════════════════ HTTP SESSIONS ═══════════════════════
def _session(retry_post: bool) -> requests.Session:
    s = requests.Session()
    methods = frozenset(["GET", "POST"]) if retry_post else frozenset(["GET"])
    retry = Retry(total=3, backoff_factor=1.5, status_forcelist=(429, 500, 502, 503, 504),
                  allowed_methods=methods, raise_on_status=False)
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.mount("http://", HTTPAdapter(max_retries=retry))
    s.headers["User-Agent"] = "fb-publisher/2.0"
    return s


API_S = _session(retry_post=True)    # your API: mark-done is idempotent, safe to retry
FB_S  = _session(retry_post=False)   # Facebook: never auto-retry a POST (would duplicate posts)


# ═══════════════════════ STATE ═══════════════════════
app        = Flask(__name__)
START_TIME = time.time()
RUN_LOCK   = threading.Lock()
STATE_LOCK = threading.Lock()
POSTED_IDS = set()
LAST_RUN   = {"started": None, "finished": None, "trigger": None, "dry_run": DRY_RUN, "found": 0,
              "posted": 0, "marked": 0, "skipped": 0, "failed": 0, "error": None}


def load_state() -> None:
    try:
        with open(STATE_FILE) as f:
            POSTED_IDS.update(str(i) for i in json.load(f))
        log(f"state loaded: {len(POSTED_IDS)} posted id(s) from {STATE_FILE}")
    except FileNotFoundError:
        pass
    except Exception as e:
        log(f"⚠ could not load state: {e}", logging.WARNING)


def save_state() -> None:
    try:
        with STATE_LOCK:
            os.makedirs(os.path.dirname(STATE_FILE) or ".", exist_ok=True)
            fd, tmp = tempfile.mkstemp(dir=os.path.dirname(STATE_FILE) or ".")
            with os.fdopen(fd, "w") as f:
                json.dump(sorted(POSTED_IDS)[-5000:], f)
            os.replace(tmp, STATE_FILE)
    except Exception as e:
        log(f"⚠ could not save state: {e}", logging.WARNING)


load_state()


# ═══════════════════════ API HELPERS ═══════════════════════
def is_video(url: str) -> bool:
    return url.split("?")[0].lower().endswith((".mp4", ".mov", ".m4v"))


def fetch_pending() -> list:
    seen, items = set(), []
    for cat in CATEGORIES:
        try:
            r = API_S.get(API_URL, params={"category": cat}, timeout=HTTP_TIMEOUT)
            r.raise_for_status()
            body = r.json()
        except Exception as e:
            log(f"⚠ fetch failed [{cat}]: {e}", logging.WARNING)
            continue
        if body.get("status") != "success":
            log(f"⚠ [{cat}] API status: {body.get('status')}", logging.WARNING)
            continue
        data = body.get("data", []) or []
        log(f"[{cat}] {len(data)} pending")
        for it in data:
            key = str(it.get("id"))
            if key not in seen:
                seen.add(key)
                items.append(it)
    return items


def mark_done(item_id, field: str) -> bool:
    if DRY_RUN:
        log(f"[{item_id}] DRY_RUN would POST {{id:{item_id}, {field}:true}}")
        return True
    try:
        r = API_S.post(API_URL, json={"id": item_id, field: True}, timeout=HTTP_TIMEOUT)
        if r.status_code != 200:
            log(f"⚠ mark_done HTTP {r.status_code}: {r.text[:200]}", logging.WARNING)
        return r.status_code == 200
    except Exception as e:
        log(f"⚠ mark_done error: {e}", logging.WARNING)
        return False


def download_to_temp(url: str):
    fd, path = tempfile.mkstemp(suffix=".mp4" if is_video(url) else ".jpg", prefix="fb_pub_")
    os.close(fd)
    try:
        with API_S.get(url, stream=True, timeout=180) as r:
            r.raise_for_status()
            with open(path, "wb") as f:
                for chunk in r.iter_content(1 << 15):
                    if chunk:
                        f.write(chunk)
        if os.path.getsize(path) == 0:
            raise ValueError("empty file")
        return path
    except Exception as e:
        log(f"⚠ download failed: {e}", logging.WARNING)
        try:
            os.unlink(path)
        except OSError:
            pass
        return None


# ═══════════════════════ FACEBOOK ═══════════════════════
def _json(r):
    try:
        return r.json()
    except Exception:
        return {"error": {"message": r.text[:300]}}


def fb_err(resp) -> dict:
    e = resp.get("error") if isinstance(resp, dict) else None
    return e if isinstance(e, dict) else {"message": str(e or resp)[:300]}


def fb_post_photo(page_id, token, url, caption, publish_at) -> dict:
    data = {"access_token": token, "url": url, "message": caption,
            "published": "false", "scheduled_publish_time": publish_at}
    return _json(FB_S.post(f"{GRAPH}/{page_id}/photos", data=data, timeout=60))


def fb_post_video(page_id, token, url, caption, publish_at) -> dict:
    data = {"access_token": token, "file_url": url, "description": caption,
            "published": "false", "scheduled_publish_time": publish_at}
    resp = _json(FB_S.post(f"{GRAPH}/{page_id}/videos", data=data, timeout=180))
    if "id" in resp:
        return resp
    log(f"⚠ file_url rejected ({fb_err(resp).get('message')}); downloading from R2…", logging.WARNING)
    tmp = download_to_temp(url)
    if not tmp:
        return resp
    try:
        data.pop("file_url", None)
        with open(tmp, "rb") as f:
            return _json(FB_S.post(f"{GRAPH}/{page_id}/videos", data=data,
                                   files={"source": (os.path.basename(tmp), f, "video/mp4")}, timeout=600))
    finally:
        try:
            os.unlink(tmp)
        except OSError:
            pass


# ═══════════════════════ PROCESSING ═══════════════════════
def process_item(item: dict, slot: int, ctx: dict) -> str:
    """returns posted | marked | skipped | failed"""
    item_id   = item.get("id")
    sender    = str(item.get("sender_id", ""))
    media_url = (item.get("cloudflermediaurl") or "").strip()
    caption   = (item.get("caption") or "").strip()
    log(f"[{item_id}] type={item.get('type', '?')} sender={sender}")

    if not media_url:
        log(f"[{item_id}] ⚠ no cloudflermediaurl — skipped", logging.WARNING)
        return "skipped"

    if sender in MARK_ONLY_SENDERS:
        field = MARK_FIELD_BY_SENDER[sender]
        ok = mark_done(item_id, field)
        log(f"[{item_id}] {'✅' if ok else '❌'} mark {field}")
        return "marked" if ok else "failed"

    if sender in UPLOAD_SENDERS:
        category, field = SENDER_TO_CATEGORY[sender], MARK_FIELD_BY_SENDER[sender]
        page = FB_PAGES[category]

        if str(item_id) in POSTED_IDS:
            log(f"[{item_id}] already posted earlier — retrying mark only")
            return "marked" if mark_done(item_id, field) else "failed"

        if ctx.get("fb_abort"):
            log(f"[{item_id}] ⏭ skipped: Facebook aborted earlier this run (token problem)", logging.WARNING)
            return "failed"

        caption_out = (caption + FOLLOW_TEMPLATE.format(page_name=page["name"])).strip()
        publish_at  = int(time.time()) + DELAY_SEC + slot * STAGGER_SEC
        publish_at  = min(publish_at, int(time.time()) + 74 * 86400)        # FB max 75 days
        kind        = "VIDEO" if is_video(media_url) else "PHOTO"
        when        = dt.datetime.fromtimestamp(publish_at, dt.timezone.utc).strftime("%H:%M")

        if DRY_RUN:
            log(f"[{item_id}] DRY_RUN would schedule {kind} → {page['name']} @ {when}Z, then mark {field}")
            return "posted"

        log(f"[{item_id}] 🚀 scheduling {kind} → {page['name']} @ {when}Z")
        fn   = fb_post_video if kind == "VIDEO" else fb_post_photo
        resp = fn(page["page_id"], page["token"], media_url, caption_out, publish_at)

        if "id" in resp or "post_id" in resp:
            POSTED_IDS.add(str(item_id))
            save_state()
            log(f"[{item_id}] ✅ FB scheduled id={resp.get('id') or resp.get('post_id')}")
            ok = mark_done(item_id, field)
            log(f"[{item_id}] {'✅' if ok else '❌'} mark {field}")
            return "posted" if ok else "failed"

        err = fb_err(resp)
        log(f"[{item_id}] ❌ FB failed: code={err.get('code')} msg={err.get('message')}", logging.ERROR)
        if err.get("code") in (190, 102, 463, 467):                          # token expired/invalid
            ctx["fb_abort"] = True
            log("🔑 Facebook token invalid/expired — regenerate FB_HOTBUDDY_TOKEN", logging.CRITICAL)
        return "failed"

    log(f"[{item_id}] ⚠ unknown sender {sender!r} — skipped", logging.WARNING)
    return "skipped"


def run_once(trigger: str) -> None:
    if not RUN_LOCK.acquire(blocking=False):
        log(f"run ({trigger}) ignored — already running")
        return
    try:
        LAST_RUN.update(started=now_iso(), finished=None, trigger=trigger, dry_run=DRY_RUN,
                        found=0, posted=0, marked=0, skipped=0, failed=0, error=None)
        log(f"═══ run start ({trigger}){' [DRY RUN]' if DRY_RUN else ''} ═══")
        items = fetch_pending()
        LAST_RUN["found"] = len(items)
        if len(items) > MAX_ITEMS:
            log(f"⚠ {len(items)} pending, capping at MAX_ITEMS_PER_RUN={MAX_ITEMS}", logging.WARNING)
            items = items[:MAX_ITEMS]
        ctx, slot = {}, 0
        for item in items:
            try:
                res = process_item(item, slot, ctx)
                if str(item.get("sender_id", "")) in UPLOAD_SENDERS:
                    slot += 1
                LAST_RUN[res] += 1
            except Exception as e:
                LAST_RUN["failed"] += 1
                log(f"❌ unhandled error id={item.get('id')}: {e}", logging.ERROR)
        log(f"═══ run done: found={LAST_RUN['found']} posted={LAST_RUN['posted']} marked={LAST_RUN['marked']} "
            f"skipped={LAST_RUN['skipped']} failed={LAST_RUN['failed']} ═══")
    except Exception as e:
        LAST_RUN["error"] = str(e)
        log(f"❌ run crashed: {e}", logging.ERROR)
    finally:
        LAST_RUN["finished"] = now_iso()
        RUN_LOCK.release()


def scheduler_loop():
    time.sleep(20)
    while True:
        run_once("scheduler")
        time.sleep(RUN_EVERY_MIN * 60)


if ENABLE_SCHEDULER:
    threading.Thread(target=scheduler_loop, daemon=True, name="scheduler").start()
    log(f"built-in scheduler on: every {RUN_EVERY_MIN} min")


# ═══════════════════════ AUTH + HEADERS ═══════════════════════
def _eq(a: str, b: str) -> bool:
    return hmac.compare_digest((a or "").encode(), (b or "").encode())


def basic_ok() -> bool:
    a = request.authorization
    return bool(a and a.username is not None and _eq(a.username, DASH_USER) and _eq(a.password, DASH_PASSWORD))


def key_ok() -> bool:
    k = request.headers.get("X-Run-Key") or request.args.get("key", "")
    return bool(RUN_KEY) and _eq(k, RUN_KEY)


def deny():
    return Response("Authentication required", 401, {"WWW-Authenticate": 'Basic realm="FB Publisher"'})


@app.after_request
def secure(resp):
    resp.headers["X-Content-Type-Options"] = "nosniff"
    resp.headers["X-Frame-Options"] = "DENY"
    resp.headers["Referrer-Policy"] = "no-referrer"
    resp.headers["Cache-Control"] = "no-store"
    resp.headers["Content-Security-Policy"] = "default-src 'self'; style-src 'unsafe-inline'; script-src 'unsafe-inline'"
    return resp


# ═══════════════════════ ROUTES ═══════════════════════
@app.route("/health")
def health():
    out = {"status": "ok", "uptime_sec": int(time.time() - START_TIME), "running": RUN_LOCK.locked()}
    if request.args.get("deep") != "1":
        return jsonify(out), 200
    if not (basic_ok() or key_ok()):
        return deny()

    deep = {}
    try:
        r = API_S.get(API_URL, params={"category": CATEGORIES[0]}, timeout=15)
        deep["api"] = "ok" if r.status_code == 200 else f"http {r.status_code}"
    except Exception as e:
        deep["api"] = f"error: {e}"
    for cat, p in FB_PAGES.items():
        if not p["token"]:
            deep[cat] = "no token (skipped)"
            continue
        try:
            j = _json(FB_S.get(f"{GRAPH}/{p['page_id']}", params={"fields": "name", "access_token": p["token"]}, timeout=15))
            deep[cat] = f"ok ({j['name']})" if "name" in j else f"error: {fb_err(j).get('message')}"
        except Exception as e:
            deep[cat] = f"error: {e}"
    bad = [k for k, v in deep.items() if not (str(v).startswith("ok") or "skipped" in str(v))]
    out.update(deep=deep, last_run=LAST_RUN, dry_run=DRY_RUN,
               scheduler={"enabled": ENABLE_SCHEDULER, "every_min": RUN_EVERY_MIN},
               status="degraded" if bad else "ok")
    return jsonify(_json_safe(out)), (503 if bad else 200)


def _json_safe(o):
    return json.loads(_redact(json.dumps(o)))


@app.route("/run", methods=["GET", "POST"])
def run_route():
    if not (basic_ok() or key_ok()):
        return deny()
    if RUN_LOCK.locked():
        return jsonify({"started": False, "reason": "already running"}), 409
    threading.Thread(target=run_once, args=("manual/cron",), daemon=True).start()
    return jsonify({"started": True, "dry_run": DRY_RUN}), 202


@app.route("/status")
def status():
    if not basic_ok():
        return deny()
    return jsonify({"last_run": LAST_RUN, "running": RUN_LOCK.locked(), "dry_run": DRY_RUN,
                    "uptime_sec": int(time.time() - START_TIME), "logs": list(LOGS)[-150:]})


DASH = """<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>FB Publisher</title>
<style>
:root{--bg:#fff;--fg:#1a1a1a;--card:#f4f4f5;--mut:#666;--ok:#15803d;--warn:#b45309}
@media(prefers-color-scheme:dark){:root{--bg:#111;--fg:#eee;--card:#1c1c1e;--mut:#999;--ok:#4ade80;--warn:#fbbf24}}
body{background:var(--bg);color:var(--fg);font:14px/1.5 system-ui,sans-serif;max-width:920px;margin:auto;padding:16px}
.card{background:var(--card);border-radius:10px;padding:14px;margin-bottom:14px}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(100px,1fr));gap:10px}
.n{font-size:24px;font-weight:600}.l{color:var(--mut);font-size:12px}
button{padding:8px 14px;border-radius:8px;border:0;background:#2563eb;color:#fff;font-size:14px;cursor:pointer}
pre{max-height:440px;overflow:auto;white-space:pre-wrap;word-break:break-word;font-size:12px;margin:0}
.badge{font-size:12px;padding:2px 8px;border-radius:99px;background:var(--warn);color:#000;margin-left:8px}
</style></head><body>
<h2>FB Publisher <span id="st" class="l"></span><span id="dry" class="badge" hidden>DRY RUN</span></h2>
<div class="card grid" id="stats"></div>
<div class="card"><button onclick="runNow()">Run now</button>
<a class="l" href="/health?deep=1" target="_blank" style="margin-left:12px">deep health check</a></div>
<div class="card"><pre id="logs">loading…</pre></div>
<script>
async function load(){
 try{const r=await fetch('/status');if(!r.ok)return;const j=await r.json();const L=j.last_run;
 document.getElementById('st').textContent=j.running?'● running':'● idle';
 document.getElementById('dry').hidden=!j.dry_run;
 const s=[['found',L.found],['posted',L.posted],['marked',L.marked],['skipped',L.skipped],['failed',L.failed]];
 document.getElementById('stats').innerHTML=s.map(x=>'<div><div class="n">'+x[1]+'</div><div class="l">'+x[0]+'</div></div>').join('')
  +'<div style="grid-column:1/-1" class="l">last run: '+(L.started||'never')+' → '+(L.finished||'…')+' ('+(L.trigger||'-')+')</div>';
 const el=document.getElementById('logs');const atEnd=el.scrollTop+el.clientHeight>=el.scrollHeight-20;
 el.textContent=j.logs.join('\\n')||'no logs yet';if(atEnd)el.scrollTop=el.scrollHeight;}catch(e){}
}
async function runNow(){const r=await fetch('/run',{method:'POST'});
 alert(r.status==202?'Started':r.status==409?'Already running':'Failed ('+r.status+')');load();}
load();setInterval(load,5000);
</script></body></html>"""


@app.route("/")
def dashboard():
    if not basic_ok():
        return deny()
    return Response(DASH, mimetype="text/html")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=_int("PORT", 10000))
