# C1C – WelcomeCrew (v15)
# - Live watchers for welcome & promo threads:
#   * Detect "Ticket Closed by", infer/ask for clantag, rename (welcome) and log
# - Forgiving parsers; F-IT/multi-part tags; clanlist tags from column B (configurable)
# - Throttled Sheets writes + backoff; live backfill + stop; auto attach details

import os, json, re, asyncio, time, io, random
from datetime import datetime, timezone as _tz
from typing import Optional, Tuple, Dict, Any, List

import discord
from discord.ext import commands
import gspread
from gspread.exceptions import APIError

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

# ---------- Flags / Env ----------
def env_bool(key: str, default: bool=True) -> bool:
    raw = (os.getenv(key) or "").strip().upper()
    if raw == "": return default
    return raw == "ON"

TOKEN = os.getenv("DISCORD_TOKEN") or os.getenv("TOKEN") or ""
WELCOME_CHANNEL_ID = int(os.getenv("WELCOME_CHANNEL_ID", "0"))
PROMO_CHANNEL_ID   = int(os.getenv("PROMO_CHANNEL_ID", "0"))
GSHEET_ID          = os.getenv("GSHEET_ID", "")
TIMEZONE           = os.getenv("TIMEZONE", "UTC")
SHEET1_NAME        = os.getenv("SHEET1_NAME", "Sheet1")
SHEET4_NAME        = os.getenv("SHEET4_NAME", "Sheet4")
CLANLIST_TAB_NAME  = os.getenv("CLANLIST_TAB_NAME", "clanlist")
CLANLIST_TAG_COLUMN = int(os.getenv("CLANLIST_TAG_COLUMN", "2"))  # 1-based; default B

SHEETS_THROTTLE_MS = int(os.getenv("SHEETS_THROTTLE_MS", "200"))

# Feature toggles (commands)
ENABLE_INFER_TAG_FROM_THREAD = env_bool("ENABLE_INFER_TAG_FROM_THREAD", True)
ENABLE_WELCOME_SCAN        = env_bool("ENABLE_WELCOME_SCAN", True)
ENABLE_PROMO_SCAN          = env_bool("ENABLE_PROMO_SCAN", True)
ENABLE_CMD_SHEETSTATUS     = env_bool("ENABLE_CMD_SHEETSTATUS", True)
ENABLE_CMD_BACKFILL        = env_bool("ENABLE_CMD_BACKFILL", True)
ENABLE_CMD_BACKFILL_STATUS = env_bool("ENABLE_CMD_BACKFILL_STATUS", True)
ENABLE_CMD_DEDUPE          = env_bool("ENABLE_CMD_DEDUPE", True)
ENABLE_CMD_RELOAD          = env_bool("ENABLE_CMD_RELOAD", True)
ENABLE_CMD_HEALTH          = env_bool("ENABLE_CMD_HEALTH", True)
ENABLE_CMD_PING            = env_bool("ENABLE_CMD_PING", True)
ENABLE_CMD_CHECKSHEET      = env_bool("ENABLE_CMD_CHECKSHEET", True)
ENABLE_CMD_REBOOT          = env_bool("ENABLE_CMD_REBOOT", True)
ENABLE_WEB_SERVER          = env_bool("ENABLE_WEB_SERVER", True)

# Live watchers
ENABLE_LIVE_WATCH          = env_bool("ENABLE_LIVE_WATCH", True)
ENABLE_LIVE_WATCH_WELCOME  = env_bool("ENABLE_LIVE_WATCH_WELCOME", True)
ENABLE_LIVE_WATCH_PROMO    = env_bool("ENABLE_LIVE_WATCH_PROMO", True)

# Auto-post results after backfill
AUTO_POST_BACKFILL_DETAILS = env_bool("AUTO_POST_BACKFILL_DETAILS", True)
POST_BACKFILL_SUMMARY      = env_bool("POST_BACKFILL_SUMMARY", False)

# ---------- Discord ----------
intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents, help_command=None)

START_TS = time.time()
def uptime_str():
    s = int(time.time() - START_TS); h, s = divmod(s,3600); m, s = divmod(s,60)
    return f"{h:02d}:{m:02d}:{s:02d}"

def fmt_tz(dt: datetime) -> str:
    try:
        if ZoneInfo:
            tz = ZoneInfo(TIMEZONE) if TIMEZONE else ZoneInfo("UTC")
            return dt.astimezone(tz).strftime("%Y-%m-%d %H:%M")
    except Exception:
        pass
    return (dt or datetime.utcnow().replace(tzinfo=_tz.utc)).strftime("%Y-%m-%d %H:%M")

def _print_boot_info():
    print("=== WelcomeCrew v15 boot ===", flush=True)
    print(f"Sheets: {SHEET1_NAME} / {SHEET4_NAME} / clanlist:{CLANLIST_TAB_NAME} (tags col={CLANLIST_TAG_COLUMN})", flush=True)
    print(f"Welcome={WELCOME_CHANNEL_ID} Promo={PROMO_CHANNEL_ID}", flush=True)
    print(f"TZ={TIMEZONE} | infer-from-thread={ENABLE_INFER_TAG_FROM_THREAD}", flush=True)
    print(f"LiveWatch: {ENABLE_LIVE_WATCH} (welcome={ENABLE_LIVE_WATCH_WELCOME}, promo={ENABLE_LIVE_WATCH_PROMO})", flush=True)

# ---------- Sheets ----------
_gs_client = None
_ws_cache: Dict[str, Any] = {}
_index_simple: Dict[str, Dict[str,int]] = {}  # Sheet1: ticket -> row
_index_promo:  Dict[str, Dict[str,int]] = {}  # Sheet4: ticket||type||created -> row

HEADERS_SHEET1 = ["ticket number","username","clantag","date closed"]
HEADERS_SHEET4 = ["ticket number","username","clantag","date closed","type","thread created"]

def service_account_email() -> str:
    try:
        data = json.loads(os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON") or "{}")
        return data.get("client_email","")
    except Exception:
        return ""

def gs_client():
    global _gs_client
    if _gs_client is None:
        raw = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON") or ""
        if not raw: raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON not set")
        _gs_client = gspread.service_account_from_dict(json.loads(raw))
    return _gs_client

def get_ws(name: str, want_headers: List[str]):
    if not GSHEET_ID: raise RuntimeError("GSHEET_ID not set")
    if name in _ws_cache: return _ws_cache[name]
    sh = gs_client().open_by_key(GSHEET_ID)
    try:
        ws = sh.worksheet(name)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=name, rows=4000, cols=max(10,len(want_headers)))
        ws.append_row(want_headers)
    else:
        try:
            head = ws.row_values(1)
            if [h.strip().lower() for h in head] != [h.strip().lower() for h in want_headers]:
                ws.update("A1", [want_headers])
        except Exception:
            pass
    _ws_cache[name] = ws
    return ws

# ---------- Rate limit helpers ----------
def _sleep_ms(ms:int):
    if ms > 0:
        time.sleep(ms/1000.0)

def _with_backoff(callable_fn, *a, **k):
    delay = 0.5
    for attempt in range(6):
        try:
            return callable_fn(*a, **k)
        except APIError as e:
            if "429" in str(e):
                _sleep_ms(int(delay*1000 + random.randint(0,200)))
                delay *= 2
                continue
            raise

# ---------- Clanlist & tag matching ----------
_clan_tags_cache: List[str] = []
_clan_tags_norm_set: set = set()
_last_clan_fetch = 0.0
_tag_regex_cache = None
_tag_regex_key = ""

def _normalize_dashes(s: str) -> str:
    return re.sub(r"[\u2010\u2011\u2012\u2013\u2014\u2015]", "-", s or "")

def _fmt_ticket(s: str) -> str:
    return (s or "").strip().lstrip("#").zfill(4)

def _load_clan_tags(force: bool=False) -> List[str]:
    global _clan_tags_cache, _clan_tags_norm_set, _last_clan_fetch, _tag_regex_cache, _tag_regex_key
    now = time.time()
    if not force and _clan_tags_cache and (now - _last_clan_fetch < 300):
        return _clan_tags_cache

    tags: List[str] = []
    try:
        sh = gs_client().open_by_key(GSHEET_ID)
        ws = sh.worksheet(CLANLIST_TAB_NAME)
        values = ws.get_all_values() or []
        if values:
            header = [h.strip().lower() for h in values[0]] if values else []
            col_idx = None
            for key in ("clantag", "tag", "abbr", "code"):
                if key in header:
                    col_idx = header.index(key)
                    break
            if col_idx is None:
                col_idx = max(0, CLANLIST_TAG_COLUMN - 1)
            for row in values[1:]:
                cell = row[col_idx] if col_idx < len(row) else ""
                t = _normalize_dashes(cell).strip().upper()
                if t:
                    tags.append(t)

        _clan_tags_cache = list(dict.fromkeys(tags))
        _clan_tags_norm_set = { _normalize_dashes(t).upper() for t in _clan_tags_cache }
        _last_clan_fetch = now

        parts = sorted((_normalize_dashes(t).upper() for t in _clan_tags_cache), key=len, reverse=True)
        if parts:
            alt = "|".join(re.escape(p) for p in parts)
            _tag_regex_cache = re.compile(rf"(?<![A-Za-z0-9_])(?:{alt})(?![A-Za-z0-9_])", re.IGNORECASE)
            _tag_regex_key = "|".join(parts)
        else:
            _tag_regex_cache = None
            _tag_regex_key = ""
    except Exception as e:
        print("Failed to load clanlist:", e, flush=True)
        _clan_tags_cache = []; _clan_tags_norm_set = set(); _tag_regex_cache = None; _tag_regex_key = ""
    return _clan_tags_cache

def _match_tag_in_text(text: str) -> Optional[str]:
    if not text: return None
    _load_clan_tags(False)
    if not _tag_regex_cache: return None
    s = _normalize_dashes(text).upper()
    m = _tag_regex_cache.search(s)
    return m.group(0).upper() if m else None

def _pick_tag_by_suffix(remainder: str, known_tags: List[str]) -> Optional[Tuple[str, str]]:
    s = _normalize_dashes(remainder).strip()
    parts = [p for p in s.split("-") if p != ""]
    if not parts:
        return None
    norm_tags = _clan_tags_norm_set or { _normalize_dashes(t).upper() for t in known_tags }
    max_k = min(3, len(parts))
    for k in range(max_k, 0, -1):
        cand = "-".join(parts[-k:]).upper()
        if cand in norm_tags:
            username = "-".join(parts[:-k])
            return (username.strip(), cand)
    return None

# ---------- Indexers ----------
def _key_promo(ticket: str, typ: str, created: str) -> str:
    return f"{_fmt_ticket(ticket)}||{(typ or '').strip().lower()}||{(created or '').strip()}"

def ws_index_welcome(name: str, ws) -> Dict[str,int]:
    idx = {}
    try:
        colA = ws.col_values(1)[1:]
        for i, val in enumerate(colA, start=2):
            t = _fmt_ticket(val)
            if t: idx[t] = i
    except Exception: pass
    _index_simple[name] = idx
    return idx

def ws_index_promo(name: str, ws) -> Dict[str,int]:
    idx = {}
    try:
        values = ws.get_all_values()
        if not values: return {}
        header = [h.strip().lower() for h in values[0]]
        col_ticket  = header.index("ticket number") if "ticket number" in header else 0
        col_type    = header.index("type") if "type" in header else 4
        col_created = header.index("thread created") if "thread created" in header else 5
        for r_i, row in enumerate(values[1:], start=2):
            t   = _fmt_ticket(row[col_ticket]  if col_ticket  < len(row) else "")
            typ = (row[col_type]    if col_type    < len(row) else "").strip().lower()
            cr  = (row[col_created] if col_created < len(row) else "").strip()
            if t:
                idx[_key_promo(t, typ, cr)] = r_i
    except Exception: pass
    _index_promo[name] = idx
    return idx

# ---------- Diff helpers ----------
def _calc_diffs(header: List[str], before: List[str], after: List[str]) -> List[str]:
    diffs = []
    for i, col in enumerate(header):
        old = (before[i] if i < len(before) else "").strip()
        new = (after[i]  if i < len(after)  else "").strip()
        if old != new:
            diffs.append(f"{col}: '{old}' → '{new}'")
    return diffs

# ---------- Backfill state ----------
def _new_bucket():
    return {
        "scanned":0,"added":0,"updated":0,"skipped":0,
        "added_ids":[], "updated_ids":[], "skipped_ids":[],
        "updated_details":[],
        "skipped_reasons":{}
    }

backfill_state = {
    "running": False,
    "welcome": _new_bucket(),
    "promo":   _new_bucket(),
    "last_msg": ""
}

# ---------- Upserts (throttled + backoff) ----------
def upsert_welcome(name: str, ws, ticket: str, rowvals: List[str], st_bucket: dict) -> str:
    ticket = _fmt_ticket(ticket)
    idx = _index_simple.get(name) or ws_index_welcome(name, ws)
    header = HEADERS_SHEET1
    try:
        if ticket in idx and idx[ticket] > 0:
            row = idx[ticket]
            before = _with_backoff(ws.row_values, row)
            rng = f"A{row}:{chr(ord('A')+len(rowvals)-1)}{row}"
            _sleep_ms(SHEETS_THROTTLE_MS)
            _with_backoff(ws.batch_update, [{"range": rng, "values": [rowvals]}])
            diffs = _calc_diffs(header, before, rowvals)
            if diffs:
                st_bucket["updated_details"].append(f"{ticket}: " + "; ".join(diffs))
            return "updated"
        _sleep_ms(SHEETS_THROTTLE_MS)
        _with_backoff(ws.append_row, rowvals, value_input_option="RAW")
        _index_simple.setdefault(name, {})[ticket] = _index_simple[name].get(ticket, -1)
        return "inserted"
    except Exception as e:
        st_bucket["skipped_reasons"][ticket] = f"upsert error: {e}"
        print("Welcome upsert error:", e, flush=True)
        return "error"

def _find_promo_row_pair(ws, ticket: str, typ: str) -> Optional[int]:
    try:
        values = ws.get_all_values()
        if not values: return None
        header = [h.strip().lower() for h in values[0]]
        col_ticket  = header.index("ticket number") if "ticket number" in header else 0
        col_type    = header.index("type") if "type" in header else 4
        for r_i, row in enumerate(values[1:], start=2):
            t   = _fmt_ticket(row[col_ticket] if col_ticket < len(row) else "")
            ty2 = (row[col_type] if col_type < len(row) else "").strip().lower()
            if t == _fmt_ticket(ticket) and ty2 == (typ or "").strip().lower():
                return r_i
    except Exception:
        pass
    return None

def upsert_promo(name: str, ws, ticket: str, typ: str, created_str: str, rowvals: List[str], st_bucket: dict) -> str:
    ticket = _fmt_ticket(ticket)
    key = _key_promo(ticket, typ, created_str)
    idx = _index_promo.get(name) or ws_index_promo(name, ws)
    header = HEADERS_SHEET4
    try:
        if key in idx:
            row = idx[key]
            before = _with_backoff(ws.row_values, row)
            rng = f"A{row}:{chr(ord('A')+len(rowvals)-1)}{row}"
            _sleep_ms(SHEETS_THROTTLE_MS)
            _with_backoff(ws.batch_update, [{"range": rng, "values": [rowvals]}])
            diffs = _calc_diffs(header, before, rowvals)
            if diffs:
                st_bucket["updated_details"].append(f"{ticket}:{typ}:{created_str}: " + "; ".join(diffs))
            return "updated"
        rpair = _find_promo_row_pair(ws, ticket, typ)
        if rpair:
            before = _with_backoff(ws.row_values, rpair)
            rng = f"A{rpair}:{chr(ord('A')+len(rowvals)-1)}{rpair}"
            _sleep_ms(SHEETS_THROTTLE_MS)
            _with_backoff(ws.batch_update, [{"range": rng, "values": [rowvals]}])
            ws_index_promo(name, ws)
            diffs = _calc_diffs(header, before, rowvals)
            if diffs:
                st_bucket["updated_details"].append(f"{ticket}:{typ}:{created_str}: " + "; ".join(diffs))
            return "updated"
        _sleep_ms(SHEETS_THROTTLE_MS)
        _with_backoff(ws.append_row, rowvals, value_input_option="RAW")
        ws_index_promo(name, ws)
        return "inserted"
    except Exception as e:
        st_bucket["skipped_reasons"][f"{ticket}:{typ}:{created_str}"] = f"upsert error: {e}"
        print("Promo upsert error:", e, flush=True)
        return "error"

def dedupe_sheet(name: str, ws, has_type: bool=False) -> Tuple[int,int]:
    values = ws.get_all_values()
    if len(values) <= 1: return (0,0)
    rows = values[1:]
    header = [h.strip().lower() for h in values[0]]

    col_ticket  = header.index("ticket number") if "ticket number" in header else 0
    col_date    = header.index("date closed")   if "date closed"   in header else 3

    if has_type:
        col_type    = header.index("type")            if "type" in header else 4
        col_created = header.index("thread created")  if "thread created" in header else 5

    winners: Dict[str, Tuple[int, Optional[datetime]]] = {}
    for i, row in enumerate(rows, start=2):
        t = _fmt_ticket(row[col_ticket] if col_ticket < len(row) else "")
        if not t: continue
        if has_type:
            typ = (row[col_type] if col_type < len(row) else "").strip().lower()
            cr  = (row[col_created] if col_created < len(row) else "").strip()
            key = _key_promo(t, typ, cr)
        else:
            key = t
        dt = None
        try:
            dt = datetime.strptime((row[col_date] if col_date < len(row) else "").strip(), "%Y-%m-%d %H:%M").replace(tzinfo=_tz.utc)
        except Exception:
            pass
        keep = winners.get(key)
        if not keep or ((dt or datetime.min.replace(tzinfo=_tz.utc)) > (keep[1] or datetime.min.replace(tzinfo=_tz.utc))):
            winners[key] = (i, dt)

    keep_rows = {r for (r,_dt) in winners.values()}
    to_delete = [i for i,_ in enumerate(rows,start=2) if i not in keep_rows]
    deleted = 0
    for r in sorted(to_delete, reverse=True):
        try: ws.delete_rows(r); deleted += 1
        except Exception: pass

    if has_type: ws_index_promo(name, ws)
    else: ws_index_welcome(name, ws)
    return (len(winners), deleted)

# ---------- Parsing + inference ----------
WELCOME_START_RX = re.compile(r'(?i)^(?:closed[- ]*)?(\d{4})[- ]+(.+)$')
PROMO_START_RX   = re.compile(r'(?i)^.*?(\d{4})-(.+)$')

def _clean_username(s: str) -> str:
    s = (s or "").strip()
    return re.sub(r'^[\s\-]+|[\s\-]+$', '', s)

def _aggregate_msg_text(msg: discord.Message) -> str:
    parts = [msg.content or ""]
    for e in msg.embeds or []:
        parts += [e.title or "", e.description or ""]
        if e.author and e.author.name: parts.append(e.author.name)
        for f in e.fields or []:
            parts += [f.name or "", f.value or ""]
    return " | ".join(parts)

async def infer_clantag_from_thread(thread: discord.Thread) -> Optional[str]:
    if not ENABLE_INFER_TAG_FROM_THREAD:
        return None
    try: await thread.join()
    except Exception: pass
    try:
        async for msg in thread.history(limit=500, oldest_first=False):
            text = _aggregate_msg_text(msg)
            tag = _match_tag_in_text(text)
            if tag:
                return tag
    except discord.Forbidden:
        return None
    except Exception:
        return None
    return None

def parse_welcome_thread_name_allow_missing(name: str) -> Optional[Tuple[str,str,Optional[str]]]:
    if not name:
        return None
    s = _normalize_dashes(name).strip()

    m = WELCOME_START_RX.match(s)
    if not m:
        m2 = re.search(r'(\d{4})', s)
        if not m2:
            return None
        ticket = _fmt_ticket(m2.group(1))
        remainder = s[m2.end():].lstrip(" -")
    else:
        ticket = _fmt_ticket(m.group(1))
        remainder = m.group(2)

    picked = _pick_tag_by_suffix(remainder, _load_clan_tags())
    if picked:
        username, tag = picked
        return (ticket, _clean_username(username), tag)

    any_tag = _match_tag_in_text(remainder)
    if any_tag:
        left = remainder.upper().split(any_tag.upper(), 1)[0]
        left = re.sub(r'\bclosed\b', '', left, flags=re.IGNORECASE)
        return (ticket, _clean_username(left), any_tag)

    return (ticket, _clean_username(remainder), None)

def parse_promo_thread_name(name: str) -> Optional[Tuple[str,str,str]]:
    if not name: return None
    m = PROMO_START_RX.match((name or "").strip())
    if not m: return None
    ticket = _fmt_ticket(m.group(1))
    remainder = m.group(2)
    picked = _pick_tag_by_suffix(remainder, _load_clan_tags())
    if picked:
        username, tag = picked
        return (ticket, _clean_username(username), tag)
    return (ticket, _clean_username(remainder), "")

async def find_close_timestamp(thread: discord.Thread) -> Optional[datetime]:
    try: await thread.join()
    except Exception: pass
    try:
        async for msg in thread.history(limit=500, oldest_first=False):
            text = _aggregate_msg_text(msg)
            if "ticket closed by" in text.lower():
                return msg.created_at
    except discord.Forbidden: pass
    except Exception: pass
    return None

# ---------- Scans (live progress + cancel-safe) ----------
def _new_report_bucket(): return _new_bucket()

async def scan_welcome_channel(channel: discord.TextChannel, progress_cb=None):
    st = backfill_state["welcome"] = _new_report_bucket()
    if not ENABLE_WELCOME_SCAN:
        backfill_state["last_msg"] = "welcome scan disabled"; return

    ws = get_ws(SHEET1_NAME, HEADERS_SHEET1)
    ws_index_welcome(SHEET1_NAME, ws)

    async def handle(th: discord.Thread):
        if not backfill_state["running"]: return
        await _handle_welcome_thread(th, ws, st)
        if progress_cb: await progress_cb()

    try:
        for th in channel.threads:
            if not backfill_state["running"]: break
            await handle(th)
    except Exception: pass

    try:
        async for th in channel.archived_threads(limit=None, private=False):
            if not backfill_state["running"]: break
            await handle(th)
    except discord.Forbidden:
        backfill_state["last_msg"] += " | no access to public archived welcome threads"
    try:
        async for th in channel.archived_threads(limit=None, private=True):
            if not backfill_state["running"]: break
            await handle(th)
    except discord.Forbidden:
        backfill_state["last_msg"] += " | no access to private archived welcome threads"

async def _handle_welcome_thread(th: discord.Thread, ws, st):
    if not backfill_state["running"]: return
    st["scanned"] += 1
    parsed = parse_welcome_thread_name_allow_missing(th.name or "")
    if not parsed:
        key = f"name:{th.name}"
        st["skipped"] += 1; st["skipped_ids"].append(key); st["skipped_reasons"][key] = "name parse fail"
        return
    ticket, username, clantag = parsed
    if not clantag:
        inferred = await infer_clantag_from_thread(th)
        clantag = inferred or ""
    dt = await find_close_timestamp(th)
    date_str = fmt_tz(dt or datetime.utcnow().replace(tzinfo=_tz.utc))
    row = [ticket, username, clantag, date_str]
    status = upsert_welcome(SHEET1_NAME, ws, ticket, row, st)
    if status == "inserted":
        st["added"] += 1; st["added_ids"].append(ticket)
    elif status == "updated":
        st["updated"] += 1; st["updated_ids"].append(ticket)
    else:
        st["skipped"] += 1; st["skipped_ids"].append(ticket); st["skipped_reasons"].setdefault(ticket, "unknown")

async def scan_promo_channel(channel: discord.TextChannel, progress_cb=None):
    st = backfill_state["promo"] = _new_report_bucket()
    if not ENABLE_PROMO_SCAN:
        backfill_state["last_msg"] = "promo scan disabled"; return

    ws = get_ws(SHEET4_NAME, HEADERS_SHEET4)
    ws_index_promo(SHEET4_NAME, ws)

    async def handle(th: discord.Thread):
        if not backfill_state["running"]: return
        await _handle_promo_thread(th, ws, st)
        if progress_cb: await progress_cb()

    try:
        for th in channel.threads:
            if not backfill_state["running"]: break
            await handle(th)
    except Exception: pass

    try:
        async for th in channel.archived_threads(limit=None, private=False):
            if not backfill_state["running"]: break
            await handle(th)
    except discord.Forbidden:
        backfill_state["last_msg"] += " | no access to public archived promo threads"
    try:
        async for th in channel.archived_threads(limit=None, private=True):
            if not backfill_state["running"]: break
            await handle(th)
    except discord.Forbidden:
        backfill_state["last_msg"] += " | no access to private archived promo threads"

async def _handle_promo_thread(th: discord.Thread, ws, st):
    if not backfill_state["running"]: return
    st["scanned"] += 1
    parsed = parse_promo_thread_name(th.name or "")
    if not parsed:
        key = f"name:{th.name}"
        st["skipped"] += 1; st["skipped_ids"].append(key); st["skipped_reasons"][key] = "name parse fail"
        return
    ticket, username, clantag = parsed
    typ = await detect_promo_type(th) or ""
    dt_close = await find_close_timestamp(th)
    date_str = fmt_tz(dt_close or datetime.utcnow().replace(tzinfo=_tz.utc))
    created_str = fmt_tz(th.created_at)

    row = [ticket, username, clantag, date_str, typ, created_str]
    status = upsert_promo(SHEET4_NAME, ws, ticket, typ, created_str, row, st)
    key = f"{ticket}:{typ or 'unknown'}:{created_str}"
    if status == "inserted":
        st["added"] += 1; st["added_ids"].append(key)
    elif status == "updated":
        st["updated"] += 1; st["updated_ids"].append(key)
    else:
        st["skipped"] += 1; st["skipped_ids"].append(key); st["skipped_reasons"].setdefault(key, "unknown")

# ---------- Promo type detection ----------
PROMO_TYPE_PATTERNS = [
    (re.compile(r"(?i)we['’]re excited to have you returning"), "returning player"),
    (re.compile(r"(?i)thanks for sending in your move request"), "player move request"),
    (re.compile(r"(?i)we['’]ve received your request to help one of your clan members find a new home"), "clan lead move request"),
]
async def detect_promo_type(thread: discord.Thread) -> Optional[str]:
    try: await thread.join()
    except Exception: pass
    try:
        async for msg in thread.history(limit=500, oldest_first=False):
            text = _aggregate_msg_text(msg)
            for rx, typ in PROMO_TYPE_PATTERNS:
                if rx.search(text):
                    return typ
    except discord.Forbidden: pass
    except Exception: pass
    return None

# ---------- Auto-post helper for details ----------
def _build_backfill_details_text() -> str:
    w = backfill_state["welcome"]; p = backfill_state["promo"]
    def section(title, lines):
        return [title] + (lines if lines else ["(none)"]) + [""]
    lines: List[str] = []
    lines += section("WELCOME — UPDATED (with diffs):", w["updated_details"])
    lines += section("WELCOME — SKIPPED (id -> reason):", [f"{k} -> {v}" for k,v in w["skipped_reasons"].items()])
    lines += section("PROMO — UPDATED (with diffs):", p["updated_details"])
    lines += section("PROMO — SKIPPED (id -> reason):", [f"{k} -> {v}" for k,v in p["skipped_reasons"].items()])
    return "\n".join(lines) or "(empty)"

# ---------- Commands ----------
def cmd_enabled(flag: bool):
    def deco(func):
        async def wrapper(ctx: commands.Context, *a, **k):
            if not flag:
                return await ctx.reply("This command is disabled by env flag.", mention_author=False)
            return await func(ctx, *a, **k)
        return wrapper
    return deco

@bot.command(name="ping")
@cmd_enabled(ENABLE_CMD_PING)
async def cmd_ping(ctx): await ctx.reply("🏓 Pong — Live and listening.", mention_author=False)

@bot.command(name="sheetstatus")
@cmd_enabled(ENABLE_CMD_SHEETSTATUS)
async def cmd_sheetstatus(ctx):
    email = service_account_email() or "(no service account)"
    try:
        ws1 = get_ws(SHEET1_NAME, HEADERS_SHEET1)
        ws4 = get_ws(SHEET4_NAME, HEADERS_SHEET4)
        title = ws1.spreadsheet.title
        await ctx.reply(
            f"✅ Sheets OK: **{title}**\n• Tabs: `{SHEET1_NAME}`, `{SHEET4_NAME}`, `{CLANLIST_TAB_NAME}` (tags col {CLANLIST_TAG_COLUMN})\n• Share with: `{email}`",
            mention_author=False
        )
    except Exception as e:
        await ctx.reply(f"⚠️ Cannot open sheet: `{e}`\nShare with: `{email}`", mention_author=False)

def _render_status() -> str:
    st = backfill_state; w = st["welcome"]; p = st["promo"]
    return (
        f"Running: **{st['running']}** | Last: {st.get('last_msg','')}\n"
        f"Welcome — scanned: **{w['scanned']}**, added: **{w['added']}**, updated: **{w['updated']}**, skipped: **{w['skipped']}**\n"
        f"Promo   — scanned: **{p['scanned']}**, added: **{p['added']}**, updated: **{p['updated']}**, skipped: **{p['skipped']}**"
    )

@bot.command(name="backfill_tickets")
@cmd_enabled(ENABLE_CMD_BACKFILL)
async def cmd_backfill(ctx):
    if backfill_state["running"]:
        return await ctx.reply("A backfill is already running. Use !backfill_status.", mention_author=False)
    backfill_state["running"] = True; backfill_state["last_msg"] = ""
    progress_msg = await ctx.reply("Starting backfill…", mention_author=False)

    async def progress_loop():
        while backfill_state["running"]:
            try: await progress_msg.edit(content=_render_status())
            except Exception: pass
            await asyncio.sleep(5.0)
    updater_task = asyncio.create_task(progress_loop())

    try:
        async def tick():
            try: await progress_msg.edit(content=_render_status())
            except Exception: pass

        if ENABLE_WELCOME_SCAN and WELCOME_CHANNEL_ID:
            ch = bot.get_channel(WELCOME_CHANNEL_ID)
            if isinstance(ch, discord.TextChannel):
                await scan_welcome_channel(ch, progress_cb=tick)
        if ENABLE_PROMO_SCAN and PROMO_CHANNEL_ID:
            ch2 = bot.get_channel(PROMO_CHANNEL_ID)
            if isinstance(ch2, discord.TextChannel):
                await scan_promo_channel(ch2, progress_cb=tick)
    finally:
        backfill_state["running"] = False
        try: updater_task.cancel()
        except Exception: pass

    await progress_msg.edit(content=_render_status() + "\nDone.")

    if POST_BACKFILL_SUMMARY:
        w = backfill_state["welcome"]; p = backfill_state["promo"]
        def _fmt_list(ids: List[str], max_items=10) -> str:
            if not ids: return "—"
            show = ids[:max_items]; extra = len(ids) - len(show)
            return ", ".join(show) + (f" …(+{extra})" if extra>0 else "")
        msg = (
            "**Backfill report (top 10 each)**\n"
            f"**Welcome** added: {len(w['added_ids'])} — {_fmt_list(w['added_ids'])}\n"
            f"updated: {len(w['updated_ids'])} — {_fmt_list(w['updated_ids'])}\n"
            f"skipped: {len(w['skipped_ids'])} — {_fmt_list(w['skipped_ids'])}\n"
            f"**Promo** added: {len(p['added_ids'])} — {_fmt_list(p['added_ids'])}\n"
            f"updated: {len(p['updated_ids'])} — {_fmt_list(p['updated_ids'])}\n"
            f"skipped: {len(p['skipped_ids'])} — {_fmt_list(p['skipped_ids'])}\n"
        )
        await ctx.send(msg)

    if AUTO_POST_BACKFILL_DETAILS:
        data = _build_backfill_details_text()
        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        buf = io.BytesIO(data.encode("utf-8"))
        await ctx.send(file=discord.File(buf, filename=f"backfill_details_{ts}.txt"))

@bot.command(name="backfill_stop")
async def cmd_backfill_stop(ctx):
    if not backfill_state["running"]:
        return await ctx.reply("No backfill is running.", mention_author=False)
    backfill_state["running"] = False
    backfill_state["last_msg"] = "cancel requested"
    await ctx.reply("Stopping backfill… will halt after the current thread.", mention_author=False)

@bot.command(name="backfill_details")
async def cmd_backfill_details(ctx: commands.Context):
    data = _build_backfill_details_text()
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    buf = io.BytesIO(data.encode("utf-8"))
    await ctx.reply(file=discord.File(buf, filename=f"backfill_details_{ts}.txt"), mention_author=False)

@bot.command(name="clan_tags_debug")
async def cmd_clan_tags_debug(ctx):
    tags = _load_clan_tags(force=True)
    norm_set = { _normalize_dashes(t).upper() for t in tags }
    has_fit = "F-IT" in norm_set
    sample = ", ".join(list(tags)[:20]) or "(none)"
    await ctx.reply(
        f"Loaded {len(tags)} clan tags from column {CLANLIST_TAG_COLUMN}. Has F-IT: {has_fit}\nSample: {sample}",
        mention_author=False
    )

@bot.command(name="dedupe_sheet")
@cmd_enabled(ENABLE_CMD_DEDUPE)
async def cmd_dedupe(ctx):
    try:
        ws1 = get_ws(SHEET1_NAME, HEADERS_SHEET1)
        ws4 = get_ws(SHEET4_NAME, HEADERS_SHEET4)
        kept1, deleted1 = dedupe_sheet(SHEET1_NAME, ws1, has_type=False)
        kept4, deleted4 = dedupe_sheet(SHEET4_NAME, ws4, has_type=True)
        await ctx.reply(
            f"Sheet1: kept **{kept1}** unique tickets, deleted **{deleted1}** dupes.\n"
            f"Sheet4: kept **{kept4}** unique (ticket+type+created), deleted **{deleted4}** dupes.",
            mention_author=False
        )
    except Exception as e:
        await ctx.reply(f"Dedup failed: `{e}`", mention_author=False)

@bot.command(name="reload")
@cmd_enabled(ENABLE_CMD_RELOAD)
async def cmd_reload(ctx):
    _ws_cache.clear(); _index_simple.clear(); _index_promo.clear()
    global _gs_client, _clan_tags_cache, _clan_tags_norm_set, _last_clan_fetch, _tag_regex_cache, _tag_regex_key
    _gs_client = None; _clan_tags_cache = []; _clan_tags_norm_set = set(); _last_clan_fetch = 0.0; _tag_regex_cache=None; _tag_regex_key=""
    await ctx.reply("Caches cleared. Reconnect to Sheets on next use.", mention_author=False)

@bot.command(name="health")
@cmd_enabled(ENABLE_CMD_HEALTH)
async def cmd_health(ctx):
    lat = int(bot.latency*1000)
    try:
        ws1 = get_ws(SHEET1_NAME, HEADERS_SHEET1)
        ok = f"🟢 OK ({ws1.title})"
    except Exception:
        ok = "🔴 FAILED"
    await ctx.reply(f"🟢 Bot OK | Latency: {lat} ms | Sheets: {ok} | Uptime: {uptime_str()}", mention_author=False)

@bot.command(name="checksheet")
@cmd_enabled(ENABLE_CMD_CHECKSHEET)
async def cmd_checksheet(ctx):
    try:
        ws1 = get_ws(SHEET1_NAME, HEADERS_SHEET1)
        ws4 = get_ws(SHEET4_NAME, HEADERS_SHEET4)
        await ctx.reply(
            f"{SHEET1_NAME} rows: {len(ws1.col_values(1))} | {SHEET4_NAME} rows: {len(ws4.col_values(1))}",
            mention_author=False
        )
    except Exception as e:
        await ctx.reply(f"checksheet failed: `{e}`", mention_author=False)

@bot.command(name="reboot")
@cmd_enabled(ENABLE_CMD_REBOOT)
async def cmd_reboot(ctx):
    await ctx.reply("Rebooting…", mention_author=False)
    await asyncio.sleep(1.0); os._exit(0)

# ---------- LIVE WATCHERS ----------
_pending_welcome: Dict[int, Dict[str, Any]] = {}  # thread_id -> {ticket, username, close_dt}
_pending_promo:   Dict[int, Dict[str, Any]] = {}

def _is_thread_in_parent(thread: discord.Thread, parent_id: int) -> bool:
    try:
        return thread and thread.parent_id == parent_id
    except Exception:
        return False

async def _rename_welcome_thread_if_needed(thread: discord.Thread, ticket: str, username: str, clantag: str):
    """Rename to '0000-username-CLAN' if different."""
    try:
        desired = f"{_fmt_ticket(ticket)}-{username}-{clantag}".strip("-")
        if (thread.name or "").strip() != desired:
            await thread.edit(name=desired)
    except discord.Forbidden:
        pass
    except Exception:
        pass

async def _finalize_welcome(thread: discord.Thread, ticket: str, username: str, clantag: str, close_dt: Optional[datetime]):
    ws = get_ws(SHEET1_NAME, HEADERS_SHEET1)
    await _rename_welcome_thread_if_needed(thread, ticket, username, clantag or "")
    date_str = fmt_tz(close_dt or await find_close_timestamp(thread) or datetime.utcnow().replace(tzinfo=_tz.utc))
    row = [_fmt_ticket(ticket), username, clantag or "", date_str]
    dummy_bucket = _new_bucket()
    upsert_welcome(SHEET1_NAME, ws, ticket, row, dummy_bucket)

async def _finalize_promo(thread: discord.Thread, ticket: str, username: str, clantag: str, close_dt: Optional[datetime]):
    ws = get_ws(SHEET4_NAME, HEADERS_SHEET4)
    typ = await detect_promo_type(thread) or ""
    created_str = fmt_tz(thread.created_at)
    date_str = fmt_tz(close_dt or await find_close_timestamp(thread) or datetime.utcnow().replace(tzinfo=_tz.utc))
    row = [_fmt_ticket(ticket), username, clantag or "", date_str, typ, created_str]
    dummy_bucket = _new_bucket()
    upsert_promo(SHEET4_NAME, ws, ticket, typ, created_str, row, dummy_bucket)

def _who_to_ping(msg: discord.Message, thread: discord.Thread) -> Optional[discord.User]:
    # Prefer an explicitly mentioned user in the "Ticket Closed by" post; fall back to thread.owner
    if msg.mentions:
        return msg.mentions[0]
    try:
        return thread.owner
    except Exception:
        return None

async def _prompt_for_tag(thread: discord.Thread, ticket: str, username: str, msg_to_reply: Optional[discord.Message]):
    tags = _load_clan_tags(False)
    sample = ", ".join(list(tags)[:15]) + ("…" if len(tags) > 15 else "")
    ping_user = _who_to_ping(msg_to_reply, thread)
    mention = f"{ping_user.mention} " if ping_user else ""
    txt = (
        f"{mention}Which clan tag for **{username}**? "
        f"Reply in this thread with a valid tag (e.g., `C1C9`, `F-IT`).\n"
        f"_Known tags:_ {sample}"
    )
    try:
        await thread.send(txt, suppress_embeds=True)
    except Exception:
        pass

@bot.event
async def on_message(message: discord.Message):
    # Don't react to ourselves
    if message.author.id == getattr(bot.user, "id", None):
        return

    # Only care about thread messages
    if not isinstance(message.channel, discord.Thread):
        return

    thread: discord.Thread = message.channel

    # WELCOME watcher ---------------------------------------------------------
    if ENABLE_LIVE_WATCH and ENABLE_LIVE_WATCH_WELCOME and _is_thread_in_parent(thread, WELCOME_CHANNEL_ID):
        text = _aggregate_msg_text(message).lower()

        # 1) "Ticket Closed by" detected -> capture ticket/username, maybe tag, prompt if missing
        if "ticket closed by" in text:
            parsed = parse_welcome_thread_name_allow_missing(thread.name or "")
            if parsed:
                ticket, username, tag = parsed
                close_dt = message.created_at
                if tag:
                    await _finalize_welcome(thread, ticket, username, tag, close_dt)
                else:
                    _pending_welcome[thread.id] = {"ticket": ticket, "username": username, "close_dt": close_dt}
                    await _prompt_for_tag(thread, ticket, username, message)
            # continue to allow same message to also contain a tag (rare)
        # 2) If we’re waiting for a tag, check replies for a known tag
        elif thread.id in _pending_welcome:
            if not message.author.bot:
                tag = _match_tag_in_text(_aggregate_msg_text(message))
                if tag:
                    info = _pending_welcome.pop(thread.id, {})
                    ticket = info.get("ticket"); username = info.get("username"); close_dt = info.get("close_dt")
                    if ticket and username:
                        await _finalize_welcome(thread, ticket, username, tag, close_dt)
                        try:
                            await thread.send(f"Got it — set clan tag to **{tag}** and logged to the sheet. ✅")
                        except Exception:
                            pass

    # PROMO watcher -----------------------------------------------------------
    if ENABLE_LIVE_WATCH and ENABLE_LIVE_WATCH_PROMO and _is_thread_in_parent(thread, PROMO_CHANNEL_ID):
        text = _aggregate_msg_text(message).lower()

        if "ticket closed by" in text:
            parsed = parse_promo_thread_name(thread.name or "")
            if parsed:
                ticket, username, tag = parsed
                close_dt = message.created_at
                if tag:
                    await _finalize_promo(thread, ticket, username, tag, close_dt)
                else:
                    _pending_promo[thread.id] = {"ticket": ticket, "username": username, "close_dt": close_dt}
                    await _prompt_for_tag(thread, ticket, username, message)
        elif thread.id in _pending_promo:
            if not message.author.bot:
                tag = _match_tag_in_text(_aggregate_msg_text(message))
                if tag:
                    info = _pending_promo.pop(thread.id, {})
                    ticket = info.get("ticket"); username = info.get("username"); close_dt = info.get("close_dt")
                    if ticket and username:
                        await _finalize_promo(thread, ticket, username, tag, close_dt)
                        try:
                            await thread.send(f"Got it — set clan tag to **{tag}** and logged to the sheet. ✅")
                        except Exception:
                            pass

    # Make sure commands still work
    await bot.process_commands(message)

# ---------- Ready + health server ----------
@bot.event
async def on_ready():
    print(f"Logged in as {bot.user}", flush=True)

if ENABLE_WEB_SERVER:
    try:
        from aiohttp import web
        async def _health(request): return web.Response(text="ok")
        async def web_main():
            app = web.Application()
            app.router.add_get("/", _health); app.router.add_get("/health", _health)
            port = int(os.getenv("PORT","10000"))
            runner = web.AppRunner(app); await runner.setup()
            site = web.TCPSite(runner,"0.0.0.0",port); await site.start()
            print(f"Health server on :{port}", flush=True)
        async def start_all():
            _print_boot_info()
            if not TOKEN:
                print("FATAL: DISCORD_TOKEN/TOKEN not set.", flush=True); raise SystemExit(2)
            await asyncio.gather(web_main(), bot.start(TOKEN))
        if __name__ == "__main__":
            asyncio.run(start_all())
    except Exception:
        if __name__ == "__main__":
            _print_boot_info()
            if TOKEN: bot.run(TOKEN)
            else: print("FATAL: DISCORD_TOKEN/TOKEN not set.", flush=True)
else:
    if __name__ == "__main__":
        _print_boot_info()
        if TOKEN: bot.run(TOKEN)
        else: print("FATAL: DISCORD_TOKEN/TOKEN not set.", flush=True)
