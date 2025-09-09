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
