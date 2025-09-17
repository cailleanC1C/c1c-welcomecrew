# C1C – WelcomeCrew - v1.0

# - Live watchers; notify-channel fallback (no DMs)
# - Auto-join new threads / on-mention join
# - Forgiving parsers; F-IT/multi-part tags; clanlist tags from column B
# - Backfill leaves date blank if not closed; auto details attachment
# - watch_status with last 5 actions
# - Throttled/backoff Sheets writes; 4-digit tickets
# - Dropdown tag picker w/ paging, timeout reload, and plain-text tag fallback
# - Promo threads now also renamed to Closed-####-username-TAG

import os, json, re, asyncio, time, io, random
from datetime import datetime, timezone as _tz, timedelta as _td
from typing import Optional, Tuple, Dict, Any, List
from collections import deque

import discord
from discord.ext import commands
import gspread
from gspread.exceptions import APIError

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

from aiohttp import web, ClientSession
from discord.ext import tasks
import sys


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
# Scheduled refresh config
REFRESH_TIMES = os.getenv("REFRESH_TIMES", "02:00,10:00,18:00")  # 24h times, comma-separated
CLAN_TAGS_CACHE_TTL_SEC = int(os.getenv("CLAN_TAGS_CACHE_TTL_SEC", "28800"))  # 8h default
LOG_CHANNEL_ID = int(os.getenv("LOG_CHANNEL_ID", "0"))  # optional: where to post "refreshed" pings


SHEETS_THROTTLE_MS = int(os.getenv("SHEETS_THROTTLE_MS", "200"))

# Feature toggles (commands / scans)
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

# Private-thread fallback (NO DMs)
ENABLE_NOTIFY_FALLBACK    = env_bool("ENABLE_NOTIFY_FALLBACK", True)
NOTIFY_CHANNEL_ID         = int(os.getenv("NOTIFY_CHANNEL_ID", "0"))    # e.g., coordinators channel
NOTIFY_PING_ROLE_ID       = int(os.getenv("NOTIFY_PING_ROLE_ID", "0"))  # optional role to ping
ALLOW_SELF_JOIN_PRIVATE   = env_bool("ALLOW_SELF_JOIN_PRIVATE", True)

# Require close marker? (you asked to leave date blank instead, so defaults OFF)
REQUIRE_CLOSE_MARKER_WELCOME = env_bool("REQUIRE_CLOSE_MARKER_WELCOME", False)
REQUIRE_CLOSE_MARKER_PROMO   = env_bool("REQUIRE_CLOSE_MARKER_PROMO", False)

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
    print("=== WelcomeCrew v16 boot ===", flush=True)
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

# ---------- Rate-limit helpers ----------
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

# --- HELP CARD (mobile, two-line bullets) ------------------------------------
try:
    bot.remove_command("help")
except Exception:
    pass

HELP_ICON_URL = os.getenv("HELP_ICON_URL")
EMBED_COLOR = 0x55CCFF

def _mk_help_embed_mobile(guild: discord.Guild | None = None) -> discord.Embed:
    e = discord.Embed(
        title=" 🌿 C1C-WelcomeCrew — Help",
        color=EMBED_COLOR,
        description="I help to track Welcome & Promotion/Move threads and keep things tidy."
    )
    if HELP_ICON_URL:
        e.set_thumbnail(url=HELP_ICON_URL)

    # ——— User Actions ———
    e.add_field(
        name="User Actions — Recruiters & Mods",
        value=(
            "On Close Ticket, I pick up **`Ticket closed by <name>`** and log it.\n"
            "I’ll prompt in-thread with a tag picker (or you can type just the tag, e.g., `C1C9`).\n"
            "I rename the thread to **`Closed-####-username-TAG`** and write the record to the stats sheet."
        ),
        inline=False,
    )

    # ——— Commands (two-line layout per item) ———
    commands_pairs = [
        ("!env_check",        "show required env + hints"),
        ("!sheetstatus",      "tabs + service account email"),
        ("!backfill_tickets", "scan threads, show live status"),
        ("!backfill_details", "upload diffs/skips as a file"),
        ("!dedupe_sheet",     "keep newest entry"),
        ("!watch_status",     "watcher ON/OFF + last actions"),
        ("!reload",           "clear sheet cache"),
        ("!checksheet",       "sheet row counts"),
        ("!health",           "bot & Sheets health"),
        ("!reboot",           "soft restart"),
    ]
    commands_lines = "\n".join([f"🔹 `{cmd}`\n  → {desc}" for cmd, desc in commands_pairs])
    e.add_field(name="Commands — Admin & Maintenance", value=commands_lines, inline=False)

    watchers = (
        f"Watchers: **{'ON' if ENABLE_LIVE_WATCH else 'OFF'}** "
        f"(welcome={'ON' if ENABLE_LIVE_WATCH_WELCOME else 'OFF'}, "
        f"promo={'ON' if ENABLE_LIVE_WATCH_PROMO else 'OFF'})"
    )
    e.add_field(name="Status", value=watchers, inline=False)

    e.set_footer(text="C1C 🔹 tidy logs, happy recruiters")
    return e

@bot.command(name="help")
async def help_cmd(ctx: commands.Context, *, topic: str = None):
    topic = (topic or "").strip().lower()

    pages = {
        "env_check": "`!env_check`\nCheck required env vars, toggles, and IDs.",
        "sheetstatus": "`!sheetstatus`\nShow tabs, service account email, and share info.",
        "backfill_tickets": "`!backfill_tickets`\nScan Welcome & Promo threads and log to Sheets.",
        "backfill_details": "`!backfill_details`\nExport skipped/updated diffs as a text file.",
        "dedupe_sheet": "`!dedupe_sheet`\nDelete duplicate tickets in both sheets.",
        "watch_status": "`!watch_status`\nShow ON/OFF state of watchers and last 5 actions.",
        "reload": "`!reload`\nClear cache so next call reopens Sheets fresh.",
        "checksheet": "`!checksheet`\nRow counts for both sheets.",
        "health": "`!health`\nShow bot latency, Sheets health, and uptime.",
        "reboot": "`!reboot`\nSoft restart the bot.",
        "ping": "`!ping`\nSimple bot-alive check (Pong).",
    }

    # overview help if no topic
    if not topic:
        return await ctx.reply(embed=_mk_help_embed_mobile(ctx.guild), mention_author=False)

    txt = pages.get(topic)
    if not txt:
        # behave like unknown command: stay silent, log it
        import logging
        log = logging.getLogger("welcomecrew")
        log.warning("Unknown help topic requested: %s", topic)
        return

    e = discord.Embed(title=f"!help {topic}", description=txt, color=EMBED_COLOR)
    if HELP_ICON_URL:
        e.set_thumbnail(url=HELP_ICON_URL)
    await ctx.reply(embed=e, mention_author=False)


@bot.tree.command(name="help", description="Show WelcomeCrew help")
async def slash_help(interaction: discord.Interaction):
    await interaction.response.send_message(
        embed=_mk_help_embed_mobile(interaction.guild),
        ephemeral=True
    )

# --- Ensure slash commands are visible (once per boot) -----------------------
@bot.event
async def setup_hook():
    try:
        await bot.tree.sync()
        print("Slash commands synced.", flush=True)
    except Exception as e:
        print(f"Slash sync failed: {e}", flush=True)

# ---------- Clanlist & tag matching ----------
_clan_tags_cache: List[str] = []
_clan_tags_norm_set: set = set()
_last_clan_fetch = 0.0
_tag_regex_cache = None

def _normalize_dashes(s: str) -> str:
    # en/em/figure hyphens -> ASCII '-'
    return re.sub(r"[\u2010\u2011\u2012\u2013\u2014\u2015]", "-", s or "")

def _fmt_ticket(s: str) -> str:
    return (s or "").strip().lstrip("#").zfill(4)

def _load_clan_tags(force: bool=False) -> List[str]:
    global _clan_tags_cache, _clan_tags_norm_set, _last_clan_fetch, _tag_regex_cache
    now = time.time()
    if not force and _clan_tags_cache and (now - _last_clan_fetch < CLAN_TAGS_CACHE_TTL_SEC):
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
        else:
            _tag_regex_cache = None
    except Exception as e:
        print("Failed to load clanlist:", e, flush=True)
        _clan_tags_cache = []; _clan_tags_norm_set = set(); _tag_regex_cache = None
    return _clan_tags_cache

def _match_tag_in_text(text: str) -> Optional[str]:
    if not text: return None
    _load_clan_tags(False)
    if not _tag_regex_cache: return None
    s = _normalize_dashes(text).upper()
    m = _tag_regex_cache.search(s)
    return m.group(0).upper() if m else None

def _pick_tag_by_suffix(remainder: str, known_tags: List[str]) -> Optional[Tuple[str, str]]:
    """
    Try to find a '-TAG' suffix where TAG is in known_tags.
    Supports multi-segment tags like 'F-IT'. Checks last 1..3 segments.
    """
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
        # Refresh index just before any insert to avoid duplicates if another row was added recently
        idx = ws_index_welcome(name, ws)
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

# ---------- Close marker detection (forgiving) ----------
CLOSE_RX = re.compile(r'(?i)\b(ticket)?\s*closed\b[\s:\-–—•]*\bby\b')

def is_close_marker(text: str) -> bool:
    if not text:
        return False
    return bool(CLOSE_RX.search(text))

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
        if e.author and e.author.name:
            parts.append(e.author.name)
        for f in e.fields or []:
            parts += [f.name or "", f.value or ""]
        # NEW: also read footer text, many bots put "Ticket Closed by ..." here
        try:
            if getattr(e, "footer", None) and getattr(e.footer, "text", None):
                parts.append(e.footer.text or "")
        except Exception:
            pass
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
            if is_close_marker(text):
                return msg.created_at
    except discord.Forbidden: pass
    except Exception: pass
    return None
    
# ---- keepalive / watchdog state ----
BOT_CONNECTED: bool = False
_LAST_READY_TS: float = 0.0
_LAST_DISCONNECT_TS: float = 0.0
_LAST_EVENT_TS: float = 0.0

STRICT_PROBE = (os.getenv("STRICT_PROBE", "0") == "1")  # default: off

WATCHDOG_CHECK_SEC = int(os.getenv("WATCHDOG_CHECK_SEC", "60"))
WATCHDOG_MAX_DISCONNECT_SEC = int(os.getenv("WATCHDOG_MAX_DISCONNECT_SEC", "600"))  # 10m

def _now() -> float: return time.time()

def _mark_event() -> None:
    global _LAST_EVENT_TS
    _LAST_EVENT_TS = _now()

def _last_event_age_s() -> int | None:
    return int(_now() - _LAST_EVENT_TS) if _LAST_EVENT_TS else None

# ---------- Watch status log ----------
WATCH_LOG = deque(maxlen=50)

def thread_link(thread: discord.Thread) -> str:
    gid = getattr(thread.guild, "id", 0)
    return f"https://discord.com/channels/{gid}/{thread.id}"

def log_action(scope: str, action: str, **data):
    ts = datetime.utcnow().replace(tzinfo=_tz.utc)
    WATCH_LOG.appendleft({"ts": ts, "scope": scope, "action": action, "data": data})

def render_watch_status_text() -> str:
    on = "ON" if ENABLE_LIVE_WATCH else "OFF"
    on_w = "ON" if ENABLE_LIVE_WATCH_WELCOME else "OFF"
    on_p = "ON" if ENABLE_LIVE_WATCH_PROMO else "OFF"
    lines = [f"👀 **Watchers**: {on} (welcome={on_w}, promo={on_p})"]
    if WATCH_LOG:
        lines.append("**Recent (latest 5):**")
        for item in list(WATCH_LOG)[:5]:
            ts = fmt_tz(item["ts"])
            scope = item["scope"]; act = item["action"]
            d = item["data"]
            ticket   = d.get("ticket","")
            username = d.get("username","")
            clantag  = d.get("clantag","")
            status   = d.get("status","")
            link     = d.get("link","")
            bits = [b for b in [ticket, username, clantag, status] if b]
            summary = " | ".join(bits) if bits else ""
            line = f"• [{ts}] {scope} · {act}"
            if summary: line += f" · {summary}"
            if link:    line += f" · <{link}>"
            lines.append(line)
    else:
        lines.append("_No recent actions yet._")
    return "\n".join(lines)

# ---------- Fallback notify helpers ----------
def _notify_prefix(guild: discord.Guild, closer: Optional[discord.User]) -> str:
    parts = []
    if NOTIFY_PING_ROLE_ID:
        r = guild.get_role(NOTIFY_PING_ROLE_ID)
        if r: parts.append(r.mention)
    if closer:
        parts.append(closer.mention)
    return (" ".join(parts) + " ") if parts else ""

async def _notify_channel(guild: discord.Guild, content: str):
    if not ENABLE_NOTIFY_FALLBACK or not NOTIFY_CHANNEL_ID:
        return False
    ch = guild.get_channel(NOTIFY_CHANNEL_ID)
    if not isinstance(ch, (discord.TextChannel, discord.Thread)):
        return False
    try:
        await ch.send(content)
        return True
    except Exception:
        return False

async def _try_join_private_thread(thread: discord.Thread) -> bool:
    try:
        await thread.join(); return True
    except Exception:
        pass
    if not ALLOW_SELF_JOIN_PRIVATE:
        return False
    try:
        me = thread.guild.me
        if me:
            await thread.add_user(me)
            return True
    except Exception:
        pass
    return False

def _who_to_ping(msg: discord.Message, thread: discord.Thread) -> Optional[discord.User]:
    if msg and msg.mentions:
        return msg.mentions[0]
    try:
        return thread.owner
    except Exception:
        return None

# ---------- Tag prompt (dropdown + fallback) ----------
async def _prompt_for_tag(thread: discord.Thread, ticket: str, username: str,
                          msg_to_reply: Optional[discord.Message], mode: str):
    tags = _load_clan_tags(False) or []
    closer = _who_to_ping(msg_to_reply, thread)
    mention = f"{closer.mention} " if closer else ""
    content = (
        f"{mention}Which clan tag for **{username}** (ticket **{_fmt_ticket(ticket)}**)?\n"
        "Pick one from the menu below, or simply type the tag as a message."
    )

    try:
        view = TagPickerView(mode, thread, ticket, username, tags)
        sent = await thread.send(content, view=view, suppress_embeds=True)
        view.message = sent
        return
    except discord.Forbidden:
        if await _try_join_private_thread(thread):
            try:
                view = TagPickerView(mode, thread, ticket, username, tags)
                sent = await thread.send(content, view=view, suppress_embeds=True)
                view.message = sent
                return
            except Exception:
                pass
    except Exception:
        pass

    prefix = _notify_prefix(thread.guild, closer)
    await _notify_channel(
        thread.guild,
        f"{prefix}Need clan tag for **{username}** (ticket **{_fmt_ticket(ticket)}**) → {thread_link(thread)}"
    )

# ---------- Finalizers (log + rename) ----------
async def _rename_welcome_thread_if_needed(thread: discord.Thread, ticket: str, username: str, clantag: str) -> bool:
    """
    Ensure threads are named exactly: Closed-####-username-TAG
    (keeps a single 'Closed-' prefix; avoids double-prefixing)
    """
    try:
        core = f"{_fmt_ticket(ticket)}-{username}-{clantag}".strip("-")
        desired = f"Closed-{core}"
        current = (thread.name or "").strip()

        cur_norm = _normalize_dashes(current)
        if cur_norm.lower().startswith("closed-"):
            cur_norm = "Closed-" + cur_norm[7:]  # normalize case of prefix

        if cur_norm != desired and clantag:
            await thread.edit(name=desired)
            return True
    except discord.Forbidden:
        pass
    except Exception:
        pass
    return False

async def _finalize_welcome(thread: discord.Thread, ticket: str, username: str, clantag: str, close_dt: Optional[datetime]):
    ws = get_ws(SHEET1_NAME, HEADERS_SHEET1)
    renamed = await _rename_welcome_thread_if_needed(thread, ticket, username, clantag or "")
    if renamed:
        log_action("welcome", "renamed", ticket=_fmt_ticket(ticket), username=username, clantag=clantag or "", link=thread_link(thread))
    date_str = fmt_tz(close_dt) if close_dt else ""
    row = [_fmt_ticket(ticket), username, clantag or "", date_str]
    dummy_bucket = _new_bucket()
    status = upsert_welcome(SHEET1_NAME, ws, ticket, row, dummy_bucket)
    log_action("welcome", "logged", ticket=_fmt_ticket(ticket), username=username, clantag=clantag or "", status=status, link=thread_link(thread))

async def _finalize_promo(thread: discord.Thread, ticket: str, username: str, clantag: str, close_dt: Optional[datetime]):
    ws = get_ws(SHEET4_NAME, HEADERS_SHEET4)

    # Rename promo/move threads too (same canonical format as welcome)
    renamed = await _rename_welcome_thread_if_needed(thread, ticket, username, clantag or "")
    if renamed:
        log_action("promo", "renamed",
                   ticket=_fmt_ticket(ticket), username=username,
                   clantag=clantag or "", link=thread_link(thread))

    typ = await detect_promo_type(thread) or ""
    created_str = fmt_tz(thread.created_at)
    date_str = fmt_tz(close_dt) if close_dt else ""
    row = [_fmt_ticket(ticket), username, clantag or "", date_str, typ, created_str]
    dummy_bucket = _new_bucket()
    status = upsert_promo(SHEET4_NAME, ws, ticket, typ, created_str, row, dummy_bucket)
    log_action("promo", "logged",
               ticket=_fmt_ticket(ticket), username=username,
               clantag=clantag or "", status=status, link=thread_link(thread))

# ---------- Scans (backfill) ----------
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
    if REQUIRE_CLOSE_MARKER_WELCOME and not dt:
        date_str = ""
    else:
        date_str = fmt_tz(dt) if dt else ""
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
    if REQUIRE_CLOSE_MARKER_PROMO and not dt_close:
        date_str = ""
    else:
        date_str = fmt_tz(dt_close) if dt_close else ""
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

def _red(s: str, keep: int = 6) -> str:
    if not s: return "(empty)"
    if len(s) <= keep: return "*" * len(s)
    return s[:keep] + "…" + "*" * 6

@bot.command(name="env_check")
async def cmd_env_check(ctx):
    def ok(b): return "✅" if b else "❌"

    req = {
        "DISCORD_TOKEN": bool(os.getenv("DISCORD_TOKEN")),
        "GSHEET_ID": bool(os.getenv("GSHEET_ID")),
        "GOOGLE_SERVICE_ACCOUNT_JSON": bool(os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")),
        "WELCOME_CHANNEL_ID": bool(int(os.getenv("WELCOME_CHANNEL_ID", "0"))),
        "PROMO_CHANNEL_ID": bool(int(os.getenv("PROMO_CHANNEL_ID", "0"))),
    }

    try:
        sa_ok = bool(json.loads(os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON") or "{}").get("client_email"))
    except Exception:
        sa_ok = False

    notify_id = int(os.getenv("NOTIFY_CHANNEL_ID", "0"))
    notify_role = int(os.getenv("NOTIFY_PING_ROLE_ID", "0"))
    tz = os.getenv("TIMEZONE", "UTC")
    clan_col = int(os.getenv("CLANLIST_TAG_COLUMN", "2"))

    toggles = {
        "ENABLE_LIVE_WATCH": ENABLE_LIVE_WATCH,
        "  welcome": ENABLE_LIVE_WATCH_WELCOME,
        "  promo": ENABLE_LIVE_WATCH_PROMO,
        "ENABLE_WELCOME_SCAN": ENABLE_WELCOME_SCAN,
        "ENABLE_PROMO_SCAN": ENABLE_PROMO_SCAN,
        "ENABLE_INFER_TAG_FROM_THREAD": ENABLE_INFER_TAG_FROM_THREAD,
        "ENABLE_NOTIFY_FALLBACK": ENABLE_NOTIFY_FALLBACK,
        "AUTO_POST_BACKFILL_DETAILS": AUTO_POST_BACKFILL_DETAILS,
        "POST_BACKFILL_SUMMARY": POST_BACKFILL_SUMMARY,
        "REQUIRE_CLOSE_MARKER_WELCOME": REQUIRE_CLOSE_MARKER_WELCOME,
        "REQUIRE_CLOSE_MARKER_PROMO": REQUIRE_CLOSE_MARKER_PROMO,
    }

    lines = []
    lines.append("**Env check**")
    lines.append("Required:")
    for k, v in req.items():
        val_preview = ""
        if k == "DISCORD_TOKEN":
            val_preview = f" ({_red(os.getenv(k,''))})"
        elif k in ("GSHEET_ID",):
            val_preview = f" ({_red(os.getenv(k,''))})"
        lines.append(f"• {ok(v)} {k}{val_preview}")

    lines.append(f"• {ok(sa_ok)} GOOGLE_SERVICE_ACCOUNT_JSON → service account email readable")

    lines.append("")
    lines.append("IDs / misc:")
    lines.append(f"• {ok(bool(notify_id))} NOTIFY_CHANNEL_ID = {notify_id or '(off)'}")
    lines.append(f"• {ok(True)} NOTIFY_PING_ROLE_ID = {notify_role or '(off)'}")
    lines.append(f"• {ok(True)} TIMEZONE = {tz}")
    lines.append(f"• {ok(clan_col >= 1)} CLANLIST_TAG_COLUMN = {clan_col} (1=A, 2=B, …)")

    lines.append("")
    lines.append("Toggles:")
    for k, v in toggles.items():
        lines.append(f"• {ok(bool(v))} {k} = {'ON' if v else 'OFF'}")

    hints = []
    if not req["WELCOME_CHANNEL_ID"] or not req["PROMO_CHANNEL_ID"]:
        hints.append("set numeric IDs for WELCOME_CHANNEL_ID and PROMO_CHANNEL_ID")
    if ENABLE_NOTIFY_FALLBACK and not notify_id:
        hints.append("set NOTIFY_CHANNEL_ID or turn ENABLE_NOTIFY_FALLBACK=OFF")
    if clan_col != CLANLIST_TAG_COLUMN:
        hints.append("CLANLIST_TAG_COLUMN didn’t parse as expected")
    if hints:
        lines.append("")
        lines.append("_Hints:_ " + "; ".join(hints))

    lines.append("")
    lines.append("_Sheets tip:_ set **column A** (ticket number) to **Plain text** to keep leading zeros.")
    await ctx.reply("\n".join(lines), mention_author=False)

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
    global _gs_client, _clan_tags_cache, _clan_tags_norm_set, _last_clan_fetch, _tag_regex_cache
    _gs_client = None; _clan_tags_cache = []; _clan_tags_norm_set = set(); _last_clan_fetch = 0.0; _tag_regex_cache=None
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

@bot.command(name="watch_status")
async def cmd_watch_status(ctx):
    await ctx.reply(render_watch_status_text(), mention_author=False)

# --- Clan Tag Picker (timeout UX: reload button + type fallback, no re-ping) --
def _chunks(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i:i+n]

class TagPickerReloadView(discord.ui.View):
    """Shown after timeout; lets a recruiter reload the picker (no re-ping)."""
    def __init__(self, original: "TagPickerView"):
        super().__init__(timeout=600)
        self.original = original

    @discord.ui.button(label="Reload picker", style=discord.ButtonStyle.primary, emoji="🔄")
    async def reload(self, interaction: discord.Interaction, button: discord.ui.Button):
        pending = (_pending_welcome if self.original.mode == "welcome" else _pending_promo)
        if self.original.thread.id not in pending:
            await interaction.response.edit_message(content="Already logged — picker closed.", view=None)
            return
        new_view = TagPickerView(
            self.original.mode,
            self.original.thread,
            self.original.ticket,
            self.original.username,
            self.original.tags
        )
        await interaction.response.edit_message(
            content=(f"Which clan tag for **{self.original.username}** (ticket **{self.original.ticket}**)?\n"
                     "Pick one from the menu below, or simply type the tag as a message."),
            view=new_view
        )
        new_view.message = interaction.message

class TagPickerView(discord.ui.View):
    """Dropdown tag picker. mode ∈ {'welcome','promo'}."""
    def __init__(self, mode: str, thread: discord.Thread, ticket: str, username: str,
                 tags: List[str]):
        super().__init__(timeout=600)
        self.mode = mode
        self.thread = thread
        self.ticket = _fmt_ticket(ticket)
        self.username = username
        self.tags = [t.strip().upper() for t in tags if t and t.strip()]
        self.pages = list(_chunks(self.tags, 25)) or [[]]
        self.page  = 0
        self.message: Optional[discord.Message] = None

        # Dropdown
        self.select = discord.ui.Select(
            placeholder=f"Choose clan tag • Page 1/{len(self.pages)}",
            min_values=1, max_values=1,
            options=[discord.SelectOption(label=t, value=t) for t in self.pages[0]]
        )
        async def _on_select(interaction: discord.Interaction):
            tag = self.select.values[0]
            await self._handle_pick(interaction, tag)
        self.select.callback = _on_select
        self.add_item(self.select)

        # Pager if >25 options
        if len(self.pages) > 1:
            prev_btn = discord.ui.Button(label="◀ Prev", style=discord.ButtonStyle.secondary)
            next_btn = discord.ui.Button(label="Next ▶", style=discord.ButtonStyle.secondary)

            async def _prev_cb(interaction: discord.Interaction):
                self.page = (self.page - 1) % len(self.pages)
                self._refresh()
                await interaction.response.edit_message(view=self)

            async def _next_cb(interaction: discord.Interaction):
                self.page = (self.page + 1) % len(self.pages)
                self._refresh()
                await interaction.response.edit_message(view=self)

            prev_btn.callback = _prev_cb
            next_btn.callback = _next_cb
            self.add_item(prev_btn); self.add_item(next_btn)

    def _refresh(self):
        self.select.options = [discord.SelectOption(label=t, value=t) for t in self.pages[self.page]]
        self.select.placeholder = f"Choose clan tag • Page {self.page+1}/{len(self.pages)}"

    async def _handle_pick(self, interaction: discord.Interaction, tag: str):
        pending = (_pending_welcome if self.mode == "welcome" else _pending_promo)
        info = pending.get(self.thread.id) or {}
        ticket   = info.get("ticket", self.ticket)
        username = info.get("username", self.username)
        close_dt = info.get("close_dt")

        pending.pop(self.thread.id, None)

        if self.mode == "welcome":
            await _finalize_welcome(self.thread, ticket, username, tag, close_dt)
        else:
            await _finalize_promo(self.thread, ticket, username, tag, close_dt)

        for item in self.children:
            item.disabled = True
        await interaction.response.edit_message(
            content=f"Got it — set clan tag to **{tag}** and logged to the sheet. ✅",
            view=self
        )

    async def on_timeout(self):
        """Expire quietly, offer reload, suggest typing the tag. No re-ping."""
        pending = (_pending_welcome if self.mode == "welcome" else _pending_promo)
        if self.thread.id not in pending:
            return
        try:
            for item in self.children:
                item.disabled = True
            note = "⏳ Tag picker expired. You can **Reload picker** below, or just type the tag."
            if self.message:
                await self.message.edit(content=note, view=TagPickerReloadView(self))
        except Exception:
            pass

# ---------- LIVE WATCHERS ----------
@bot.event
async def on_socket_response(_payload):
    _mark_event()

@bot.event
async def on_connect():
    global BOT_CONNECTED
    BOT_CONNECTED = True
    _mark_event()

@bot.event
async def on_resumed():
    global BOT_CONNECTED
    BOT_CONNECTED = True
    _mark_event()

@bot.event
async def on_ready():
    global BOT_CONNECTED, _LAST_READY_TS
    BOT_CONNECTED = True
    _LAST_READY_TS = _now()
    _mark_event()
    # start watchdog once
    try:
        if not _watchdog.is_running():
            _watchdog.start()
    except NameError:
        pass

    # start the 3×/day cache refresh (idempotent)
    global _refresh_task
    if _refresh_task is None or _refresh_task.done():
        _refresh_task = bot.loop.create_task(scheduled_refresh_loop())


@bot.event
async def on_disconnect():
    global BOT_CONNECTED, _LAST_DISCONNECT_TS
    BOT_CONNECTED = False
    _LAST_DISCONNECT_TS = _now()

async def _maybe_restart(reason: str):
    try:
        print(f"[WATCHDOG] Restarting: {reason}", flush=True)
    finally:
        try:
            await bot.close()
        finally:
            sys.exit(1)

@tasks.loop(seconds=WATCHDOG_CHECK_SEC)
async def _watchdog():
    now = _now()

    if BOT_CONNECTED:
        idle_for = (now - _LAST_EVENT_TS) if _LAST_EVENT_TS else 0
        try:
            latency = float(getattr(bot, "latency", 0.0)) if bot.latency is not None else None
        except Exception:
            latency = None

        # If connected but no events for >10m and latency missing/huge → likely zombied gateway
        if _LAST_EVENT_TS and idle_for > 600 and (latency is None or latency > 10):
            await _maybe_restart(f"zombie: no events {int(idle_for)}s, latency={latency}")
        return

    # Disconnected: count real downtime since last disconnect
    global _LAST_DISCONNECT_TS
    if not _LAST_DISCONNECT_TS:
        _LAST_DISCONNECT_TS = now
        return

    if (now - _LAST_DISCONNECT_TS) > WATCHDOG_MAX_DISCONNECT_SEC:
        await _maybe_restart(f"disconnected too long: {int(now - _LAST_DISCONNECT_TS)}s")

async def _health_json(_req):
    # Deep status: 200 if connected, 503 if disconnected, 206 if "zombie-ish"
    connected = BOT_CONNECTED
    age = _last_event_age_s()
    try:
        latency = float(bot.latency) if bot.latency is not None else None
    except Exception:
        latency = None

    status = 200 if connected else 503
    if connected and age is not None and age > 600 and (latency is None or latency > 10):
        status = 206

    body = {
        "ok": connected,
        "connected": connected,
        "uptime": uptime_str(),
        "last_event_age_s": age,
        "latency_s": latency,
    }
    return web.json_response(body, status=status)

async def _health_json_ok_always(_req):
    # Same payload as above, but **always** HTTP 200 (prevents platform flaps)
    connected = BOT_CONNECTED
    age = _last_event_age_s()
    try:
        latency = float(bot.latency) if bot.latency is not None else None
    except Exception:
        latency = None
    body = {
        "ok": connected,
        "connected": connected,
        "uptime": uptime_str(),
        "last_event_age_s": age,
        "latency_s": latency,
        "strict_probe": STRICT_PROBE,
    }
    return web.json_response(body, status=200)

async def start_webserver():
    app = web.Application()
    app["session"] = ClientSession()
    async def _close_session(app):
        await app["session"].close()
    app.on_cleanup.append(_close_session)

    # If STRICT_PROBE=0 (default): / and /ready always 200 to avoid flaps
    if STRICT_PROBE:
        app.router.add_get("/", _health_json)
        app.router.add_get("/ready", _health_json)
        app.router.add_get("/health", _health_json)
    else:
        app.router.add_get("/", _health_json_ok_always)
        app.router.add_get("/ready", _health_json_ok_always)
        app.router.add_get("/health", _health_json_ok_always)

    # Deep check for Render’s Health Check Path
    app.router.add_get("/healthz", _health_json)

    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.getenv("PORT", "10000"))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    print(f"[keepalive] HTTP server on :{port} | STRICT_PROBE={int(STRICT_PROBE)}", flush=True)

# ---------- Scheduled refresh (3x/day via REFRESH_TIMES) ----------
_refresh_task: Optional[asyncio.Task] = None

def _parse_times_csv(s: str) -> List[Tuple[int, int]]:
    out: List[Tuple[int, int]] = []
    for tok in (s or "").split(","):
        tok = tok.strip()
        if not tok: 
            continue
        try:
            h, m = map(int, tok.split(":"))
            h = max(0, min(23, h)); m = max(0, min(59, m))
            out.append((h, m))
        except Exception:
            pass
    # de-dup + sort; default to 02:00,10:00,18:00 if none valid
    out = sorted(set(out))
    return out or [(2, 0), (10, 0), (18, 0)]

async def _sleep_until(dt: datetime):
    now = datetime.now(dt.tzinfo)
    secs = (dt - now).total_seconds()
    if secs > 0:
        await asyncio.sleep(secs)

async def scheduled_refresh_loop():
    # timezone
    try:
        tz = ZoneInfo(TIMEZONE) if ZoneInfo else _tz.utc
    except Exception:
        tz = _tz.utc
    times = _parse_times_csv(REFRESH_TIMES)
    print(f"[refresh] TZ={TIMEZONE} times={times}", flush=True)

    while True:
        now = datetime.now(tz)
        today_candidates = [
            now.replace(hour=h, minute=m, second=0, microsecond=0)
            for (h, m) in times
            if now.replace(hour=h, minute=m, second=0, microsecond=0) > now
        ]
        if today_candidates:
            next_dt = min(today_candidates)
        else:
            h, m = times[0]
            next_dt = (now + _td(days=1)).replace(hour=h, minute=m, second=0, microsecond=0)

        await _sleep_until(next_dt)

        # Do the actual refresh:
        try:
            # force-reload clan tags (main read this bot does)
            _ = _load_clan_tags(force=True)

            # (Optional) warm worksheets so first write after refresh is snappy
            try:
                get_ws(SHEET1_NAME, HEADERS_SHEET1)
                get_ws(SHEET4_NAME, HEADERS_SHEET4)
            except Exception:
                pass

            # Log to channel if configured
            if LOG_CHANNEL_ID:
                ch = bot.get_channel(LOG_CHANNEL_ID)
                if isinstance(ch, (discord.TextChannel, discord.Thread)):
                    when_local = next_dt.astimezone(tz).strftime("%Y-%m-%d %H:%M")
                    try:
                        await ch.send(f"🔄 WelcomeCrew: refreshed caches at {when_local} ({TIMEZONE})")
                    except Exception:
                        pass

            print("[refresh] clan tags + sheet handles refreshed", flush=True)
        except Exception as e:
            print(f"[refresh] failed: {type(e).__name__}: {e}", flush=True)

_pending_welcome: Dict[int, Dict[str, Any]] = {}  # thread_id -> {ticket, username, close_dt}
_pending_promo:   Dict[int, Dict[str, Any]] = {}

def _is_thread_in_parent(thread: discord.Thread, parent_id: int) -> bool:
    try:
        return thread and thread.parent_id == parent_id
    except Exception:
        return False

@bot.event
async def on_thread_create(thread: discord.Thread):
    # Auto-join new threads in our target channels (even if not pinged)
    try:
        if thread.parent_id in {WELCOME_CHANNEL_ID, PROMO_CHANNEL_ID}:
            await thread.join()
    except Exception:
        pass

@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.CommandNotFound):
        return  # ignore silently
    try:
        await ctx.reply(f"⚠️ Command error: `{type(error).__name__}: {error}`")
    except:
        pass

@bot.event
async def on_message(message: discord.Message):
    # Only handle thread messages for watchers (commands still processed at end)
    if isinstance(message.channel, discord.Thread):
        th = message.channel

        # Ignore activity on reopened (unarchived/unlocked) threads for pending prompts
        try:
            if th.id in _pending_welcome or th.id in _pending_promo:
                # If the thread is no longer archived/locked, nuke pending state
                if not getattr(th, "archived", False) and not getattr(th, "locked", False):
                    _pending_welcome.pop(th.id, None)
                    _pending_promo.pop(th.id, None)
        except Exception:
            pass
        # If mentioned, join to ensure we can speak
        if th.parent_id in {WELCOME_CHANNEL_ID, PROMO_CHANNEL_ID}:
            if bot.user and bot.user.mentioned_in(message):
                try: await th.join()
                except Exception: pass

        # WELCOME watcher
        if ENABLE_LIVE_WATCH and ENABLE_LIVE_WATCH_WELCOME and _is_thread_in_parent(th, WELCOME_CHANNEL_ID):
            text = _aggregate_msg_text(message)
            if is_close_marker(text):
                parsed = parse_welcome_thread_name_allow_missing(th.name or "")
                if parsed:
                    ticket, username, tag = parsed
                    close_dt = message.created_at
                    log_action("welcome", "close_detected", ticket=_fmt_ticket(ticket), username=username, clantag=tag or "", link=thread_link(th))
                    if tag:
                        await _finalize_welcome(th, ticket, username, tag, close_dt)
                    else:
                        _pending_welcome[th.id] = {"ticket": ticket, "username": username, "close_dt": close_dt}
                        await _prompt_for_tag(th, ticket, username, message, mode="welcome")
                        log_action("welcome", "prompt_sent", ticket=_fmt_ticket(ticket), username=username, link=thread_link(th))
            elif th.id in _pending_welcome:
                if not message.author.bot:
                    tag = _match_tag_in_text(_aggregate_msg_text(message))
                    if tag:
                        info = _pending_welcome.pop(th.id, {})
                        ticket = info.get("ticket"); username = info.get("username"); close_dt = info.get("close_dt")
                        if ticket and username:
                            log_action("welcome", "tag_received", ticket=_fmt_ticket(ticket), clantag=tag, link=thread_link(th))
                            await _finalize_welcome(th, ticket, username, tag, close_dt)
                            try:
                                await th.send(f"Got it — set clan tag to **{tag}** and logged to the sheet. ✅")
                            except Exception:
                                pass

        # PROMO watcher
        if ENABLE_LIVE_WATCH and ENABLE_LIVE_WATCH_PROMO and _is_thread_in_parent(th, PROMO_CHANNEL_ID):
            text = _aggregate_msg_text(message)
            if is_close_marker(text):
                parsed = parse_promo_thread_name(th.name or "")
                if parsed:
                    ticket, username, tag = parsed
                    close_dt = message.created_at
                    log_action("promo", "close_detected", ticket=_fmt_ticket(ticket), username=username, clantag=tag or "", link=thread_link(th))
                    if tag:
                        await _finalize_promo(th, ticket, username, tag, close_dt)
                    else:
                        _pending_promo[th.id] = {"ticket": ticket, "username": username, "close_dt": close_dt}
                        await _prompt_for_tag(th, ticket, username, message, mode="promo")
                        log_action("promo", "prompt_sent", ticket=_fmt_ticket(ticket), username=username, link=thread_link(th))
            elif th.id in _pending_promo:
                if not message.author.bot:
                    tag = _match_tag_in_text(_aggregate_msg_text(message))
                    if tag:
                        info = _pending_promo.pop(th.id, {})
                        ticket = info.get("ticket"); username = info.get("username"); close_dt = info.get("close_dt")
                        if ticket and username:
                            log_action("promo", "tag_received", ticket=_fmt_ticket(ticket), clantag=tag, link=thread_link(th))
                            await _finalize_promo(th, ticket, username, tag, close_dt)
                            try:
                                await th.send(f"Got it — set clan tag to **{tag}** and logged to the sheet. ✅")
                            except Exception:
                                pass

    # Always let commands run
    await bot.process_commands(message)

@bot.event
async def on_thread_update(before: discord.Thread, after: discord.Thread):
    try:
        # Only watch our channels
        if after.parent_id not in {WELCOME_CHANNEL_ID, PROMO_CHANNEL_ID}:
            return

        # Use safe defaults = False (never True)
        b_arch = bool(getattr(before, "archived", False))
        a_arch = bool(getattr(after,  "archived", False))
        b_lock = bool(getattr(before, "locked",   False))
        a_lock = bool(getattr(after,  "locked",   False))

        # Fire only when transitioning TO archived/locked (i.e., close)
        just_archived = (not b_arch) and a_arch
        just_locked   = (not b_lock) and a_lock

        # If it’s a reopen (arch->false or lock->false), clear any pending prompts and bail
        just_reopened = (b_arch and not a_arch) or (b_lock and not a_lock)
        if just_reopened:
            _pending_welcome.pop(after.id, None)
            _pending_promo.pop(after.id, None)
            return

        if not (just_archived or just_locked):
            return

        # Parse and proceed like before
        if after.parent_id == WELCOME_CHANNEL_ID:
            parsed = parse_welcome_thread_name_allow_missing(after.name or "")
            scope  = "welcome"
        else:
            parsed = parse_promo_thread_name(after.name or "")
            scope  = "promo"

        if not parsed:
            log_action(scope, "skip_on_update", status="name parse fail", link=thread_link(after))
            return

        ticket, username, tag = parsed
        close_dt = await find_close_timestamp(after) or after.updated_at or after.created_at

        if scope == "welcome":
            if tag:
                await _finalize_welcome(after, ticket, username, tag, close_dt)
                log_action(scope, "close_detected_on_update",
                           ticket=_fmt_ticket(ticket), username=username, clantag=tag, link=thread_link(after))
            else:
                _pending_welcome[after.id] = {"ticket": ticket, "username": username, "close_dt": close_dt}
                await _prompt_for_tag(after, ticket, username, None, mode="welcome")
                log_action(scope, "prompt_sent_on_update",
                           ticket=_fmt_ticket(ticket), username=username, link=thread_link(after))
        else:
            if tag:
                await _finalize_promo(after, ticket, username, tag, close_dt)
                log_action(scope, "close_detected_on_update",
                           ticket=_fmt_ticket(ticket), username=username, clantag=tag, link=thread_link(after))
            else:
                _pending_promo[after.id] = {"ticket": ticket, "username": username, "close_dt": close_dt}
                await _prompt_for_tag(after, ticket, username, None, mode="promo")
                log_action(scope, "prompt_sent_on_update",
                           ticket=_fmt_ticket(ticket), username=username, link=thread_link(after))
    except Exception as e:
        print(f"on_thread_update error: {type(e).__name__}: {e}", flush=True)


# ------------------------ start -----------------------
async def _boot():
    if not TOKEN or len(TOKEN) < 20:
        raise RuntimeError("Missing/short DISCORD_TOKEN.")
    asyncio.create_task(start_webserver())
    await bot.start(TOKEN)

if __name__ == "__main__":
    asyncio.run(_boot())

