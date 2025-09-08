# clean_welcomecrew_bot_v2.py
# C1C – WelcomeCrew (clean slate, v2)
# Prefix-only commands; sheet logging only; feature flags via env (strict ON/OFF).
#
# Requires:
#   pip install discord.py gspread aiohttp
#
# Env vars:
#   DISCORD_TOKEN (or TOKEN)
#   WELCOME_CHANNEL_ID              # integer
#   PROMO_CHANNEL_ID                # integer (promotion-or-clan-move-requests)
#   GSHEET_ID
#   GOOGLE_SERVICE_ACCOUNT_JSON     # full JSON blob
#   TIMEZONE                        # e.g., Europe/Vienna
#   SHEET1_NAME                     # default: Sheet1
#   SHEET4_NAME                     # default: Sheet4
#
# Feature flags (default ON; set to OFF to disable):
#   ENABLE_WELCOME_SCAN
#   ENABLE_PROMO_SCAN
#   ENABLE_CMD_SHEETSTATUS
#   ENABLE_CMD_BACKFILL
#   ENABLE_CMD_BACKFILL_STATUS
#   ENABLE_CMD_DEDUPE
#   ENABLE_CMD_RELOAD
#   ENABLE_CMD_HEALTH
#   ENABLE_CMD_PING
#   ENABLE_CMD_CHECKSHEET
#   ENABLE_CMD_REBOOT
#   ENABLE_WEB_SERVER
#
# Permissions you need on the server:
#   - View Channels, Read Message History
#   - Manage Threads (to read archived private threads)
#   - Send Messages, Embed Links
#
import os, json, re, asyncio, time
from datetime import datetime, timezone as _tz
from typing import Optional, Tuple, Dict, Any, List

import discord
from discord.ext import commands

import gspread

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

# ---------------- Config & feature flags ----------------
def env_bool(key: str, default: bool=True) -> bool:
    raw = (os.getenv(key) or "").strip().upper()
    if raw == "":
        return default
    return raw == "ON"

TOKEN = os.getenv("DISCORD_TOKEN") or os.getenv("TOKEN") or ""
WELCOME_CHANNEL_ID = int(os.getenv("WELCOME_CHANNEL_ID", "0"))
PROMO_CHANNEL_ID   = int(os.getenv("PROMO_CHANNEL_ID", "0"))
GSHEET_ID          = os.getenv("GSHEET_ID", "")
TIMEZONE           = os.getenv("TIMEZONE", "UTC")
SHEET1_NAME        = os.getenv("SHEET1_NAME", "Sheet1")
SHEET4_NAME        = os.getenv("SHEET4_NAME", "Sheet4")

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

# ---------------- Discord setup ----------------
intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents, help_command=None)

START_TS = time.time()

def uptime_str() -> str:
    secs = int(time.time() - START_TS)
    h, r = divmod(secs, 3600)
    m, s = divmod(r, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"

def fmt_tz(dt: datetime) -> str:
    try:
        if ZoneInfo:
            tz = ZoneInfo(TIMEZONE) if TIMEZONE else ZoneInfo("UTC")
            return dt.astimezone(tz).strftime("%Y-%m-%d %H:%M")
    except Exception:
        pass
    try:
        return dt.strftime("%Y-%m-%d %H:%M")
    except Exception:
        return datetime.utcnow().strftime("%Y-%m-%d %H:%M")

# ---------------- Google Sheets ----------------
_gs_client = None
_ws_cache: Dict[str, Any] = {}      # name -> worksheet
_index_cache: Dict[str, Dict[str, int]] = {}  # name -> {ticket -> row}

def service_account_email() -> str:
    try:
        raw = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON") or ""
        data = json.loads(raw)
        return data.get("client_email","")
    except Exception:
        return ""

def gs_client():
    global _gs_client
    if _gs_client is None:
        raw = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON") or ""
        if not raw:
            raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON not set")
        sa = json.loads(raw)
        _gs_client = gspread.service_account_from_dict(sa)
    return _gs_client

def get_ws(name: str, want_headers: List[str]) -> Any:
    """Open worksheet by name and ensure headers exist."""
    if not GSHEET_ID:
        raise RuntimeError("GSHEET_ID not set")
    if name in _ws_cache:
        return _ws_cache[name]
    sh = gs_client().open_by_key(GSHEET_ID)
    try:
        ws = sh.worksheet(name)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=name, rows=1000, cols=max(10, len(want_headers)))
        ws.append_row(want_headers)
        _ws_cache[name] = ws
        _index_cache[name] = {}
        return ws

    # ensure headers
    try:
        head = ws.row_values(1)
        head_norm = [h.strip().lower() for h in head]
        want_norm = [h.strip().lower() for h in want_headers]
        if head_norm[:len(want_norm)] != want_norm:
            ws.insert_row(want_headers, 1)
    except Exception:
        pass

    _ws_cache[name] = ws
    _index_cache.setdefault(name, {})
    return ws

def parse_dt(s: str) -> Optional[datetime]:
    s = (s or "").strip()
    for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d/%m/%Y %H:%M", "%m/%d/%Y %H:%M"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=_tz.utc)
        except Exception:
            pass
    return None

def normalize_ticket(t: str) -> str:
    return (t or "").strip().lstrip("#")

def ws_index(name: str, ws) -> Dict[str,int]:
    """Build or return cached ticket->row map (column A)."""
    if _index_cache.get(name):
        return _index_cache[name]
    idx: Dict[str,int] = {}
    try:
        colA = ws.col_values(1)[1:]  # skip header
        for i, val in enumerate(colA, start=2):
            t = normalize_ticket(val)
            if t:
                idx[t] = i
    except Exception:
        pass
    _index_cache[name] = idx
    return idx

def upsert_row(name: str, ws, ticket: str, rowvals: List[str]) -> str:
    """Upsert by ticket (column A)."""
    ticket = normalize_ticket(ticket)
    idx = ws_index(name, ws)
    try:
        if ticket in idx:
            row = idx[ticket]
            rng = f"A{row}:{chr(ord('A')+len(rowvals)-1)}{row}"
            ws.batch_update([{"range": rng, "values": [rowvals]}])
            return "updated"
        else:
            ws.append_row(rowvals, value_input_option="USER_ENTERED")
            # Update cache (append at bottom)
            try:
                last_row = len(ws.col_values(1))
                _index_cache[name][ticket] = last_row
            except Exception:
                pass
            return "inserted"
    except Exception as e:
        print("Upsert error:", e, flush=True)
        return "error"

def dedupe_sheet(name: str, ws, has_type: bool=False) -> Tuple[int,int]:
    """Remove duplicate tickets; keep newest by date (col D)."""
    values = ws.get_all_values()
    if len(values) <= 1:
        return (0,0)
    header = values[0]
    rows = values[1:]
    winners: Dict[str, Tuple[int, Optional[datetime]]] = {}
    for i, row in enumerate(rows, start=2):
        ticket = normalize_ticket(row[0] if len(row)>0 else "")
        if not ticket:
            continue
        dt = parse_dt(row[3] if len(row)>3 else "")
        keep = winners.get(ticket)
        if not keep or ((dt or datetime.min.replace(tzinfo=_tz.utc)) > (keep[1] or datetime.min.replace(tzinfo=_tz.utc))):
            winners[ticket] = (i, dt)
    # delete everything not a winner
    to_delete = []
    keep_rows = {row for (row, _dt) in winners.values()}
    for i, _ in enumerate(rows, start=2):
        if i not in keep_rows:
            to_delete.append(i)
    # delete from bottom to top
    deleted = 0
    for r in sorted(to_delete, reverse=True):
        try:
            ws.delete_rows(r)
            deleted += 1
        except Exception:
            pass
    # rebuild index
    _index_cache[name] = {}
    ws_index(name, ws)
    return (len(winners), deleted)

# ---------------- Thread parsing helpers ----------------
WELCOME_PATTERN = re.compile(r'(?i)^closed-(\d{4})-([^-]+)-([A-Za-z0-9_]+)$')
FALLBACK_NUM = re.compile(r'(?i)(\d{4})')

PROMO_TYPE_PATTERNS = [
    (re.compile(r"(?i)we['’]re excited to have you returning"), "returning player"),
    (re.compile(r"(?i)thanks for sending in your move request"), "player move request"),
    (re.compile(r"(?i)we['’]ve received your request to help one of your clan members find a new home"), "clan lead move request"),
]

async def find_close_timestamp(thread: discord.Thread) -> Optional[datetime]:
    """Return created_at of the message that contains 'Ticket Closed by' (case-insensitive)."""
    try:
        await thread.join()
    except Exception:
        pass
    try:
        async for msg in thread.history(limit=300, oldest_first=False):
            # scan embeds + content
            parts = [msg.content or ""]
            for e in msg.embeds or []:
                parts.append(e.title or "")
                parts.append(e.description or "")
                if e.author and e.author.name: parts.append(e.author.name)
                for f in e.fields or []:
                    parts.append(f.name or ""); parts.append(f.value or "")
            merged = " | ".join(parts).lower()
            if "ticket closed by" in merged:
                return msg.created_at
    except discord.Forbidden:
        pass
    except Exception:
        pass
    return None

def parse_welcome_thread_name(name: str) -> Optional[Tuple[str,str,str]]:
    """Return (ticket, username, clantag) or None."""
    m = WELCOME_PATTERN.match(name or "")
    if not m: return None
    return (m.group(1), m.group(2).strip(), m.group(3).strip().upper())

def parse_generic_ticket_user_tag(name: str) -> Optional[Tuple[str,str,str]]:
    """Fallback: find 4digits-username-tag at end."""
    m = re.match(r'(?i).*(\d{4})-([^-]+)-([A-Za-z0-9_]+)$', name or "")
    if m:
        return (m.group(1), m.group(2).strip(), m.group(3).strip().upper())
    # any 4-digit group at least?
    m2 = FALLBACK_NUM.search(name or "")
    if m2:
        return (m2.group(1), "", "")
    return None

async def detect_promo_type(thread: discord.Thread) -> Optional[str]:
    """Look for any of the A/B/C marker lines (case-insensitive)."""
    try:
        await thread.join()
    except Exception:
        pass
    try:
        async for msg in thread.history(limit=300, oldest_first=False):
            parts = [msg.content or ""]
            for e in msg.embeds or []:
                parts.append(e.title or ""); parts.append(e.description or "")
                if e.author and e.author.name: parts.append(e.author.name)
                for f in e.fields or []:
                    parts.append(f.name or ""); parts.append(f.value or "")
            merged = " | ".join(parts)
            for rx, typ in PROMO_TYPE_PATTERNS:
                if rx.search(merged):
                    return typ
    except discord.Forbidden:
        pass
    except Exception:
        pass
    return None

# ---------------- Backfill engine ----------------
backfill_state = {
    "running": False,
    "welcome": {"scanned":0, "added":0, "updated":0, "skipped":0},
    "promo":   {"scanned":0, "added":0, "updated":0, "skipped":0},
    "last_msg": ""
}

HEADERS_SHEET1 = ["ticket number","username","clantag","date closed"]
HEADERS_SHEET4 = ["ticket number","username","clantag","date closed","type"]

async def scan_welcome_channel(channel: discord.TextChannel):
    st = backfill_state["welcome"] = {"scanned":0, "added":0, "updated":0, "skipped":0}
    if not ENABLE_WELCOME_SCAN: 
        backfill_state["last_msg"] = "welcome scan disabled"
        return

    ws = get_ws(SHEET1_NAME, HEADERS_SHEET1)
    idx = ws_index(SHEET1_NAME, ws)

    # public archived
    try:
        async for th in channel.archived_threads(limit=None, private=False):
            await _handle_welcome_thread(th, ws, idx, st)
    except discord.Forbidden:
        backfill_state["last_msg"] = "no access to public archived welcome threads"
    # private archived
    try:
        async for th in channel.archived_threads(limit=None, private=True):
            await _handle_welcome_thread(th, ws, idx, st)
    except discord.Forbidden:
        backfill_state["last_msg"] += " | no access to private archived welcome threads"

async def _handle_welcome_thread(th: discord.Thread, ws, idx_map, st):
    st["scanned"] += 1
    parsed = parse_welcome_thread_name(th.name or "")
    if not parsed:
        st["skipped"] += 1
        return
    ticket, username, clantag = parsed
    dt = await find_close_timestamp(th)
    date_str = fmt_tz(dt or datetime.utcnow().replace(tzinfo=_tz.utc))
    row = [ticket, username, clantag, date_str]
    status = upsert_row(SHEET1_NAME, ws, ticket, row)
    if status == "inserted": st["added"] += 1
    elif status == "updated": st["updated"] += 1
    else: st["skipped"] += 1

async def scan_promo_channel(channel: discord.TextChannel):
    st = backfill_state["promo"] = {"scanned":0, "added":0, "updated":0, "skipped":0}
    if not ENABLE_PROMO_SCAN:
        backfill_state["last_msg"] = "promo scan disabled"
        return

    ws = get_ws(SHEET4_NAME, HEADERS_SHEET4)
    idx = ws_index(SHEET4_NAME, ws)

    # public archived
    try:
        async for th in channel.archived_threads(limit=None, private=False):
            await _handle_promo_thread(th, ws, idx, st)
    except discord.Forbidden:
        backfill_state["last_msg"] = "no access to public archived promo threads"
    # private archived
    try:
        async for th in channel.archived_threads(limit=None, private=True):
            await _handle_promo_thread(th, ws, idx, st)
    except discord.Forbidden:
        backfill_state["last_msg"] += " | no access to private archived promo threads"

async def _handle_promo_thread(th: discord.Thread, ws, idx_map, st):
    st["scanned"] += 1
    parsed = parse_generic_ticket_user_tag(th.name or "")
    if not parsed:
        st["skipped"] += 1
        return
    ticket, username, clantag = parsed
    typ = await detect_promo_type(th) or ""
    dt = await find_close_timestamp(th)
    date_str = fmt_tz(dt or datetime.utcnow().replace(tzinfo=_tz.utc))
    row = [ticket, username, clantag, date_str, typ]
    status = upsert_row(SHEET4_NAME, ws, ticket, row)
    if status == "inserted": st["added"] += 1
    elif status == "updated": st["updated"] += 1
    else: st["skipped"] += 1

# ---------------- Commands (prefix only) ----------------
def cmd_enabled(flag: bool):
    def deco(func):
        async def wrapper(ctx: commands.Context, *args, **kwargs):
            if not flag:
                return await ctx.reply("This command is disabled by env flag.", mention_author=False)
            return await func(ctx, *args, **kwargs)
        return wrapper
    return deco

# removed duplicate ping
@cmd_enabled(ENABLE_CMD_PING)
async def cmd_ping(ctx: commands.Context):
    await ctx.reply("🏓 Pong — Live and listening.", mention_author=False)

@bot.command(name="sheetstatus")
@cmd_enabled(ENABLE_CMD_SHEETSTATUS)
async def cmd_sheetstatus(ctx: commands.Context):
    email = service_account_email() or "(no service account)"
    try:
        ws1 = get_ws(SHEET1_NAME, HEADERS_SHEET1)
        ws4 = get_ws(SHEET4_NAME, HEADERS_SHEET4)
        title = ws1.spreadsheet.title
        await ctx.reply(
            f"✅ Sheets OK: **{title}**\n• Tabs: `{SHEET1_NAME}`, `{SHEET4_NAME}`\n"
            f"• Share with: `{email}`",
            mention_author=False
        )
    except Exception as e:
        await ctx.reply(
            f"⚠️ Cannot open sheet: `{e}`\nShare with: `{email}`",
            mention_author=False
        )

@bot.command(name="backfill_tickets")
@cmd_enabled(ENABLE_CMD_BACKFILL)
async def cmd_backfill(ctx: commands.Context):
    if backfill_state["running"]:
        return await ctx.reply("A backfill is already running. Use !backfill_status.", mention_author=False)
    backfill_state["running"] = True
    backfill_state["last_msg"] = ""
    await ctx.reply("Starting backfill… Use !backfill_status for progress.", mention_author=False)
    try:
        if ENABLE_WELCOME_SCAN and WELCOME_CHANNEL_ID:
            ch = bot.get_channel(WELCOME_CHANNEL_ID)
            if isinstance(ch, discord.TextChannel):
                await scan_welcome_channel(ch)
        if ENABLE_PROMO_SCAN and PROMO_CHANNEL_ID:
            ch2 = bot.get_channel(PROMO_CHANNEL_ID)
            if isinstance(ch2, discord.TextChannel):
                await scan_promo_channel(ch2)
    finally:
        backfill_state["running"] = False
    w = backfill_state["welcome"]; p = backfill_state["promo"]
    await ctx.send(
        f"Done.\n"
        f"Welcome — scanned: **{w['scanned']}**, added: **{w['added']}**, updated: **{w['updated']}**, skipped: **{w['skipped']}**\n"
        f"Promo   — scanned: **{p['scanned']}**, added: **{p['added']}**, updated: **{p['updated']}**, skipped: **{p['skipped']}**\n"
        f"{backfill_state.get('last_msg','')}"
    )

@bot.command(name="backfill_status")
@cmd_enabled(ENABLE_CMD_BACKFILL_STATUS)
async def cmd_backfill_status(ctx: commands.Context):
    st = backfill_state
    w = st["welcome"]; p = st["promo"]
    await ctx.reply(
        f"Running: **{st['running']}** | Last: {st.get('last_msg','')}\n"
        f"Welcome — scanned: **{w['scanned']}**, added: **{w['added']}**, updated: **{w['updated']}**, skipped: **{w['skipped']}**\n"
        f"Promo   — scanned: **{p['scanned']}**, added: **{p['added']}**, updated: **{p['updated']}**, skipped: **{p['skipped']}**",
        mention_author=False
    )

@bot.command(name="dedupe_sheet")
@cmd_enabled(ENABLE_CMD_DEDUPE)
async def cmd_dedupe(ctx: commands.Context):
    try:
        ws1 = get_ws(SHEET1_NAME, HEADERS_SHEET1)
        ws4 = get_ws(SHEET4_NAME, HEADERS_SHEET4)
        kept1, deleted1 = dedupe_sheet(SHEET1_NAME, ws1, has_type=False)
        kept4, deleted4 = dedupe_sheet(SHEET4_NAME, ws4, has_type=True)
        await ctx.reply(
            f"Sheet1: kept **{kept1}** unique tickets, deleted **{deleted1}** dupes.\n"
            f"Sheet4: kept **{kept4}** unique tickets, deleted **{deleted4}** dupes.",
            mention_author=False
        )
    except Exception as e:
        await ctx.reply(f"Dedup failed: `{e}`", mention_author=False)

@bot.command(name="reload")
@cmd_enabled(ENABLE_CMD_RELOAD)
async def cmd_reload(ctx: commands.Context):
    _ws_cache.clear()
    _index_cache.clear()
    global _gs_client
    _gs_client = None
    await ctx.reply("Caches cleared. Reconnect to Sheets on next use.", mention_author=False)

@bot.command(name="health")
@cmd_enabled(ENABLE_CMD_HEALTH)
async def cmd_health(ctx: commands.Context):
    latency_ms = int(bot.latency * 1000)
    # check sheet quick
    try:
        ws1 = get_ws(SHEET1_NAME, HEADERS_SHEET1)
        ws1_title = ws1.title
        sheets_ok = f"OK ({ws1_title})"; circle = "🟢"
    except Exception:
        sheets_ok = "FAILED"; circle = "🔴"
    await ctx.reply(
        f"🟢 Bot OK | Latency: {latency_ms} ms | Sheets: {circle} {sheets_ok} | Uptime: {uptime_str()}",
        mention_author=False
    )

