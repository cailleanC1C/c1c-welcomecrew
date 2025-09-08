# C1C – WelcomeCrew (v8)
# Backfill + admin commands; strict ON/OFF; verified upserts; movers key = ticket+type+thread created;
# detailed audit + on-demand debug tools.

import os, json, re, asyncio, time, io
from datetime import datetime, timezone as _tz
from typing import Optional, Tuple, Dict, Any, List

import discord
from discord.ext import commands
import gspread

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

# ---------- Flags ----------
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
    except Exception: pass
    return (dt or datetime.utcnow().replace(tzinfo=_tz.utc)).strftime("%Y-%m-%d %H:%M")

def _print_boot_info():
    print("=== WelcomeCrew v8 boot ===", flush=True)
    print(f"Sheet tabs: {SHEET1_NAME} / {SHEET4_NAME}", flush=True)
    print(f"Welcome={WELCOME_CHANNEL_ID} Promo={PROMO_CHANNEL_ID}", flush=True)
    print(f"TZ={TIMEZONE}", flush=True)

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
    """Open worksheet; if header mismatches, overwrite header row in place."""
    if not GSHEET_ID: raise RuntimeError("GSHEET_ID not set")
    if name in _ws_cache: return _ws_cache[name]
    sh = gs_client().open_by_key(GSHEET_ID)
    try:
        ws = sh.worksheet(name)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=name, rows=2000, cols=max(10,len(want_headers)))
        ws.append_row(want_headers)
    else:
        try:
            head = ws.row_values(1)
            head_norm = [h.strip().lower() for h in head]
            want_norm = [h.strip().lower() for h in want_headers]
            if head_norm != want_norm:
                ws.update("A1", [want_headers])
        except Exception:
            pass
    _ws_cache[name] = ws
    return ws

def _key_promo(ticket: str, typ: str, created: str) -> str:
    return f"{(ticket or '').strip()}||{(typ or '').strip().lower()}||{(created or '').strip()}"

def ws_index_welcome(name: str, ws) -> Dict[str,int]:
    idx = {}
    try:
        colA = ws.col_values(1)[1:]  # skip header
        for i, val in enumerate(colA, start=2):
            t = (val or "").strip().lstrip("#")
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
            t   = (row[col_ticket]  if col_ticket  < len(row) else "").strip().lstrip("#")
            typ = (row[col_type]    if col_type    < len(row) else "").strip().lower()
            cr  = (row[col_created] if col_created < len(row) else "").strip()
            if t:
                idx[_key_promo(t, typ, cr)] = r_i
    except Exception: pass
    _index_promo[name] = idx
    return idx

# ---- VERIFIED upserts (no phantom adds) ----
def _verify_find_exact(ws, value: str) -> Optional[int]:
    """Return row of exact match anywhere on the sheet, else None."""
    try:
        cell = ws.find(f"^{re.escape(value)}$", in_column=1, regex=True)
        if cell: return cell.row
    except Exception:
        try:
            # fallback: scan last ~200 rows of col A
            vals = ws.col_values(1)
            for i in range(len(vals)-1, max(0, len(vals)-200), -1):
                if vals[i].strip() == value:
                    return i+1  # 1-indexed
        except Exception:
            pass
    return None

def upsert_welcome(name: str, ws, ticket: str, rowvals: List[str]) -> str:
    ticket = (ticket or "").strip().lstrip("#")
    idx = _index_simple.get(name) or ws_index_welcome(name, ws)
    try:
        if ticket in idx:
            row = idx[ticket]
            rng = f"A{row}:{chr(ord('A')+len(rowvals)-1)}{row}"
            ws.batch_update([{"range": rng, "values": [rowvals]}])
            return "updated"
        ws.append_row(rowvals, value_input_option="USER_ENTERED")
        # verify
        found = _verify_find_exact(ws, ticket)
        if found:
            _index_simple.setdefault(name, {})[ticket] = found
            return "inserted"
        else:
            return "error"
    except Exception as e:
        print("Welcome upsert error:", e, flush=True)
        return "error"

def upsert_promo(name: str, ws, ticket: str, typ: str, created_str: str, rowvals: List[str]) -> str:
    ticket = (ticket or "").strip().lstrip("#")
    key = _key_promo(ticket, typ, created_str)
    idx = _index_promo.get(name) or ws_index_promo(name, ws)
    try:
        if key in idx:
            row = idx[key]
            rng = f"A{row}:{chr(ord('A')+len(rowvals)-1)}{row}"
            ws.batch_update([{"range": rng, "values": [rowvals]}])
            return "updated"
        ws.append_row(rowvals, value_input_option="USER_ENTERED")
        # verify (look for exact ticket in col A AND match created/type near tail)
        # cheap verify: ensure ticket exists somewhere; then rebuild index
        found = _verify_find_exact(ws, ticket)
        ws_index_promo(name, ws)  # rebuild to include new row
        if found and (_key_promo(ticket, typ, created_str) in _index_promo[name]):
            return "inserted"
        else:
            return "error"
    except Exception as e:
        print("Promo upsert error:", e, flush=True)
        return "error"

def dedupe_sheet(name: str, ws, has_type: bool=False) -> Tuple[int,int]:
    """If has_type=True (Sheet4), dedupe by (ticket,type,created). Else by ticket only. Keep newest by date closed."""
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
        t = (row[col_ticket] if col_ticket < len(row) else "").strip().lstrip("#")
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

# ---------- Parsing ----------
WELCOME_PATTERN = re.compile(r'(?i)^closed-(\d{4})-([^-]+)-([A-Za-z0-9_]+)$')
FALLBACK_NUM    = re.compile(r'(?i)(\d{4})')

PROMO_TYPE_PATTERNS = [
    (re.compile(r"(?i)we['’]re excited to have you returning"), "returning player"),
    (re.compile(r"(?i)thanks for sending in your move request"), "player move request"),
    (re.compile(r"(?i)we['’]ve received your request to help one of your clan members find a new home"), "clan lead move request"),
]

async def find_close_timestamp(thread: discord.Thread) -> Optional[datetime]:
    try: await thread.join()
    except Exception: pass
    try:
        async for msg in thread.history(limit=300, oldest_first=False):
            parts = [msg.content or ""]
            for e in msg.embeds or []:
                parts += [e.title or "", e.description or ""]
                if e.author and e.author.name: parts.append(e.author.name)
                for f in e.fields or []: parts += [f.name or "", f.value or ""]
            if "ticket closed by" in " | ".join(parts).lower():
                return msg.created_at
    except discord.Forbidden: pass
    except Exception: pass
    return None

def parse_welcome_thread_name(name: str) -> Optional[Tuple[str,str,str]]:
    m = WELCOME_PATTERN.match(name or "")
    if not m: return None
    return (m.group(1), m.group(2).strip(), m.group(3).strip().upper())

def parse_generic_ticket_user_tag(name: str) -> Optional[Tuple[str,str,str]]:
    m = re.match(r'(?i).*(\d{4})-([^-]+)-([A-Za-z0-9_]+)$', name or "")
    if m: return (m.group(1), m.group(2).strip(), m.group(3).strip().upper())
    m2 = FALLBACK_NUM.search(name or ""); 
    return (m2.group(1), "", "") if m2 else None

async def detect_promo_type(thread: discord.Thread) -> Optional[str]:
    try: await thread.join()
    except Exception: pass
    try:
        async for msg in thread.history(limit=300, oldest_first=False):
            parts = [msg.content or ""]
            for e in msg.embeds or []:
                parts += [e.title or "", e.description or ""]
                if e.author and e.author.name: parts.append(e.author.name)
                for f in e.fields or []: parts += [f.name or "", f.value or ""]
            merged = " | ".join(parts)
            for rx, typ in PROMO_TYPE_PATTERNS:
                if rx.search(merged): return typ
    except discord.Forbidden: pass
    except Exception: pass
    return None

# ---------- Backfill state (with ticket lists) ----------
def _new_bucket():
    return {"scanned":0,"added":0,"updated":0,"skipped":0,
            "added_ids":[], "updated_ids":[], "skipped_ids":[]}

backfill_state = {
    "running": False,
    "welcome": _new_bucket(),
    "promo":   _new_bucket(),
    "last_msg": ""
}

# ---------- Scans ----------
async def scan_welcome_channel(channel: discord.TextChannel):
    st = backfill_state["welcome"] = _new_bucket()
    if not ENABLE_WELCOME_SCAN:
        backfill_state["last_msg"] = "welcome scan disabled"; return

    ws = get_ws(SHEET1_NAME, HEADERS_SHEET1)
    ws_index_welcome(SHEET1_NAME, ws)

    try:
        async for th in channel.archived_threads(limit=None, private=False):
            await _handle_welcome_thread(th, ws, st)
    except discord.Forbidden:
        backfill_state["last_msg"] = "no access to public archived welcome threads"
    try:
        async for th in channel.archived_threads(limit=None, private=True):
            await _handle_welcome_thread(th, ws, st)
    except discord.Forbidden:
        backfill_state["last_msg"] += " | no access to private archived welcome threads"

async def _handle_welcome_thread(th: discord.Thread, ws, st):
    st["scanned"] += 1
    parsed = parse_welcome_thread_name(th.name or "")
    if not parsed:
        st["skipped"] += 1; st["skipped_ids"].append(f"name:{th.name}")
        return
    ticket, username, clantag = parsed
    dt = await find_close_timestamp(th)
    date_str = fmt_tz(dt or datetime.utcnow().replace(tzinfo=_tz.utc))
    row = [ticket, username, clantag, date_str]
    status = upsert_welcome(SHEET1_NAME, ws, ticket, row)
    if status == "inserted":
        st["added"] += 1; st["added_ids"].append(ticket)
    elif status == "updated":
        st["updated"] += 1; st["updated_ids"].append(ticket)
    else:
        st["skipped"] += 1; st["skipped_ids"].append(ticket)

async def scan_promo_channel(channel: discord.TextChannel):
    st = backfill_state["promo"] = _new_bucket()
    if not ENABLE_PROMO_SCAN:
        backfill_state["last_msg"] = "promo scan disabled"; return

    ws = get_ws(SHEET4_NAME, HEADERS_SHEET4)
    ws_index_promo(SHEET4_NAME, ws)

    try:
        async for th in channel.archived_threads(limit=None, private=False):
            await _handle_promo_thread(th, ws, st)
    except discord.Forbidden:
        backfill_state["last_msg"] = "no access to public archived promo threads"
    try:
        async for th in channel.archived_threads(limit=None, private=True):
            await _handle_promo_thread(th, ws, st)
    except discord.Forbidden:
        backfill_state["last_msg"] += " | no access to private archived promo threads"

async def _handle_promo_thread(th: discord.Thread, ws, st):
    st["scanned"] += 1
    parsed = parse_generic_ticket_user_tag(th.name or "")
    if not parsed:
        st["skipped"] += 1; st["skipped_ids"].append(f"name:{th.name}")
        return
    ticket, username, clantag = parsed
    typ = await detect_promo_type(th) or ""
    dt_close = await find_close_timestamp(th)
    date_str = fmt_tz(dt_close or datetime.utcnow().replace(tzinfo=_tz.utc))
    created_str = fmt_tz(th.created_at)

    row = [ticket, username, clantag, date_str, typ, created_str]
    status = upsert_promo(SHEET4_NAME, ws, ticket, typ, created_str, row)
    key = f"{ticket}:{typ or 'unknown'}:{created_str}"
    if status == "inserted":
        st["added"] += 1; st["added_ids"].append(key)
    elif status == "updated":
        st["updated"] += 1; st["updated_ids"].append(key)
    else:
        st["skipped"] += 1; st["skipped_ids"].append(key)

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
            f"✅ Sheets OK: **{title}**\n• Tabs: `{SHEET1_NAME}`, `{SHEET4_NAME}`\n• Share with: `{email}`",
            mention_author=False
        )
    except Exception as e:
        await ctx.reply(f"⚠️ Cannot open sheet: `{e}`\nShare with: `{email}`", mention_author=False)

@bot.command(name="backfill_tickets")
@cmd_enabled(ENABLE_CMD_BACKFILL)
async def cmd_backfill(ctx):
    if backfill_state["running"]:
        return await ctx.reply("A backfill is already running. Use !backfill_status.", mention_author=False)
    backfill_state["running"] = True; backfill_state["last_msg"] = ""
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
        "Done.\n"
        f"Welcome — scanned: **{w['scanned']}**, added: **{w['added']}**, updated: **{w['updated']}**, skipped: **{w['skipped']}**\n"
        f"Promo   — scanned: **{p['scanned']}**, added: **{p['added']}**, updated: **{p['updated']}**, skipped: **{p['skipped']}**\n"
        f"{backfill_state.get('last_msg','')}"
    )
    await _post_short_report(ctx)

def _fmt_list(ids: List[str], max_items=10) -> str:
    if not ids: return "—"
    show = ids[:max_items]
    extra = len(ids) - len(show)
    return ", ".join(show) + (f" …(+{extra})" if extra>0 else "")

async def _post_short_report(ctx):
    w = backfill_state["welcome"]; p = backfill_state["promo"]
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

@bot.command(name="backfill_status")
@cmd_enabled(ENABLE_CMD_BACKFILL_STATUS)
async def cmd_backfill_status(ctx):
    st = backfill_state; w = st["welcome"]; p = st["promo"]
    await ctx.reply(
        f"Running: **{st['running']}** | Last: {st.get('last_msg','')}\n"
        f"Welcome — scanned: **{w['scanned']}**, added: **{w['added']}**, updated: **{w['updated']}**, skipped: **{w['skipped']}**\n"
        f"Promo   — scanned: **{p['scanned']}**, added: **{p['added']}**, updated: **{p['updated']}**, skipped: **{p['skipped']}**\n"
        f"Welcome added: {_fmt_list(w['added_ids'])}\n"
        f"Welcome updated: {_fmt_list(w['updated_ids'])}\n"
        f"Promo added: {_fmt_list(p['added_ids'])}\n"
        f"Promo updated: {_fmt_list(p['updated_ids'])}",
        mention_author=False
    )

@bot.command(name="backfill_report")
async def cmd_backfill_report(ctx: commands.Context):
    w = backfill_state["welcome"]; p = backfill_state["promo"]
    lines = []
    lines += ["WELCOME — added:", *w["added_ids"], "", "WELCOME — updated:", *w["updated_ids"], "", "WELCOME — skipped:", *w["skipped_ids"], ""]
    lines += ["PROMO — added:",   *p["added_ids"], "", "PROMO — updated:",   *p["updated_ids"], "", "PROMO — skipped:",   *p["skipped_ids"], ""]
    data = "\n".join(lines) or "(empty)"
    buf = io.BytesIO(data.encode("utf-8"))
    await ctx.reply(file=discord.File(buf, filename="backfill_report.txt"), mention_author=False)

@bot.command(name="sheet_debug")
async def cmd_sheet_debug(ctx: commands.Context):
    try:
        ws1 = get_ws(SHEET1_NAME, HEADERS_SHEET1)
        ws4 = get_ws(SHEET4_NAME, HEADERS_SHEET4)
        title = ws1.spreadsheet.title
        last1 = ws1.get(f"A{max(2, ws1.row_count-5)}:D{ws1.row_count}")  # rough tail
        last4 = ws4.get(f"A{max(2, ws4.row_count-5)}:F{ws4.row_count}")
        def fmt_tail(rows):
            rows = [r for r in rows if any(c for c in r)]
            return "\n".join(" | ".join(r) for r in rows[-5:]) or "(no rows)"
        await ctx.reply(
            f"Spreadsheet: **{title}**\n"
            f"Tab `{SHEET1_NAME}` tail:\n```\n{fmt_tail(last1)}\n```\n"
            f"Tab `{SHEET4_NAME}` tail:\n```\n{fmt_tail(last4)}\n```",
            mention_author=False
        )
    except Exception as e:
        await ctx.reply(f"sheet_debug failed: `{e}`", mention_author=False)

@bot.command(name="sheet_probe")
async def cmd_sheet_probe(ctx: commands.Context):
    """Append then delete a test row to prove write perms."""
    try:
        ws1 = get_ws(SHEET1_NAME, HEADERS_SHEET1)
        token = f"TEST-{int(time.time())}"
        row = [token,"probe","ZZZ",fmt_tz(datetime.utcnow().replace(tzinfo=_tz.utc))]
        ws1.append_row(row, value_input_option="USER_ENTERED")
        found = _verify_find_exact(ws1, token)
        if not found:
            return await ctx.reply("Probe append failed (not found).", mention_author=False)
        ws1.delete_rows(found)
        await ctx.reply("Probe write+delete succeeded.", mention_author=False)
    except Exception as e:
        await ctx.reply(f"sheet_probe failed: `{e}`", mention_author=False)

@bot.command(name="dedupe_sheet")
@cmd_enabled(ENABLE_CMD_DEDUPE)
async def cmd_dedupe(ctx):
    try:
        ws1 = get_ws(SHEET1_NAME, HEADERS_SHEET1)
        ws4 = get_ws(SHEET4_NAME, HEADERS_SHEET4)
        kept1, deleted1 = dedupe_sheet(SHEET1_NAME, ws1, has_type=False)
        kept4, deleted4 = dedupe_sheet(SHEET4_NAME, ws4, has_type=True)  # ticket+type+created
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
    global _gs_client; _gs_client = None
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
