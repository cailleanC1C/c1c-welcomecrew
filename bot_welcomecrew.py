# bot_welcomecrew.py
# ------------------------------------------------------------
# Requires Python 3.10+ and:
#   pip install -U discord.py gspread tzdata aiohttp
#
# ENV VARS (Render → Environment):
#   DISCORD_TOKEN
#   WELCOME_CHANNEL_ID
#   TICKET_TOOL_BOT_ID (public Ticket Tool: 557628352828014614)
#   RECRUITER_ROLE_ID (optional)
#   GUILD_ID (optional)
#   GSHEET_ID
#   GOOGLE_SERVICE_ACCOUNT_JSON (paste full JSON)
#   GSHEET_WORKSHEET (optional)
#   TIMEZONE (e.g., Europe/Vienna)
#   CLAN_TAGS (e.g., C1CM,C1CE,C1CB,VGR,MRTRS)
#   PYTHONUNBUFFERED=1   <-- recommended for real-time logs
# ------------------------------------------------------------

import os
import re
import asyncio
import json
from datetime import datetime
from typing import Optional

print("[boot] starting bot_welcomecrew.py", flush=True)

import discord
from discord.ext import commands
from discord import app_commands

import gspread
try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None  # fallback to UTC

# ---------- Config ----------
TOKEN = os.getenv("DISCORD_TOKEN")
GUILD_ID = int(os.getenv("GUILD_ID", "0"))
WELCOME_CHANNEL_ID = int(os.getenv("WELCOME_CHANNEL_ID", "0"))
TICKET_TOOL_BOT_ID = int(os.getenv("TICKET_TOOL_BOT_ID", "0"))
RECRUITER_ROLE_ID = int(os.getenv("RECRUITER_ROLE_ID", "0"))

GSHEET_ID = os.getenv("GSHEET_ID", "")
GSHEET_WORKSHEET = os.getenv("GSHEET_WORKSHEET", "")
TIMEZONE = os.getenv("TIMEZONE", "UTC")
CLAN_TAGS = [t.strip() for t in os.getenv("CLAN_TAGS", "C1CM,C1CE,C1CB").split(",") if t.strip()]

THREAD_NAME_REGEX = r"^\d{3,6}-"  # e.g., 0298-username
COLOR_PRIMARY = 0x5865F2
COLOR_SUCCESS = 0x2ECC71
COLOR_WARN = 0xF1C40F

# ---------- Bot ----------
intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)

# Backfill live status
_backfill_state = {"running": False, "phase": "", "scanned": 0, "added": 0, "skipped": 0}

# ---------- Session store ----------
class Session:
    def __init__(self, user_id: int, thread_id: int, start_msg_id: Optional[int] = None):
        self.user_id = user_id
        self.thread_id = thread_id
        self.start_msg_id = start_msg_id
        self.answers: dict[str, str | list[str]] = {}

        self.done_basic = False
        self.chosen_playstyle = False
        self.have_cb = False
        self.have_hydra_levels = False
        self.have_hydra_nums = False
        self.have_chimera_levels = False
        self.have_chimera_nums = False
        self.have_siege = False
        self.have_cvc_interest = False
        self.have_cvc_points = False
        self.have_cvc_style = False
        self.have_ref = False

_sessions: dict[int, Session] = {}
_thread_locks: dict[int, int] = {}          # thread_id -> applicant_user_id
_thread_prompt_msg_id: dict[int, int] = {}  # thread_id -> start message id

# ---------- Embeds ----------
def start_embed(mention: Optional[str]) -> discord.Embed:
    e = discord.Embed(
        title="C1C Application",
        description=(f"Hey {mention or 'there'}! ✨\n"
                     "Tap **Start** to begin. This panel is locked to you."),
        color=COLOR_PRIMARY
    )
    e.set_footer(text="Mobile/desktop friendly • Answers will post here in-thread")
    return e

def submitted_embed(mention: str) -> discord.Embed:
    return discord.Embed(
        title="Application started",
        description=f"Thanks, {mention}! Your answers are posted below. A recruiter will follow up here.",
        color=COLOR_SUCCESS
    )

def step_embed(title: str, hints: str = "") -> discord.Embed:
    e = discord.Embed(title=title, color=COLOR_PRIMARY)
    if hints:
        e.description = hints
    return e

def summary_embed(user: discord.User, a: dict) -> discord.Embed:
    e = discord.Embed(
        title="C1C Match Application",
        description=f"Applicant: {user.mention} (`{user.name}`)",
        color=COLOR_PRIMARY
    )
    def F(name: str, key: str, default: str = "-"):
        val = a.get(key, default)
        if isinstance(val, list): val = ", ".join(val) if val else default
        sval = str(val).strip()
        if len(sval) > 900: sval = sval[:897] + "..."
        e.add_field(name=f"🔹 {name}", value=sval or default, inline=False)

    F("In-game name", "ign")
    F("Account level", "acc_level")
    F("Clan style (what you want)", "clan_style")
    F("Playstyle", "playstyle")
    F("Clan Boss - Level", "cb_level")
    F("Clan Boss - Damage", "cb_damage")
    F("Hydra - Levels", "hydra_levels")
    F("Hydra - Damage", "hydra_damage")
    F("Hydra - Clashpoints", "hydra_clash")
    F("Chimera - Levels", "chimera_levels")
    F("Chimera - Damage", "chimera_damage")
    F("Chimera - Clashpoints", "chimera_clash")
    F("Siege - Interested?", "siege_interest")
    F("Siege - Teams prepared?", "siege_teams")
    F("CvC - Interested?", "cvc_interest")
    F("CvC - Min points (guarantee)", "cvc_min")
    F("CvC - Style", "cvc_style")
    F("Who sent you? How did you find us?", "ref")
    e.set_footer(text="Tip: Ping a Recruitment Coordinator to start matching.")
    return e

# ---------- Modals ----------
class BasicInfoModal(discord.ui.Modal, title="Application - Basics"):
    ign = discord.ui.TextInput(label="In-game player name", style=discord.TextStyle.short, max_length=64, required=True)
    acc_level = discord.ui.TextInput(label="Account level", placeholder="e.g., 72", style=discord.TextStyle.short, max_length=8, required=True)
    clan_style = discord.ui.TextInput(label="Clan style - what's important to you?", style=discord.TextStyle.paragraph, max_length=500, required=True)
    def __init__(self, sess: Session): super().__init__(); self.sess = sess
    async def on_submit(self, itx: discord.Interaction):
        if itx.user.id != self.sess.user_id: return await itx.response.send_message("Locked to the applicant. Use /apply for your own.", ephemeral=True)
        lvl = self.acc_level.value.strip()
        if not re.fullmatch(r"\d{1,3}", lvl): return await itx.response.send_message("Enter a numeric account level (e.g., 72).", ephemeral=True)
        self.sess.answers["ign"] = self.ign.value.strip()
        self.sess.answers["acc_level"] = lvl
        self.sess.answers["clan_style"] = self.clan_style.value.strip()
        self.sess.done_basic = True
        await itx.response.send_message("Saved basics ✔️", ephemeral=True)

class CBDamageModal(discord.ui.Modal, title="Clan Boss - Damage"):
    cb_damage = discord.ui.TextInput(label="Damage", placeholder="e.g., 200M on UNM", style=discord.TextStyle.short, max_length=48, required=True)
    def __init__(self, sess: Session): super().__init__(); self.sess = sess
    async def on_submit(self, itx: discord.Interaction):
        if itx.user.id != self.sess.user_id: return await itx.response.send_message("Locked to applicant.", ephemeral=True)
        self.sess.answers["cb_damage"] = self.cb_damage.value.strip()
        self.sess.have_cb = True
        await itx.response.send_message("Saved CB damage ✔️", ephemeral=True)

class HydraNumsModal(discord.ui.Modal, title="Hydra - Numbers"):
    hydra_damage = discord.ui.TextInput(label="Damage", placeholder="e.g., 20M per key", style=discord.TextStyle.short, max_length=48, required=True)
    hydra_clash  = discord.ui.TextInput(label="Clashpoints (guarantee)", placeholder="e.g., 50k", style=discord.TextStyle.short, max_length=48, required=True)
    def __init__(self, sess: Session): super().__init__(); self.sess = sess
    async def on_submit(self, itx: discord.Interaction):
        if itx.user.id != self.sess.user_id: return await itx.response.send_message("Locked to applicant.", ephemeral=True)
        self.sess.answers["hydra_damage"] = self.hydra_damage.value.strip()
        self.sess.answers["hydra_clash"]  = self.hydra_clash.value.strip()
        self.sess.have_hydra_nums = True
        await itx.response.send_message("Saved Hydra numbers ✔️", ephemeral=True)

class ChimeraNumsModal(discord.ui.Modal, title="Chimera - Numbers"):
    chimera_damage = discord.ui.TextInput(label="Damage", placeholder="e.g., 6M on Hard", style=discord.TextStyle.short, max_length=48, required=True)
    chimera_clash  = discord.ui.TextInput(label="Clashpoints (guarantee)", placeholder="e.g., 50k", style=discord.TextStyle.short, max_length=48, required=True)
    def __init__(self, sess: Session): super().__init__(); self.sess = sess
    async def on_submit(self, itx: discord.Interaction):
        if itx.user.id != self.sess.user_id: return await itx.response.send_message("Locked to applicant.", ephemeral=True)
        self.sess.answers["chimera_damage"] = self.chimera_damage.value.strip()
        self.sess.answers["chimera_clash"]  = self.chimera_clash.value.strip()
        self.sess.have_chimera_nums = True
        await itx.response.send_message("Saved Chimera numbers ✔️", ephemeral=True)

class CvCPointsModal(discord.ui.Modal, title="CvC - Minimum points"):
    cvc_min = discord.ui.TextInput(label="Minimum points you can commit", placeholder="e.g., 100000", style=discord.TextStyle.short, max_length=12, required=True)
    def __init__(self, sess: Session): super().__init__(); self.sess = sess
    async def on_submit(self, itx: discord.Interaction):
        if itx.user.id != self.sess.user_id: return await itx.response.send_message("Locked to applicant.", ephemeral=True)
        pts = self.cvc_min.value.strip().replace(",", "")
        if not re.fullmatch(r"\d{1,9}", pts): return await itx.response.send_message("Digits only for CvC points (e.g., 100000).", ephemeral=True)
        self.sess.answers["cvc_min"] = pts
        self.sess.have_cvc_points = True
        await itx.response.send_message("Saved CvC points ✔️", ephemeral=True)

class ReferralModal(discord.ui.Modal, title="Who sent you? How did you find us?"):
    ref = discord.ui.TextInput(label="Referral / how you found us", style=discord.TextStyle.paragraph, max_length=300, required=True)
    def __init__(self, sess: Session): super().__init__(); self.sess = sess
    async def on_submit(self, itx: discord.Interaction):
        if itx.user.id != self.sess.user_id: return await itx.response.send_message("Locked to applicant.", ephemeral=True)
        self.sess.answers["ref"] = self.ref.value.strip()
        self.sess.have_ref = True
        await itx.response.send_message("Saved referral ✔️", ephemeral=True)

# ---------- Selects ----------
def locked(itx: discord.Interaction, sess: Session) -> bool:
    if itx.user.id != sess.user_id:
        asyncio.create_task(itx.response.send_message("This panel is locked to the applicant.", ephemeral=True))
        return True
    return False

class PlaystyleSelect(discord.ui.Select):
    def __init__(self, sess: Session):
        opts = [discord.SelectOption(label=o) for o in ["stress-free", "casual", "semi-competitive", "competitive"]]
        super().__init__(placeholder="Choose your playstyle", min_values=1, max_values=1, options=opts)
        self.sess = sess
    async def callback(self, itx: discord.Interaction):
        if locked(itx, self.sess): return
        self.sess.answers["playstyle"] = self.values[0]
        self.sess.chosen_playstyle = True
        await itx.response.send_message(f"Playstyle set to {self.values[0]} ✔️", ephemeral=True)

class CBLevelSelect(discord.ui.Select):
    def __init__(self, sess: Session):
        opts = [discord.SelectOption(label=o) for o in ["Easy","Normal","Hard","Brutal","Nightmare","Ultra-Nightmare"]]
        super().__init__(placeholder="Clan Boss level", min_values=1, max_values=1, options=opts)
        self.sess = sess
    async def callback(self, itx: discord.Interaction):
        if locked(itx, self.sess): return
        self.sess.answers["cb_level"] = self.values[0]
        await itx.response.send_message(f"CB level set to {self.values[0]} ✔️", ephemeral=True)

class HydraLevelsSelect(discord.ui.Select):
    def __init__(self, sess: Session):
        opts = [discord.SelectOption(label=o) for o in ["Easy","Normal","Hard","Brutal","Nightmare"]]
        super().__init__(placeholder="Hydra levels (up to 3)", min_values=1, max_values=3, options=opts)
        self.sess = sess
    async def callback(self, itx: discord.Interaction):
        if locked(itx, self.sess): return
        self.sess.answers["hydra_levels"] = self.values
        self.sess.have_hydra_levels = True
        await itx.response.send_message(f"Hydra levels: {', '.join(self.values)} ✔️", ephemeral=True)

class ChimeraLevelsSelect(discord.ui.Select):
    def __init__(self, sess: Session):
        opts = [discord.SelectOption(label=o) for o in ["Easy","Normal","Hard","Brutal","Nightmare","Ultra-Nightmare"]]
        super().__init__(placeholder="Chimera levels (up to 2)", min_values=1, max_values=2, options=opts)
        self.sess = sess
    async def callback(self, itx: discord.Interaction):
        if locked(itx, self.sess): return
        self.sess.answers["chimera_levels"] = self.values
        self.sess.have_chimera_levels = True
        await itx.response.send_message(f"Chimera levels: {', '.join(self.values)} ✔️", ephemeral=True)

class YesNoSelect(discord.ui.Select):
    def __init__(self, placeholder: str, key: str, sess: Session):
        super().__init__(placeholder=placeholder, min_values=1, max_values=1,
                         options=[discord.SelectOption(label="yes"), discord.SelectOption(label="no")])
        self.key, self.sess = key, sess
    async def callback(self, itx: discord.Interaction):
        if locked(itx, self.sess): return
        self.sess.answers[self.key] = self.values[0]
        if self.key == "siege_interest": self.sess.have_siege = True
        if self.key == "cvc_interest": self.sess.have_cvc_interest = True
        await itx.response.send_message(f"{self.placeholder}: {self.values[0]} ✔️", ephemeral=True)

class CvCStyleSelect(discord.ui.Select):
    def __init__(self, sess: Session):
        super().__init__(placeholder="CvC style", min_values=1, max_values=1,
                         options=[discord.SelectOption(label=o) for o in ["competitive","rather chill","don't care about CvC"]])
        self.sess = sess
    async def callback(self, itx: discord.Interaction):
        if locked(itx, self.sess): return
        self.sess.answers["cvc_style"] = self.values[0]
        self.sess.have_cvc_style = True
        await itx.response.send_message(f"CvC style: {self.values[0]} ✔️", ephemeral=True)

# ---------- Views ----------
class StartApplyView(discord.ui.View):
    def __init__(self, applicant_id: Optional[int], thread_id: int, disabled: bool = False):
        super().__init__(timeout=3600)
        self.applicant_id = applicant_id
        self.thread_id = thread_id
        for c in self.children:
            if isinstance(c, discord.ui.Button):
                c.disabled = disabled

    @discord.ui.button(label="Start", style=discord.ButtonStyle.primary, custom_id="start_apply_btn")
    async def start(self, itx: discord.Interaction, btn: discord.ui.Button):
        locked_uid = _thread_locks.get(self.thread_id)
        if locked_uid is None:
            _thread_locks[self.thread_id] = self.applicant_id or itx.user.id
            locked_uid = _thread_locks[self.thread_id]
        if itx.user.id != locked_uid:
            return await itx.response.send_message("Locked to the applicant. Use /apply for your own.", ephemeral=True)

        sess = _sessions.get(itx.user.id)
        if not sess:
            sess = Session(itx.user.id, self.thread_id, start_msg_id=_thread_prompt_msg_id.get(self.thread_id))
            _sessions[itx.user.id] = sess

        await itx.response.send_message("Starting...", ephemeral=True)
        thread = itx.channel
        e = step_embed("Step 1 - Basics", "Fill basics (modal) and choose your Playstyle. Then press Next.")
        await thread.send(embed=e, view=Step1View(sess))

class Step1View(discord.ui.View):
    def __init__(self, sess: Session):
        super().__init__(timeout=1800); self.sess = sess
        self.add_item(PlaystyleSelect(sess))

    @discord.ui.button(label="Fill basics", style=discord.ButtonStyle.secondary)
    async def basics(self, itx: discord.Interaction, _: discord.ui.Button):
        if locked(itx, self.sess): return
        await itx.response.send_modal(BasicInfoModal(self.sess))

    @discord.ui.button(label="Next ➜", style=discord.ButtonStyle.primary)
    async def next(self, itx: discord.Interaction, _: discord.ui.Button):
        if locked(itx, self.sess): return
        if not (self.sess.done_basic and self.sess.chosen_playstyle):
            return await itx.response.send_message("Finish Basics and choose Playstyle first.", ephemeral=True)
        await itx.response.send_message("On to Step 2...", ephemeral=True)
        thread = itx.channel
        e = step_embed("Step 2 - Bosses",
                       "Pick CB level, Hydra levels (<=3), Chimera levels (<=2), then enter damage/clashpoints. Press Next.")
        await thread.send(embed=e, view=Step2View(self.sess))

class Step2View(discord.ui.View):
    def __init__(self, sess: Session):
        super().__init__(timeout=1800); self.sess = sess
        self.add_item(CBLevelSelect(sess)); self.add_item(HydraLevelsSelect(sess)); self.add_item(ChimeraLevelsSelect(sess))

    @discord.ui.button(label="Enter CB damage", style=discord.ButtonStyle.secondary)
    async def cb(self, itx: discord.Interaction, _: discord.ui.Button):
        if locked(itx, self.sess): return
        await itx.response.send_modal(CBDamageModal(self.sess))

    @discord.ui.button(label="Hydra damage & clash", style=discord.ButtonStyle.secondary)
    async def hydra(self, itx: discord.Interaction, _: discord.ui.Button):
        if locked(itx, self.sess): return
        await itx.response.send_modal(HydraNumsModal(self.sess))

    @discord.ui.button(label="Chimera damage & clash", style=discord.ButtonStyle.secondary)
    async def chimera(self, itx: discord.Interaction, _: discord.ui.Button):
        if locked(itx, self.sess): return
        await itx.response.send_modal(ChimeraNumsModal(self.sess))

    @discord.ui.button(label="Next ➜", style=discord.ButtonStyle.primary)
    async def next(self, itx: discord.Interaction, _: discord.ui.Button):
        if locked(itx, self.sess): return
        ok = all([
            "cb_level" in self.sess.answers, self.sess.have_cb,
            self.sess.have_hydra_levels, self.sess.have_hydra_nums,
            self.sess.have_chimera_levels, self.sess.have_chimera_nums
        ])
        if not ok:
            return await itx.response.send_message("Complete CB level+damage, Hydra levels+numbers, and Chimera levels+numbers.", ephemeral=True)
        await itx.response.send_message("On to Step 3...", ephemeral=True)
        thread = itx.channel
        e = step_embed("Step 3 - Siege, CvC & Referral",
                       "Answer Siege (interest + teams), CvC (interest + min points + style), and Who sent you? Then Submit.")
        await thread.send(embed=e, view=Step3View(self.sess))

class Step3View(discord.ui.View):
    def __init__(self, sess: Session):
        super().__init__(timeout=1800); self.sess = sess
        self.add_item(YesNoSelect("Siege - interested?", "siege_interest", sess))
        self.add_item(YesNoSelect("Siege - teams prepared?", "siege_teams", sess))
        self.add_item(YesNoSelect("CvC - interested?", "cvc_interest", sess))
        self.add_item(CvCStyleSelect(sess))

    @discord.ui.button(label="Enter CvC points", style=discord.ButtonStyle.secondary)
    async def cvc_pts(self, itx: discord.Interaction, _: discord.ui.Button):
        if locked(itx, self.sess): return
        await itx.response.send_modal(CvCPointsModal(self.sess))

    @discord.ui.button(label="Who sent you? (modal)", style=discord.ButtonStyle.secondary)
    async def referral(self, itx: discord.Interaction, _: discord.ui.Button):
        if locked(itx, self.sess): return
        await itx.response.send_modal(ReferralModal(self.sess))

    @discord.ui.button(label="Submit ✅", style=discord.ButtonStyle.success)
    async def submit(self, itx: discord.Interaction, _: discord.ui.Button):
        if locked(itx, self.sess): return
        ok = all([
            "siege_interest" in self.sess.answers, "siege_teams" in self.sess.answers,
            "cvc_interest" in self.sess.answers, "cvc_min" in self.sess.answers,
            "cvc_style" in self.sess.answers, "ref" in self.sess.answers
        ])
        if not ok: return await itx.response.send_message("Please finish Siege, CvC (points + style), and Referral.", ephemeral=True)

        await itx.response.send_message("Posting your application...", ephemeral=True)
        thread = itx.channel
        emb = summary_embed(itx.user, self.sess.answers)
        ping = f"{itx.user.mention} " + (f"<@&{RECRUITER_ROLE_ID}>" if RECRUITER_ROLE_ID else "")
        await thread.send(content=ping.strip(), embed=emb)

        start_id = self.sess.start_msg_id or _thread_prompt_msg_id.get(thread.id)
        if start_id:
            try:
                msg = await thread.fetch_message(start_id)
                v = StartApplyView(applicant_id=self.sess.user_id, thread_id=self.sess.thread_id, disabled=True)
                await msg.edit(embed=submitted_embed(itx.user.mention), view=v)
            except Exception:
                pass
        _sessions.pop(self.sess.user_id, None)

# ---------- Applicant guessing + thread watcher ----------
async def guess_applicant_from_thread(thread: discord.Thread) -> Optional[discord.Member]:
    try: await thread.join()
    except: pass

    async for msg in thread.history(limit=5, oldest_first=True):
        if TICKET_TOOL_BOT_ID and msg.author.id != TICKET_TOOL_BOT_ID: continue
        for u in msg.mentions:
            if not u.bot and isinstance(u, discord.Member): return u

    if thread.name and re.match(THREAD_NAME_REGEX, thread.name):
        after = thread.name.split("-", 1)[1].strip()
        for mem in thread.guild.members:
            if mem.name == after or mem.display_name == after: return mem
        after_lower = after.lower()
        for mem in thread.guild.members:
            if mem.name.lower() == after_lower or mem.display_name.lower() == after_lower: return mem
    return None

@bot.event
async def on_thread_create(thread: discord.Thread):
    if not thread.parent or thread.parent.id != WELCOME_CHANNEL_ID: return
    if not re.match(THREAD_NAME_REGEX, thread.name or ""): return

    applicant = await guess_applicant_from_thread(thread)
    mention = applicant.mention if applicant else None
    view = StartApplyView(applicant_id=(applicant.id if applicant else None), thread_id=thread.id)

    try: await thread.join()
    except: pass

    try:
        m = await thread.send(embed=start_embed(mention), view=view)
        _thread_prompt_msg_id[thread.id] = m.id
        print(f"[on_thread_create] posted start panel in {thread.name}", flush=True)
    except discord.Forbidden:
        await thread.parent.send(f"Could not post in {thread.name}. Check bot thread permissions.")

# ---------- Google Sheets helpers ----------
def _get_ws():
    if not GSHEET_ID: return None
    raw = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
    if not raw: return None
    sa_info = json.loads(raw)
    gc = gspread.service_account_from_dict(sa_info)
    sh = gc.open_by_key(GSHEET_ID)
    if GSHEET_WORKSHEET:
        try: return sh.worksheet(GSHEET_WORKSHEET)
        except gspread.WorksheetNotFound:
            ws = sh.add_worksheet(GSHEET_WORKSHEET, rows=1000, cols=10)
            ws.append_row(["ticket number","username","clantag","date closed"])
            return ws
    else:
        ws = sh.sheet1
        try:
            head = ws.row_values(1)
            want = ["ticket number","username","clantag","date closed"]
            if not head or [h.lower() for h in head][:4] != want:
                ws.insert_row(want, 1)
        except Exception:
            pass
        return ws

def _now_str():
    if ZoneInfo:
        tz = ZoneInfo(TIMEZONE) if TIMEZONE else ZoneInfo("UTC")
        return datetime.now(tz).strftime("%Y-%m-%d %H:%M")
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M")

def _parse_closed_name(name: str):
    m = re.match(r"(?i)closed-(\d{3,6})-([^-]+)", name or "")
    if not m: return None, None
    return m.group(1), m.group(2).strip()

def _service_account_email() -> str:
    raw = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
    if not raw: return "(GOOGLE_SERVICE_ACCOUNT_JSON not set)"
    try: return json.loads(raw).get("client_email", "(no client_email in JSON)")
    except Exception: return "(malformed JSON)"

# ------ Upsert + dedupe helpers ------
def _norm_ticket(x: str) -> str:
    return re.sub(r"[^\d]", "", str(x or ""))

def _find_ticket_row(ws, ticket: str) -> Optional[int]:
    try:
        colA = ws.col_values(1)
    except Exception as e:
        print("Sheets read col A failed:", e, flush=True)
        return None
    wanted = _norm_ticket(ticket)
    for i, v in enumerate(colA[1:], start=2):  # row 1 header
        if _norm_ticket(v) == wanted:
            return i
    return None

def _upsert_ticket_row(ticket: str, username: str, tag: str, date_closed: str) -> str:
    ws = _get_ws()
    if not ws:
        print("Upsert: no worksheet (check GSHEET env/share).", flush=True)
        return "no_ws"
    try:
        row = _find_ticket_row(ws, ticket)
        if row:
            ws.batch_update([{"range": f"A{row}:D{row}", "values": [[ticket, username, tag, date_closed]]}])
            print(f"Upsert: updated ticket {ticket} at row {row}", flush=True)
            return "updated"
        else:
            ws.append_row([ticket, username, tag, date_closed], value_input_option="USER_ENTERED")
            print(f"Upsert: inserted ticket {ticket}", flush=True)
            return "inserted"
    except Exception as e:
        print("Upsert error:", e, flush=True)
        return "error"

def _parse_date_str(s: str) -> Optional[datetime]:
    if not s: return None
    s = s.strip()
    for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d/%m/%Y %H:%M", "%m/%d/%Y %H:%M"):
        try: return datetime.strptime(s, fmt)
        except Exception: pass
    return None

# ---------- Diagnostics ----------
@bot.tree.command(name="sheet_status", description="Check Google Sheets connectivity")
async def sheet_status(itx: discord.Interaction):
    await itx.response.defer(ephemeral=True, thinking=True)
    email = _service_account_email()
    try:
        ws = _get_ws()
    except Exception as e:
        return await itx.followup.send(f"Could not open sheet: `{e}`\nService account: `{email}`", ephemeral=True)

    if not ws:
        return await itx.followup.send("Sheet not configured or not accessible.\n• Check GSHEET_ID\n• Share the sheet with the service account as Editor\n"
                                       f"Service account: `{email}`", ephemeral=True)
    try:
        title = ws.spreadsheet.title
        tab = ws.title
        rows = len(ws.col_values(1))
        await itx.followup.send(f"Connected to **{title} / {tab}**. Rows in column A: **{rows}**.\nService account: `{email}`", ephemeral=True)
    except Exception as e:
        await itx.followup.send(f"Opened worksheet but failed to read values: `{e}`\nService account: `{email}`", ephemeral=True)

@bot.tree.command(name="backfill_status", description="Show current backfill progress")
async def backfill_status(itx: discord.Interaction):
    st = _backfill_state
    await itx.response.send_message(
        f"Running: **{st['running']}**\n"
        f"Phase: **{st.get('phase','')}**\n"
        f"Scanned: **{st['scanned']}** | Added: **{st['added']}** | Skipped: **{st['skipped']}**",
        ephemeral=True
    )

@bot.tree.command(name="dedupe_sheet", description="Remove duplicate tickets (keeps newest by 'date closed')")
async def dedupe_sheet(itx: discord.Interaction):
    await itx.response.defer(ephemeral=True, thinking=True)
    ws = _get_ws()
    if not ws: return await itx.followup.send("No worksheet. Check GSHEET env/share.", ephemeral=True)
    try:
        values = ws.get_all_values()
        if len(values) <= 1:
            return await itx.followup.send("Nothing to dedupe (only header present).", ephemeral=True)

        winners = {}    # ticket -> (best_row_idx, best_dt)
        all_rows = set()

        for idx, row in enumerate(values[1:], start=2):
            ticket = _norm_ticket(row[0] if len(row) > 0 else "")
            if not ticket: continue
            date_str = row[3] if len(row) > 3 else ""
            dt = _parse_date_str(date_str)
            all_rows.add(idx)

            if ticket not in winners:
                winners[ticket] = (idx, dt)
            else:
                best_idx, best_dt = winners[ticket]
                def is_newer(cur_idx, cur_dt, old_idx, old_dt):
                    if cur_dt and old_dt: return cur_dt > old_dt
                    if cur_dt and not old_dt: return True
                    if not cur_dt and old_dt: return False
                    return cur_idx > old_idx  # neither parseable, keep bottom-most (newest append)
                if is_newer(idx, dt, best_idx, best_dt):
                    winners[ticket] = (idx, dt)

        keep_rows = {row for (row, _) in winners.values()}
        to_delete = sorted(list(all_rows - keep_rows), reverse=True)
        for r in to_delete:
            ws.delete_rows(r)

        await itx.followup.send(f"Deduped. Kept newest rows for {len(winners)} tickets. Removed {len(to_delete)} duplicates.", ephemeral=True)
    except Exception as e:
        await itx.followup.send(f"Dedupe failed: `{e}`", ephemeral=True)

# ---------- Placement prompt on Ticket Close ----------
_pending_close: dict[int, int] = {}      # thread_id -> closer_user_id
_close_prompt_msg: dict[int, int] = {}   # thread_id -> message id

class ClanTagModal(discord.ui.Modal, title="Set placement clan tag"):
    tag = discord.ui.TextInput(label="Clan tag", placeholder="e.g., C1CM", max_length=16, required=True)
    def __init__(self, thread_id: int, closer_id: int): super().__init__(); self.thread_id = thread_id; self.closer_id = closer_id
    async def on_submit(self, itx: discord.Interaction):
        if itx.user.id != self.closer_id: return await itx.response.send_message("Locked to the closer of this ticket.", ephemeral=True)
        await _finalize_tag(itx, self.thread_id, self.closer_id, self.tag.value)

class ClanTagSelect(discord.ui.Select):
    def __init__(self, thread_id: int, closer_id: int):
        opts = [discord.SelectOption(label=t) for t in CLAN_TAGS]
        opts.append(discord.SelectOption(label="Custom...", description="Type a custom tag"))
        super().__init__(placeholder="Select clan tag", min_values=1, max_values=1, options=opts)
        self.thread_id = thread_id; self.closer_id = closer_id
    async def callback(self, itx: discord.Interaction):
        if itx.user.id != self.closer_id: return await itx.response.send_message("Locked to the closer of this ticket.", ephemeral=True)
        if self.values[0].lower().startswith("custom"):
            return await itx.response.send_modal(ClanTagModal(self.thread_id, self.closer_id))
        await _finalize_tag(itx, self.thread_id, self.closer_id, self.values[0])

class ClanTagView(discord.ui.View):
    def __init__(self, thread_id: int, closer_id: int):
        super().__init__(timeout=1800); self.add_item(ClanTagSelect(thread_id, closer_id))

async def _finalize_tag(itx: discord.Interaction, thread_id: int, closer_id: int, raw_tag: str):
    tag = re.sub(r"[^A-Za-z0-9_-]+", "", raw_tag).upper()
    thread = itx.client.get_channel(thread_id)
    if not isinstance(thread, discord.Thread):
        return await itx.response.send_message("Could not find the ticket thread.", ephemeral=True)

    new_name = thread.name
    if not re.search(rf"(?i)-{re.escape(tag)}$", new_name or ""):
        new_name = f"{new_name}-{tag}"
        try: await thread.edit(name=new_name, reason=f"Placement set by {itx.user}")
        except discord.Forbidden:
            return await itx.response.send_message("Missing permission to rename this thread (Manage Channels).", ephemeral=True)

    ticket, uname = _parse_closed_name(thread.name)
    _upsert_ticket_row(ticket or "", uname or "", tag, _now_str())

    msg_id = _close_prompt_msg.get(thread_id)
    if msg_id:
        try:
            msg = await thread.fetch_message(msg_id)
            done = discord.Embed(title="Placement recorded",
                                 description=f"Clan tag {tag} set.\nThread renamed to {new_name} and logged to the sheet.",
                                 color=COLOR_SUCCESS)
            await msg.edit(embed=done, view=None)
        except Exception:
            pass

    _pending_close.pop(thread_id, None)
    await itx.response.send_message(f"Saved: {tag} ✔️", ephemeral=True)

@bot.event
async def on_message(message: discord.Message):
    if not isinstance(message.channel, discord.Thread): return
    if TICKET_TOOL_BOT_ID and message.author.id != TICKET_TOOL_BOT_ID: return
    if not message.embeds: return

    def _txt(e: discord.Embed) -> str:
        return " | ".join([x for x in [e.title or "", e.description or "", (e.author.name if e.author else "")] if x])
    merged = " ".join(_txt(e) for e in message.embeds).lower()
    if "ticket closed" not in merged: return
    closer_id = message.mentions[0].id if message.mentions else None
    if not closer_id:
        m = re.search(r"<@!?(\d+)>", merged)
        if m: closer_id = int(m.group(1))
    if not re.match(r"(?i)closed-", message.channel.name or ""): return

    _pending_close[message.channel.id] = closer_id or 0
    prompt = discord.Embed(
        title="Set placement clan",
        description=(f"{f'<@{closer_id}>' if closer_id else 'Closer'}, select the clan tag "
                     "the applicant joined. I will append it to the thread name and log it to the sheet.\n\n"
                     "Example: Closed-0298-User -> Closed-0298-User-C1CM"),
        color=COLOR_WARN
    )
    try:
        m = await message.channel.send(embed=prompt, view=ClanTagView(message.channel.id, closer_id or 0))
        _close_prompt_msg[message.channel.id] = m.id
    except discord.Forbidden:
        try:
            if closer_id:
                user = message.guild.get_member(closer_id) or await bot.fetch_user(closer_id)
                dm = await user.create_dm()
                link = f"https://discord.com/channels/{message.guild.id}/{message.channel.id}"
                prompt.description += f"\n\nIf you cannot interact in-thread, click here: {link}"
                m = await dm.send(embed=prompt, view=ClanTagView(message.channel.id, closer_id))
                _close_prompt_msg[message.channel.id] = m.id
        except Exception:
            pass

# ---------- Backfill (with cached sheet index + live progress) ----------
async def _find_closed_timestamp(thread: discord.Thread) -> str:
    try:
        async for msg in thread.history(limit=50, oldest_first=False):
            if TICKET_TOOL_BOT_ID and msg.author.id != TICKET_TOOL_BOT_ID: continue
            if msg.embeds:
                for e in msg.embeds:
                    text = " ".join(filter(None, [e.title, e.description, e.author.name if e.author else None])).lower()
                    if "ticket closed" in text:
                        return msg.created_at.strftime("%Y-%m-%d %H:%M")
    except Exception:
        pass
    if getattr(thread, "archived_at", None):
        return thread.archived_at.strftime("%Y-%m-%d %H:%M")
    try:
        last = [m async for m in thread.history(limit=1, oldest_first=False)]
        if last: return last[0].created_at.strftime("%Y-%m-%d %H:%M")
    except Exception:
        pass
    return thread.created_at.strftime("%Y-%m-%d %H:%M")

def _extract_tag_from_thread_name(name: str) -> str:
    m = re.match(r"(?i)closed-\d{3,6}-([^-]+)(?:-([A-Za-z0-9_]+))?$", name or "")
    if m and m.group(2): return m.group(2).upper()
    return ""

# ------ Cached sheet index to avoid read-rate limits ------
class SheetIndex:
    """Cache column A once, then upsert without further reads during backfill."""
    def __init__(self, ws):
        self.ws = ws
        self.map: dict[str, int] = {}   # ticket(norm) -> row (1-based)
        self.next_row = 2               # first free row index

    @classmethod
    def from_sheet(cls, ws: gspread.Worksheet) -> "SheetIndex":
        idx = cls(ws)
        values = ws.get_all_values()  # ONE read for the whole sheet
        idx.next_row = len(values) + 1 if values else 2
        for row_idx, row in enumerate(values[1:], start=2):  # skip header
            if not row:
                continue
            t = _norm_ticket(row[0] if len(row) > 0 else "")
            if t:
                idx.map[t] = row_idx
        print(f"[sheet-index] cached {len(idx.map)} tickets, next_row={idx.next_row}", flush=True)
        return idx

    def upsert(self, ticket: str, username: str, tag: str, date_closed: str) -> str:
        key = _norm_ticket(ticket)
        if not key:
            return "error"
        if key in self.map:
            row = self.map[key]
            self.ws.batch_update([{
                "range": f"A{row}:D{row}",
                "values": [[ticket, username, tag, date_closed]],
            }])
            print(f"Upsert(indexed): updated ticket {ticket} at row {row}", flush=True)
            return "updated"
        else:
            self.ws.append_row([ticket, username, tag, date_closed], value_input_option="USER_ENTERED")
            self.map[key] = self.next_row
            print(f"Upsert(indexed): inserted ticket {ticket} at row {self.next_row}", flush=True)
            self.next_row += 1
            return "inserted"

async def _append_if_new(ticket: str, username: str, tag: str, date_closed: str, idx: SheetIndex | None) -> bool:
    """Backfill path: use the cached SheetIndex to avoid read-rate limits."""
    if not idx:
        status = _upsert_ticket_row(ticket, username, tag, date_closed)
    else:
        status = idx.upsert(ticket, username, tag, date_closed)

    if status == "inserted":
        return True
    if status == "updated":
        print(f"Backfill: updated existing ticket {ticket}", flush=True)
        return False
    if status == "no_ws":
        print("Backfill: skip – worksheet is None (check GSHEET_ID / share).", flush=True)
    return False

async def _scan_threads_in_channel(
    channel: discord.TextChannel,
    max_threads: int = 0,
    progress_cb=None
) -> tuple[int, int, int]:
    added = skipped = scanned = 0

    # build ONE sheet index for the whole backfill
    ws = _get_ws()
    idx = SheetIndex.from_sheet(ws) if ws else None

    async def report(phase_local: str):
        _backfill_state.update(running=True, phase=phase_local, scanned=scanned, added=added, skipped=skipped)
        if progress_cb:
            try:
                await progress_cb(scanned, added, skipped, phase_local)
            except Exception:
                pass

    async def handle_thread(t: discord.Thread):
        nonlocal added, skipped, scanned
        if not re.match(r"(?i)closed-", t.name or ""):
            return
        scanned += 1
        try:
            await t.join()
        except Exception:
            pass
        ticket, username = _parse_closed_name(t.name)
        if not ticket:
            return
        tag = _extract_tag_from_thread_name(t.name)
        date_closed = await _find_closed_timestamp(t)

        ok = await _append_if_new(ticket, username or "", tag, date_closed, idx)
        if ok: added += 1
        else: skipped += 1

        if scanned % 20 == 0:
            await report(_backfill_state.get("phase", ""))

        # gentle on write quota
        await asyncio.sleep(0.15)

    # Active
    await report("active")
    for t in channel.threads:
        await handle_thread(t)
        if max_threads and scanned >= max_threads:
            await report("active"); _backfill_state["running"] = False
            return (added, skipped, scanned)

    # Archived public
    await report("archived public")
    try:
        async for t in channel.archived_threads(limit=None, private=False):
            await handle_thread(t)
            if max_threads and scanned >= max_threads:
                await report("archived public"); _backfill_state["running"] = False
                return (added, skipped, scanned)
    except discord.Forbidden:
        print("Backfill: forbidden reading archived public threads. Check permissions.", flush=True)
    except Exception as e:
        print("Backfill: error reading archived public threads:", e, flush=True)

    # Archived private (needs Manage Threads)
    await report("archived private")
    try:
        async for t in channel.archived_threads(limit=None, private=True):
            await handle_thread(t)
            if max_threads and scanned >= max_threads:
                await report("archived private"); _backfill_state["running"] = False
                return (added, skipped, scanned)
    except discord.Forbidden:
        print("Backfill: forbidden reading archived private threads. Grant Manage Threads.", flush=True)
    except Exception as e:
        print("Backfill: error reading archived private threads:", e, flush=True)

    _backfill_state["running"] = False
    await report("done")
    return (added, skipped, scanned)

@bot.tree.command(name="backfill_tickets", description="Scan closed ticket threads and log them to the Google Sheet")
@app_commands.describe(max_threads="Max threads to scan (0 = all)")
async def backfill_tickets(itx: discord.Interaction, max_threads: int = 0):
    await itx.response.defer(ephemeral=True, thinking=True)
    channel = bot.get_channel(WELCOME_CHANNEL_ID)
    if channel is None or not isinstance(channel, discord.TextChannel):
        return await itx.followup.send("WELCOME_CHANNEL_ID is not a text channel I can see.", ephemeral=True)

    _backfill_state.update(running=True, phase="starting", scanned=0, added=0, skipped=0)
    status = await itx.followup.send("Starting backfill…", ephemeral=True)

    async def progress_cb(scanned, added, skipped, phase):
        _backfill_state.update(running=True, phase=phase, scanned=scanned, added=added, skipped=skipped)
        try:
            await itx.followup.edit_message(
                status.id,
                content=f"Backfill running…\nPhase: **{phase}**\nScanned: **{scanned}** | Added: **{added}** | Skipped: **{skipped}**"
            )
        except Exception:
            pass

    added, skipped, scanned = await _scan_threads_in_channel(channel, max_threads=max_threads, progress_cb=progress_cb)
    _backfill_state.update(running=False, phase="done", scanned=scanned, added=added, skipped=skipped)

    await itx.followup.edit_message(
        status.id,
        content=f"Backfill complete. Scanned **{scanned}**, added **{added}**, skipped **{skipped}**."
    )

# ---------- Optional manual command ----------
@bot.tree.command(name="apply", description="Start the C1C application wizard here")
async def apply_cmd(itx: discord.Interaction):
    uid = itx.user.id
    if uid in _sessions:
        return await itx.response.send_message("You already have an active application.", ephemeral=True)
    thread_id = itx.channel.id if isinstance(itx.channel, discord.Thread) else 0
    _sessions[uid] = Session(uid, thread_id)
    await itx.response.send_message("Sent Step 1 above. If nothing appears, run this inside your ticket thread.", ephemeral=True)

@bot.event
async def on_ready():
    try:
        if GUILD_ID: await bot.tree.sync(guild=discord.Object(id=GUILD_ID))
        else:        await bot.tree.sync()
    except Exception as e:
        print("Slash sync error:", e, flush=True)
    print(f"Logged in as {bot.user}.", flush=True)

# ---------- Render web service runner ----------
from aiohttp import web

async def _health(request):
    return web.Response(text="ok")

async def run_web():
    app = web.Application()
    app.router.add_get("/", _health)
    app.router.add_get("/health", _health)
    port = int(os.getenv("PORT", "10000"))
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    print(f"Health server up on :{port}", flush=True)

async def run_bot():
    await bot.start(TOKEN)

async def main():
    await asyncio.gather(run_web(), run_bot())

if __name__ == "__main__":
    asyncio.run(main())
