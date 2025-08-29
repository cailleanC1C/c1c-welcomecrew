# bot.py
# ------------------------------------------------------------
# Requires Python 3.10+ and:
#   pip install -U discord.py gspread tzdata aiohttp
#
# ENV VARS you must set on Render:
#   DISCORD_TOKEN
#   WELCOME_CHANNEL_ID        (channel where Ticket Tool opens threads)
#   TICKET_TOOL_BOT_ID        (public Ticket Tool: 557628352828014614)
#   RECRUITER_ROLE_ID         (optional; ping on summary)
#   GUILD_ID                  (optional; speeds slash sync)
#
# For Google Sheets logging (placement + backfill):
#   GSHEET_ID
#   GOOGLE_SERVICE_ACCOUNT_JSON   (full JSON; paste as env var)
#   GSHEET_WORKSHEET              (optional tab name)
#   TIMEZONE                      (e.g., Europe/Vienna)
#   CLAN_TAGS                     (comma list, e.g. "C1CM,C1CE,C1CB,VGR,MRTRS")
# ------------------------------------------------------------

import os, re, asyncio, textwrap, json
from datetime import datetime
from typing import Optional

import discord
from discord.ext import commands
from discord import app_commands

import gspread
try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None  # older Pythons: will fall back to UTC

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

THREAD_NAME_REGEX = r"^\d{3,6}-"   # e.g., 0297-swerve13
COLOR_PRIMARY = 0x5865F2
COLOR_SUCCESS = 0x2ECC71
COLOR_WARN    = 0xF1C40F

# ---------- Bot ----------
intents = discord.Intents.default()
# Needed to see Ticket Tool's close messages/embeds
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)

# ---------- Session store ----------
class Session:
    def __init__(self, user_id: int, thread_id: int, start_msg_id: Optional[int] = None):
        self.user_id = user_id
        self.thread_id = thread_id
        self.start_msg_id = start_msg_id
        self.answers: dict[str, str | list[str]] = {}

        # gate flags
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

_sessions: dict[int, Session] = {}             # user_id -> Session
_thread_locks: dict[int, int] = {}             # thread_id -> applicant_user_id
_thread_prompt_msg_id: dict[int, int] = {}     # thread_id -> start message id

# ---------- Embeds ----------
def start_embed(mention: Optional[str]) -> discord.Embed:
    e = discord.Embed(
        title="C1C Application",
        description=(f"Hey {mention or 'there'}! ✨\n"
                     "Tap **Start** to begin. This panel is **locked to you**."),
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
    def F(name, key, default="—"):
        val = a.get(key, default)
        if isinstance(val, list):
            val = ", ".join(val) if val else default
        sval = str(val).strip()
        if len(sval) > 900: sval = sval[:897] + "…"
        e.add_field(name=f"🔹 {name}", value=sval or default, inline=False)

    F("In-game name", "ign")
    F("Account level", "acc_level")
    F("Clan style (what you want)", "clan_style")
    F("Playstyle", "playstyle")
    F("Clan Boss — Level", "cb_level")
    F("Clan Boss — Damage", "cb_damage")
    F("Hydra — Levels",
