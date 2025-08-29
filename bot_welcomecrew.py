# bot.py
# ------------------------------------------------------------
# Requires Python 3.10+ and:
#   pip install -U discord.py gspread tzdata aiohttp
#
# ENV VARS to set on Render:
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
# ------------------------------------------------------------

import os
import re
import asyncio
import json
from datetime import datetime
from typing import Optional

import discord
from discord.ext import commands
from discord import app_commands

import gspread
try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None  # fallback to UTC if zoneinfo isn't available

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

THREAD_NAME_REGEX = r"^\d{3,6}-"  # e.g., 0298-swerve13
COLOR_PRIMARY = 0x5865F2
COLOR_SUCCESS = 0x2ECC71
COLOR_WARN = 0xF1C40F

# ---------- Bot ----------
intents = discord.Intents.default()
intents.message_content = True  # needed to read Ticket Tool close embeds
bot = commands.Bot(command_prefix="!", intents=intents)

# ---------- Session store ----------
class Session:
    def __init__(self, use
