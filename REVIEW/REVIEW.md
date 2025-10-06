# WelcomeCrew Review

## Executive Summary — **Red**
The current WelcomeCrew extraction ships most of the reminder-derived surface area, but two high-severity gaps block a safe carve-out. Admin/maintenance commands (`!reboot`, `!backfill`, etc.) are exposed to any member with the prefix, and several hot paths still perform blocking Google Sheets fetches directly on the Discord gateway loop when clan tags are stale or a request fails. Either issue can take the bot offline (voluntarily or via timeouts) during onboarding rushes.

## Findings by Severity & Category

### High Severity

#### Security — F-01: Prefix commands lack permission guards
* **Location:** `bot_welcomecrew.py:1008-1287`
* **Snippet:**
  ```py
  @bot.command(name="reboot")
  @cmd_enabled(ENABLE_CMD_REBOOT)
  async def cmd_reboot(ctx):
      await ctx.reply("Rebooting…", mention_author=False)
      await asyncio.sleep(1.0); os._exit(0)
  ```
* **Issue:** Every prefix command (including destructive ones such as `!reboot`, `!dedupe_sheet`, `!backfill_tickets`) is callable by any user who can speak where the bot is present. Reminder’s admin role gate is missing, so a newcomer can kill the process or spam Sheets writes.
* **Fix (diff-ready):**
  ```diff
  diff --git a/bot_welcomecrew.py b/bot_welcomecrew.py
  --- a/bot_welcomecrew.py
  +++ b/bot_welcomecrew.py
  @@
  -from discord.ext import commands
  +from discord.ext import commands
  +from functools import wraps
  @@
  -def cmd_enabled(flag: bool):
  -    def deco(func):
  -        async def wrapper(ctx: commands.Context, *a, **k):
  -            if not flag:
  -                return await ctx.reply("This command is disabled by env flag.", mention_author=False)
  -            return await func(ctx, *a, **k)
  -        return wrapper
  -    return deco
  +ADMIN_PERMS = commands.has_guild_permissions(manage_guild=True)
  +
  +def cmd_enabled(flag: bool):
  +    def deco(func):
  +        @wraps(func)
  +        async def wrapper(ctx: commands.Context, *a, **k):
  +            if not flag:
  +                return await ctx.reply("This command is disabled by env flag.", mention_author=False)
  +            return await func(ctx, *a, **k)
  +        return wrapper
  +    return deco
  @@
  -@bot.command(name="env_check")
  +@bot.command(name="env_check")
  +@ADMIN_PERMS
   async def cmd_env_check(ctx):
       ...
  @@
  -@bot.command(name="backfill_tickets")
  -@cmd_enabled(ENABLE_CMD_BACKFILL)
  +@bot.command(name="backfill_tickets")
  +@ADMIN_PERMS
  +@cmd_enabled(ENABLE_CMD_BACKFILL)
   async def cmd_backfill(ctx):
       ...
  @@
  -@bot.command(name="reboot")
  -@cmd_enabled(ENABLE_CMD_REBOOT)
  +@bot.command(name="reboot")
  +@ADMIN_PERMS
  +@cmd_enabled(ENABLE_CMD_REBOOT)
   async def cmd_reboot(ctx):
       ...
  ```
* **Verify:**
  - Confirm `!reboot`/`!backfill_tickets` fail for users without `Manage Server`.
  - Confirm commands still work for admins and respect env toggles.
  - Regression-test `!help` & watcher flows (no permission regressions).

#### Robustness — F-02: Clan tag cache reload blocks the gateway loop
* **Location:** `bot_welcomecrew.py:246-655`, `1667-1716`
* **Snippet:**
  ```py
  def _match_tag_in_text(text: str) -> Optional[str]:
      if not text: return None
      _load_clan_tags(False)
      if not _tag_regex_cache: return None
      ...
  ```
* **Issue:** `_load_clan_tags` performs a blocking `worksheet.get_all_values()` call. When the cache is empty (first run, TTL expiry, or repeated API errors), every thread message, parse helper, and backfill path calls `_load_clan_tags` directly on the event loop. During Sheets outages this hard-blocks message handling, so close events are missed and the bot appears dead.
* **Fix (diff-ready):**
  ```diff
  diff --git a/bot_welcomecrew.py b/bot_welcomecrew.py
  --- a/bot_welcomecrew.py
  +++ b/bot_welcomecrew.py
  @@
  -def _load_clan_tags(force: bool=False) -> List[str]:
  +def _load_clan_tags(force: bool=False) -> List[str]:
       global _clan_tags_cache, _clan_tags_norm_set, _last_clan_fetch, _tag_regex_cache
       now = time.time()
       if not force and _clan_tags_cache and (now - _last_clan_fetch < CLAN_TAGS_CACHE_TTL_SEC):
           return _clan_tags_cache
  @@
       except Exception as e:
           print("Failed to load clanlist:", e, flush=True)
  -        _clan_tags_cache = []; _clan_tags_norm_set = set(); _tag_regex_cache = None
  +        _last_clan_fetch = now
  +        _clan_tags_cache = []; _clan_tags_norm_set = set(); _tag_regex_cache = None
       return _clan_tags_cache
  +
  +def _clan_tags_need_refresh() -> bool:
  +    if not _clan_tags_cache:
  +        return True
  +    return (time.time() - _last_clan_fetch) >= CLAN_TAGS_CACHE_TTL_SEC
  +
  +async def ensure_clan_tags_loaded(force: bool = False) -> List[str]:
  +    if force or _clan_tags_need_refresh():
  +        await _run_blocking(_load_clan_tags, force)
  +    return list(_clan_tags_cache)
  @@
  -def _match_tag_in_text(text: str) -> Optional[str]:
  -    if not text: return None
  -    _load_clan_tags(False)
  +def _match_tag_in_text(text: str) -> Optional[str]:
  +    if not text: return None
       if not _tag_regex_cache: return None
       ...
  @@
  -    picked = _pick_tag_by_suffix(remainder, _load_clan_tags())
  +    picked = _pick_tag_by_suffix(remainder, list(_clan_tags_cache))
  @@
  -    picked = _pick_tag_by_suffix(remainder, _load_clan_tags())
  +    picked = _pick_tag_by_suffix(remainder, list(_clan_tags_cache))
  @@
  async def infer_clantag_from_thread(thread: discord.Thread) -> Optional[str]:
  -    if not ENABLE_INFER_TAG_FROM_THREAD:
  +    if not ENABLE_INFER_TAG_FROM_THREAD:
           return None
  +    await ensure_clan_tags_loaded(False)
  @@
  async def scan_welcome_channel(channel: discord.TextChannel, progress_cb=None):
  -    st = backfill_state["welcome"] = _new_report_bucket()
  +    st = backfill_state["welcome"] = _new_report_bucket()
       if not ENABLE_WELCOME_SCAN:
           backfill_state["last_msg"] = "welcome scan disabled"; return
  +    await ensure_clan_tags_loaded(False)
  @@
  async def scan_promo_channel(channel: discord.TextChannel, progress_cb=None):
  +    await ensure_clan_tags_loaded(False)
  @@
  async def on_message(message: discord.Message):
       if isinstance(message.channel, discord.Thread):
           th = message.channel
  +        await ensure_clan_tags_loaded(False)
  ```
* **Verify:**
  - Simulate Sheets outage (invalid credentials) and ensure `on_message` continues to process commands.
  - Observe clan-tag prompts still populate options after TTL expiry.
  - Run `!backfill_tickets` and confirm no blocking warnings in logs.

### Medium & Low Severity
_None beyond the above high-priority issues were found within the review scope._

## Additional Notes
- Keep the existing scheduled refresh loop; it benefits from the non-blocking loader and now short-circuits when tags are fresh.
- Consider re-enabling Reminder’s structured logging once the carve-out progresses; audit trails ease live triage.
