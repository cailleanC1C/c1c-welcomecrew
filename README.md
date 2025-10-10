# C1C – WelcomeCrew v1.0.2

A Discord helper that watches **Welcome** and **Promo/Move** threads, logs them to Google Sheets, and keeps threads tidy. It never DMs; if it can’t speak in a thread it posts to a **notify** channel instead.

## What it does

* **Live watchers** (opt-in): detect “Ticket closed by …” and log the result.
* **Auto-join threads**: joins new threads in the configured channels; joins on mention.
* **Tag detection**: forgiving parsers for thread names; supports multi-part tags like `F-IT`. Also detects tags anywhere in messages/embeds (title, desc, fields, **footers**).
* **Prompt if missing**: when a close is detected but no tag is found, shows an in-thread **dropdown tag picker** (with paging). If the picker times out, it offers a **Reload picker** button and allows plain-text tag replies.
* **Canonical renaming**: Welcome and Promo threads are normalized to `Closed-####-username-TAG`.
* **Backfill**: scans archived + live threads; writes or updates rows; leaves the date blank if no close marker (configurable).
* **Sheets writes**: throttled (delay between writes) + exponential backoff for 429/5xx. Upserts with diff reporting.
* **Watch log & status**: `!watch_status` shows ON/OFF and the last five actions.
* **Health & watchdog**: `/healthz` endpoint + a watchdog that restarts the process if the gateway looks “zombied”.
* **Scheduled refresh**: reload clan tags (and warm worksheet handles) **3×/day**. Optional “refreshed” ping to a log channel.

---

## Quick start

1. **Requirements**

   * Python 3.10+
   * `discord.py`, `gspread`, `google-auth`, `aiohttp`

   ```bash
   pip install discord.py gspread google-auth aiohttp
   ```

2. **Discord setup**

   * Create a bot; enable **Message Content Intent**.
   * Invite with permissions: View Channels, Send Messages, Embed Links, Read History, Add Reactions, Manage Threads, Manage Messages (optional, for cleanup/renames).

3. **Google Sheets**

   * Create a spreadsheet and share it with your **Service Account** email (see `GOOGLE_SERVICE_ACCOUNT_JSON`).
   * The bot will create tabs and headers if missing:

     * **Sheet1** (welcome): `ticket number, username, clantag, date closed`
     * **Sheet4** (promo): `ticket number, username, clantag, date closed, type, thread created`
   * **Clan tags**: create a `clanlist` tab. Either include a header column named one of `clantag / tag / abbr / code` **or** put tags in column **B** by default (configurable).

4. **Config (.env) – minimum**

   ```env
   DISCORD_TOKEN=xxxx
   GOOGLE_SERVICE_ACCOUNT_JSON={"type":"service_account",...}
   GSHEET_ID=your_spreadsheet_id

   WELCOME_CHANNEL_ID=123456789012345678
   PROMO_CHANNEL_ID=234567890123456789
   TIMEZONE=Europe/Vienna
   ```

5. **Run**

   ```bash
   python welcome.py
   ```

---

## Commands

All commands are prefix (`!…`). A minimal slash command `/help` is also provided.

* `!help` — shows the mobile help card.
  `!help <topic>` for details (`env_check`, `sheetstatus`, `backfill_tickets`, `backfill_details`, `dedupe_sheet`, `watch_status`, `reload`, `checksheet`, `health`, `reboot`, `ping`).
* `!env_check` — checks required env vars and toggles.
* `!sheetstatus` — confirms tabs and which SA email to share with.
* `!backfill_tickets` — scans both channels; live progress; writes/updates rows.
* `!backfill_details` — uploads a text file with diffs/skips from the last backfill.
* `!dedupe_sheet` — keeps the newest row per ticket (Welcome) and per (ticket+type+created) (Promo).
* `!reload` — clears Sheet + tag caches; next access reopens sheets.
* `!checksheet` — shows row counts for Sheet1/Sheet4.
* `!watch_status` — current watcher toggles + last five actions.
* `!health` — latency, Sheets availability, uptime.
* `!reboot` — soft restart (process exit).
* `!ping` — “Pong”.

> Command availability is controlled by env toggles (see below).

---

## Behavior details

### Thread name parsing

* **Welcome**: accepts `Closed-####-username-TAG` or any `#### …` where the ticket can be found; tag may be missing. Multi-segment tags (`F-IT`) supported.
* **Promo**: `####-username[-TAG]` pattern (tag optional; detected from content if present).
* If a tag is missing at close, the bot:

  1. **defers** prompting until the thread is archived/locked (prevents duplicates),
  2. shows a **dropdown picker** with paging (25 per page),
  3. accepts **typed** tags as plain text too,
  4. if it times out, replaces with a **Reload picker** button (no re-ping).

### “Closed by” detection

* Matches `ticket closed by` / `closed by` in message **content or embed title/description/fields/footer**.
* The **close time** is taken from the close message’s timestamp; if not found, the code leaves the date empty (by default) or can require the marker via flags below.

### Finalization

* **Welcome** row: `[ticket, username, tag, date_closed]`
* **Promo** row: `[ticket, username, tag, date_closed, type, thread_created]`
  `type` is detected by phrases like *“returning player”* / *“move request”*; see `PROMO_TYPE_PATTERNS`.
* Both Welcome and Promo threads are normalized to **`Closed-####-username-TAG`** if the bot has permission.

### Upserts

* Uses in-memory indices to find rows fast; computes diffs when updating.
* Writes are **throttled** (`SHEETS_THROTTLE_MS`) and retried with backoff on 429/5xx.

### Backfill

* Scans live and archived threads (public and private, when permitted).
* Produces a compact running status and, optionally, a final **details file** with diffs/skips.

### Watchdog & health

* Watchdog checks every `WATCHDOG_CHECK_SEC` (default 60s):

  * If connected but no socket activity for >10m and latency is bad → restart.
  * If disconnected > `WATCHDOG_MAX_DISCONNECT_SEC` (default 10m) → restart.
* Web server:

  * `/` and `/ready` return **200** by default (or deep status when `STRICT_PROBE=1`).
  * `/healthz` always returns deep status (200/206/503).

---

## Environment variables

### Required

* `DISCORD_TOKEN` — bot token.
* `GOOGLE_SERVICE_ACCOUNT_JSON` — JSON **string** for the service account.
* `GSHEET_ID` — spreadsheet ID.
* `WELCOME_CHANNEL_ID` — numeric ID for the Welcome parent channel.
* `PROMO_CHANNEL_ID` — numeric ID for the Promo/Move parent channel.

### Core options

* `TIMEZONE` — IANA time zone (default `UTC`).
* `SHEET1_NAME` / `SHEET4_NAME` — tab names (default `Sheet1` / `Sheet4`).
* `CLANLIST_TAB_NAME` — tab with clan tags (default `clanlist`).
* `CLANLIST_TAG_COLUMN` — **1-based** column index for tags when no header is found (default `2`, i.e., column **B**).
* `SHEETS_THROTTLE_MS` — delay between writes (default `200`).

### Watchers & features (ON/OFF via `ON`/empty; see `env_bool`)

* `ENABLE_LIVE_WATCH` (default ON)
  `ENABLE_LIVE_WATCH_WELCOME`, `ENABLE_LIVE_WATCH_PROMO` (default ON)
* `ENABLE_WELCOME_SCAN`, `ENABLE_PROMO_SCAN` (default ON)
* `ENABLE_INFER_TAG_FROM_THREAD` (default ON) — scans thread history for tags.
* **Notify fallback (no DMs)**:

  * `ENABLE_NOTIFY_FALLBACK` (default ON)
  * `NOTIFY_CHANNEL_ID` — where to post if the bot can’t speak in thread.
  * `NOTIFY_PING_ROLE_ID` — optional role to ping alongside the closer.
  * `ALLOW_SELF_JOIN_PRIVATE` (default ON) — try adding the bot to private threads.
* **Close marker requirement**:

  * `REQUIRE_CLOSE_MARKER_WELCOME` (default OFF)
  * `REQUIRE_CLOSE_MARKER_PROMO` (default OFF)
* **Backfill output**:

  * `AUTO_POST_BACKFILL_DETAILS` (default ON) — uploads diffs/skips file.
  * `POST_BACKFILL_SUMMARY` (default OFF) — quick summary post.

### Refresh & logging

* `REFRESH_TIMES` — CSV of local times `HH:MM` for cache refresh (default `02:00,10:00,18:00`).
* `CLAN_TAGS_CACHE_TTL_SEC` — extra guard (default 28800 = 8h).
* `LOG_CHANNEL_ID` — optional channel/thread ID to ping after refresh.

### Health server

* `PORT` — HTTP port (default `10000`).
* `STRICT_PROBE` — `1` = deep probes on `/` and `/ready` (default `0`).

---

## Sheet schema (what the bot writes)

### `Sheet1` (Welcome)

| ticket number | username | clantag | date closed |
| ------------- | -------- | ------- | ----------- |

* `ticket number` is **4-digits** zero-padded (e.g., `0042`).
  Set the column to **Plain text** to keep leading zeros.

### `Sheet4` (Promo/Move)

| ticket number | username | clantag | date closed | type | thread created |
| ------------- | -------- | ------- | ----------- | ---- | -------------- |

* `type` is inferred from thread content when possible.
* `thread created` is the thread’s creation time.

### `clanlist` (tags source)

* If a header exists, one of the columns must be named `clantag`/`tag`/`abbr`/`code`.
* If no header match is found, the bot uses **column B** (configurable).

---

## Troubleshooting

* **Bot replies nothing**: ensure Message Content Intent is on; check `WELCOME_CHANNEL_ID` and `PROMO_CHANNEL_ID` IDs; run `!env_check`.
* **Not logging / permission errors**: verify it can **join** threads and **send messages** in them. For private threads, keep `ALLOW_SELF_JOIN_PRIVATE=ON`.
* **No tag detected**: the picker appears once the thread is archived/locked; users can also type the tag. For multi-part tags, hyphens are normalized (`–—` → `-`).
* **429 from Sheets**: writes are throttled and retried; avoid running multiple backfills at once.
* **Reopened threads**: pending prompts are cleared when a thread is unarchived/unlocked.

---

## Design notes

* All parsing is **forgiving**: it tries thread name first, then content/embeds (including footers), then prompts.
* The watchers keep a **small action log** (`deque(maxlen=50)`); `!watch_status` shows the last five.
* Renaming is idempotent and case-normalized; it won’t double-prefix `Closed-`.
* The service auto-warms caches on the three scheduled refresh times and can post a small “refreshed” note if a log channel is set.
* The slash `/help` is synced once at boot (ignore failures silently).
