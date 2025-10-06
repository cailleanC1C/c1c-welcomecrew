# Architecture & Flow Map

## Current Flow (Monolith snapshot)
1. **Gateway startup** — `bot_welcomecrew.py` bootstraps the Discord bot, syncs slash commands, and (attempts to) preload clan tags via `_load_clan_tags`.
2. **Member activity** — Thread messages are handled in `on_message`; close markers trigger `_finalize_{welcome,promo}` to write to Google Sheets, while missing tags enqueue `_pending_*` entries and prompt recruiters.
3. **Sheets access** — Helpers such as `get_ws`, `upsert_welcome`, and `_load_clan_tags` use gspread synchronously and are offloaded via `_run_blocking` in some call sites.
4. **Backfill** — `!backfill_tickets` walks both channels, reuses the same parsing stack, and pushes writes through throttled upserts.
5. **Ops surface** — Prefix commands expose maintenance tools (env check, dedupe, reboot) with no permission layer; `/help` points users to the command catalog.
6. **Keepalive** — An aiohttp web server provides `/healthz`, while a scheduled refresh task (`scheduled_refresh_loop`) reloads clan tags three times per day.

## Target Carve-out Interfaces to Stabilize
- **Sheets Adapter Layer** — gspread client factory, worksheet cache, and upsert/dedupe helpers (candidate for extraction into `welcomecrew/sheets.py`).
- **Clan Tag Cache** — Non-blocking loader with TTL + refresh (should expose async `ensure_clan_tags_loaded(force: bool=False)` and sync `match_tag(text)` APIs).
- **Ticket Router** — Parsing (`parse_welcome_thread_name_allow_missing`, `parse_promo_thread_name`), pending-state tracking, and prompt UX.
- **Command Surface** — Prefix + slash commands gated for admins, mirroring Reminder’s permission scaffolding; keep as thin wrappers over service layer functions.
- **Observers** — `on_message`/`on_thread_update` watchers and stale-ticket sweep (future work) should remain isolated from Sheets concerns via dependency injection.

## Immediate Refactors Post-Fix
1. Extract a `permissions` module that defines recruiter/admin checks and reuse across commands/tests.
2. Move `_pending_*` management + prompt orchestration behind a ticket service so it can be unit-tested without Discord objects.
3. Wrap Sheets retries/backoff in a reusable utility (shared with Reminder) before moving into a dedicated package.
