# Threat Model & Risks

## Assets & Trust Boundaries
- **Discord guilds** — Threads/channels where onboarding happens; bot must respect channel permissions.
- **Google Sheets data** — Ticket logs, recruiter info, clan templates accessed via service account.
- **Bot token & service account JSON** — Secrets provided through environment variables.
- **Render deployment** — Hosts aiohttp health endpoints and long-lived Discord session.

## Key Threats
1. **Privilege escalation via prefix commands (F-01).** Attackers can reboot the bot, spam Sheets writes, or dump env hints by issuing admin commands from any channel.
2. **Gateway starvation (F-02).** Blocking Sheets reads on the event loop causes missed Discord events, making the bot unreliable during outages.
3. **Sheets abuse.** Without tighter scoping, repeated backfills or dedupe operations can thrash rate limits (mitigated by throttling but still manual-triggered).
4. **Token leakage.** `!env_check` currently redacts poorly (first 6 chars only); ensure logs and command output avoid dumping secrets beyond operational need.

## Mitigations
- Add `Manage Server` (or recruiter role) checks before executing maintenance commands.
- Offload Sheets access via `_run_blocking` and guard clan tag cache fetches with TTL checks.
- Extend logging with correlation IDs so audit log (#audit-log) can trace actions without storing PII.
- Store secrets in environment variables only; avoid writing service account JSON to disk during runtime.

## Future Work
- Implement a stale-ticket sweeper with idempotent retries and alerting instead of silent backlog.
- Consider caching clan templates in memory with TTL + background refresh to reduce hot-path Sheets calls.
- Add structured logging (JSON) for Render to make watchdog restarts observable.
