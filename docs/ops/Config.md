# WelcomeCrew Configuration Guide

## Watchdog and Health Environment Variables

Use these settings to tune watchdog and health behaviour (shared with Matchmaker/Clanmatch). Defaults shown in parentheses.

- `WATCHDOG_CHECK_SEC` (60): Interval between watchdog checks.
- `WATCHDOG_ZOMBIE_SEC` (600): If connected but no events have been seen for this many seconds **and** latency is bad, restart.
- `WATCHDOG_DISCONNECT_AGE_SEC` (600): If disconnected from Discord for longer than this, restart.
- `WATCHDOG_LATENCY_SEC` (10.0): Latency above this threshold counts as bad when combined with long idle time.
- `WATCHDOG_MAX_DISCONNECT_SEC`: **Legacy alias** for `WATCHDOG_DISCONNECT_AGE_SEC` (WelcomeCrew only). Prefer the new name; the legacy value is still honoured when the new variable is absent.
- `STRICT_PROBE` (0): Health probe mode. When `0`, `/` and `/ready` always return 200 while `/healthz` returns deep health (200/206/503). When `1`, `/`, `/ready`, `/health`, and `/healthz` all return deep health responses.

## Notes

- WelcomeCrew now uses the same heartbeat-driven watchdog thresholds as Matchmaker/Clanmatch. Adjust the thresholds above rather than relying on hard-coded 10-minute limits.
- When connected but idle/slow, `/healthz` returns HTTP 206 with `connected: true` so observability remains intact without triggering premature restarts.

---

Doc last updated: 2025-12-10 (v0.9.8.x)
