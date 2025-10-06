# TODOs (≤60 min each)

## P0
- [F-01] Gate all admin/maintenance prefix commands behind a `Manage Server` (or equivalent recruiter/admin role) permission check, matching Reminder.
- [F-02] Introduce a non-blocking clan-tag cache loader and ensure all hot paths preload via the thread pool helper before parsing.

## P1
- After F-02, add telemetry for cache refresh latency and failures to surface Sheets outages without blocking the gateway loop.

## P2
- Align docstrings/comments with the new permission model once F-01 lands (README command table currently implies admin-only).
