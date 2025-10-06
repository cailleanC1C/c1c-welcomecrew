# Performance Notes

- Clan tag cache uses `get_all_values()`; acceptable with 8h TTL but should avoid blocking the gateway loop (see F-02 for fix).
- Backfill throttles Sheets writes via `SHEETS_THROTTLE_MS`; keep under review when moving to multi-guild deployment.
- Consider batching `ws.delete_rows` in `dedupe_sheet` if duplicates become frequent; current per-row deletes incur multiple API calls.
