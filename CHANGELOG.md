# Changelog
## [1.0.2] — 2025-10-10
- Preserve manual edits: Updates no longer overwrite non-empty cells in Sheets (default ON).
- New env toggles: PRESERVE_EXISTING_NONEMPTY (ON/OFF) and INSERT_ONLY (insert without updating existing rows).
- Backfill safety: Existing rows are merged; empty incoming fields won’t wipe data.
- GSpread warning fixed: Use named args in ws.update(...).
- !env_check now shows the two new toggles.
________________________________________________
## [1.0.1] — 2025-10-07
### Tooling & Process
- Unified label taxonomy across bots via `.github/labels/labels.json` (synced by **Sync Labels**): one `P0–P4`, plus `bot:*` and `comp:*` scopes.
- Migrated off legacy `area:*` labels and pruned leftovers; boards and filters are now consistent org-wide.
- Added per-repo workflow to auto-add issues to **C1C Cross-Bot Hotlist** and set project **Priority** from `P*` labels; saved views added for **Data Sheets — Perf**, **Ops Parity**, **Security Hotlist**, and **Needs Triage**.
________________________________________________
