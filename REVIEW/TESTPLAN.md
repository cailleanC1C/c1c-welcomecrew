# Test Plan (Post-Fix)

## Happy Paths
1. **Welcome close with tag present**
   - Create a welcome thread with canonical name + tag.
   - Post “Ticket closed by …” message.
   - Expect rename to `Closed-####-username-TAG`, Sheets row inserted, audit log entry.
2. **Promo close with picker prompt**
   - Close a promo thread without tag.
   - Ensure prompt view appears once archived; pick a tag from dropdown.
   - Confirm Sheets row added with detected promo type.
3. **Dual-role ping in notify fallback**
   - Close a private thread where the bot lacks send perms.
   - Verify notify channel receives role + closer mentions.

## Edge & Failure Cases
1. **DM disabled / private thread fallback** — Ensure `_notify_channel` fires and pending state clears once tag provided.
2. **Clan tag TTL expiry** — Advance clock or lower TTL, trigger close; ensure non-blocking refresh loads tags and watchers keep responding.
3. **Sheets outage** — Inject invalid credentials, trigger close & prompt; verify Discord handlers remain responsive and notify fallback fires.
4. **Duplicate ticket prevention** — Run backfill twice; ensure upsert updates existing rows instead of appending duplicates.
5. **Rate limit resilience** — Simulate 429 from Sheets (mock `_with_backoff`) and confirm retries obey throttle.
6. **Stale ticket sweep (future)** — Once implemented, simulate threads idle past SLA; expect reminder → auto-close flow.

## Admin Surface
1. **Permission checks** — Verify non-admins receive rejection for `!reboot`, `!backfill_tickets`, `!dedupe_sheet`, `!reload`, etc.
2. **Env/Health commands** — As admin, call `!env_check`, `!health`, `!sheetstatus` and confirm output accuracy.
3. **Slash `/help` parity** — Ensure slash and prefix help both reflect current command availability.
