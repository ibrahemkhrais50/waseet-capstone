# Phase 0 - What I found in the feed (checked on scans_2026-05-04.csv, ~1285 rows)

1. Duplicate scan_id: 3 rows - same event sent twice. REJECT (keep first, drop resend).
2. scanned_at in two formats: 1255 ISO, 30 slash (DD/MM/YYYY) = ~2.3%. REPAIR (parse both into one datetime).
3. weight_kg problems: 36 comma-decimal, 9 blank, 12 impossible (>100kg or <=0).
   - comma + blank = REPAIR. impossible = REJECT.
4. hub_id not in hubs.csv: 8 rows, all value 99 (not a real hub). REJECT (unknown hub).
5. courier_id blank: 11 rows - valid unassigned scans. REPAIR to NULL (not a reject).
6. scan_type mixed case: 6 real types written in both upper and lower case (~27 lowercase rows). REPAIR (uppercase).

## Special days across the three weeks
- Day 10: file never arrives (missing). -> sensor times out (Phase 5)
- Day 13: 2546 rows, ~double - file sent twice. -> dedup (Phase 4)
- Day 15: 0 rows (empty, Eid). -> DAG skips it (Phase 5)
- Day 19: column renamed weight_kg -> weight. -> contract check fails it (Phase 4)
- A hidden 4th bad day exists - reconciliation will catch it (Phase 6)