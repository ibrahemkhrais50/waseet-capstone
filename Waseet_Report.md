Waseet Logistics - Capstone Report
1. Schema and why

The grain of parcel_scans is one scan event - one scan of one parcel at one point in its journey, at one moment in time. Each line in the file is already one scan, so this matches the feed and keeps a parcel's whole journey queryable.

Five decisions. Grain: one row per scan event, not per parcel - a parcel is scanned about six times and each scan is worth keeping, and you can always roll events up to parcels later but never back. Nullability: I let the data decide. A scan with no courier is real (unassigned), so courier_id is nullable; a scan with no timestamp is not real, so scanned_at is NOT NULL; the keys and scan_type are NOT NULL, and weight_kg is nullable because a blank weight is a repair, not a reject. Foreign keys: I declared them to hubs, couriers, customers and service_levels as a second line of defence - my transform already rejects unknown references, but the FKs guarantee a bad one can never reach the table. scan_id is NOT a primary key, on purpose: the feed resends scans (the whole file on the 13th), and a key would only turn that into a crash. The Najm note in lecture 13 makes the point that the real danger is the quiet double-load, and the fix for that is an idempotent load, not a key. Index: one on scan_date, because the daily load and report both filter by date and more indexes would only add write cost.

2. What I found in the data

I read everything with dtype=str first so pandas would not reinterpret values like 12,5 and hide the problem. Checking on the 4th (1285 rows):

scan_type is mixed case (about 27 lower-case rows) - a repair, I uppercase them.
scanned_at comes in two formats, ISO and DD/MM/YYYY, about 3% in the second (30 rows) - a repair, I parse both into one datetime.
weight_kg: 36 comma decimals and 9 blanks (repairs), and 12 impossible values over 100 kg or zero (a reject).
hub_id 99 is not a real hub - 8 rows, a reject (unknown hub).
courier_id blank (11 rows) is a real unassigned scan, not an error - a repair to null, not a reject. Telling this apart from the real rejects was most of the work.
duplicate scan_id is the same event twice (3 on the 4th; the 13th is the whole file resent, 1282 duplicates) - I dedup keeping the first.

Four structural days: the 10th never arrives, the 15th is Eid (empty), the 19th has weight_kg renamed to weight, the 13th is the double-send. On the fourth undescribed bad day I will be honest - my reconciliation closes on every day (each gap is fully explained by dupes plus rejects), so I did not isolate a fifth anomaly. The 17th is the biggest day and is where I would look next, but I did not want to invent a finding I could not prove.

3. The quality suite

The suite is rules held as data, not if-statements inside the transform, so it can be read without knowing Python. Nine rules across four dimensions: not-null on scan_id and scanned_at (completeness), unique on scan_id (uniqueness), numeric on customer_id and hub_id, in-range 0 to 100 on weight_kg, and in-set on service_code (validity), and in-set plus uppercase on scan_type (consistency). Every check returns a boolean series of bad rows, and an unknown check name raises rather than passing silently.

It fails on real data, which is the point - on the 4th it catches 6 unique failures, 12 in-range failures and 27 uppercase failures. On top of the suite, schema drift compares columns to a stored baseline and catches the 19th (weight_kg missing, weight new), and reconciliation is section 4.

Where each check runs: the contract check at the boundary fails the run before any row is touched; validity and consistency run in the transform and quarantine the bad row with a reason; reconciliation runs after the load. In the DAG the suite runs after the load but only warns (|| true), because the row-level issues are expected and already quarantined - the script still exits non-zero so it could be a hard gate if wanted.

4. Reconciliation

File rows vs warehouse rows for all 21 days. The gap on every day is dupes + rejects, and it closes exactly.

Day	File	Dupes	Rejects	Warehouse	Explanation
05-01	1230	5	21	1204	dupes + rejects
05-02	1164	8	16	1140	dupes + rejects
05-03	1156	4	13	1139	dupes + rejects
05-04	1285	3	20	1262	dupes + rejects
05-05	1212	8	17	1187	dupes + rejects
05-06	1291	7	21	1263	dupes + rejects
05-07	1187	8	13	1166	dupes + rejects
05-08	1205	5	19	1181	dupes + rejects
05-09	1307	9	14	1284	dupes + rejects
05-10	-	-	-	0	file never arrived, sensor times out
05-11	1242	5	20	1217	dupes + rejects
05-12	1103	3	16	1084	dupes + rejects
05-13	2546	1282	15	1249	file sent twice, dedup removes the copy
05-14	1063	3	16	1044	dupes + rejects
05-15	0	-	-	0	empty (Eid), branch skips the load
05-16	1123	4	24	1095	dupes + rejects
05-17	1340	5	15	1320	dupes + rejects (biggest day)
05-18	1167	3	15	1149	dupes + rejects
05-19	1066	-	-	0	column renamed, contract check fails
05-20	1132	8	22	1102	dupes + rejects
05-21	1222	3	14	1205	dupes + rejects

Total after a full backfill is 21291 rows, and a second backfill gives the same 21291 - the idempotency proof. The 10th and 19th load nothing (missing file / failed contract), the 15th loads nothing but as a skip not a failure, and the 13th has 2546 rows but only 1249 land, all of the difference being the 1282 duplicates. Every gap is explained, which is the whole point - an explained gap is fine, an unexplained one would be the real problem.

5. What I would do next

I would fix the fourth bad day first. My reconciliation proves the pipeline is internally consistent, but the brief says there is an anomaly it should catch that I did not isolate. I would compare each day against the history file's normal daily volume and the trailing average, not just file-vs-warehouse, since an anomaly that is consistent but abnormal in size only shows up that way. The 17th is where I would start.

I would also finish the Spark partitioned write. The read, both aggregations and the broadcast joins run, but the Parquet write fails on my Windows machine with an UnsatisfiedLinkError in NativeIO - the hadoop.dll is not loading next to winutils. That is an environment problem, not the code.

For monitoring, freshness does not belong inside waseet_daily, because it cannot fire on the morning the daily DAG never ran - the exact case it is for. It belongs in a separate DAG that just checks how old the newest row is.

Finally, what is still unsafe: the quality suite warns but does not stop a bad load. That is right for the row-level dirt that is already quarantined, but a genuinely broken day would still load its good-looking rows and only warn. Before production I would add an after-load gate that can fail or roll back a day when the reject rate jumps far above normal, so a quietly broken feed cannot slip through.