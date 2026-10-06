# First SQL Server dev import, release 2026-09-29 (INCOMPLETE: transforms stopped)

A full `npd-loader run` against `https://directory.cms.gov/downloads/` on 2026-10-05, into `cssnpi.npd_dev` with
catalog `cssnpi.HIE_WAREHOUSE_META_DEV`, data folder `E:\npd_dev_data`. Pre-check: no `PROJECT = 'NPD'` catalog rows.
The run was **stopped by hand at 04:46 on 2026-10-06** during the first transform; it did not reach publish.

## Environment

- `cssnpi`: SQL Server 2019 CU32 Developer, 4 CPUs, Windows Server 2019, Windows auth.
- Per the user, SQL Server maintenance plans were running on `cssnpi` during this import. Timings are not
  representative of a quiet server.
- Between about 23:05 and 23:26 a controller diagnostic query (`COUNT_BIG` with NOLOCK over the raw table) competed
  for I/O; it was cancelled at about 23:26. Measured raw-load rates (`sys.dm_db_partition_stats.row_count`): about
  295-525 rows/s at 23:25-23:26 under contention, about 1,290 rows/s at 23:27-23:28 after cancellation (maintenance
  still running). The loader session's top waits were PAGEIOLATCH_EX/SH (server disk I/O), not Python CPU.
- Progress should be read from `sys.dm_db_partition_stats`, not `COUNT(*)` over the raw table (a full scan of the LOB
  heap slows the load).

## Stage timings

| Stage | Start | End | Duration |
|---|---|---|---|
| DOWNLOAD (2.35 GB) | 22:29:09 | 22:30:00 | 51 s |
| EXTRACT (34.6 GB) | 22:30:01 | 22:31:12 | 71 s |
| IMPORT, raw load (24,852,222 rows) | 22:31:12 | 00:42:49 | 2 h 12 min |
| IMPORT, between raw load and first transform (indexing / setup, not logged) | 00:42:49 | 00:47:02 | 4 min |
| IMPORT, transforms: `010_practitioner.sql` | 00:47:02 | stopped 04:46 | 4 h (unfinished) |
| Publish (partition SPLIT) | not reached | | not measured |

Raw load per file (log timestamps): Organization 22:31 -> 22:38 (7 min), Location 4 min, Endpoint 2 min,
HealthcareService and InsurancePlan under 1 min, Practitioner (16.7 GB) 22:44:50 -> 23:19:36 (35 min),
PractitionerRole (11.1 GB) 23:19:36 -> 00:41:09 (82 min), OrganizationAffiliation 2 min.
Postgres reference (2026-09-29-full-import.md): raw load 3 h 7 min, so SQL Server was faster here (2 h 12 min) even with
maintenance plans and the contention above.

The publish-step duration (where the partition SPLIT runs) could not be measured because the run never got there.

## Failure: transform `010_practitioner.sql` is far too slow

The first INSERT (`npd.practitioner`, 7,481,906 expected rows) ran for 4 h with no completion. At 04:46 only
about 1.16M rows (15%) had passed through the final Table Insert and the rate was about 40 rows/s (sampled over
80 s with `sys.dm_exec_query_statistics_xml(103)`), projecting 20+ more hours for this statement alone. Postgres ran the
whole file in 38 min. I stopped the process (killed `npd-loader.exe`); the open INSERT rolled back. No loader source
was changed.

Observations (read-only):

- Session state: status suspended, wait CXCONSUMER, dop 4, CPU time about 38,500 s at 237 min (about 2.7 cores busy),
  only about 212k logical reads and 16 writes, session tempdb internal objects 20.8 GB. CPU bound inside the plan,
  not I/O bound.
- The estimated plan subtree cost was 587,499. The plan has a Sort per `OUTER APPLY` (name, then the four
  `npd.ext()` calls and `identifier_value`), with Table Spool operators feeding Sort/Filter nodes. The `OPENJSON`
  calls take `r.resource` through `CONVERT_IMPLICIT` from `varchar(max)` (UTF-8 collation) to `nvarchar(max)`, so every
  call re-converts and re-parses the full resource document (about 2 KB average, 16.7 GB over 7.48M rows).
- The statement has 1 `identifier_value`, 3 `join_text`, 4 `ext` inline TVFs and 2 `OPENJSON` passes, so each row is
  parsed about 8 times. The inline TVFs `npd.ext`, `npd.identifier_value`, `npd.join_text` are all
  SQL_INLINE_TABLE_VALUED_FUNCTION (not multi-statement), so the cost is the repeated LOB convert and parse, plus
  the sorts.

Likely fixes for the controller (not applied here): parse the extension array once per row (single `OPENJSON` with
`WITH`, or CROSS APPLY the extension array with conditional aggregation instead of four `ext()` calls), avoid the
implicit varchar(max) to nvarchar(max) conversion, and benchmark the statement on a 100k-row sample before another
full run.

## State left behind

- `npd_dev`: raw table `npd_raw.resource__20260929__r7009` with 24,852,222 rows, plus empty release-scoped tables.
  Database size 42,768 MB (data 41.5 GB used).
- Catalog: run 7007 (download) and 7008 (extract) are `Success`; import run 7009 has `COMPLETION_STATUS` NULL (open),
  because the process was killed. Orphan cleanup on the next `run` is expected to handle the release tables
  (not exercised).
- Downloaded and extracted files remain in `E:\npd_dev_data` (`run_7007_*`, `run_7008_*`).

## Row counts (raw layer; no flattened `npd.*` rows exist)

| Table | Rows |
|---|---|
| npd_raw.resource__20260929__r7009 | 24,852,222 |
| - Practitioner | 7,481,906 |
| - PractitionerRole | 11,059,682 |
| - Location | 2,558,069 |
| - Organization | 2,058,139 |
| - Endpoint | 1,140,616 |
| - OrganizationAffiliation | 493,222 |
| - HealthcareService | 54,445 |
| - InsurancePlan | 6,143 |

These match the Postgres run exactly. `npd.*` tables: all 0 rows.
