# SQL Server transform benchmark: parse-once rewrite (Task 15), 2026-10-06

`tests/mssql_bench.py` copies the first N rows (by `resource_id`) of each resource type from the dev raw table
`npd_dev.npd_raw.resource__20260929__r7009` (read-only `SELECT TOP (N) ... INTO`) into scratch schemas in `npd_test`,
runs `init_db` + `run_transforms`, prints seconds and rows/s per script, extrapolates each script linearly to the full
2026-09-29 release (per-type counts below), and drops the scratch schemas.

    set NPD_TEST_MSSQL_DB={"type":"mssql","server":"cssnpi","database":"npd_test","trusted":"yes"}
    .venv\Scripts\python tests\mssql_bench.py --per-type 100000 [--types Practitioner --scripts 010 --statements --chunk-rows N]

"full min" = full-release rows of the script's type(s) / measured rows/s. Full-release counts: Practitioner 7,481,906;
PractitionerRole 11,059,682; Location 2,558,069; Organization 2,058,139; Endpoint 1,140,616; OrganizationAffiliation
493,222; HealthcareService 54,445; InsurancePlan 6,143 (24,852,222 in all).

## Environment and noise

`cssnpi`: SQL Server 2019 CU32 Developer, 4 CPUs, 14 GB max server memory, compat 150. The server was shared all day:
another user's parallel `INSERT`/`SELECT INTO` in `HIE_MA` (2+ cores for most of the morning), a backup, an index build on
`npd_dev` (not ours) and about 20% CPU used outside SQL Server. The same statement on the same data varied 2-3x in
elapsed and CPU time between consecutive runs, so single-statement micro-comparisons were unreliable; the whole-run
numbers below were repeated (runs a-d) and agree within about 5%.

## Old vs new, per script

Old = the scripts at commit `0d647b0`. They are far too slow for the 100k sample (010 alone would take ~37 min), so the
baseline was measured on 10,000 rows per type (09:33-09:41; that sample was `TOP (N)` without `ORDER BY`, the later ones
are ordered by `resource_id`). New was measured on the same 10k size (12:12) and on 100,000 per type (runs a-d,
10:48-11:55; d includes the chunking code, one chunk per type at that size). 10k runs are dominated by per-statement
overhead, so their extrapolation is pessimistic; use the 100k numbers.

| Script | Old 10k rows/s | Old full min | New 10k rows/s | New 100k rows/s (a / b / c / d) | New full min (100k, median) |
|---|---:|---:|---:|---:|---:|
| 010_practitioner | 45 | 2,759 | 2,110 | 2,990 / 2,992 / 3,070 / 2,807 | 41.7 |
| 020_organization | 1,029 | 33.3 | 2,358 | 4,116 / 4,003 / 3,832 / 3,989 | 8.6 |
| 030_location | 4,348 | 9.8 | 5,183 | 18,233 / 18,261 / 17,344 / 15,422 | 2.4 |
| 040_endpoint | 4,745 | 4.0 | 19,724 | 25,270 / 25,510 / 22,407 / 22,773 | 0.8 |
| 050_practitioner_role | 1,171 | 157.4 | 5,682 | 8,288 / 8,426 / 8,213 / 8,160 | 22.3 |
| 060_organization_affiliation | 4,542 | 1.8 | 15,843 | 21,883 / 25,365 / 29,073 / 25,288 | 0.3 |
| 070_healthcare_service | 4,379 | 0.2 | 17,887 | ~25,000 | 0.0 |
| 080_insurance_plan | 6,630 | 0.0 | 11,410 | ~9,000 | 0.0 |
| 090_identifier | 33,015 | 12.5 | (other types only) | (other types only) | 0.3 |
| **All nine scripts** | | **2,978** | | | **76.2 / 75.8 / 75.9 / 79.9** |

Old 090 parsed every raw document again for its identifiers; new 010-080 write their type's identifiers from their
stage, so 090 only covers resource types without a script (none in this release), and the identifier cost moved into
010-080.

Index build + counts after the scripts (`_clone_indexes`, unchanged by this task) took 24.6-25.4 s per 100k sample,
about 15.5 min extrapolated linearly; it is not part of the 58 min target.

### Practitioner at 1M rows, chunked (11:55-12:04)

`--types Practitioner --per-type 1000000 --scripts 010` with the default 200,000-row chunks (5 chunks):
309.4 s, **3,232 rows/s, 38.6 min extrapolated**. Per chunk: stage 11.6-18.7 s, practitioner 11.6-14.2 s, name 5.3-6.6 s,
address 8.7-10.5 s, telecom 4.9-6.0 s, qualification 10.3-12.4 s, identifier 4.2-5.1 s. Scaling is linear, so the
chunked full-release estimate for 010 is about 38.6 min.

Chunking overhead at 25,000-row chunks (4 per type on the 100k sample): 86.5 min extrapolated vs 76-80 min unchunked.

### Best full-release estimate

010 from the 1M chunked run (38.6) + the 100k medians for the rest (8.6 + 2.4 + 0.8 + 22.3 + 0.3 + 0.0 + 0.0 + 0.3)
= **about 73 min for the nine scripts** (target 58 min: missed by ~25%), **010 about 38.6 min** (target 38: on the
line), plus ~15 min of index builds. Old scripts: ~2,980 min (about 40x slower; 010 about 70x slower).

## What changed and what mattered

1. **Parse once per resource type.** Each of 010-080 converts the raw `varchar(max)` UTF-8 document to `nvarchar(max)`
   once and parses it with one `OPENJSON ... WITH` into a stage heap (`<<stage:<type>>>`, `stage_<type>__<release>__r<run>`
   in the data schema, dropped at the end, matched by orphan cleanup) of scalars plus one JSON fragment per array. All
   INSERTs read the stage. A stage pass costs about 1.3-9 s per 100k practitioners depending on server load.
2. **`OPTION (NO_PERFORMANCE_SPOOL)`** on statements with per-row APPLYs. Without it the optimizer sorted all rows on the
   `nvarchar(max)` fragment (Sort + Repartition + Lazy Spool) to cache the inner side: the practitioner_name INSERT took
   15 s per 100k with the hint off, 3 s with it on. This is the same plan shape the dev run diagnosed (Sort/Spool per
   apply).
3. **No `TOP 1 ... ORDER BY` applies.** Measured on the stage: NPI pick via `TOP 1 ORDER BY` 7-12 s vs 2 s with a keyed
   `MIN()`; four extension lookups via `TOP 1` 36 s vs 1.3-2 s with `MAX(CASE WHEN url = ...)` over
   `OPENJSON(extension) WITH (url, value)`; first-match extension lookups keyed by array index cost 4-7 s.
4. **Child arrays**: elements with many fields parsed once with `OPENJSON(element) WITH (...)` (qualification ~2x faster
   than seven `JSON_VALUE` calls); elements with 1-3 fields keep `JSON_VALUE` (faster than a second OPENJSON).
5. **Chunking** (`-- chunked: <Type>` directive, `<<chunk>>` token, `MssqlDialect.chunk_rows` = 200,000): the runner
   runs a script once per `resource_id` range of that type (one seek on `resource_key` each). Not for speed on the
   sample, but because a full-release Practitioner stage would be ~35 GB (about 500 MB per 100k rows) against 14 GB of
   server memory; every INSERT re-reads the stage, so unchunked it would be read from disk six times.
6. `join_text` sorts `nvarchar(4000)` elements instead of `nvarchar(max)` (1.35 s vs 2.3 s per 100k names, isolated).

## Correctness check

Old (HEAD) and new scripts on the same real sample (5,000 rows per type, 40,000 resources): all 26 tables identical
(row counts equal, `EXCEPT` empty both ways). `pytest tests -k mssql`: 32 passed, including a new test that runs every
script in one-resource chunks and compares with the one-pass result.

## Remaining levers (not done)

- During the runs SQL Server used about 50% of the 4 CPUs (`RING_BUFFER_SCHEDULER_MONITOR`); stage passes ran nearly
  serial (CPU/elapsed ~1.2-2), INSERTs at ~2.5 of 4 cores. Running independent scripts concurrently (e.g. 010 and 050 on
  separate connections) is the obvious next step for wall-clock time but is a runner design change.
- A quiet server: all numbers above were taken with other heavy workloads on `cssnpi`.
