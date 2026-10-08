# Phase 2 dev import — timings, sizes and comparison with Phase 1

Branch `feature/phase2-flatten` @ 0075330 (surrogate int keys). Target `npd_dev` on cssnpi (SQL Server 2019, SIMPLE
recovery, READ_COMMITTED_SNAPSHOT ON), catalog `HIE_WAREHOUSE_META_DEV`. Loader host TonyServer (16 GB RAM, ~1 GB
free during the runs). `flatten_workers = 4`, `bcp_workers = 8`. Data file pre-sized to 60 GB, log to 40 GB
(growth 4 GB, raised to 16 GB during run 7010).

## Release 2026-09-29 — first load (import run 7010, `import --force`)

| phase | start–end | duration |
|---|---|---|
| flatten (Python, 8 files) | 18:48–19:06 | ~18 min |
| bcp into staging | 19:06–19:13 | ~7 min |
| apply (one transaction) | 19:13–20:32 | ~79 min |
| total | 18:47:53–20:32:25 | 1 h 45 min |

The apply overlapped a `DBCC SHRINKFILE` on `npd_proof` (same server, same E: volume) from 19:27, so the apply time is
an upper bound.

- 24,852,222 resources, all new: PractitionerRole 11,059,682; Practitioner 7,481,906; Location 2,558,069;
  Organization 2,058,139; Endpoint 1,140,616; OrganizationAffiliation 493,222; HealthcareService 54,445;
  InsurancePlan 6,143.
- 128 referenced-only keys (`hash IS NULL`), all Organizations referenced by `endpoint.managing_organization`
  (842 endpoints). None of them exist in `npd_proof` either: real gaps in the CMS data.
- Peak log used 65.9 GB (the inserts were fully logged despite `TABLOCK`: nonclustered indexes and one
  multi-statement transaction). tempdb never below 49 GB free.
- Data used after the load: 46.4 GB, of which staging ~33 GB (truncated by the next import) and `npd` ~13.4 GB.

### Comparison with Phase 1 (`npd_proof`, release 2026-09-29)

All 26 tables compared with `EXCEPT` both ways after translating keys back to `Type-` prefixed text ids: **every
table identical** (0 rows only in Phase 1, 0 only in Phase 2; 188,341,047 rows in total). Script:
`E:\npd_dev_data\task12\compare.sql` (generated from the specs as in plan Task 12 Step 3; ids compared with
`COLLATE DATABASE_DEFAULT`).

### Size: text ids vs surrogate keys

| | `npd_proof` (text ids) | `npd_dev` (int keys) |
|---|---|---|
| 26 data tables | ~17.1 GB | ~9.9 GB (−42%) |
| key registry `resource_state` | — | 3.5 GB |
| total | ~17.1 GB | ~13.4 GB (−22%) |
| e.g. `practitioner_role_location` | 2,329 MB | 664 MB |
| e.g. `practitioner_role_telecom` | 1,833 MB | 1,044 MB |

## Release 2026-10-07 — first weekly update (`run`: download 7011, extract 7012, import 7013)

| phase | start–end | duration |
|---|---|---|
| download (8 .zst) | 00:45:45–00:46:39 | 54 s |
| extract | 00:46:39–00:47:47 | 68 s |
| flatten | 00:47:47–01:04 | ~17 min |
| bcp | 01:04–01:13 | ~9 min |
| apply (one transaction) | 01:13–03:19 | ~2 h 06 min |
| total | 00:45:39–03:20:42 | 2 h 35 min |

Delta: new 116,588; **changed 24,707,366**; unchanged 5; not seen 144,851 (kept as aging data).
Peak log used 205.6 GB (log file grew to 216 GB); tempdb grew to 59 GB (row-version store ~28 GB under RCSI while
the delete/insert ran); data used 56.1 GB.

### Why almost everything was "changed"

The change hash covers the raw ndjson line. Sampled 20,000 resources per type across both releases:
`meta.lastUpdated` differs in **every** resource — it is the CMS export timestamp, not a content change.
Resources whose only difference is `meta.lastUpdated`: PractitionerRole 100%, OrganizationAffiliation 100%,
HealthcareService 100%, Organization 95%, Practitioner 72% (real changes there: qualification, telecom). With
`meta.lastUpdated` excluded from the hash, a weekly apply would touch roughly a tenth of the resources instead of
all of them. Decision pending (see below).

## Findings for production sizing

1. Weekly cost today is a full rewrite: ~2 h apply, ~205 GB log, ~30 GB version store — driven by the hash, not by
   real change volume.
2. First load: ~66 GB log, ~1 h 45 min end to end.
3. `resource_state_last_seen` index (ruling R4): kept for now; with the hash fixed, the weekly `last_seen` update still
   touches every resource, so measure again after the fix before deciding.
4. Production `npd` needs READ_COMMITTED_SNAPSHOT ON, and tempdb sized for the version store of one apply.
