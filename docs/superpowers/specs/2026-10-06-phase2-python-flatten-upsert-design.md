# Phase 2: Python flattening, per-type staging and delta upserts — design

Date: 2026-10-06. Builds on `2026-10-05-sqlserver-dataengine-port-design.md` (Phase 1, branch `feature/sqlport-v1`).
Evidence: `docs/profile/2026-10-06-mssql-dev-import.md`, `docs/profile/2026-10-06-mssql-transform-benchmark.md`, and the
flatten spike below.

## Goal

Keep one current copy of the National Provider Directory in SQL Server and bring it up to date from each weekly CMS
release by applying only what changed. Replace the Phase 1 import (raw JSON table, T-SQL transforms, per-release
tables swapped in by partition SWITCH) with: parse and flatten each resource once in Python, bulk-load per-type staging
tables, compute the delta, upsert it into the permanent tables in one transaction.

## Why (what Phase 1 showed)

| Phase 1 step | Measured | Problem |
|---|---|---|
| Raw load of all 8 files into one table | 2 h 12 min, 24.85M rows | one shared LOB heap; per-row inserts |
| T-SQL transforms (after the parse-once rewrite) | ~73–80 min extrapolated | JSON parsed in the database; tempdb-heavy; chunked |
| Publish | per-release tables SWITCHed into partitioned parents | keeps whole snapshots; needs ~2 copies of space; partitioning machinery |

Space on the server is limited and only one current version is wanted (user, 2026-10-05).

## Spike result (2026-10-06, throwaway code)

Python flattened real Practitioner resources with declarative table specs (one generic flattener, `orjson`) into the 6
tables Practitioner feeds, and `bcp` loaded them into a scratch schema in `npd_test`. Values matched the Phase 1
transform tests' expectations.

| Step | 1,000,004 Practitioners → 11,861,311 rows |
|---|---|
| Parse + flatten, 4 worker processes | 24.7 s (480,000 rows/s) |
| `bcp`, 24 concurrent loads, TABLOCK into heaps | 96.9 s (122,000 rows/s; server busy with another load) |

Extrapolated: the full release (~170M flattened rows) in roughly 25–35 min, `bcp`-bound — against ~3.3 h for Phase 1's
raw load plus transforms. Weekly deltas are expected to be a small fraction of that.

## Decisions

| Topic | Decision |
|---|---|
| Parsing | In Python, once per resource, with `orjson`, while reading the `.ndjson`. |
| Flattening | Declarative table specs (one per output table) applied by one generic flattener; no per-type code. |
| Raw JSON in SQL Server | Not stored. The `.ndjson.zst` files stay on disk as the raw record (catalog-tracked, as today). |
| Staging | Per-run, per-table heaps in a staging schema, bulk-loaded with `bcp` (minimal logging, TABLOCK). |
| Permanent tables | One current dataset; not partitioned; keyed by `resource_id` (+ `seq` for child tables). |
| Delta | Per-resource content hash compared with the stored hash of the current version. |
| Apply | One transaction per import: replace every changed/deleted resource's rows, insert new ones. Readers see the old or the new state, never a mix (`READ_COMMITTED_SNAPSHOT ON`). |
| Removed | `npd_raw`, the T-SQL transform scripts, partition functions/schemes, standalone tables and SWITCH publish, retention by release. |
| Kept | DOWNLOAD and EXTRACT stages, the catalog (`HIE_WAREHOUSE_META*`), `database.env` connections, Windows auth, the CLI commands. |

### Proposed decisions — confirm in review

| # | Question | Proposed | Alternative |
|---|---|---|---|
| P1 | Resources missing from a new release | Hard delete their rows; count them in the run output | Soft delete (`deleted_in_release` column) and keep rows |
| P2 | Hash basis | SHA-1 of the raw line bytes (cheap; a CMS key-order change shows as "changed" once) | Hash of `orjson.dumps(..., OPT_SORT_KEYS)` (robust, ~2× parse cost) |
| P3 | Postgres flavor | SQL Server only for Phase 2 (`main` remains the Postgres loader) | Keep the dialect split and add a Postgres COPY/MERGE path later |
| P4 | Where the work lives | New branch `feature/phase2-flatten` off `feature/sqlport-v1` | Continue on `feature/sqlport-v1` |

## Design

### 1. Table specs (`src/npd_loader/flatten/specs.py`)

One `Table` per output table: its name, an optional repeating element (`each="telecom"` → one row per element,
`seq = index + 1`) and its columns as small path/converter expressions. Converters mirror the Phase 1 SQL helpers:
`ts` (FHIR dateTime, partial dates, UTC), `bool`, `join(sep, skip)`, `ref` (reference → id), `ext(url, key)` (first
extension by url), `identifier(systems)` (first identifier by system), `official_name` (official, else with a use,
else first). Every row gets `resource_id` and the file lineage columns automatically. `identifier` is one spec applied
to every resource type. The 26 specs replace the nine T-SQL scripts one for one (same tables, columns and semantics);
`mapped_paths.txt` for `profile --unmapped` is generated from the specs.

### 2. Flattener and workers (`src/npd_loader/flatten/engine.py`)

- One worker process per `.ndjson` file, at most 4 at a time (Practitioner and PractitionerRole dominate).
- Each worker streams its file line by line: `orjson.loads`, validate (JSON object, `resourceType`, non-empty `id`;
  as `raw_load.validate_line` today), hash the line (P2), flatten with its type's specs.
- Rows go to one buffered writer per table; the stage file format is `bcp -c` with field terminator `0x1F` and row
  terminator `0x1E` (characters that do not occur in FHIR text, so no escaping and nothing lost; a value containing
  either is rejected with the file and line). Every 500,000 rows or at end of file a buffer is handed to `bcp`.
- A hash row per resource (`resource_type, resource_id, hash, last_updated`) goes to the staging hash table.
- Duplicate `id`s within a file are detected in staging (unique index build), as today.

### 3. Staging (`stage` schema in the npd database)

- Per run: `stage.<table>__r<run>` heaps created from the permanent tables' shape, plus `stage.resource_hash__r<run>`.
- `bcp ... -h TABLOCK -b 500000` into empty heaps (minimally logged under SIMPLE recovery); several files of one table
  load concurrently (BU locks are compatible).
- After loading: per-type row counts checked against the rows the flattener emitted; unique index on
  `(resource_type, resource_id)` for the hash table.
- Names end in `__r<run>` so the existing orphan cleanup drops a killed run's staging.

### 4. Delta and apply (`MssqlDialect.apply_delta`)

`npd.resource_state (resource_type, resource_id, hash, last_updated, release_date, run_id)` holds the current version of
every resource.

1. Classify by joining `stage.resource_hash__r<run>` with `npd.resource_state`: **new** (not in state), **changed**
   (hash differs), **unchanged**, **deleted** (in state, not in stage — only for resource types present in this
   release, so a missing file never deletes a whole type).
2. Safety check before applying: if `deleted` exceeds a configurable share of a type's rows (default 20%), fail the
   run instead of applying (protects against a truncated CMS file).
3. One transaction (`SET XACT_ABORT ON`, `LOCK_TIMEOUT` with the Phase 1 retry policy):
   - `DELETE` the rows of changed and deleted resources from every permanent table of their type (and `identifier`);
   - `INSERT … SELECT` the staging rows of new and changed resources into the permanent tables;
   - update `npd.resource_state` (insert new, update changed, delete deleted); write `npd.release` (release, run,
     counts).
4. Drop the run's staging tables; update statistics on the touched tables (best effort).

First load: `resource_state` is empty, every resource is new, and step 3 is a plain bulk `INSERT … SELECT WITH
(TABLOCK)` into empty tables.

Child tables are replaced per resource (delete all its rows, insert the new set) rather than merged row by row:
repeating elements have no stable identity across releases.

### 5. Readers

`ALTER DATABASE npd SET READ_COMMITTED_SNAPSHOT ON` (one-time, needs a moment without other connections): readers keep
seeing the last committed state while a delta applies. The `v_*` views are no longer needed (the tables hold only the
current version); they can stay as plain pass-through views for compatibility.

### 6. Schema changes

- Permanent tables: drop partitioning; primary keys `(resource_id)` / `(resource_id, seq)` /
  `(resource_type, resource_id, seq)` for `identifier`; `release_date` stays as "release this row came from".
- New: `npd.resource_state`, `npd.release` extended with counts (new/changed/unchanged/deleted).
- Migration from Phase 1 data: not needed — the first Phase 2 import is a first load into new tables (Phase 1 dev data
  can be dropped).

### 7. CLI and catalog

Commands unchanged (`run`, `download`, `extract`, `import`, `status`, `init-db`, `profile`). `import --force` re-applies
a release (every resource is compared again; unchanged ones are no-ops). The IMPORT run's `XML_OUTPUT` records the
delta counts per type and rows per table. Catalog file tracking is unchanged.

## Error handling

- Validation or flatten error in a file: the run fails naming the file and line; staging is dropped; nothing applied.
- `bcp` failure: detected from its output and copied-row counts (never from the exit code alone — the spike showed
  `bcp` exits 0 when rows fail); the run fails.
- Apply: one transaction; any error rolls back the whole delta.
- Killed process: next run's orphan cleanup drops `__r<run>` staging; `fail_open_runs` closes the catalog run.

## Testing

- Unit tests for every converter and for the flattener on the existing fixture release, asserting the same expected
  values as the Phase 1 transform tests (they become the golden tests for the specs).
- Delta tests on SQL Server (`npd_test` scratch schemas): first load; second release with new, changed, unchanged and
  deleted resources; the deletion safety check; a failure mid-apply leaves the previous state intact.
- Acceptance: flatten the full 2026-09-29 release and compare table by table with the Phase 1 tables loaded in
  `npd_dev` (row counts and an `EXCEPT` both ways on keys and values), documenting any deliberate differences.
- Performance: full first load and a second-release delta in `npd_dev`, recorded in `docs/profile/`.

## Targets

- First load of a full release: ≤ 45 min end to end (download/extract excluded).
- Weekly delta: ≤ 15 min end to end for a typical release.
- Peak space: one permanent copy plus staging (no second snapshot).

## Out of scope

Filegroups (later, per user), MongoDB (considered; SQL Server stays the target), parallel T-SQL transforms (replaced),
a Postgres Phase 2 path (P3).
