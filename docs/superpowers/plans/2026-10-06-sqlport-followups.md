# SQL Server port — follow-ups after feature/sqlport-v1

Carried over from the implementation ledger when the plan's workspace was deleted. The plan and spec are
`2026-10-05-sqlserver-dataengine-port.md` and `../specs/2026-10-05-sqlserver-dataengine-port-design.md`.

## Before production scheduling

- Re-import in `npd_dev` (run `npd-loader init-db` first: npd_dev still has the pre-Task-15 helper functions). This
  confirms the ~73–80 min transform extrapolation on the full 25M-row raw heap. Orphan cleanup will drop run 7009's
  42.8 GB raw table (the benchmark source), and fail_open_runs will mark run 7009 Failed in HIE_WAREHOUSE_META_DEV.
- Publish a second release in `npd_dev` to measure the partition SPLIT of the non-empty rightmost partition (it may
  scan ~40 GB under a Sch-M lock). Fix if needed by keeping an empty partition at the right end. Phase 2 removes
  partitioning entirely.
- Verify the Postgres flavor against a real server (`NPD_TEST_PG_DSN`); it was ported to DataEngine but never run.

## Phase 2 direction (user, 2026-10-05/06)

- Follow-up (user, 2026-10-05): distribute IO with filegroups later (npd is a big DB); not in this plan. Constraint: standalone tables must be created ON the target partition's filegroup (SELECT INTO ... ON [fg]); schemes/NEXT USED currently hardcode PRIMARY.
- Follow-up (user, 2026-10-05): no concurrent versions (limited server space) — pruning later. Knob: [retention] keep_releases=1 (example config still 5); peak ~2 copies during import is inherent; later options: drop raw layer after transforms, delete .ndjson after import.
- Follow-up (user, 2026-10-05): PHASE 2 = merge/upsert deltas into one current dataset instead of full snapshots. Implications noted to user: drop partitioning/SWITCH publish; keys without release_date; delta detection via per-resource hash/lastUpdated + deletion policy; child tables replace-per-changed-resource; atomic apply. Supersedes the keep_releases/filegroup-per-release discussion for the long term.
- Follow-up (user, 2026-10-05, Phase 2 perf): per-file multithreaded import into per-resource-type staging (user proposed partitioning raw by resource_type over 8 filegroups). Advised: separate per-type heaps (not partitioned by type — can't SWITCH into release-partitioned parent), each ON its own filegroup; expect ~2-3x from threads (PractitionerRole/Practitioner dominate; 3-4 workers); filegroups help only on separate disks; first switch to bulk path (bcp 36k rows/s vs fast_executemany 12k) with TABLOCK minimal logging; then parallel transforms per type (identifier = UNION ALL).
- Follow-up (user, 2026-10-05): confirmed Phase 2 has no snapshot switching at all — staging is scratch; upsert into one current dataset; drop partitioning/SWITCH/CHECK/boundaries. Replace SWITCH's reader-consistency with one transaction per delta + READ_COMMITTED_SNAPSHOT ON for npd.

## Deferred minor findings from the task reviews

- Task 1: minor (deferred): test_schemas_are_created_and_dropped name overstates; drop test doesn't assert ps_/table/view gone; conftest mid-file `import json as _json`; drop_schemas failure masks test result
- Task 1: minor (deferred): pre-existing DeprecationWarning "testcontainers.postgres is deprecated" from tests/conftest.py:38 (newer testcontainers in the fresh venv)
- Task 3: minor (deferred): sql_scripts duplicates schema._init_scripts and transform._scripts until Task 14 removes them
- Task 4: minor (deferred): mssql env entry without "trusted" silently defaults to "no" (SQL login with empty creds) — consider requiring it; open_connection re-reads env file per call; write_env_file breaks on values containing a single quote (test-only)
- Task 5: minor (deferred): RUN_DESCRIPTION nvarchar(200)/PROJECT varchar(1000) not truncated (plan-mandated, low risk); xml round-trip normalization relies on parse_release; pg_engine test helper hardcodes psycopg2 and doesn't URL-escape password
- Task 6: minor (deferred): helpers.pg_dialect hard-codes NpdDbConfig (can drift from ctx.config.npd_db); leftover `ctx.sleep = sleeps.append` in test_import.py:81/test_retention.py:117 is dead; cli.py:55 long line
- Task 7: minor (deferred): OBJECT_ID guards use unquoted names (odd schema names break re-run); ref_id returns '' not NULL for trailing slash (Postgres NULL); init_db batch errors carry no script context; tests lack fhir_ts date-only/Z-no-fraction and join_text >=10 elements; raw resource_type varchar(40) vs identifier.resource_type varchar(128); is_inlineable not verified
- Task 8: minor (deferred): _clone_indexes ignores included/desc/filtered/constraint-backed indexes (fine for current plain indexes); bad lastUpdated reported as "cannot read <path>"; truncation test matches "Organization" (weak); batch-commit not directly asserted; count check per resource_type assumes one file per type; fast_executemany datetime fraction worth a sub-second test if driver changes
- Task 9: minor (deferred): unused <<release>> token (plan-mandated); practitioner flags medicare/hhs/aligned, location address/lat-long, endpoint values not value-asserted
- Task 10: minor (deferred): role test doesn't assert organization_id/accepting_patients/network_organization_id (nested ext path unasserted); every-table test depends on fixture coverage
- Task 11: minor (deferred): SET XACT_ABORT ON never reset on pooled connection; finally-block exec can mask original error; UPDATE STATISTICS failure raises after commit (and has no lock timeout); failed sp_releaseapplock returns lock-holding connection to pool (invalidate instead); lock test doesn't assert 3 attempts / rollback-after-SPLIT path untested
- Task 12: minor (deferred): README schtasks example split over two lines without ^ (fails if pasted into cmd; from the plan); intro paragraph over 120 chars; no-op run not asserted beyond exit code
- Task 14: minor (deferred): advisory-lock raw connection returned to pool (invalidate at end of run_lock); SET TIME ZONE 'UTC' persists on pooled conn; psycopg2 imported but only declared transitively via Python-DataEngine; test engines not disposed; partial leaves after failed batch until cleanup (plan-mandated)
- Task 15: minor (deferred): chunk-boundary query binds nvarchar param vs varchar(40) column (CONVERT_IMPLICIT, use CAST(? AS varchar(40))); `-- chunked:` without <<chunk>> would duplicate rows — add guard, CHUNK_RE matches anywhere; chunk progress only at DEBUG; 010 name pick lacks type=5 guard (array-in-name edge) and JSON null name skipped; 030 description check uses LEN (trailing spaces) → DATALENGTH; chunk test mutates fixture without monkeypatch, one-way EXCEPT; old/new benchmark samples differ (documented)
