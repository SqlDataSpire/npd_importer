# SQL Server port on Python-DataEngine — design

Date: 2026-10-05. Branch: `feature/sqlport-v1` (the fork; `main` stays the self-contained Postgres + psycopg loader).
Predecessor: `2026-10-03-npd-fhir-loader-design.md`.

## Goal

Load the CMS NPD FHIR release into SQL Server with the same stages, commands and run semantics as today, with every
database connection made through Python-DataEngine connection objects. Postgres stays supported as a second flavor,
also through DataEngine. This is a prototype: the aim is working load infrastructure on SQL Server. Data quality and
cross-engine parity are not goals of this phase.

## Decisions

| Topic | Decision |
|---|---|
| Fork | Branch `feature/sqlport-v1` in this repo; its origin moves to Azure DevOps after the prototype. |
| Flavors | SQL Server (new) and Postgres (existing), chosen by the data connection's `type` in `database.env`. |
| Data access | DataEngine only: `SqlConnectionObject` (mssql) and `PgConnectionObject` (postgres). Parameterized and transactional work uses the object's SQLAlchemy `engine`. No ORM. |
| Dependencies | `Python-DataEngine>=2.4`, `zstandard`, `httpx`. `psycopg` is removed (Postgres goes through DataEngine's psycopg2). |
| SQL Server | 2019 (`cssnpi` is 2019 CU32 Developer). Features up to 2019 allowed, including `STRING_AGG` and UTF-8 collations. |
| Auth | Windows auth (`trusted: yes`) for SQL Server. The loader runs on Windows under Task Scheduler. |
| Data databases | `cssnpi.npd` (prod), `cssnpi.npd_dev` (dev), `cssnpi.npd_test` (tests). None exist yet; a DBA creates them. |
| Catalog | Parameterized connection. Prod `cssnpi.HIE_WAREHOUSE_META`, dev `cssnpi.HIE_WAREHOUSE_META_DEV`, both `dbo.MASTER_WAREHOUSE_RUN` / `dbo.DATA_FILE` (existing tables; no DDL is run there). Moving to the cloud later is a `database.env` change. |
| Catalog procedures | Not used. `SP_START_NEW_RUN` cannot set `PROJECT`/`RUN_CLASS`/`XML_CONFIG` and returns the GUID; `LOG_DATA_FILE` truncates `FILE_NAME` to 1000 and fills null dates. Direct parameterized `INSERT … OUTPUT` / `UPDATE` instead. |
| Postgres flavor | Ported to DataEngine in the last phase. Verification deferred: its database tests skip unless `NPD_TEST_PG_DB` is set and do not gate the prototype. |
| Parity | Out of scope. SQL Server tests check that things run and fill tables, not that values match Postgres. |

## 1. Architecture and connections

### Configuration

`config.toml` loses every host/port/dbname/user/password key and `[credentials]`. It gains:

```toml
[databases]
env_file = 'C:\npd-loader\database.prd.env'   # DataEngine connection documents

[npd_db]
connection = "data"          # name in env_file; its type (mssql|postgres) selects the flavor
raw_schema = "npd_raw"
schema = "npd"
lock_timeout_seconds = 30

[catalog]
connection = "catalog"
run_table = "dbo.MASTER_WAREHOUSE_RUN"
file_table = "dbo.DATA_FILE"
project = "NPD"
# … other existing [catalog] keys unchanged
```

Two committed example env files (Windows auth, no secrets), following the nppes_* loaders:

```
# database.dev.env
databases = '{"data":    {"type":"mssql","server":"cssnpi","database":"npd_dev","trusted":"yes"},
              "catalog": {"type":"mssql","server":"cssnpi","database":"HIE_WAREHOUSE_META_DEV","trusted":"yes"}}'
# database.prd.env
databases = '{"data":    {"type":"mssql","server":"cssnpi","database":"npd","trusted":"yes"},
              "catalog": {"type":"mssql","server":"cssnpi","database":"HIE_WAREHOUSE_META","trusted":"yes"}}'
```

### Modules

- `connections.py` (new): reads `env_file` with `dotenv_values`, parses the `databases` JSON, and builds the two
  named objects with `DataEngine.connectionGenerator(dict)`. Reading the path from config means the loader never
  depends on `database.env` being in the working directory. Missing names, or a type other than mssql/postgres,
  raise `ConfigError`.
- `dialect/__init__.py`: the `Dialect` protocol and `dialect_for(engine)`, picked from `engine.dialect.name`
  (`mssql` / `postgresql`). Everything engine-specific is behind it: identifier quoting and length limit (63 / 128),
  rendering SQL tokens, raw bulk load, listing published partitions, publish, drop release, run lock, orphan
  listing, init-db script execution.
- `dialect/mssql.py`, `dialect/postgres.py`: the two implementations.
- `sql/postgres/{init,transform}/` (today's files, moved unchanged) and `sql/mssql/{init,transform}/` (new).
- `catalog.py`: one `SqlCatalog` on SQLAlchemy Core with lightweight `Table()` definitions (no reflection, no ORM).
  `insert(...).returning(id)` compiles to `RETURNING` (Postgres) or `OUTPUT inserted.ID` (SQL Server), so
  `css_catalog_local` and `HIE_WAREHOUSE_META` share one code path. The `Catalog` protocol, `Run`, `DataFile` and
  the helpers are unchanged. Table names come from config and are split on the first `.` into schema and table.
  SQL Server column names are uppercase but the default collation is case-insensitive, so the lowercase column
  names work on both.
- `stages.Context`: `npd_conninfo` is replaced by `npd` (the DataEngine object) and `dialect`. `lock` is built from
  `dialect.run_lock`.
- Removed: `credentials.py`, `db.py` (its contents move into the dialects and a shared `sqltext.py` for token
  rendering and `standalone_name`).
- Unchanged: download, extract, manifest, storage, profile, runxml, CLI commands, exit codes, log output.

### Shared, engine-neutral code

`raw_load.iter_lines` and `raw_load.validate_line` stay shared. `import_stage`, `transform.run_transforms`,
`retention.apply_retention` keep their control flow and call the dialect for every database operation.

## 2. SQL Server data model and raw load

### Partitioning

`init-db` creates, per data schema, partition function `pf_<schema>_release (date) AS RANGE RIGHT FOR VALUES ()` and
scheme `ps_<schema>_release AS PARTITION pf_<schema>_release ALL TO ([PRIMARY])` (for production, `pf_npd_release`;
the raw schema gets its own `pf_npd_raw_release`). The schema in the name lets test schemas share `npd_test`
without colliding. Every parent table is created `ON ps_<schema>_release (release_date)`, with aligned indexes
mirroring today's unique and secondary indexes. Readers keep the same contract: `npd.<table>` holds all kept
releases; `npd.v_<table>` shows the latest published one.

### Types

| Postgres | SQL Server |
|---|---|
| `text` used in a key or index (`resource_id`, `npi`, `state`, references, codes) | `varchar(n)`, sized from the 2026-09-29 profile maxima with headroom, under the 1,700-byte key limit |
| other `text` | `nvarchar(n)` or `nvarchar(max)` |
| `boolean` | `bit` |
| `integer` / `bigint` | same |
| `date` | `date` |
| `timestamptz` | `datetime2(3)`, UTC |
| raw `resource jsonb` | `varchar(max) COLLATE Latin1_General_100_CI_AS_SC_UTF8` |

All tables use `DATA_COMPRESSION = PAGE`.

### Raw load

One standalone table per release: `npd_raw.resource__<yyyymmdd>__r<run>`, a heap with
`CHECK (release_date = '<release>')`. SQL Server has no sub-partitioning, so `resource_type` is an ordinary column.
For each `.ndjson`, the shared loop reads and validates lines and inserts batches of 5,000 rows with
`executemany` on a connection from the `SqlConnectionObject` engine (`fast_executemany` is already on). The
line-count check stays. After all files, the unique index `(release_date, resource_type, resource_id)` is built; on
error 1505 the loader queries the duplicates and raises `RawLoadError` naming the file, as today.

**Each batch commits as soon as it lands.** No transaction spans a file, which bounds transaction-log growth and lock
duration. This is safe because the batches go into an unpublished standalone table that nothing reads: if a file
fails partway, its committed rows stay in that table and the existing failure path drops it
(`drop_standalone_tables` for the run, or orphan cleanup if the process was killed), so partial data is never
published. The end-of-file line-count check still catches missing rows. A rerun restarts the import; resuming
mid-file is out of scope.

### Transaction boundaries

| Step | Transaction |
|---|---|
| Raw load | one per batch of 5,000 rows |
| Each transform script | one per script (each `INSERT … SELECT` is atomic regardless); minimal logging via `TABLOCK` and `SIMPLE` recovery keeps the log small. If the first full run shows log pressure, the fallback is chunking the largest scripts by `resource_id` range. |
| Index builds, row counts | one per step, as today |
| Publish, drop release | one each; metadata-only (`SPLIT` / `SWITCH` / `MERGE`), so seconds long. This is what keeps publication atomic. |
| Catalog writes | one per call (autocommit), independent of the data, as today |

**First plan task: throughput spike.** Bulk loading `varchar(max)` through pyodbc `fast_executemany` has unknown
throughput; Postgres `COPY` already took 3 h. Load one real file (Organization, 2.06M rows) and measure rows/s. If it
is too slow, the dialect method uses `bcp.exe -T` from the loader host instead (still Windows auth). The rest of the
design does not change either way.

## 3. Transforms

`sql/mssql/transform/010…090_*.sql` mirror the Postgres scripts one for one: same target tables and columns. Same
tokens (`<<raw>>`, `<<t:table>>`, `<<schema>>`, `<<release>>`), rendered with bracket quoting. The runner is shared:
create standalone tables (heaps with the release `CHECK`), run scripts in name order committing after each, clone
the parent's indexes, count rows.

Translation:

- `r.resource->>'x'` → `JSON_VALUE(r.resource, '$.x')`
- `jsonb_array_elements(...) WITH ORDINALITY` → `CROSS APPLY OPENJSON(r.resource, '$.name') AS x` with
  `seq = CAST(x.[key] AS int) + 1`, and fields via `CROSS APPLY OPENJSON(x.value) WITH (...)`
- `LEFT JOIN LATERAL (... LIMIT 1) ON true` → `OUTER APPLY (SELECT TOP 1 ... ORDER BY ...)`
- helpers become inline table-valued functions used with `OUTER APPLY`: `ext(@resource, @url)`,
  `identifier_value(@resource, @systems)` (systems as a JSON array string), `join_text(@arr, @sep)` (`STRING_AGG …
  WITHIN GROUP (ORDER BY key)`), `fhir_ts(@v)` (`YYYY`, `YYYY-MM`, full with offset → UTC `datetime2(3)` via
  `datetimeoffset`). `ref_id` becomes an inline `RIGHT`/`CHARINDEX` expression.
- Plain `CAST`, not `TRY_CAST`: a malformed value fails the import, as in Postgres.

Inserts use `WITH (TABLOCK)` into the empty heaps for minimal logging; indexes are built after all scripts.

## 4. Publish, retention, locking, init-db (SQL Server)

**Publish**, one transaction, with `SET LOCK_TIMEOUT <lock_timeout_seconds * 1000>`; on error 1222 the whole
transaction is retried 3 times with backoff (today's policy):

1. For each schema's function, if `@release` is not a boundary: `ALTER PARTITION SCHEME … NEXT USED [PRIMARY]`;
   `ALTER PARTITION FUNCTION … SPLIT RANGE (@release)` (the new partition is empty; no rows move).
2. For each target table, if its partition for `@release` has rows: without `--force` raise `PublishConflict`;
   with `--force` `SWITCH` it out to a throwaway table and drop that.
3. `ALTER TABLE <standalone> SWITCH TO <parent> PARTITION $PARTITION.pf_<schema>_release(@release)` (metadata
   only, thanks to the `CHECK` and aligned indexes).
4. `MERGE` into `<schema>.release`; commit. Then `UPDATE STATISTICS` on the published tables (replaces `ANALYZE`).

**Published releases** per table come from `sys.partitions` joined to `sys.partition_range_values`: boundaries
whose partition has rows. `<schema>.release` remains the published record.

**Retention**: same policy code. Dropping a release switches each table's partition out, drops the staging tables,
`MERGE RANGE (@release)` on each function (empty partition; no rows move), and deletes from `<schema>.release`. A
lock timeout is a warning, retried on the next run.

**Run lock**: `sp_getapplock @Resource = 'npd_loader:<stage>', @LockMode = 'Exclusive', @LockOwner = 'Session',
@LockTimeout = 0` on a dedicated connection held for the stage; `sp_releaseapplock` on exit. A result < 0 means
another run holds it (exit 0, as today).

**Orphan cleanup**: candidate tables from `sys.tables` in the raw and data schemas, filtered in Python with today's
`__r<run>(__[a-z]+)?$` pattern, excluding partitioned tables; dropped.

**init-db**: runs `sql/mssql/init/*.sql` in name order, each split into batches on lines that are exactly `GO`.
Every statement is idempotent (`IF NOT EXISTS (SELECT … FROM sys.…)` guards; `CREATE OR ALTER` for functions and
views). `900_migrations.sql` keeps its role for guarded `ALTER TABLE … ADD`. The partition functions start with no
boundaries. The `v_*` views are recreated at the end with `CREATE OR ALTER VIEW`.

## 5. Postgres flavor on DataEngine (last phase)

Connections from `PgConnectionObject.engine` (SQLAlchemy + psycopg2). SQL files unchanged. Glue ported
mechanically: identifiers quoted by the engine's `identifier_preparer`; transactions via `engine.begin()`; lock
timeout detected by pgcode `55P03` on the wrapped DBAPI error; `COPY … FROM STDIN (FORMAT csv)` through
`raw_connection().cursor().copy_expert` with a file-like wrapper that yields CSV rows as the `.ndjson` is read,
one `COPY` and commit per batch of rows to match the SQL Server batch-commit rule.
`PgConnectionObject` only supports user/password auth and takes the port as `server = "host:port"`. Verification is
deferred: the Postgres database tests run only when `NPD_TEST_PG_DB` is set.

## Testing

- Connections from environment variables holding one `database.env`-style JSON document each:
  `NPD_TEST_MSSQL_DB` (`cssnpi.npd_test`, `trusted: yes`) and `NPD_TEST_PG_DB` (a Postgres admin login). Tests
  needing one skip when it is unset.
- SQL Server tests create and drop their own schemas in `npd_test` (raw schema, data schema, and a scratch copy of
  `MASTER_WAREHOUSE_RUN` / `DATA_FILE` with the real column types). They never touch either catalog database.
- SQL Server coverage: the catalog contract (`tests/catalog_contract.py`); `init-db` run twice; raw load, including
  duplicate ids and bad lines; each transform fills its tables from the fixture release; publish with and without
  `--force`; retention; the run lock; orphan cleanup; the end-to-end test against the fake CMS server.
- Unit tests that use no database are unchanged.
- Manual prototype runs use `database.dev.env` (`npd_dev` + `HIE_WAREHOUSE_META_DEV`).

## Installation and deployment

- Prototype: `.venv` in the repo (gitignored), Python 3.12, `pip install -e .[test]`, which installs
  `Python-DataEngine` 2.4.0 from the configured package index. DataEngine picks ODBC Driver 18 or 17.
- Production: Windows host, Task Scheduler running `npd-loader --config <path> run` daily under a service account.
- DBA prerequisites: create `npd`, `npd_dev`, `npd_test` on `cssnpi` with `SIMPLE` recovery; grant the service
  account (and developers, for `npd_dev`/`npd_test`) `db_owner`, or `db_ddladmin` + `db_datareader` +
  `db_datawriter` + `ALTER ANY DATASPACE`; grant `SELECT`, `INSERT`, `UPDATE` on `dbo.MASTER_WAREHOUSE_RUN` and
  `dbo.DATA_FILE` in each catalog database.
- Disk: the Postgres test used 73 GB for one release. Page compression should reduce that; plan for several hundred
  GB at `keep_releases = 5`.
- README gains a SQL Server section (install, env files, Task Scheduler, DBA prerequisites) next to the Postgres one.

## Out of scope

Cross-engine parity; an ORM; Postgres Windows/AD auth; changes to Python-DataEngine; moving the catalog to the cloud
(only made possible).
