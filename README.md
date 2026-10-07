# npd-loader

Loads the CMS National Provider Directory FHIR bulk release into SQL Server 2019+
(`https://directory.cms.gov/downloads/`) as one current dataset: flattened tables in `npd` (with `v_*` views), updated
in place by each release. Every run and file is recorded in the catalog (`HIE_WAREHOUSE_META`; `dbo.MASTER_WAREHOUSE_RUN`,
`dbo.DATA_FILE`). SQL Server only: the earlier Postgres loader (raw JSONB, one partition per release) is on the `main`
branch.
Design: `docs/superpowers/specs/2026-10-06-phase2-python-flatten-upsert-design.md`.

## SQL Server (Windows)

The loader runs against SQL Server 2019+ through Python-DataEngine with Windows authentication. Connections live in a
DataEngine env file (`database.dev.env` / `database.prd.env` in this repo; no passwords), named by
`[databases] env_file` in `config.toml`. The `data` connection must be of `type` `mssql`; a `postgres` entry is
rejected. The catalog is the `catalog` connection: `HIE_WAREHOUSE_META_DEV` for development, `HIE_WAREHOUSE_META` for
production.

### DBA prerequisites

- Databases `npd`, `npd_dev`, `npd_test` on `cssnpi`, recovery model `SIMPLE`.
- The service account (and developers for `npd_dev`/`npd_test`): `db_owner`, or `db_ddladmin` + `db_datareader` +
  `db_datawriter`.
- `SELECT`, `INSERT`, `UPDATE` on `dbo.MASTER_WAREHOUSE_RUN` and `dbo.DATA_FILE` in each catalog database.
- Host: Microsoft ODBC Driver 17 or 18 for SQL Server installed on the machine that runs npd-loader.
- Once per npd database, with no other connections: `ALTER DATABASE [npd] SET READ_COMMITTED_SNAPSHOT ON`, so
  readers see the last committed state while a delta applies.
- Host: `bcp` (SQL Server command line utilities) on the PATH of the machine that runs npd-loader.
- Disk: the `.ndjson.zst` originals are the backup and are kept; the extracted `.ndjson` files of the newest
  `[retention] keep_releases` releases are kept (`config.example.toml` sets 1). The current dataset is stored once,
  and staging holds one release during an import. During an import the flattened stage files are also written under
  `storage.root/stage` (roughly the size of the flattened release) and removed afterwards.
- `init-db` expects a database without Phase 1 tables: use a fresh database (Phase 1 data lives in `npd_proof`).

### Install

    py -3.12 -m venv C:\npd-loader\.venv
    C:\npd-loader\.venv\Scripts\pip install <path to this repo>
    copy config.example.toml C:\npd-loader\config.toml
    copy database.prd.env C:\npd-loader\database.prd.env
    C:\npd-loader\.venv\Scripts\npd-loader --config C:\npd-loader\config.toml init-db

### Schedule (Task Scheduler)

    mkdir C:\npd-loader\scripts
    copy scripts\npd-loader-run.cmd C:\npd-loader\scripts\
    schtasks /Create /TN "npd-loader" /SC DAILY /ST 06:00 /RU <DOMAIN\service-account> /RP * /TR "C:\npd-loader\scripts\npd-loader-run.cmd"

Logs go to stderr and Task Scheduler discards it, so the task runs `scripts\npd-loader-run.cmd`: it appends stdout and
stderr to `C:\npd-loader\logs\npd-loader.log` (creating the folder) and exits with npd-loader's exit code, which
Task Scheduler shows as the last run result. The wrapper finds `npd-loader.exe` in the `.venv` one folder above it, so keep it in `C:\npd-loader\scripts\` (beside the venv) or edit the path.

### How it works

Python parses and flattens every resource of the release (`flatten/`, one worker per `.ndjson` file) before anything
reaches SQL Server: each resource becomes rows for its flat tables plus a content hash, written as `bcp` stage files and
loaded into the fixed staging tables in `[npd_db] stage_schema`, which are truncated at the start of every import.
Counts and duplicate resource ids are checked after the load.

The delta is then applied in one transaction (`apply_delta`): resources are classified new, changed or unchanged by
hash against `resource_state`; the rows of changed resources are replaced, new ones inserted, and every resource in
the release gets `last_seen_release` set. Resources missing from a release stay live (aging data: they keep an older
`last_seen_release`). The run lock is `sp_getapplock`.

There is no raw table and no backup inside the database: the `.ndjson.zst` originals are kept on disk and are the
backup. Retention deletes only older extracted `.ndjson` files. `import --force` applies a release older than the
current one.

## Upgrading

Re-run `npd-loader init-db` after every upgrade of npd-loader. It applies new tables and the idempotent schema changes
in `src/npd_loader/sql/mssql/init/900_migrations.sql` (the only place post-deployment changes to existing tables go),
and recreates the `v_*` views automatically. A staging table whose columns no longer match the specs is dropped and
recreated by `init-db` (staging is disposable).

Existing rows are only rewritten when their resource changes. After a change to the flatten specs or a new column that
must be filled for existing resources, bump `SPEC_VERSION` in `src/npd_loader/flatten/specs.py`: it is part of every
resource hash, so the next import classifies every resource as changed and rewrites all rows.

## Commands

| Command | What it does |
|---|---|
| `npd-loader run` | download the current release, then import the newest downloaded release (extracting first if needed); the import is attempted even if the download failed, and the exit code is 1 if either failed |
| `npd-loader download [--force]` | DOWNLOAD run: manifest + `.zst` files |
| `npd-loader extract [--release YYYY-MM-DD] [--force]` | EXTRACT run: `.zst` → `.ndjson`; restores deleted files in place |
| `npd-loader import [--release YYYY-MM-DD] [--force]` | IMPORT run: flatten, stage, apply the delta in one transaction, apply retention |
| `npd-loader status` | recent releases, their successful runs, and whether each has been applied |
| `npd-loader profile FILE [--unmapped]` | JSON paths in an `.ndjson[.zst]` file; `--unmapped` exits 1 if any path is unmapped |

Exit code 0 means success, nothing to do, or another run holds the lock; anything else is a failure. Logs go to stderr.

`run` imports only the newest downloaded release. To load an older downloaded release, use
`npd-loader import --release YYYY-MM-DD`.

## Querying

`npd.v_<table>` views show the current dataset. Every row has `release_date` (the release it came from),
`ndjson_file_id` and `zst_file_id`, which are `dbo.DATA_FILE.id` values in the catalog. `npd.resource_state` is the key
registry: one row per resource with its `resource_key` (`int`, assigned the first time an id is seen as a resource or
as a reference, never reused), hash and `last_seen_release`; a resource whose `last_seen_release` is older than the
newest release was missing from it. Data tables use `resource_key` and reference columns are named `<name>_key`; ids
are stored without their `Type-` prefix. A key whose `hash` is NULL is a referenced id with no data yet (find such
references with a `LEFT JOIN`).

Flattened code and type columns (e.g. `*_code`, `*_system`, `*_display`) take the first entry only:
`coding[0]` of a CodeableConcept and `type[0]` where `type` repeats. The full original resource is in the
`.ndjson.zst` file named by `zst_file_id`.

## Tests

Use a virtualenv (Python 3.12+):

```bash
python -m venv .venv
.venv/bin/python -m pip install -e ".[test]"        # Windows: .venv\Scripts\python
```

SQL Server tests need `NPD_TEST_MSSQL_DB`; without it they are skipped:

    $env:NPD_TEST_MSSQL_DB = '{"type":"mssql","server":"cssnpi","database":"npd_test","trusted":"yes"}'
    .\.venv\Scripts\python -m pytest

They create and drop their own schemas in `npd_test` and never touch either catalog database.
