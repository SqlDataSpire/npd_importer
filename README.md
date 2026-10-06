# npd-loader

Loads the CMS National Provider Directory FHIR bulk release into SQL Server or Postgres
(`https://directory.cms.gov/downloads/`). On Postgres: raw JSONB in `npd_raw.resource`, flattened tables in `npd`,
one partition per release, the newest `[retention] keep_releases` releases kept (5 by default; the example config
sets 1, which is what the SQL Server host uses). Every run and file is recorded in `css_catalog_local` (`master_warehouse_run`, `data_file`).
Design: `docs/superpowers/specs/2026-10-03-npd-fhir-loader-design.md`.

## SQL Server (Windows)

The loader runs against SQL Server 2019+ through Python-DataEngine with Windows authentication. Connections live in a
DataEngine env file (`database.dev.env` / `database.prd.env` in this repo; no passwords), named by
`[databases] env_file` in `config.toml`. The `type` of the `data` connection (`mssql` or `postgres`) selects the
flavor. The catalog is the `catalog` connection: `HIE_WAREHOUSE_META_DEV` for development, `HIE_WAREHOUSE_META` for
production (`dbo.MASTER_WAREHOUSE_RUN`, `dbo.DATA_FILE`).

### DBA prerequisites

- Databases `npd`, `npd_dev`, `npd_test` on `cssnpi`, recovery model `SIMPLE`.
- The service account (and developers for `npd_dev`/`npd_test`): `db_owner`, or `db_ddladmin` + `db_datareader` +
  `db_datawriter` + `ALTER ANY DATASPACE`.
- `SELECT`, `INSERT`, `UPDATE` on `dbo.MASTER_WAREHOUSE_RUN` and `dbo.DATA_FILE` in each catalog database.
- Host: Microsoft ODBC Driver 17 or 18 for SQL Server installed on the machine that runs npd-loader.
- Disk: one release is about 43 GB in the raw layer on SQL Server (page compressed; 24.9M rows, see
  `docs/profile/2026-10-06-mssql-dev-import.md`; 73 GB on Postgres). `config.example.toml` keeps one release
  (`keep_releases = 1`); during an import the new release coexists with the old one until publish, so plan for about
  2 copies at peak.

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

### How it works on SQL Server

Raw lines go into `npd_raw.resource` (`varchar(max)` with a UTF-8 collation), in batches of 5,000 rows, each committed
as it lands, into a standalone table for the release. T-SQL transforms (`OPENJSON`) fill standalone `npd.*` tables:
each script parses its resource type once into a scratch stage table, in chunks of 200,000 resources, and fills its
tables from the stage. `tests\mssql_bench.py` times them on a sample of a real raw table (see
`docs/profile/2026-10-06-mssql-transform-benchmark.md`).
Publish switches every standalone table into its parent's release partition in one transaction (`SPLIT RANGE`,
`SWITCH`); retention switches old partitions out and `MERGE`s their boundaries. The run lock is `sp_getapplock`.

### Tests

    $env:NPD_TEST_MSSQL_DB = '{"type":"mssql","server":"cssnpi","database":"npd_test","trusted":"yes"}'
    .\.venv\Scripts\python -m pytest

SQL Server tests create and drop their own schemas in `npd_test` and never touch either catalog database.

## Install on Postgres (192.10.0.7)

```bash
sudo mkdir -p /opt/npd-loader /etc/npd-loader /data/npd
git clone <repo> /opt/npd-loader/src
python3.12 -m venv /opt/npd-loader/.venv
/opt/npd-loader/.venv/bin/pip install /opt/npd-loader/src
sudo cp /opt/npd-loader/src/config.example.toml /etc/npd-loader/config.toml
sudo chmod 600 /etc/npd-loader/config.toml   # connections: see database.*.env
/opt/npd-loader/.venv/bin/npd-loader init-db
```

`init-db` is idempotent. The `npd` database must already exist, and the `npd_db` user needs CREATE on it.

Connections come from the env file named by `[databases] env_file`; for Postgres an entry looks like (the `npd_db`
and `catalog` connections may be separate entries pointing at the same server):

```
databases = '{"data": {"type": "postgres", "server": "192.10.0.7:5432", "database": "npd", "UN": "npd_db", "PW": "<password>"}, "catalog": {"type": "postgres", "server": "192.10.0.7:5432", "database": "css_catalog_local", "UN": "catalog", "PW": "<password>"}}'
```

A `database.env` that holds a password must not be committed; keep it outside the repo (or `chmod 600`) and point
`env_file` at it.

**Upgrading:** re-run `npd-loader init-db` after every upgrade of npd-loader. It applies new tables and the
idempotent schema changes in `src/npd_loader/sql/{postgres,mssql}/init/900_migrations.sql` (the only place post-deployment changes to
existing tables go, as `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`), and recreates the `v_*` views automatically.
The `catalog` user needs INSERT/UPDATE/SELECT on `master_warehouse_run` and `data_file`.

## Commands

| Command | What it does |
|---|---|
| `npd-loader run` | download the current release, then import the newest downloaded release (extracting first if needed); the import is attempted even if the download failed, and the exit code is 1 if either failed |
| `npd-loader download [--force]` | DOWNLOAD run: manifest + `.zst` files |
| `npd-loader extract [--release YYYY-MM-DD] [--force]` | EXTRACT run: `.zst` → `.ndjson`; restores deleted files in place |
| `npd-loader import [--release YYYY-MM-DD] [--force]` | IMPORT run: load, transform, publish atomically, apply retention |
| `npd-loader status` | recent releases, their successful runs, and whether each is published |
| `npd-loader profile FILE [--unmapped]` | JSON paths in an `.ndjson[.zst]` file; `--unmapped` exits 1 if any path is unmapped |

Exit code 0 means success, nothing to do, or another run holds the lock; anything else is a failure. Logs go to stderr.

`run` imports only the newest downloaded release. To load an older downloaded release, use
`npd-loader import --release YYYY-MM-DD`.

Publishing with `--force` and retention detach partitions, which needs a brief exclusive lock on each parent table.
They wait at most `[npd_db] lock_timeout_seconds` (default 30) per attempt, 3 attempts, so a long-running query
cannot make every reader queue behind them. A forced import that cannot get the lock fails; retention logs a
warning and tries again on the next import.

## Schedule

systemd (`/etc/systemd/system/npd-loader.service` and `.timer`):

```ini
[Unit]
Description=Load the CMS NPD FHIR release

[Service]
Type=oneshot
User=npd
ExecStart=/opt/npd-loader/.venv/bin/npd-loader --config /etc/npd-loader/config.toml run
```

```ini
[Unit]
Description=Daily NPD FHIR load

[Timer]
OnCalendar=*-*-* 06:00:00
Persistent=true

[Install]
WantedBy=timers.target
```

```bash
sudo systemctl daemon-reload && sudo systemctl enable --now npd-loader.timer
journalctl -u npd-loader.service
```

or cron: `0 6 * * * /opt/npd-loader/.venv/bin/npd-loader --config /etc/npd-loader/config.toml run >> /var/log/npd-loader.log 2>&1`

## Querying

`npd.v_<table>` views show the newest published release. Query the base tables with `release_date = ...` for older
ones. Every row has `ndjson_file_id` and `zst_file_id`, which are `data_file.id` values in the catalog: `css_catalog_local`
on Postgres (`master_warehouse_run`, `data_file`), `HIE_WAREHOUSE_META` (`HIE_WAREHOUSE_META_DEV` for development) on
SQL Server (`dbo.MASTER_WAREHOUSE_RUN`, `dbo.DATA_FILE`).

Flattened code and type columns (e.g. `*_code`, `*_system`, `*_display`) take the first entry only:
`coding[0]` of a CodeableConcept and `type[0]` where `type` repeats. The full resource, with every coding, is in
`npd_raw.resource.resource`: JSONB on Postgres; on SQL Server a `varchar(max)` column of UTF-8 JSON, queried with
`JSON_VALUE` / `OPENJSON`, for example:

```sql
SELECT resource_id, JSON_VALUE(resource, '$.name') AS name
FROM npd_raw.v_resource WHERE resource_type = 'Organization';
```

## Tests

Use a virtualenv (Python 3.12+):

```bash
python -m venv .venv
.venv/bin/python -m pip install -e ".[test]"        # Windows: .venv\Scripts\python
```

The Postgres-backed tests need a server. Either point them at an existing one (no Docker needed; each test
creates and drops its own database, so the user needs CREATEDB):

```bash
NPD_TEST_PG_DSN=postgresql://test:test@127.0.0.1:55432/postgres .venv/bin/python -m pytest -v
```

or leave `NPD_TEST_PG_DSN` unset and the tests start a disposable `postgres:16` container with Docker
(`NPD_TEST_PG_IMAGE=postgres:<server major>` to match the server). Without either, the Postgres-backed tests are
skipped.
