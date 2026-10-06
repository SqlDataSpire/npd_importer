# SQL Server Port on Python-DataEngine Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Load the CMS NPD FHIR release into SQL Server 2019 through Python-DataEngine connection objects, with the
same stages, commands and run semantics as the Postgres loader, and the catalog in `HIE_WAREHOUSE_META`.

**Architecture:** Connections are built from a DataEngine `database.env` named in `config.toml`. Every engine-specific
operation on the npd database sits behind a `Dialect` (`dialect/mssql.py`, `dialect/postgres.py`); the stages talk only
to the dialect. The catalog is one SQLAlchemy Core implementation that works on both engines. SQL Server uses one
partition function per schema, standalone per-release tables, and `SWITCH` for atomic publish and retention.

**Tech Stack:** Python 3.12, Python-DataEngine 2.4 (SQLAlchemy 2 + pyodbc / psycopg2), SQL Server 2019 (`OPENJSON`,
partitioning, `sp_getapplock`), pytest.

**Spec:** `docs/superpowers/specs/2026-10-05-sqlserver-dataengine-port-design.md`

## Global Constraints

- Branch `feature/sqlport-v1`. Never commit to `main`.
- Python `>=3.12`. Dependencies: `Python-DataEngine>=2.4`, `zstandard>=0.22`, `httpx>=0.27`; `psycopg` stays only until Task 14 removes it.
- SQL Server 2019 features at most (`STRING_AGG`, UTF-8 collations allowed; nothing 2022-only such as `GREATEST`).
- SQL Server auth is Windows auth only (`"trusted": "yes"`). No passwords in committed files.
- All SQL Server connections come from `SqlConnectionObject`; parameterized/transactional work uses its `.engine`. No ORM.
- Raw load commits every batch of 5,000 rows. Publish and drop-release are one transaction each.
- Tests never touch `HIE_WAREHOUSE_META` or `HIE_WAREHOUSE_META_DEV`. SQL Server tests use `NPD_TEST_MSSQL_DB` (`cssnpi.npd_test`) and create/drop their own schemas; they skip when the variable is unset.
- Postgres database tests skip unless `NPD_TEST_PG_DSN` is set; they do not gate this prototype.
- Cross-engine parity is out of scope: SQL Server transform tests check that rows land with the right keys, not that every value matches Postgres.
- SQL Server scripts separate statements with lines that are exactly `GO`; each batch is executed on its own (pyodbc can swallow errors from later statements in a multi-statement batch).
- Partition function / scheme names: `pf_<schema>_release`, `ps_<schema>_release`.

## Review Focus

1. Importing DataEngine prints to stdout and auto-loads `./database.env` → the loader must import it with stdout suppressed so `npd-loader status` output stays clean (Task 4 test).
2. A config still holding the old `host`/`user`/`password`/`backend` keys → a clear `ConfigError` saying they moved to `database.env`, not a silent ignore (Task 4 test).
3. A value longer than its SQL Server column (e.g. a 300-character city) → the import fails naming the error rather than silently truncating; documented, and pinned by a raw-load test that a failing batch leaves no published data (Task 8 test).
4. A second `npd-loader import` started while one runs → the second exits 0 with "another import is running" because `sp_getapplock` is held on its own session (Task 11 test).
5. `--force` import of a release that is already published → the old partition is switched out and replaced in one transaction, the parent never shows both or neither (Task 11 test).

---

## File Structure

| File | Responsibility |
|---|---|
| `src/npd_loader/connections.py` (new) | Read the DataEngine env file, build `SqlConnectionObject` / `PgConnectionObject` by name |
| `src/npd_loader/sqltext.py` (new) | Token rendering, `GO` batch splitting, standalone table names, packaged SQL script loading |
| `src/npd_loader/dialect/__init__.py` (new) | `Dialect` protocol, shared exceptions/results, `dialect_for()` |
| `src/npd_loader/dialect/mssql.py` (new) | Every SQL Server npd-database operation |
| `src/npd_loader/dialect/postgres.py` (new) | Postgres dialect (wraps the existing psycopg modules until Task 14 ports them) |
| `src/npd_loader/sql/mssql/init/*.sql` (new) | Schemas, partition functions, helper functions, tables |
| `src/npd_loader/sql/mssql/transform/*.sql` (new) | T-SQL transforms |
| `src/npd_loader/sql/postgres/{init,transform}/*.sql` (moved) | Today's Postgres SQL, unchanged |
| `src/npd_loader/catalog.py` (rewrite) | `SqlCatalog` on SQLAlchemy Core |
| `src/npd_loader/config.py`, `cli.py`, `stages.py`, `import_stage.py`, `retention.py` (modify) | Use connections + dialect |
| `src/npd_loader/credentials.py` (delete) | Replaced by `database.env` |
| `database.dev.env`, `database.prd.env` (new) | Committed example connection documents |
| `tests/mssql_helpers.py` (new) | SQL Server test fixtures/helpers |
| `tests/sql/hie_catalog_schema.sql` (new) | Scratch copy of the `HIE_WAREHOUSE_META` catalog tables |
| `tests/test_mssql_*.py` (new) | SQL Server tests |

---

### Task 1: Environment, DataEngine install, SQL Server test fixtures

**Files:**
- Modify: `pyproject.toml`
- Create: `tests/mssql_helpers.py`
- Modify: `tests/conftest.py`
- Test: `tests/test_mssql_fixture.py`

**Interfaces:**
- Produces: fixtures `mssql_doc` (dict), `mssql_engine` (`sqlalchemy.Engine`), `mssql_schemas` (factory `(*suffixes: str) -> list[str]`); helper `mssql_helpers.drop_schemas(engine, schemas: list[str]) -> None`.

Prerequisite (stop and ask the user if missing): the database `npd_test` exists on `cssnpi` and the current Windows
account can `CREATE SCHEMA` / `CREATE TABLE` / `CREATE PARTITION FUNCTION` in it. Check with:
`sqlcmd -S cssnpi -d npd_test -E -Q "SELECT HAS_PERMS_BY_NAME(DB_NAME(), 'DATABASE', 'CREATE SCHEMA'), HAS_PERMS_BY_NAME(DB_NAME(), 'DATABASE', 'ALTER ANY DATASPACE')"` → both `1`.

- [ ] **Step 1: Create the venv and add DataEngine**

In `pyproject.toml` change the dependencies and the package data:

```toml
dependencies = [
    "Python-DataEngine>=2.4",
    "psycopg[binary]>=3.1",
    "zstandard>=0.22",
    "httpx>=0.27",
]
```

```toml
[tool.setuptools.package-data]
npd_loader = ["sql/*/init/*.sql", "sql/*/transform/*.sql", "sql/init/*.sql", "sql/transform/*.sql",
              "sql/mapped_paths.txt"]
```

Run (PowerShell):

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\python -m pip install -U pip
.\.venv\Scripts\python -m pip install -e ".[test]"
.\.venv\Scripts\python -c "import contextlib, io; f = io.StringIO(); ctx = contextlib.redirect_stdout(f); ctx.__enter__(); import DataEngine; ctx.__exit__(None, None, None); print(DataEngine.__version__)"
```

Expected: last line `2.4.0`.

- [ ] **Step 2: Write the failing fixture test**

`tests/test_mssql_fixture.py`:

```python
from mssql_helpers import drop_schemas


def test_schemas_are_created_and_dropped(mssql_engine, mssql_schemas):
    raw, data = mssql_schemas("raw", "")
    assert raw == data + "_raw"
    with mssql_engine.connect() as conn:
        found = {r[0] for r in conn.exec_driver_sql(
            "SELECT name FROM sys.schemas WHERE name IN (?, ?)", (raw, data))}
    assert found == {raw, data}


def test_drop_schemas_removes_objects(mssql_engine, mssql_schemas):
    (s,) = mssql_schemas("x")
    with mssql_engine.begin() as conn:
        conn.exec_driver_sql(f"CREATE PARTITION FUNCTION [pf_{s}_release] (date) AS RANGE RIGHT FOR VALUES ()")
        conn.exec_driver_sql(f"CREATE PARTITION SCHEME [ps_{s}_release] AS PARTITION [pf_{s}_release] ALL TO ([PRIMARY])")
        conn.exec_driver_sql(f"CREATE TABLE [{s}].[t] (release_date date NOT NULL) ON [ps_{s}_release] (release_date)")
        conn.exec_driver_sql(f"CREATE VIEW [{s}].[v_t] AS SELECT * FROM [{s}].[t]")
    drop_schemas(mssql_engine, [s])
    with mssql_engine.connect() as conn:
        assert conn.exec_driver_sql("SELECT count(*) FROM sys.schemas WHERE name = ?", (s,)).scalar() == 0
        assert conn.exec_driver_sql("SELECT count(*) FROM sys.partition_functions WHERE name = ?",
                                    (f"pf_{s}_release",)).scalar() == 0
```

- [ ] **Step 3: Run it to verify it fails**

Run: `$env:NPD_TEST_MSSQL_DB='{"type":"mssql","server":"cssnpi","database":"npd_test","trusted":"yes"}'; .\.venv\Scripts\python -m pytest tests/test_mssql_fixture.py -v`
Expected: ERROR, `ModuleNotFoundError: No module named 'mssql_helpers'`.

- [ ] **Step 4: Write the helpers and fixtures**

`tests/mssql_helpers.py`:

```python
"""SQL Server test helpers. Tests get a connection from NPD_TEST_MSSQL_DB (a database.env-style JSON document)."""
from __future__ import annotations

import contextlib
import io

from sqlalchemy.engine import Engine

MSSQL_ENV = "NPD_TEST_MSSQL_DB"


def sql_connection_object(name: str, doc: dict):
    with contextlib.redirect_stdout(io.StringIO()):
        import DataEngine
    return DataEngine.SqlConnectionObject(name=name, server=doc["server"], database=doc["database"],
                                          UN=doc.get("UN", ""), PW=doc.get("PW", ""),
                                          trusted=doc.get("trusted", "no"))


def drop_schemas(engine: Engine, schemas: list[str]) -> None:
    """Drop every view, table and function in `schemas`, their pf_/ps_ partition objects, then the schemas."""
    order = "CASE o.type WHEN 'V' THEN 0 WHEN 'U' THEN 1 ELSE 2 END"
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        for s in schemas:
            objects = conn.exec_driver_sql(
                f"SELECT o.name, RTRIM(o.type) FROM sys.objects o WHERE o.schema_id = SCHEMA_ID(?) "
                f"AND o.type IN ('V', 'U', 'IF', 'FN', 'TF') ORDER BY {order}", (s,)).fetchall()
            for name, kind in objects:
                what = {"V": "VIEW", "U": "TABLE"}.get(kind, "FUNCTION")
                conn.exec_driver_sql(f"DROP {what} [{s}].[{name}]")
            if conn.exec_driver_sql("SELECT count(*) FROM sys.partition_schemes WHERE name = ?",
                                    (f"ps_{s}_release",)).scalar():
                conn.exec_driver_sql(f"DROP PARTITION SCHEME [ps_{s}_release]")
            if conn.exec_driver_sql("SELECT count(*) FROM sys.partition_functions WHERE name = ?",
                                    (f"pf_{s}_release",)).scalar():
                conn.exec_driver_sql(f"DROP PARTITION FUNCTION [pf_{s}_release]")
            if conn.exec_driver_sql("SELECT count(*) FROM sys.schemas WHERE name = ?", (s,)).scalar():
                conn.exec_driver_sql(f"DROP SCHEMA [{s}]")
```

Append to `tests/conftest.py`:

```python
import json as _json


@pytest.fixture(scope="session")
def mssql_doc() -> dict:
    from mssql_helpers import MSSQL_ENV
    raw = os.environ.get(MSSQL_ENV)
    if not raw:
        pytest.skip(f"{MSSQL_ENV} is not set")
    return _json.loads(raw)


@pytest.fixture(scope="session")
def mssql_engine(mssql_doc):
    from mssql_helpers import sql_connection_object
    engine = sql_connection_object("test", mssql_doc).engine
    yield engine
    engine.dispose()


@pytest.fixture
def mssql_schemas(mssql_engine):
    """Factory: mssql_schemas("raw", "") -> ["t<hex>_raw", "t<hex>"]; all dropped after the test."""
    from mssql_helpers import drop_schemas
    created: list[str] = []

    def factory(*suffixes: str) -> list[str]:
        base = f"t{uuid.uuid4().hex[:10]}"
        names = [f"{base}_{s}" if s else base for s in suffixes]
        with mssql_engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            for name in names:
                conn.exec_driver_sql(f"CREATE SCHEMA [{name}]")
        created.extend(names)
        return names

    yield factory
    drop_schemas(mssql_engine, created)
```

- [ ] **Step 5: Run the tests**

Run: `.\.venv\Scripts\python -m pytest tests/test_mssql_fixture.py -v` (with `NPD_TEST_MSSQL_DB` set as in Step 3)
Expected: 2 passed. Then `.\.venv\Scripts\python -m pytest -q` → all non-database tests pass, Postgres tests skipped.

- [ ] **Step 6: Commit**

```bash
git add pyproject.toml tests/mssql_helpers.py tests/conftest.py tests/test_mssql_fixture.py
git commit -m "build: add Python-DataEngine; SQL Server test fixtures on NPD_TEST_MSSQL_DB"
```

---

### Task 2: Raw-load throughput spike (throwaway, decision recorded)

**Files:**
- Create (scratch, not committed): `<scratchpad>/spike_raw_load.py`
- Create: `docs/profile/2026-10-05-mssql-raw-load-spike.md`

**Interfaces:**
- Produces: a recorded decision, `fast_executemany` or `bcp`, that Task 8 follows.

- [ ] **Step 1: Write the spike script** in the session scratchpad (not the repo):

```python
"""Spike: rows/s for loading NDJSON lines into a varchar(max) UTF-8 column on SQL Server.
Usage: python spike_raw_load.py <rows> [<path to a real .ndjson>]"""
import contextlib, io, json, os, subprocess, sys, tempfile, time, uuid
import pyodbc

sys.path.insert(0, "tests")
from fixture_data import ORG1  # noqa: E402

doc = json.loads(os.environ["NPD_TEST_MSSQL_DB"])
with contextlib.redirect_stdout(io.StringIO()):
    import DataEngine
obj = DataEngine.SqlConnectionObject(name="spike", server=doc["server"], database=doc["database"], trusted="yes")
N = int(sys.argv[1])
real = sys.argv[2] if len(sys.argv) > 2 else None


def lines():
    if real:
        with open(real, "rb") as f:
            for i, raw in enumerate(f, 1):
                if i > N:
                    return
                yield raw.rstrip(b"\r\n").decode()
        return
    for i in range(N):
        yield json.dumps({**ORG1, "id": f"Organization-{i}"})


table = f"dbo.spike_{uuid.uuid4().hex[:8]}"
ddl = (f"CREATE TABLE {table} (release_date date NOT NULL, resource_type varchar(40) NOT NULL, "
       f"resource_id varchar(128) NOT NULL, line_number bigint NOT NULL, "
       f"resource varchar(max) COLLATE Latin1_General_100_CI_AS_SC_UTF8 NOT NULL) WITH (DATA_COMPRESSION = PAGE)")


def run(label, setinputsizes):
    raw = obj.engine.raw_connection()
    cur = raw.cursor()
    cur.execute(ddl); raw.commit()
    cur.fast_executemany = True
    if setinputsizes:
        cur.setinputsizes([None, None, None, None, (pyodbc.SQL_WVARCHAR, 0, 0)])
    sql = f"INSERT INTO {table} VALUES (?, ?, ?, ?, ?)"
    start, batch, n = time.time(), [], 0
    for n, text in enumerate(lines(), 1):
        batch.append(("2026-09-29", "Organization", f"id-{n}", n, text))
        if len(batch) == 5000:
            cur.executemany(sql, batch); raw.commit(); batch = []
    if batch:
        cur.executemany(sql, batch); raw.commit()
    secs = time.time() - start
    print(f"{label}: {n} rows in {secs:.1f}s = {n / secs:,.0f} rows/s")
    cur.execute(f"DROP TABLE {table}"); raw.commit(); raw.close()


def run_bcp():
    raw = obj.engine.raw_connection(); cur = raw.cursor(); cur.execute(ddl); raw.commit()
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", suffix=".tsv", delete=False) as f:
        for n, text in enumerate(lines(), 1):
            f.write(f"2026-09-29\tOrganization\tid-{n}\t{n}\t{text}\n")
    start = time.time()
    subprocess.run(["bcp", table, "in", f.name, "-S", doc["server"], "-d", doc["database"], "-T", "-c",
                    "-C", "65001", "-t", "\\t", "-b", "5000"], check=True, capture_output=True)
    secs = time.time() - start
    print(f"bcp: {n} rows in {secs:.1f}s = {n / secs:,.0f} rows/s (file write excluded)")
    cur.execute(f"DROP TABLE {table}"); raw.commit(); raw.close(); os.unlink(f.name)


run("fast_executemany", setinputsizes=False)
run("fast_executemany+setinputsizes", setinputsizes=True)
run_bcp()
```

- [ ] **Step 2: Run it with 200,000 rows** (synthetic, or pass a real Organization `.ndjson` if one is on disk)

Run: `.\.venv\Scripts\python <scratchpad>\spike_raw_load.py 200000`
Expected: three `rows/s` lines. If a variant raises (for example `String data, right truncation`), record the error.

- [ ] **Step 3: Record the decision** in `docs/profile/2026-10-05-mssql-raw-load-spike.md`: environment (host, server, driver), the three measurements, and the rule applied: **use the fastest `fast_executemany` variant if it reaches at least 5,000 rows/s** (the Postgres full run averaged about 2,300 raw rows/s); otherwise use `bcp`. Write the chosen variant on its own line: `Decision: fast_executemany` or `Decision: fast_executemany+setinputsizes` or `Decision: bcp`.

- [ ] **Step 4: Commit**

```bash
git add docs/profile/2026-10-05-mssql-raw-load-spike.md
git commit -m "docs: SQL Server raw-load throughput spike and loader decision"
```

---

### Task 3: `sqltext.py` — tokens, batches, names, scripts

**Files:**
- Create: `src/npd_loader/sqltext.py`
- Test: `tests/test_sqltext.py`

**Interfaces:**
- Produces:
  - `render(text: str, tokens: Mapping[str, str]) -> str` — replaces `<<key>>` / `<<key:sub>>`; unknown key → `KeyError("unknown SQL token <<key>>")`.
  - `split_batches(text: str) -> list[str]` — split on lines that are exactly `GO` (case-insensitive, surrounding whitespace allowed); drop empty batches.
  - `standalone_name(table: str, release: date, run_id: int, max_len: int) -> str` — `f"{table}__{release:%Y%m%d}__r{run_id}"`, `ValueError` if longer than `max_len`.
  - `sql_scripts(flavor: str, kind: str) -> list[tuple[str, str]]` — `(file name, text)` of `npd_loader/sql/<flavor>/<kind>/*.sql` in name order.

- [ ] **Step 1: Write the failing tests**

`tests/test_sqltext.py`:

```python
from datetime import date

import pytest

from npd_loader.sqltext import render, split_batches, sql_scripts, standalone_name


def test_render_substitutes_and_rejects_unknown():
    assert render("SELECT * FROM <<t:practitioner>> WHERE d = <<release>>",
                  {"t:practitioner": "[npd].[p]", "release": "'2026-09-29'"}) == \
        "SELECT * FROM [npd].[p] WHERE d = '2026-09-29'"
    with pytest.raises(KeyError, match="<<nope>>"):
        render("<<nope>>", {})


def test_split_batches_on_go_lines_only():
    text = "CREATE TABLE a (x int)\nGO\n  go  \nSELECT 'GO' AS x\r\nGO\r\n"
    assert split_batches(text) == ["CREATE TABLE a (x int)", "SELECT 'GO' AS x"]


def test_standalone_name_length_limit():
    assert standalone_name("practitioner", date(2026, 9, 29), 42, 128) == "practitioner__20260929__r42"
    with pytest.raises(ValueError, match="longer than 20"):
        standalone_name("practitioner", date(2026, 9, 29), 42, 20)


def test_sql_scripts_lists_in_name_order():
    names = [n for n, _ in sql_scripts("postgres", "transform")]
    assert names[0] == "010_practitioner.sql" and names == sorted(names)
```

- [ ] **Step 2: Run to verify failure**

Run: `.\.venv\Scripts\python -m pytest tests/test_sqltext.py -v`
Expected: FAIL, `ModuleNotFoundError: No module named 'npd_loader.sqltext'`.

- [ ] **Step 3: Move the Postgres SQL** (unchanged content) and implement

```powershell
New-Item -ItemType Directory -Force src/npd_loader/sql/postgres | Out-Null
git mv src/npd_loader/sql/init src/npd_loader/sql/postgres/init
git mv src/npd_loader/sql/transform src/npd_loader/sql/postgres/transform
```

In `src/npd_loader/schema.py` change the folder to `resources.files("npd_loader") / "sql" / "postgres" / "init"`; in
`src/npd_loader/transform.py` change `_scripts()` to `resources.files("npd_loader") / "sql" / "postgres" / "transform"`.
In `pyproject.toml` package data, remove the now-unused `"sql/init/*.sql", "sql/transform/*.sql"` entries.

`src/npd_loader/sqltext.py`:

```python
"""Engine-neutral SQL text helpers: token rendering, GO batches, standalone table names, packaged scripts."""
from __future__ import annotations

import re
from datetime import date
from importlib import resources
from typing import Mapping

TOKEN_RE = re.compile(r"<<([a-z_]+(?::[a-z_]+)?)>>")
GO_RE = re.compile(r"^[ \t]*GO[ \t]*\r?$", re.IGNORECASE | re.MULTILINE)


def render(text: str, tokens: Mapping[str, str]) -> str:
    def substitute(match: re.Match) -> str:
        key = match.group(1)
        if key not in tokens:
            raise KeyError(f"unknown SQL token <<{key}>>")
        return tokens[key]
    return TOKEN_RE.sub(substitute, text)


def split_batches(text: str) -> list[str]:
    return [b.strip() for b in GO_RE.split(text) if b.strip()]


def standalone_name(table: str, release: date, run_id: int, max_len: int) -> str:
    name = f"{table}__{release:%Y%m%d}__r{run_id}"
    if len(name) > max_len:
        raise ValueError(f"table name {name!r} is longer than {max_len} characters")
    return name


def sql_scripts(flavor: str, kind: str) -> list[tuple[str, str]]:
    folder = resources.files("npd_loader") / "sql" / flavor / kind
    return [(p.name, p.read_text(encoding="utf-8")) for p in sorted(folder.iterdir(), key=lambda p: p.name)
            if p.name.endswith(".sql")]
```

- [ ] **Step 4: Run the tests**

Run: `.\.venv\Scripts\python -m pytest tests/test_sqltext.py tests/test_mapped_paths.py -v` then `.\.venv\Scripts\python -m pytest -q`
Expected: new tests pass; the full suite has no new failures.

- [ ] **Step 5: Commit**

```bash
git add -A src/npd_loader/sql src/npd_loader/sqltext.py src/npd_loader/schema.py src/npd_loader/transform.py pyproject.toml tests/test_sqltext.py
git commit -m "refactor: move Postgres SQL under sql/postgres; add engine-neutral sqltext helpers"
```

---

### Task 4: Connections from `database.env` and the new config shape

**Files:**
- Create: `src/npd_loader/connections.py`, `database.dev.env`, `database.prd.env`
- Modify: `src/npd_loader/config.py`, `config.example.toml`, `tests/helpers.py`, `tests/test_config.py`
- Delete: `src/npd_loader/credentials.py`
- Test: `tests/test_connections.py`, `tests/test_config.py`

**Interfaces:**
- Produces:
  - `config.DatabasesConfig(env_file: str)`; `NpdDbConfig(connection: str, raw_schema: str, schema: str, lock_timeout_seconds: float = 30.0)`; `CatalogConfig(connection: str, project: str, run_type: str, file_set: str, run_table=..., file_table=..., ...existing label defaults)`; `Config(source, storage, databases, npd_db, catalog, download, retention)`.
  - `connections.read_env_file(path: str) -> dict[str, dict]`
  - `connections.build_connection(name: str, doc: dict) -> object` (a `SqlConnectionObject` or `PgConnectionObject`)
  - `connections.open_connection(config: Config, target: str) -> object` (`target` is `"npd_db"` or `"catalog"`)
  - `connections.pg_conninfo(obj) -> str` (psycopg URI from a `PgConnectionObject`, used by the Postgres dialect until Task 14)
  - `tests/helpers.write_env_file(path, docs: dict) -> str` and `config_data(storage_root, manifest_url, env_file=None, schemas=("npd_raw", "npd"), catalog_tables=None, keep_releases=5)`.

- [ ] **Step 1: Write the failing tests**

`tests/test_connections.py`:

```python
import pytest

from npd_loader.config import ConfigError
from npd_loader.connections import build_connection, pg_conninfo, read_env_file
from helpers import write_env_file

DOCS = {"data": {"type": "mssql", "server": "cssnpi", "database": "npd_dev", "trusted": "yes"},
        "pg": {"type": "postgres", "server": "db.example:5433", "database": "npd", "UN": "u", "PW": "p@ss"}}


def test_read_env_file(tmp_path):
    assert read_env_file(write_env_file(tmp_path / "database.env", DOCS)) == DOCS


def test_read_env_file_errors(tmp_path):
    with pytest.raises(ConfigError, match="not found"):
        read_env_file(str(tmp_path / "missing.env"))
    bad = tmp_path / "bad.env"
    bad.write_text("other = 'x'\n")
    with pytest.raises(ConfigError, match="databases"):
        read_env_file(str(bad))


def test_build_connection_types_and_no_stdout(capsys):
    mssql = build_connection("data", DOCS["data"])
    assert type(mssql).__name__ == "SqlConnectionObject"
    assert mssql.engine.dialect.name == "mssql"
    pg = build_connection("pg", DOCS["pg"])
    assert type(pg).__name__ == "PgConnectionObject"
    assert pg_conninfo(pg) == "postgresql://u:p%40ss@db.example:5433/npd"
    assert capsys.readouterr().out == ""
    with pytest.raises(ConfigError, match="type"):
        build_connection("m", {"type": "mongo", "server": "x", "database": "y"})
```

Replace `tests/test_config.py` `minimal()` and the credentials tests:

```python
def minimal() -> dict:
    return {
        "source": {"manifest_url": "https://example.test/downloads/manifest.json"},
        "storage": {"backend": "local", "root": "/data/npd"},
        "databases": {"env_file": "database.env"},
        "npd_db": {"connection": "data"},
        "catalog": {"connection": "catalog", "project": "NPD", "run_type": "National Provider Directory",
                    "file_set": "NPD_FHIR"},
    }
```

Delete `from npd_loader.credentials import ...`, `test_credentials_from_config` and `test_credentials_missing_password`;
change the parametrize of `test_unbuilt_backends_are_rejected` to `[("storage", "s3")]`. Add:

```python
def test_connections_are_named():
    cfg = parse_config(minimal())
    assert cfg.databases.env_file == "database.env"
    assert (cfg.npd_db.connection, cfg.catalog.connection) == ("data", "catalog")


@pytest.mark.parametrize("section,key", [("npd_db", "host"), ("npd_db", "password"), ("catalog", "user"),
                                         ("catalog", "backend")])
def test_legacy_connection_keys_are_rejected(section, key):
    data = minimal()
    data[section][key] = "x"
    with pytest.raises(ConfigError, match="database.env"):
        parse_config(data)
```

In `test_example_config_loads` add `assert cfg.catalog.run_table == "dbo.MASTER_WAREHOUSE_RUN"`.

- [ ] **Step 2: Run to verify failure**

Run: `.\.venv\Scripts\python -m pytest tests/test_connections.py tests/test_config.py -v`
Expected: FAIL (`ModuleNotFoundError: npd_loader.connections`, `write_env_file` import error).

- [ ] **Step 3: Implement**

`src/npd_loader/connections.py`:

```python
"""DataEngine connection objects, built from the env file named in [databases] env_file."""
from __future__ import annotations

import contextlib
import io
import json
from pathlib import Path

from dotenv import dotenv_values

from npd_loader.config import Config, ConfigError

TYPES = {"mssql", "postgres"}


def _dataengine():
    """Import DataEngine without its import-time stdout output (it prints, and auto-loads ./database.env)."""
    with contextlib.redirect_stdout(io.StringIO()):
        import DataEngine
    return DataEngine


def read_env_file(path: str) -> dict[str, dict]:
    if not Path(path).is_file():
        raise ConfigError(f"database env file not found: {path}")
    raw = dotenv_values(path).get("databases")
    if not raw:
        raise ConfigError(f"{path} has no 'databases' entry")
    try:
        docs = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ConfigError(f"{path}: 'databases' is not valid JSON: {exc}") from exc
    if not isinstance(docs, dict):
        raise ConfigError(f"{path}: 'databases' must be a JSON object")
    return docs


def build_connection(name: str, doc: dict) -> object:
    kind = doc.get("type")
    if kind not in TYPES:
        raise ConfigError(f"connection {name!r} has type {kind!r}; expected one of {sorted(TYPES)}")
    for key in ("server", "database"):
        if not doc.get(key):
            raise ConfigError(f"connection {name!r} is missing {key!r}")
    de = _dataengine()
    common = {"name": name, "server": doc["server"], "database": doc["database"],
              "UN": doc.get("UN", ""), "PW": doc.get("PW", "")}
    if kind == "mssql":
        return de.SqlConnectionObject(**common, trusted=doc.get("trusted", "no"))
    return de.PgConnectionObject(**common)


def open_connection(config: Config, target: str) -> object:
    name = getattr(config, target).connection
    docs = read_env_file(config.databases.env_file)
    if name not in docs:
        raise ConfigError(f"[{target}] connection {name!r} is not in {config.databases.env_file}")
    return build_connection(name, docs[name])


def pg_conninfo(obj: object) -> str:
    """psycopg URI for a PgConnectionObject (used by the Postgres dialect until it moves to SQLAlchemy)."""
    return obj.engine.url.set(drivername="postgresql").render_as_string(hide_password=False)
```

`src/npd_loader/config.py` changes (replace the dataclasses and `parse_config`; keep everything else):

```python
@dataclass(frozen=True)
class DatabasesConfig:
    env_file: str


@dataclass(frozen=True)
class NpdDbConfig:
    connection: str
    raw_schema: str
    schema: str
    lock_timeout_seconds: float = 30.0   # how long publish/retention DDL waits for a parent table lock


@dataclass(frozen=True)
class CatalogConfig:
    connection: str
    project: str
    run_type: str
    file_set: str
    run_table: str = "public.master_warehouse_run"
    file_table: str = "public.data_file"
    # ... keep every existing label field and default from run_class_download to file_type_ndjson unchanged
```

Delete `CredentialsConfig`, `CREDENTIAL_BACKENDS`, `CATALOG_BACKENDS`. `Config` becomes
`source, storage, databases, npd_db, catalog, download, retention`. In `parse_config`:

```python
LEGACY_KEYS = {"host", "port", "dbname", "user", "password", "backend"}


def _no_legacy(section: dict[str, Any], name: str) -> None:
    found = sorted(LEGACY_KEYS & set(section))
    if found:
        raise ConfigError(f"[{name}] {', '.join(found)}: connection settings moved to the database.env file "
                          f"named by [databases] env_file; set [{name}] connection instead")


def parse_config(data: dict[str, Any]) -> Config:
    src = _section(data, "source")
    sto = _section(data, "storage")
    dbs = _section(data, "databases")
    npd = _section(data, "npd_db")
    cat = _section(data, "catalog")
    dl = _section(data, "download", required=False)
    ret = _section(data, "retention", required=False)
    _no_legacy(npd, "npd_db")
    _no_legacy(cat, "catalog")

    catalog_known = {"connection", "project", "run_type", "file_set"}
    catalog_extra = _optional({k: v for k, v in cat.items() if k not in catalog_known}, "catalog", CatalogConfig)

    config = Config(
        source=SourceConfig(manifest_url=_req(src, "source", "manifest_url")),
        storage=StorageConfig(backend=_backend(sto, "storage", STORAGE_BACKENDS), root=_req(sto, "storage", "root")),
        databases=DatabasesConfig(env_file=_req(dbs, "databases", "env_file")),
        npd_db=NpdDbConfig(
            connection=_req(npd, "npd_db", "connection"),
            raw_schema=npd.get("raw_schema", "npd_raw"),
            schema=npd.get("schema", "npd"),
            **_optional({k: v for k, v in npd.items() if k == "lock_timeout_seconds"}, "npd_db", NpdDbConfig),
        ),
        catalog=CatalogConfig(
            connection=_req(cat, "catalog", "connection"),
            project=_req(cat, "catalog", "project"),
            run_type=_req(cat, "catalog", "run_type"),
            file_set=_req(cat, "catalog", "file_set"),
            **catalog_extra,
        ),
        download=DownloadConfig(**_optional(dl, "download", DownloadConfig)),
        retention=RetentionConfig(**_optional(ret, "retention", RetentionConfig)),
    )
    # ... keep the three existing range checks unchanged
    return config
```

`config.example.toml` — replace `[credentials]`, `[npd_db]`, `[catalog]` and the header line:

```toml
# Copy to C:\npd-loader\config.toml. Connections live in the DataEngine env file named below (Windows auth, no
# passwords). Use database.dev.env for development and database.prd.env for production.

[databases]
env_file = 'C:\npd-loader\database.prd.env'

[npd_db]
connection = "data"           # its type (mssql|postgres) selects the flavor
raw_schema = "npd_raw"
schema = "npd"
lock_timeout_seconds = 30     # publish/retention give up waiting for a table lock after this (3 attempts)

[catalog]
connection = "catalog"
run_table = "dbo.MASTER_WAREHOUSE_RUN"
file_table = "dbo.DATA_FILE"
project = "NPD"
run_type = "National Provider Directory"
file_set = "NPD_FHIR"
```

Also set `[storage] root = 'D:\npd'` in the example. Update `test_example_config_loads` to `assert cfg.storage.root == "D:\\npd"`.

`database.dev.env`:

```
databases = '{"data": {"type": "mssql", "server": "cssnpi", "database": "npd_dev", "trusted": "yes"}, "catalog": {"type": "mssql", "server": "cssnpi", "database": "HIE_WAREHOUSE_META_DEV", "trusted": "yes"}}'
```

`database.prd.env`:

```
databases = '{"data": {"type": "mssql", "server": "cssnpi", "database": "npd", "trusted": "yes"}, "catalog": {"type": "mssql", "server": "cssnpi", "database": "HIE_WAREHOUSE_META", "trusted": "yes"}}'
```

`.gitignore` ignores `.env` only, so these two files are committable; add `database.env` (the conventional local name) to `.gitignore`.

`tests/helpers.py` — replace `_db` and `config_data`, add `write_env_file`:

```python
import json


def write_env_file(path, docs: dict) -> str:
    path.write_text(f"databases = '{json.dumps(docs)}'\n", encoding="utf-8")
    return str(path)


def config_data(storage_root: Path, manifest_url: str, env_file: str | None = None,
                schemas: tuple[str, str] = ("npd_raw", "npd"), catalog_tables: tuple[str, str] | None = None,
                keep_releases: int = 5) -> dict:
    catalog = {"connection": "catalog", "project": "NPD", "run_type": "National Provider Directory",
               "file_set": "NPD_FHIR"}
    if catalog_tables:
        catalog["run_table"], catalog["file_table"] = catalog_tables
    return {
        "source": {"manifest_url": manifest_url},
        "storage": {"backend": "local", "root": str(storage_root)},
        "databases": {"env_file": env_file or "unused.env"},
        "npd_db": {"connection": "data", "raw_schema": schemas[0], "schema": schemas[1]},
        "catalog": catalog,
        "download": {"max_attempts": 3, "backoff_seconds": 0, "timeout_seconds": 10},
        "retention": {"keep_releases": keep_releases},
    }
```

Remove the `psycopg.conninfo` import from `tests/helpers.py`. `make_ctx` keeps working with the new `config_data`
(its `npd_conninfo` parameter is replaced in Task 6). Delete `src/npd_loader/credentials.py`.

In `src/npd_loader/cli.py` (temporary until Task 6) replace the `conninfo` import and uses:

```python
from npd_loader.connections import open_connection, pg_conninfo
...
def build_context(config: Config) -> Context:
    npd = pg_conninfo(open_connection(config, "npd_db"))
    return Context(
        config=config,
        catalog=CssCatalogPg(pg_conninfo(open_connection(config, "catalog")), config.catalog),
        ...  # rest unchanged
```

and in `main`: `init_db(pg_conninfo(open_connection(config, "npd_db")), ...)`.

- [ ] **Step 4: Run the tests**

Run: `.\.venv\Scripts\python -m pytest tests/test_connections.py tests/test_config.py tests/test_cli.py -v` then `.\.venv\Scripts\python -m pytest -q`
Expected: pass; Postgres-backed tests still skip.

- [ ] **Step 5: Commit**

```bash
git add -A src/npd_loader/connections.py src/npd_loader/config.py src/npd_loader/credentials.py src/npd_loader/cli.py config.example.toml database.dev.env database.prd.env .gitignore tests/helpers.py tests/test_config.py tests/test_connections.py
git commit -m "feat: connections from a DataEngine database.env; config names connections"
```

---

### Task 5: `SqlCatalog` on SQLAlchemy Core (both engines)

**Files:**
- Modify: `src/npd_loader/catalog.py`, `src/npd_loader/cli.py`, `tests/test_catalog_pg.py`, `tests/test_e2e.py` (import only)
- Create: `tests/sql/hie_catalog_schema.sql`
- Test: `tests/test_catalog_mssql.py`

**Interfaces:**
- Consumes: `CatalogConfig` (Task 4), `build_connection` (Task 4), fixtures `mssql_engine`, `mssql_schemas` (Task 1).
- Produces: `catalog.SqlCatalog(engine: sqlalchemy.Engine, cfg: CatalogConfig)` implementing the existing `Catalog` protocol. `CssCatalogPg` is removed.

- [ ] **Step 1: Scratch DDL** `tests/sql/hie_catalog_schema.sql` (from `INFORMATION_SCHEMA` on `HIE_WAREHOUSE_META`, 2026-10-05):

```sql
CREATE TABLE <<schema>>.MASTER_WAREHOUSE_RUN (
    ID int IDENTITY(1,1) NOT NULL CONSTRAINT PK_<<name>>_MWR PRIMARY KEY,
    PARENT_RUN_ID int NULL,
    RUN_TYPE nvarchar(200) NOT NULL,
    RUN_ID uniqueidentifier NOT NULL DEFAULT (newid()),
    COMPLETION_STATUS nvarchar(2000) NULL,
    DATE_COMPLETED datetime NULL,
    DATE_STARTED datetime NULL DEFAULT (getdate()),
    NOTES nvarchar(4000) NULL,
    RUN_DESCRIPTION nvarchar(200) NULL,
    RESULT nvarchar(2000) NULL,
    [USER] nvarchar(50) NOT NULL DEFAULT (suser_sname()),
    XML_CONFIG xml NULL,
    XML_OUTPUT xml NULL,
    PROJECT varchar(1000) NULL,
    RUN_CLASS varchar(100) NULL
)
GO
CREATE TABLE <<schema>>.DATA_FILE (
    ID int IDENTITY(1,1) NOT NULL CONSTRAINT PK_<<name>>_DF PRIMARY KEY,
    RUN_ID int NOT NULL,
    FILE_SET varchar(500) NULL,
    FILE_TYPE varchar(500) NULL,
    SOURCE_URI varchar(2000) NULL,
    SOURCE_VERSION_NAME varchar(500) NULL,
    SOURCE_VERSION_NUM varchar(500) NULL,
    FILE_NAME varchar(2000) NULL,
    FILE_REL_PATH varchar(2000) NULL,
    RUN_TYPE_ROOT_DIR varchar(2000) NULL,
    PARENT_FILE int NULL,
    FILE_SIZE bigint NULL,
    FILE_HASH varchar(1000) NULL,
    DATE_LOADED datetime NULL,
    DATE_MODIFIED datetime NULL,
    DATE_CREATED datetime NULL,
    EXCEPTIONS varchar(8000) NULL
)
```

- [ ] **Step 2: Write the failing tests**

`tests/test_catalog_mssql.py`:

```python
from datetime import date

import pytest

from catalog_contract import CatalogContract, cfg
from npd_loader.catalog import FAILED, SUCCESS, SqlCatalog
from npd_loader.config import CatalogConfig
from npd_loader.sqltext import render, split_batches
from tests_paths import TESTS


def make_catalog_schema(engine, schema: str) -> CatalogConfig:
    text = render((TESTS / "sql" / "hie_catalog_schema.sql").read_text(),
                  {"schema": f"[{schema}]", "name": schema})
    with engine.begin() as conn:
        for batch in split_batches(text):
            conn.exec_driver_sql(batch)
    return CatalogConfig(connection="catalog", project="NPD", run_type="National Provider Directory",
                         file_set="NPD_FHIR", run_table=f"{schema}.MASTER_WAREHOUSE_RUN",
                         file_table=f"{schema}.DATA_FILE")


@pytest.fixture
def catalog_cfg(mssql_engine, mssql_schemas):
    (schema,) = mssql_schemas("cat")
    return make_catalog_schema(mssql_engine, schema)


class TestSqlCatalogMssql(CatalogContract):
    @pytest.fixture
    def catalog(self, mssql_engine, catalog_cfg):
        return SqlCatalog(mssql_engine, catalog_cfg)


def test_writes_hie_columns(mssql_engine, catalog_cfg):
    catalog = SqlCatalog(mssql_engine, catalog_cfg)
    run = catalog.start_run("DOWNLOAD", "NPD FHIR Download 2026-09-29", cfg(date(2026, 9, 29)))
    fid = catalog.add_data_file(run, file_type="manifest", source_version_num="2026-09-29",
                                file_name="x" * 1500)
    catalog.finish_run(run, FAILED, result="y" * 9000)
    schema = catalog_cfg.run_table.split(".")[0]
    with mssql_engine.connect() as conn:
        row = conn.exec_driver_sql(
            f"SELECT PROJECT, RUN_TYPE, RUN_CLASS, COMPLETION_STATUS, LEN(RESULT), [USER], RUN_ID "
            f"FROM [{schema}].MASTER_WAREHOUSE_RUN WHERE ID = ?", (run.id,)).one()
        assert row[:5] == ("NPD", "National Provider Directory", "DOWNLOAD", "Failed", 2000)
        assert row[5] and row[6]                      # server defaults still fill USER and the GUID RUN_ID
        assert conn.exec_driver_sql(f"SELECT LEN(FILE_NAME), FILE_SET FROM [{schema}].DATA_FILE WHERE ID = ?",
                                    (fid,)).one() == (1500, "NPD_FHIR")
```

Create `tests/tests_paths.py` with `from pathlib import Path\nTESTS = Path(__file__).resolve().parent\n`.

- [ ] **Step 3: Run to verify failure**

Run: `.\.venv\Scripts\python -m pytest tests/test_catalog_mssql.py -v`
Expected: FAIL, `ImportError: cannot import name 'SqlCatalog'`.

- [ ] **Step 4: Implement `SqlCatalog`**

In `src/npd_loader/catalog.py` remove the psycopg imports, `_qualified` and `CssCatalogPg`; add:

```python
from sqlalchemy import BigInteger, Column, DateTime, Integer, MetaData, String, Table, Text, and_, insert, select, update
from sqlalchemy.engine import Engine


def _table(md: MetaData, qualified: str, *columns: Column) -> Table:
    schema, _, name = qualified.rpartition(".")
    return Table(name, md, *columns, schema=schema or None)


class SqlCatalog:
    """Catalog in any SQLAlchemy engine (css_catalog_local on Postgres, HIE_WAREHOUSE_META on SQL Server). Each call
    runs in its own short transaction, so catalog writes commit independently of data loads and survive their
    failures. Column names are lowercase; SQL Server's case-insensitive collation matches the uppercase columns."""

    def __init__(self, engine: Engine, cfg: CatalogConfig):
        self._engine = engine
        self._cfg = cfg
        md = MetaData()
        self._runs = _table(md, cfg.run_table,
                            Column("id", Integer, primary_key=True), Column("project", String),
                            Column("run_type", String), Column("run_class", String),
                            Column("run_description", String), Column("xml_config", Text),
                            Column("xml_output", Text), Column("date_started", DateTime),
                            Column("date_completed", DateTime), Column("completion_status", String),
                            Column("result", String))
        self._files = _table(md, cfg.file_table,
                             Column("id", Integer, primary_key=True), Column("run_id", Integer),
                             Column("file_set", String), Column("source_version_name", String),
                             Column("file_type", String), Column("source_uri", String),
                             Column("source_version_num", String), Column("file_name", String),
                             Column("file_rel_path", String), Column("run_type_root_dir", String),
                             Column("parent_file", Integer), Column("file_size", BigInteger),
                             Column("file_hash", String), Column("date_modified", DateTime),
                             Column("date_created", DateTime), Column("date_loaded", DateTime),
                             Column("exceptions", String))

    def start_run(self, run_class: str, description: str, config_xml: str) -> Run:
        started = datetime.now().replace(microsecond=0)
        stmt = insert(self._runs).values(project=self._cfg.project, run_type=self._cfg.run_type,
                                         run_class=run_class, run_description=description,
                                         xml_config=config_xml, date_started=started).returning(self._runs.c.id)
        with self._engine.begin() as conn:
            run_id = conn.execute(stmt).scalar_one()
        return Run(run_id, run_class, description, config_xml, started, parse_release(config_xml))

    def finish_run(self, run: Run, status: str, result: str | None = None, output_xml: str | None = None) -> None:
        label = {SUCCESS: self._cfg.status_success, FAILED: self._cfg.status_failed}[status]
        stmt = update(self._runs).where(self._runs.c.id == run.id).values(
            completion_status=label, date_completed=datetime.now().replace(microsecond=0),
            result=truncate(result, MAX_RESULT), xml_output=output_xml)
        with self._engine.begin() as conn:
            conn.execute(stmt)

    def add_data_file(self, run: Run, **fields: object) -> int:
        fields = clean_fields(fields)
        stmt = insert(self._files).values(run_id=run.id, file_set=self._cfg.file_set,
                                          source_version_name=self._cfg.source_version_name,
                                          **fields).returning(self._files.c.id)
        with self._engine.begin() as conn:
            return conn.execute(stmt).scalar_one()

    def update_data_file(self, file_id: int, **fields: object) -> None:
        fields = clean_fields(fields)
        if not fields:
            return
        with self._engine.begin() as conn:
            conn.execute(update(self._files).where(self._files.c.id == file_id).values(**fields))

    def get_data_files(self, release: date, file_type: str, run_id: int | None = None) -> list[DataFile]:
        f = self._files.c
        cond = and_(f.file_set == self._cfg.file_set, f.source_version_num == release.isoformat(),
                    f.file_type == file_type)
        if run_id is not None:
            cond = and_(cond, f.run_id == run_id)
        cols = [f.id, f.run_id, *(f[name] for name in DATA_FILE_FIELDS)]
        with self._engine.connect() as conn:
            rows = conn.execute(select(*cols).where(cond).order_by(f.id)).mappings().all()
        return [DataFile(**row) for row in rows]

    def _successful_runs(self, run_class: str) -> list[Run]:
        r = self._runs.c
        stmt = select(r.id, r.run_class, r.run_description, r.xml_config, r.date_started).where(
            r.project == self._cfg.project, r.run_type == self._cfg.run_type, r.run_class == run_class,
            r.completion_status == self._cfg.status_success)
        with self._engine.connect() as conn:
            rows = conn.execute(stmt).all()
        return [Run(row[0], row[1], row[2] or "", row[3] or "", row[4], parse_release(row[3]), SUCCESS)
                for row in rows]

    def last_successful_run(self, run_class: str, release: date | None = None) -> Run | None:
        return newest_run(self._successful_runs(run_class), release)

    def successful_releases(self, run_class: str) -> list[date]:
        return releases_of(self._successful_runs(run_class))
```

Update the module docstring to `"""Run and file tracking. `Catalog` is the interface; SqlCatalog writes it through SQLAlchemy."""`.

`tests/test_catalog_pg.py`: replace `CssCatalogPg(catalog_db, catalog_config())` with
`SqlCatalog(pg_engine(catalog_db), catalog_config())`, where

```python
from sqlalchemy import create_engine


def pg_engine(conninfo: str):
    from psycopg.conninfo import conninfo_to_dict
    d = conninfo_to_dict(conninfo)
    return create_engine(f"postgresql+psycopg2://{d['user']}:{d['password']}@{d['host']}:{d['port']}/{d['dbname']}")
```

and `catalog_config()` returns `CatalogConfig(connection="catalog", project="NPD", run_type="National Provider Directory", file_set="NPD_FHIR")`.
In `tests/test_e2e.py` replace `CssCatalogPg(catalog_db, ...)` with `SqlCatalog(pg_engine(catalog_db), ...)` (import `pg_engine` from `test_catalog_pg`).

`src/npd_loader/cli.py` `build_context`: `catalog=SqlCatalog(open_connection(config, "catalog").engine, config.catalog)` and import `SqlCatalog` instead of `CssCatalogPg`.

- [ ] **Step 5: Run the tests**

Run: `.\.venv\Scripts\python -m pytest tests/test_catalog_mssql.py -v` then `.\.venv\Scripts\python -m pytest -q`
Expected: the contract (5+ tests) and `test_writes_hie_columns` pass on SQL Server; no new failures elsewhere.

- [ ] **Step 6: Commit**

```bash
git add src/npd_loader/catalog.py src/npd_loader/cli.py tests/sql/hie_catalog_schema.sql tests/test_catalog_mssql.py tests/tests_paths.py tests/test_catalog_pg.py tests/test_e2e.py
git commit -m "feat: SqlCatalog on SQLAlchemy Core for css_catalog_local and HIE_WAREHOUSE_META"
```

---

### Task 6: `Dialect` protocol; stages, retention and CLI go through it; Postgres dialect wraps today's code

**Files:**
- Create: `src/npd_loader/dialect/__init__.py`, `src/npd_loader/dialect/postgres.py`
- Modify: `src/npd_loader/stages.py`, `src/npd_loader/import_stage.py`, `src/npd_loader/retention.py`, `src/npd_loader/cli.py`, `src/npd_loader/publish.py`, `src/npd_loader/transform.py`, `tests/helpers.py`, `tests/test_import.py`, `tests/test_retention.py`
- Test: `tests/test_dialect_factory.py`

**Interfaces:**
- Consumes: `NpdDbConfig` (Task 4), `pg_conninfo` (Task 4).
- Produces (`npd_loader.dialect`):

```python
class LockUnavailable(Exception): ...          # a table lock was not granted within the timeout (all attempts)
class PublishConflict(Exception): ...          # release already published and force is False
@dataclass
class TransformResult:
    tables: dict[str, str]                     # parent -> standalone table name
    counts: dict[str, int]

class Dialect(Protocol):
    name: str                                  # "mssql" | "postgres"
    cfg: NpdDbConfig
    def init_db(self) -> None: ...
    def run_lock(self, stage: str) -> ContextManager[bool]: ...
    def published_releases(self) -> list[date]: ...        # rows of <schema>.release, ascending
    def partitioned_releases(self) -> set[date]: ...       # releases present as partitions in any table
    def is_published(self, release: date) -> bool: ...     # the raw parent holds `release`
    def load_raw(self, storage: Storage, release: date, run_id: int, inputs: list[NdjsonInput]) -> RawLoadResult: ...
    def run_transforms(self, raw_table: str, release: date, run_id: int) -> TransformResult: ...
    def publish(self, raw_table: str, tables: dict[str, str], release: date, run_id: int, force: bool) -> None: ...
    def drop_release(self, release: date) -> list[str]: ...   # raises LockUnavailable
    def drop_standalone_tables(self, run_id: int | None = None) -> list[str]: ...

def dialect_for(conn_obj: object, cfg: NpdDbConfig, sleep: Callable[[float], None] = time.sleep) -> Dialect
```

- `PostgresDialect(conninfo: str, cfg: NpdDbConfig, sleep=time.sleep)`; `stages.Context` loses `npd_conninfo` and gains `dialect: Dialect | None = None`.

- [ ] **Step 1: Write the failing test**

`tests/test_dialect_factory.py`:

```python
import pytest

from npd_loader.config import NpdDbConfig
from npd_loader.connections import build_connection
from npd_loader.dialect import dialect_for

CFG = NpdDbConfig(connection="data", raw_schema="npd_raw", schema="npd")


def test_postgres_dialect_selected():
    pg = build_connection("pg", {"type": "postgres", "server": "h:5432", "database": "npd", "UN": "u", "PW": "p"})
    d = dialect_for(pg, CFG)
    assert d.name == "postgres" and d.conninfo == "postgresql://u:p@h:5432/npd"


def test_unknown_engine_rejected():
    class Fake:
        class engine:
            class dialect:
                name = "sqlite"
    with pytest.raises(ValueError, match="sqlite"):
        dialect_for(Fake(), CFG)
```

- [ ] **Step 2: Run to verify failure**

Run: `.\.venv\Scripts\python -m pytest tests/test_dialect_factory.py -v`
Expected: FAIL, `ModuleNotFoundError: No module named 'npd_loader.dialect'`.

- [ ] **Step 3: Implement the protocol and the Postgres wrapper**

`src/npd_loader/dialect/__init__.py`:

```python
"""Engine-specific npd-database operations. The stages call only this protocol."""
from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import date
from typing import TYPE_CHECKING, Callable, ContextManager, Protocol

from npd_loader.config import NpdDbConfig

if TYPE_CHECKING:
    from npd_loader.raw_load import NdjsonInput, RawLoadResult
    from npd_loader.storage import Storage


class LockUnavailable(Exception):
    """A table lock was not granted within lock_timeout_seconds on every attempt."""


class PublishConflict(Exception):
    pass


@dataclass
class TransformResult:
    tables: dict[str, str]
    counts: dict[str, int]


class Dialect(Protocol):
    name: str
    cfg: NpdDbConfig

    def init_db(self) -> None: ...
    def run_lock(self, stage: str) -> ContextManager[bool]: ...
    def published_releases(self) -> list[date]: ...
    def partitioned_releases(self) -> set[date]: ...
    def is_published(self, release: date) -> bool: ...
    def load_raw(self, storage: "Storage", release: date, run_id: int,
                 inputs: "list[NdjsonInput]") -> "RawLoadResult": ...
    def run_transforms(self, raw_table: str, release: date, run_id: int) -> TransformResult: ...
    def publish(self, raw_table: str, tables: dict[str, str], release: date, run_id: int, force: bool) -> None: ...
    def drop_release(self, release: date) -> list[str]: ...
    def drop_standalone_tables(self, run_id: int | None = None) -> list[str]: ...


def dialect_for(conn_obj: object, cfg: NpdDbConfig, sleep: Callable[[float], None] = time.sleep) -> Dialect:
    name = conn_obj.engine.dialect.name
    if name == "mssql":
        from npd_loader.dialect.mssql import MssqlDialect
        return MssqlDialect(conn_obj.engine, cfg, sleep)
    if name == "postgresql":
        from npd_loader.connections import pg_conninfo
        from npd_loader.dialect.postgres import PostgresDialect
        return PostgresDialect(pg_conninfo(conn_obj), cfg, sleep)
    raise ValueError(f"unsupported database engine {name!r}")
```

Move `PublishConflict` out of `publish.py` (replace its class with `from npd_loader.dialect import PublishConflict`)
and `TransformResult` out of `transform.py` (replace with `from npd_loader.dialect import TransformResult`).

`src/npd_loader/dialect/postgres.py` (delegates to the existing psycopg modules; Task 14 ports it):

```python
"""Postgres flavor. Wraps the psycopg implementation until it moves onto PgConnectionObject's engine (Task 14)."""
from __future__ import annotations

import time
from contextlib import contextmanager
from datetime import date
from typing import Callable, Iterator

import psycopg

from npd_loader.config import NpdDbConfig
from npd_loader.db import advisory_lock, list_parent_tables, list_release_partitions
from npd_loader.db import published_releases as _published_releases
from npd_loader.dialect import LockUnavailable, TransformResult
from npd_loader.publish import drop_release as _drop_release
from npd_loader.publish import publish_release
from npd_loader.raw_load import RAW_PARENT, NdjsonInput, RawLoadResult, load_raw
from npd_loader.schema import init_db
from npd_loader.storage import Storage
from npd_loader.transform import run_transforms


class PostgresDialect:
    name = "postgres"

    def __init__(self, conninfo: str, cfg: NpdDbConfig, sleep: Callable[[float], None] = time.sleep):
        self.conninfo = conninfo
        self.cfg = cfg
        self._sleep = sleep
        self._conn: psycopg.Connection | None = None

    def init_db(self) -> None:
        init_db(self.conninfo, self.cfg.raw_schema, self.cfg.schema)

    @contextmanager
    def run_lock(self, stage: str) -> Iterator[bool]:
        with advisory_lock(self.conninfo, stage) as acquired:
            yield acquired

    def published_releases(self) -> list[date]:
        return _published_releases(self.conninfo, self.cfg.schema)

    def partitioned_releases(self) -> set[date]:
        found: set[date] = set()
        with psycopg.connect(self.conninfo) as conn:
            for schema in (self.cfg.raw_schema, self.cfg.schema):
                for parent in list_parent_tables(conn, schema):
                    found |= set(list_release_partitions(conn, schema, parent))
        return found

    def is_published(self, release: date) -> bool:
        with psycopg.connect(self.conninfo) as conn:
            return release in list_release_partitions(conn, self.cfg.raw_schema, RAW_PARENT)

    @contextmanager
    def session(self) -> Iterator[None]:
        """One connection shared by load_raw, run_transforms and publish of one import, as before."""
        with psycopg.connect(self.conninfo) as conn:
            self._conn = conn
            try:
                yield
            finally:
                self._conn = None

    def load_raw(self, storage: Storage, release: date, run_id: int, inputs: list[NdjsonInput]) -> RawLoadResult:
        return load_raw(self._conn, storage, self.cfg.raw_schema, release, run_id, inputs)

    def run_transforms(self, raw_table: str, release: date, run_id: int) -> TransformResult:
        return run_transforms(self._conn, self.cfg.raw_schema, raw_table, self.cfg.schema, release, run_id)

    def publish(self, raw_table: str, tables: dict[str, str], release: date, run_id: int, force: bool) -> None:
        publish_release(self._conn, self.cfg.raw_schema, raw_table, self.cfg.schema, tables, release, run_id,
                        force, self.cfg.lock_timeout_seconds, self._sleep)

    def drop_release(self, release: date) -> list[str]:
        with psycopg.connect(self.conninfo) as conn:
            try:
                return _drop_release(conn, self.cfg.raw_schema, self.cfg.schema, release,
                                     self.cfg.lock_timeout_seconds, self._sleep)
            except psycopg.errors.LockNotAvailable as exc:
                raise LockUnavailable(str(exc).strip()) from exc

    def drop_standalone_tables(self, run_id: int | None = None) -> list[str]:
        from npd_loader.import_stage import drop_standalone_tables
        return drop_standalone_tables(self.conninfo, [self.cfg.raw_schema, self.cfg.schema], run_id)
```

Add the `session()` method to the `Dialect` protocol as `def session(self) -> ContextManager[None]: ...`
(the SQL Server dialect implements it as a no-op).

`src/npd_loader/stages.py`: replace `npd_conninfo: str | None = None` with `dialect: "Dialect | None" = None`
(import under `TYPE_CHECKING`: `from npd_loader.dialect import Dialect`).

`src/npd_loader/import_stage.py`: remove the psycopg imports, `list_release_partitions`, `publish_release`,
`load_raw`, `run_transforms` imports; import `PublishConflict` from `npd_loader.dialect`. Keep
`drop_standalone_tables` (Postgres helper, unchanged body). Replace `_drop_orphans`, `_import`, and the cleanup call:

```python
def _drop_orphans(ctx: Context) -> None:
    """With the import lock held no import is running, so any standalone table left by an import that was
    killed (SIGTERM/SIGKILL/OOM) before its cleanup ran is an orphan."""
    try:
        dropped = ctx.dialect.drop_standalone_tables()
    except Exception:
        log.exception("could not drop orphaned standalone tables")
        return
    if dropped:
        log.warning("dropped %d orphaned standalone tables of earlier interrupted imports: %s",
                    len(dropped), ", ".join(dropped))


def _import(ctx: Context, run: Run, release: date, inputs: list[NdjsonInput], force: bool) -> list[dict]:
    db = ctx.config.npd_db
    d = ctx.dialect
    if not force and d.is_published(release):
        raise PublishConflict(f"release {release} is already published in {db.raw_schema}.{RAW_PARENT} "
                              f"but the catalog has no successful import; rerun with --force to replace it")
    with d.session():
        raw = d.load_raw(ctx.storage, release, run.id, inputs)
        transformed = d.run_transforms(raw.table, release, run.id)
        d.publish(raw.table, transformed.tables, release, run.id, force)
    return ([{"table": f"{db.raw_schema}.{RAW_PARENT}", "resource_type": t, "rows": n} for t, n in raw.rows.items()]
            + [{"table": f"{db.schema}.{t}", "rows": n} for t, n in transformed.counts.items()])
```

and in `run_import`'s `except` block: `ctx.dialect.drop_standalone_tables(run.id)` in place of
`drop_standalone_tables(ctx.npd_conninfo, [...], run.id)`.

`src/npd_loader/retention.py`: remove psycopg/db/publish imports; import `LockUnavailable` from
`npd_loader.dialect`. Replace the database part of `apply_retention`:

```python
def apply_retention(ctx: Context, just_imported: date) -> list[str]:
    cfg = ctx.config
    warnings: list[str] = []
    try:
        published = ctx.dialect.partitioned_releases()
        # Keep slots go to releases published in this database (plus the one just imported) only.
        newest = sorted(published | {just_imported}, reverse=True)[:cfg.retention.keep_releases]
        keep = set(newest) | {just_imported}
        # .ndjson only: releases extracted (or imported per the catalog) but not published here, older than
        # the oldest kept release, e.g. an extract whose import failed and was never retried.
        extracted = set(ctx.catalog.successful_releases(cfg.catalog.run_class_extract)) \
            | set(ctx.catalog.successful_releases(cfg.catalog.run_class_import))
        unpublished = {r for r in extracted - published - keep if r < min(newest)}
        locked_out = False
        for release in sorted((published - keep) | unpublished):
            try:
                if release in published:
                    if locked_out:  # the same parents are still locked; don't wait out the timeout again
                        warnings.append(f"retention of release {release}: skipped, a table lock was not "
                                        f"available (retried on the next run)")
                        continue
                    dropped = ctx.dialect.drop_release(release)
                    log.info("retention dropped %d partitions of release %s", len(dropped), release)
                _delete_ndjson(ctx, release)
            except LockUnavailable as exc:
                locked_out = True
                warnings.append(f"retention of release {release}: gave up waiting for a table lock "
                                f"({exc}); retried on the next run")
            except Exception as exc:
                warnings.append(f"retention of release {release}: {exc}")
    except Exception as exc:
        warnings.append(f"retention: {exc}")
    for warning in warnings:
        log.warning(warning)
    return warnings
```

`src/npd_loader/cli.py`:

```python
from npd_loader.connections import open_connection
from npd_loader.dialect import dialect_for
...
def build_context(config: Config) -> Context:
    dialect = dialect_for(open_connection(config, "npd_db"), config.npd_db)
    return Context(
        config=config,
        catalog=SqlCatalog(open_connection(config, "catalog").engine, config.catalog),
        storage=LocalStorage(config.storage.root),
        http=httpx.Client(timeout=config.download.timeout_seconds, headers={"User-Agent": "npd-loader/0.1"}),
        lock=dialect.run_lock,
        dialect=dialect,
    )
```

In `main`: `init-db` becomes `dialect_for(open_connection(config, "npd_db"), config.npd_db).init_db()`; `status` uses
`ctx.dialect.published_releases()`. Remove the `db`, `schema`, `pg_conninfo` imports. Change the parser description
to `"Load the CMS NPD FHIR release into SQL Server or Postgres"`.

`tests/helpers.py` `make_ctx`: replace the `npd_conninfo` parameter with `dialect=None` and pass `dialect=dialect`
to `Context`; `lock=no_lock` unchanged. Add:

```python
def pg_dialect(conninfo: str):
    from npd_loader.config import NpdDbConfig
    from npd_loader.dialect.postgres import PostgresDialect
    return PostgresDialect(conninfo, NpdDbConfig(connection="data", raw_schema="npd_raw", schema="npd"),
                           sleep=lambda s: None)
```

In `tests/test_import.py` and `tests/test_retention.py` replace `make_ctx(..., npd_conninfo=npd_db)` with
`make_ctx(..., dialect=pg_dialect(npd_db))` and `ctx.npd_conninfo` with `ctx.dialect.conninfo`; where a test
replaced the lock timeout with `dataclasses.replace(ctx.config.npd_db, ...)`, also rebuild the dialect with
`PostgresDialect(ctx.dialect.conninfo, new_cfg, sleep=lambda s: None)`.

- [ ] **Step 4: Run the tests**

Run: `.\.venv\Scripts\python -m pytest tests/test_dialect_factory.py tests/test_cli.py -v` then `.\.venv\Scripts\python -m pytest -q`
Expected: pass; Postgres-backed tests skip (they are re-verified when a Postgres server is available).

- [ ] **Step 5: Commit**

```bash
git add -A src/npd_loader tests
git commit -m "refactor: stages, retention and CLI talk to a Dialect; Postgres dialect wraps the psycopg code"
```

---

### Task 7: SQL Server `init-db`: schemas, partitioning, helper functions, tables, views

**Files:**
- Create: `src/npd_loader/dialect/mssql.py`, `src/npd_loader/sql/mssql/init/001_schemas.sql`, `002_raw.sql`, `003_tables.sql`, `900_migrations.sql`
- Create: `tests/test_mssql_schema.py`; add the `mssql_dialect` fixture to `tests/conftest.py`

**Interfaces:**
- Consumes: `sqltext.render/split_batches/sql_scripts` (Task 3), `Dialect` (Task 6), fixtures (Task 1).
- Produces: `MssqlDialect(engine, cfg, sleep)` with `init_db()`, `published_releases()`, `partitioned_releases()`, `is_published()`, `session()`; helpers on the class used by later tasks: `q(*parts) -> str` (bracket-quoted, dotted), `pf(schema) -> str`, `ps(schema) -> str`, `parent_tables(conn, schema) -> list[str]`, `_tokens() -> dict[str, str]`, `MAX_IDENTIFIER = 128`. Fixture `mssql_dialect` (an initialized `MssqlDialect` on fresh schemas).

- [ ] **Step 1: Write the failing tests**

Add to `tests/conftest.py`:

```python
@pytest.fixture
def mssql_dialect(mssql_engine, mssql_schemas):
    from npd_loader.config import NpdDbConfig
    from npd_loader.dialect.mssql import MssqlDialect
    raw, data = mssql_schemas("raw", "")
    d = MssqlDialect(mssql_engine, NpdDbConfig(connection="data", raw_schema=raw, schema=data,
                                               lock_timeout_seconds=2), sleep=lambda s: None)
    d.init_db()
    return d
```

`tests/test_mssql_schema.py`:

```python
from datetime import date

PARENTS = {"endpoint", "healthcare_service", "healthcare_service_location", "identifier", "insurance_plan",
           "insurance_plan_alias", "insurance_plan_network", "location", "location_telecom", "organization",
           "organization_address", "organization_affiliation", "organization_affiliation_network",
           "organization_endpoint", "organization_telecom", "practitioner", "practitioner_address",
           "practitioner_name", "practitioner_qualification", "practitioner_role", "practitioner_role_code",
           "practitioner_role_endpoint", "practitioner_role_location", "practitioner_role_specialty",
           "practitioner_role_telecom", "practitioner_telecom"}


def scalar(d, sql, *args):
    with d.engine.connect() as conn:
        return conn.exec_driver_sql(sql, args).scalar()


def test_init_db_creates_partitioned_parents_and_views(mssql_dialect):
    d = mssql_dialect
    with d.engine.connect() as conn:
        assert set(d.parent_tables(conn, d.cfg.schema)) == PARENTS
        assert d.parent_tables(conn, d.cfg.raw_schema) == ["resource"]
    for schema in (d.cfg.raw_schema, d.cfg.schema):
        assert scalar(d, "SELECT count(*) FROM sys.partition_functions WHERE name = ?", f"pf_{schema}_release") == 1
    assert scalar(d, "SELECT count(*) FROM sys.views WHERE schema_id = SCHEMA_ID(?)", d.cfg.schema) == len(PARENTS)
    assert scalar(d, f"SELECT count(*) FROM {d.q(d.cfg.schema, 'v_practitioner')}") == 0
    assert scalar(d, "SELECT collation_name FROM sys.columns WHERE object_id = OBJECT_ID(?) AND name = 'resource'",
                  f"{d.cfg.raw_schema}.resource") == "Latin1_General_100_CI_AS_SC_UTF8"
    assert scalar(d, "SELECT DISTINCT data_compression_desc FROM sys.partitions WHERE object_id = OBJECT_ID(?)",
                  f"{d.cfg.schema}.practitioner") == "PAGE"
    assert d.published_releases() == [] and d.partitioned_releases() == set()
    assert d.is_published(date(2026, 9, 29)) is False


def test_init_db_is_idempotent(mssql_dialect):
    mssql_dialect.init_db()
    mssql_dialect.init_db()


def test_helper_functions(mssql_dialect):
    d = mssql_dialect
    s = d.q(d.cfg.schema)
    res = '{"extension": [{"url": "a", "valueBoolean": true}], "identifier": [{"system": "x", "value": "1"}, {"system": "npi", "value": "2"}], "name": ["A", "B", "C"]}'
    assert scalar(d, f"SELECT {s}.ref_id('Organization/Organization-1')") == "Organization-1"
    assert scalar(d, f"SELECT {s}.ref_id(NULL)") is None
    assert scalar(d, f"SELECT JSON_VALUE(e.ext, '$.valueBoolean') FROM {s}.ext(?, 'a') e", res) == "true"
    assert scalar(d, f"SELECT v.value FROM {s}.identifier_value(?, '[\"npi\",\"y\"]') v", res) == "2"
    assert scalar(d, f"SELECT j.txt FROM {s}.join_text(JSON_QUERY(?, '$.name'), ' ', 0) j", res) == "A B C"
    assert scalar(d, f"SELECT j.txt FROM {s}.join_text(JSON_QUERY(?, '$.name'), ', ', 2) j", res) == "C"
    assert scalar(d, f"SELECT j.txt FROM {s}.join_text(NULL, ' ', 0) j") is None
    assert str(scalar(d, f"SELECT {s}.fhir_ts('2020')")) == "2020-01-01 00:00:00"
    assert str(scalar(d, f"SELECT {s}.fhir_ts('2020-05')")) == "2020-05-01 00:00:00"
    assert str(scalar(d, f"SELECT {s}.fhir_ts('2026-09-29T04:34:00.724328Z')")) == "2026-09-29 04:34:00.724000"
    assert str(scalar(d, f"SELECT {s}.fhir_ts('2026-09-29T01:00:00-05:00')")) == "2026-09-29 06:00:00"
```

Note: `MssqlDialect` exposes `self.engine` publicly (tests use `d.engine`).

- [ ] **Step 2: Run to verify failure**

Run: `.\.venv\Scripts\python -m pytest tests/test_mssql_schema.py -v`
Expected: ERROR, `ModuleNotFoundError: No module named 'npd_loader.dialect.mssql'`.

- [ ] **Step 3: Write the init SQL**

Tokens available in init scripts: `<<schema>>`, `<<raw_schema>>` (bracketed), `<<s:schema>>`, `<<s:raw_schema>>`
(N'...' literals), `<<pf:schema>>`, `<<ps:schema>>`, `<<pf:raw_schema>>`, `<<ps:raw_schema>>` (bracketed),
`<<pfname:schema>>`, `<<psname:schema>>`, `<<pfname:raw_schema>>`, `<<psname:raw_schema>>` (N'...' literals).

`src/npd_loader/sql/mssql/init/001_schemas.sql`:

```sql
IF SCHEMA_ID(<<s:raw_schema>>) IS NULL EXEC (N'CREATE SCHEMA ' + <<s:raw_schema>>)
GO
IF SCHEMA_ID(<<s:schema>>) IS NULL EXEC (N'CREATE SCHEMA ' + <<s:schema>>)
GO
-- One partition function and scheme per schema; boundaries are added at publish (SPLIT) and removed by retention (MERGE).
IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = <<pfname:raw_schema>>)
    CREATE PARTITION FUNCTION <<pf:raw_schema>> (date) AS RANGE RIGHT FOR VALUES ()
GO
IF NOT EXISTS (SELECT 1 FROM sys.partition_schemes WHERE name = <<psname:raw_schema>>)
    CREATE PARTITION SCHEME <<ps:raw_schema>> AS PARTITION <<pf:raw_schema>> ALL TO ([PRIMARY])
GO
IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = <<pfname:schema>>)
    CREATE PARTITION FUNCTION <<pf:schema>> (date) AS RANGE RIGHT FOR VALUES ()
GO
IF NOT EXISTS (SELECT 1 FROM sys.partition_schemes WHERE name = <<psname:schema>>)
    CREATE PARTITION SCHEME <<ps:schema>> AS PARTITION <<pf:schema>> ALL TO ([PRIMARY])
GO
-- One row per published release; written in the same transaction that switches its partitions in.
IF OBJECT_ID(<<s:schema>> + N'.release', N'U') IS NULL
    CREATE TABLE <<schema>>.release (
        release_date  date         NOT NULL CONSTRAINT pk_release PRIMARY KEY,
        import_run_id int          NOT NULL,
        published_at  datetime2(3) NOT NULL DEFAULT SYSUTCDATETIME()
    )
GO
-- "Organization/Organization-123" -> "Organization-123". Scalar and inlinable (compatibility level 150).
CREATE OR ALTER FUNCTION <<schema>>.ref_id (@ref nvarchar(4000)) RETURNS varchar(128)
WITH SCHEMABINDING AS
BEGIN
    RETURN RIGHT(@ref, CHARINDEX(N'/', REVERSE(@ref) + N'/') - 1)
END
GO
-- FHIR dateTime, which may be partial ("2020", "2020-05"), as UTC datetime2(3).
CREATE OR ALTER FUNCTION <<schema>>.fhir_ts (@v nvarchar(100)) RETURNS datetime2(3)
WITH SCHEMABINDING AS
BEGIN
    RETURN CASE
        WHEN @v IS NULL THEN NULL
        WHEN @v LIKE N'[0-9][0-9][0-9][0-9]' THEN CAST(@v + N'-01-01' AS datetime2(3))
        WHEN @v LIKE N'[0-9][0-9][0-9][0-9]-[0-9][0-9]' THEN CAST(@v + N'-01' AS datetime2(3))
        WHEN @v LIKE N'[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]' THEN CAST(@v AS datetime2(3))
        ELSE CAST(SWITCHOFFSET(CAST(@v AS datetimeoffset(7)), '+00:00') AS datetime2(3))
    END
END
GO
-- First extension element with the given url (or no row).
CREATE OR ALTER FUNCTION <<schema>>.ext (@resource nvarchar(max), @url nvarchar(4000)) RETURNS TABLE AS RETURN
    SELECT TOP 1 e.value AS ext
    FROM OPENJSON(@resource, N'$.extension') e
    WHERE JSON_VALUE(e.value, N'$.url') = @url
    ORDER BY CAST(e.[key] AS int)
GO
-- Value of the first identifier whose system is in the JSON array @systems.
CREATE OR ALTER FUNCTION <<schema>>.identifier_value (@resource nvarchar(max), @systems nvarchar(4000))
RETURNS TABLE AS RETURN
    SELECT TOP 1 JSON_VALUE(i.value, N'$.value') AS value
    FROM OPENJSON(@resource, N'$.identifier') i
    WHERE JSON_VALUE(i.value, N'$.system') IN (SELECT s.value FROM OPENJSON(@systems) s)
    ORDER BY CAST(i.[key] AS int)
GO
-- Join a JSON array of strings from position @skip (0-based); NULL when empty.
CREATE OR ALTER FUNCTION <<schema>>.join_text (@arr nvarchar(max), @sep nvarchar(10), @skip int)
RETURNS TABLE AS RETURN
    SELECT NULLIF(STRING_AGG(CAST(t.value AS nvarchar(max)), @sep) WITHIN GROUP (ORDER BY CAST(t.[key] AS int)),
                  N'') AS txt
    FROM OPENJSON(@arr) t
    WHERE CAST(t.[key] AS int) >= @skip
```

`src/npd_loader/sql/mssql/init/002_raw.sql`:

```sql
-- Raw resources. Each release is one standalone table switched into this parent at publish.
IF OBJECT_ID(<<s:raw_schema>> + N'.resource', N'U') IS NULL
    CREATE TABLE <<raw_schema>>.resource (
        release_date   date          NOT NULL,
        resource_type  varchar(40)   NOT NULL,
        resource_id    varchar(128)  NOT NULL,
        last_updated   datetime2(3)  NULL,
        ndjson_file_id int           NOT NULL,
        zst_file_id    int           NOT NULL,
        line_number    bigint        NOT NULL,
        resource       varchar(max)  COLLATE Latin1_General_100_CI_AS_SC_UTF8 NOT NULL
    ) ON <<ps:raw_schema>> (release_date) WITH (DATA_COMPRESSION = PAGE)
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(<<s:raw_schema>> + N'.resource') AND name = N'resource_key')
    CREATE UNIQUE INDEX resource_key ON <<raw_schema>>.resource (release_date, resource_type, resource_id)
    WITH (DATA_COMPRESSION = PAGE)
```

`src/npd_loader/sql/mssql/init/003_tables.sql` — same tables, columns and indexes as
`sql/postgres/init/003_tables.sql`, with these types: `release_date date NOT NULL`, `resource_id varchar(128) NOT NULL`,
`ndjson_file_id int NOT NULL`, `zst_file_id int NOT NULL`, `seq int NOT NULL`, `last_updated`/`period_*` `datetime2(3)`,
booleans `bit`, `latitude`/`longitude` `float`; identifier-like text (`npi`, `pseudo_ein`, every `*_id`,
`resource_type`) `varchar(128)`; code-like text (`use`, `type`, `system`, `code`, `status`, `mode`, `gender`,
`state`, `postal_code`, `country`, `*_code`, `*_system`, `verification_status`, `accepting_patients`)
`varchar(256)` except `system`/`*_system` `varchar(512)`; human text (`name*`, `family`, `given`, `prefix`,
`suffix`, `line1`, `line2`, `city`, `display`, `*_display`, `text`, `*_text`, `alias`, `value`) `nvarchar(1000)`;
`extra_lines`, `address` `nvarchar(2000)`; `description` `nvarchar(4000)`; `identifier.value` `nvarchar(400)` (it is
indexed). Each table follows this exact pattern (shown for the first two; write all 26 this way):

```sql
-- Every table: release_date, resource_id, ndjson_file_id, zst_file_id first; child tables add seq (1-based
-- position in the repeating element). Same columns as sql/postgres/init/003_tables.sql.
IF OBJECT_ID(<<s:schema>> + N'.practitioner', N'U') IS NULL
    CREATE TABLE <<schema>>.practitioner (
        release_date date NOT NULL, resource_id varchar(128) NOT NULL, ndjson_file_id int NOT NULL, zst_file_id int NOT NULL,
        last_updated datetime2(3),
        npi varchar(128),
        active bit,
        gender varchar(256),
        name_family nvarchar(1000),
        name_given nvarchar(1000),
        name_prefix nvarchar(1000),
        name_suffix nvarchar(1000),
        identity_verified bit,
        medicare_enrolled bit,
        in_hhs_exclusion_list bit,
        aligned_with_data_network bit
    ) ON <<ps:schema>> (release_date) WITH (DATA_COMPRESSION = PAGE)
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(<<s:schema>> + N'.practitioner') AND name = N'practitioner_key')
    CREATE UNIQUE INDEX practitioner_key ON <<schema>>.practitioner (release_date, resource_id) WITH (DATA_COMPRESSION = PAGE)
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(<<s:schema>> + N'.practitioner') AND name = N'practitioner_npi')
    CREATE INDEX practitioner_npi ON <<schema>>.practitioner (release_date, npi) WITH (DATA_COMPRESSION = PAGE)
GO
IF OBJECT_ID(<<s:schema>> + N'.practitioner_name', N'U') IS NULL
    CREATE TABLE <<schema>>.practitioner_name (
        release_date date NOT NULL, resource_id varchar(128) NOT NULL, ndjson_file_id int NOT NULL, zst_file_id int NOT NULL,
        seq int NOT NULL,
        [use] varchar(256), family nvarchar(1000), given nvarchar(1000), prefix nvarchar(1000), suffix nvarchar(1000),
        period_start datetime2(3), period_end datetime2(3)
    ) ON <<ps:schema>> (release_date) WITH (DATA_COMPRESSION = PAGE)
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(<<s:schema>> + N'.practitioner_name') AND name = N'practitioner_name_key')
    CREATE UNIQUE INDEX practitioner_name_key ON <<schema>>.practitioner_name (release_date, resource_id, seq) WITH (DATA_COMPRESSION = PAGE)
GO
```

Bracket the reserved words when used as column names: `[use]`, `[type]`, `[text]`, `[value]`, `[system]`
(harmless where not reserved). Write the remaining 24 tables and their indexes (`*_key` unique on
`(release_date, resource_id[, seq])`, `identifier_key` on `(release_date, resource_type, resource_id, seq)`, plus
`practitioner_qualification_code`, `organization_npi`, `location_managing_organization`,
`practitioner_role_practitioner`, `practitioner_role_organization`, `identifier_value` on
`(release_date, [system], [value])`) exactly as in the Postgres file.

`src/npd_loader/sql/mssql/init/900_migrations.sql`:

```sql
-- Post-deployment schema changes. init-db runs this file last, on every run.
--
-- Convention: once deployed, NEVER edit the CREATE TABLE statements in 00x files to change an existing table (the
-- OBJECT_ID guard skips tables that already exist). Append guarded statements here, separated by GO, e.g.:
--
--   IF COL_LENGTH(<<s:schema>> + N'.practitioner', N'new_column') IS NULL
--       ALTER TABLE <<schema>>.practitioner ADD new_column nvarchar(1000) NULL
--   GO
--
-- Columns added to a partitioned parent apply to every partition. Standalone tables are created from the parent,
-- so they get new columns too. init-db recreates the v_* views afterwards.
--
-- (No migrations yet.)
SELECT 1
```

- [ ] **Step 4: Write `MssqlDialect` (init and read-only parts)**

`src/npd_loader/dialect/mssql.py`:

```python
"""SQL Server flavor: every npd-database operation, on a SqlConnectionObject's engine (pyodbc)."""
from __future__ import annotations

import logging
import re
import time
from contextlib import contextmanager
from datetime import date
from typing import Callable, Iterator

from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import DBAPIError

from npd_loader.config import NpdDbConfig
from npd_loader.sqltext import render, split_batches, sql_scripts

log = logging.getLogger(__name__)
MAX_IDENTIFIER = 128
ERROR_NUMBER_RE = re.compile(r"\((\d+)\)")


def error_number(exc: BaseException) -> int | None:
    """SQL Server native error number from a pyodbc error message, e.g. '... exceeded. (1222) (SQLExecDirectW)'."""
    match = ERROR_NUMBER_RE.search(str(getattr(exc, "orig", exc)))
    return int(match.group(1)) if match else None


class MssqlDialect:
    name = "mssql"

    def __init__(self, engine: Engine, cfg: NpdDbConfig, sleep: Callable[[float], None] = time.sleep):
        self.engine = engine
        self.cfg = cfg
        self._sleep = sleep

    # -- naming -------------------------------------------------------------------------------------------
    @staticmethod
    def q(*parts: str) -> str:
        return ".".join("[" + p.replace("]", "]]") + "]" for p in parts)

    @staticmethod
    def pf(schema: str) -> str:
        return f"pf_{schema}_release"

    @staticmethod
    def ps(schema: str) -> str:
        return f"ps_{schema}_release"

    @staticmethod
    def lit(value: str) -> str:
        return "N'" + value.replace("'", "''") + "'"

    def _tokens(self) -> dict[str, str]:
        tokens: dict[str, str] = {}
        for key, schema in (("schema", self.cfg.schema), ("raw_schema", self.cfg.raw_schema)):
            tokens[key] = self.q(schema)
            tokens[f"s:{key}"] = self.lit(schema)
            tokens[f"pf:{key}"] = self.q(self.pf(schema))
            tokens[f"ps:{key}"] = self.q(self.ps(schema))
            tokens[f"pfname:{key}"] = self.lit(self.pf(schema))
            tokens[f"psname:{key}"] = self.lit(self.ps(schema))
        return tokens

    def _autocommit(self) -> Connection:
        return self.engine.connect().execution_options(isolation_level="AUTOCOMMIT")

    # -- catalog views ------------------------------------------------------------------------------------
    @staticmethod
    def parent_tables(conn: Connection, schema: str) -> list[str]:
        """Partitioned (published) tables of `schema`; standalone tables are never partitioned."""
        rows = conn.exec_driver_sql(
            "SELECT t.name FROM sys.tables t "
            "JOIN sys.indexes i ON i.object_id = t.object_id AND i.index_id IN (0, 1) "
            "JOIN sys.partition_schemes s ON s.data_space_id = i.data_space_id "
            "WHERE t.schema_id = SCHEMA_ID(?) ORDER BY t.name", (schema,)).fetchall()
        return [r[0] for r in rows]

    def _boundaries(self, conn: Connection, schema: str) -> set[date]:
        rows = conn.exec_driver_sql(
            "SELECT CAST(v.value AS date) FROM sys.partition_range_values v "
            "JOIN sys.partition_functions f ON f.function_id = v.function_id WHERE f.name = ?",
            (self.pf(schema),)).fetchall()
        return {r[0] for r in rows}

    # -- Dialect --------------------------------------------------------------------------------------------
    def init_db(self) -> None:
        tokens = self._tokens()
        with self._autocommit() as conn:
            for name, text in sql_scripts("mssql", "init"):
                for batch in split_batches(render(text, tokens)):
                    conn.exec_driver_sql(batch)
            for schema in (self.cfg.raw_schema, self.cfg.schema):
                for table in self.parent_tables(conn, schema):
                    conn.exec_driver_sql(
                        f"CREATE OR ALTER VIEW {self.q(schema, 'v_' + table)} AS SELECT * FROM {self.q(schema, table)} "
                        f"WHERE release_date = (SELECT MAX(release_date) FROM {self.q(self.cfg.schema, 'release')})")

    def published_releases(self) -> list[date]:
        with self.engine.connect() as conn:
            rows = conn.exec_driver_sql(
                f"SELECT release_date FROM {self.q(self.cfg.schema, 'release')} ORDER BY release_date").fetchall()
        return [r[0] for r in rows]

    def partitioned_releases(self) -> set[date]:
        """A release is published exactly when its boundary exists: publish SPLITs it in, retention MERGEs it out."""
        with self.engine.connect() as conn:
            return self._boundaries(conn, self.cfg.raw_schema) | self._boundaries(conn, self.cfg.schema)

    def is_published(self, release: date) -> bool:
        with self.engine.connect() as conn:
            return release in self._boundaries(conn, self.cfg.raw_schema)

    @contextmanager
    def session(self) -> Iterator[None]:
        yield
```

`pyodbc` returns `date` columns as `datetime.date`; if the driver returns strings for `CAST(... AS date)` on this
box, wrap with `date.fromisoformat(str(r[0])[:10])` — the test `test_init_db_creates_partitioned_parents_and_views`
plus the publish tests in Task 11 cover it.

- [ ] **Step 5: Run the tests**

Run: `.\.venv\Scripts\python -m pytest tests/test_mssql_schema.py -v`
Expected: 3 passed.

- [ ] **Step 6: Commit**

```bash
git add src/npd_loader/dialect/mssql.py src/npd_loader/sql/mssql/init tests/conftest.py tests/test_mssql_schema.py
git commit -m "feat(mssql): init-db with per-schema partitioning, helper functions, tables and views"
```

---

### Task 8: SQL Server raw load (batch commits)

**Files:**
- Modify: `src/npd_loader/dialect/mssql.py`
- Create: `tests/mssql_fixture_load.py`, `tests/test_mssql_raw_load.py`

**Interfaces:**
- Consumes: `raw_load.iter_lines`, `raw_load.validate_line`, `RawLoadError`, `NdjsonInput`, `RawLoadResult`, `RAW_PARENT`; `standalone_name` (Task 3); `MssqlDialect` helpers (Task 7); the spike decision (Task 2).
- Produces: `MssqlDialect.load_raw(storage, release, run_id, inputs) -> RawLoadResult`; `MssqlDialect._create_standalone(conn, schema, parent, release, run_id) -> str`; `MssqlDialect._clone_indexes(conn, schema, parent, name) -> None`; module function `to_utc_naive(text: str | None) -> datetime | None`; constant `BATCH_ROWS = 5000`. Test helper `mssql_fixture_load.load_fixture_raw(dialect, storage, ndjson=None, release=R, run_id=7) -> RawLoadResult` and `write_inputs(...)` (same as `pg_helpers.write_inputs`).

- [ ] **Step 1: Write the failing tests**

`tests/mssql_fixture_load.py`:

```python
from datetime import date

from pg_helpers import write_inputs  # pure file writing; no database
from release_builder import build_release

R = date(2026, 9, 29)


def load_fixture_raw(dialect, storage, ndjson=None, release=R, run_id=7):
    ndjson = ndjson if ndjson is not None else build_release(release.isoformat()).ndjson
    return dialect.load_raw(storage, release, run_id, write_inputs(storage, ndjson))


def fetch(dialect, sql, *args):
    with dialect.engine.connect() as conn:
        return conn.exec_driver_sql(sql, args).fetchall()
```

(`pg_helpers` imports psycopg at module level; change its `from psycopg import sql as _sql` to a lazy import inside
`rows()` so this module imports on machines without a Postgres server. psycopg itself stays installed until Task 14.)

`tests/test_mssql_raw_load.py`:

```python
from datetime import datetime

import pytest

from npd_loader.dialect.mssql import BATCH_ROWS, to_utc_naive
from npd_loader.raw_load import RawLoadError
from npd_loader.storage import LocalStorage
from mssql_fixture_load import R, fetch, load_fixture_raw
import fixture_data


def test_to_utc_naive():
    assert to_utc_naive("2026-09-29T04:34:00.724328Z") == datetime(2026, 9, 29, 4, 34, 0, 724328)
    assert to_utc_naive("2026-09-29T01:00:00-05:00") == datetime(2026, 9, 29, 6, 0)
    assert to_utc_naive(None) is None


def test_load_raw_counts_lineage_and_index(mssql_dialect, tmp_path):
    d = mssql_dialect
    res = load_fixture_raw(d, LocalStorage(tmp_path / "data"))
    assert res.table == "resource__20260929__r7"
    assert sum(res.rows.values()) == 12 and res.rows["Practitioner"] == 2
    t = d.q(d.cfg.raw_schema, res.table)
    assert fetch(d, f"SELECT count(*), count(DISTINCT ndjson_file_id), MIN(release_date) FROM {t}")[0] == (12, 8, R)
    org = fetch(d, f"SELECT last_updated, ISJSON(resource) FROM {t} WHERE resource_id = ?",
                fixture_data.ORG1["id"])[0]
    assert org == (datetime(2026, 9, 29, 4, 29, 5, 411000), 1)
    assert fetch(d, "SELECT count(*) FROM sys.indexes WHERE object_id = OBJECT_ID(?) AND name = 'resource_key'",
                 f"{d.cfg.raw_schema}.{res.table}")[0][0] == 1
    assert fetch(d, f"SELECT count(*) FROM {d.q(d.cfg.raw_schema, 'resource')}")[0][0] == 0   # not published


def test_batches_commit_and_many_rows_load(mssql_dialect, tmp_path):
    d = mssql_dialect
    base = fixture_data.ORG1
    n = BATCH_ROWS * 2 + 7
    lines = b"".join((__import__("json").dumps({**base, "id": f"Organization-{i}"}) + "\n").encode()
                     for i in range(n))
    res = load_fixture_raw(d, LocalStorage(tmp_path / "data"), ndjson={"01-Organization.ndjson": lines})
    assert res.rows == {"Organization": n}


def test_duplicate_ids_name_the_file(mssql_dialect, tmp_path):
    line = (__import__("json").dumps(fixture_data.ORG1) + "\n").encode()
    with pytest.raises(RawLoadError, match="duplicate resource ids: Organization Organization-1336200294") as e:
        load_fixture_raw(mssql_dialect, LocalStorage(tmp_path / "data"), ndjson={"01-Organization.ndjson": line * 2})
    assert e.value.file_id == 500


def test_bad_line_fails_and_nothing_is_published(mssql_dialect, tmp_path):
    d = mssql_dialect
    good = (__import__("json").dumps(fixture_data.ORG1) + "\n").encode()
    with pytest.raises(RawLoadError, match="line 2: invalid JSON"):
        load_fixture_raw(d, LocalStorage(tmp_path / "data"), ndjson={"01-Organization.ndjson": good + b"{nope\n"})
    assert fetch(d, f"SELECT count(*) FROM {d.q(d.cfg.raw_schema, 'resource')}")[0][0] == 0


def test_too_long_value_fails_loudly(mssql_dialect, tmp_path):
    rec = {**fixture_data.ORG1, "id": "x" * 200}            # resource_id is varchar(128)
    with pytest.raises(RawLoadError, match="Organization"):
        load_fixture_raw(mssql_dialect, LocalStorage(tmp_path / "data"),
                         ndjson={"01-Organization.ndjson": (__import__("json").dumps(rec) + "\n").encode()})
```

(Cleanup of the failed load's standalone table is tested in Task 11,
`tests/test_mssql_publish.py::test_failed_raw_load_leaves_droppable_table`, once `drop_standalone_tables` exists.)

- [ ] **Step 2: Run to verify failure**

Run: `.\.venv\Scripts\python -m pytest tests/test_mssql_raw_load.py -v`
Expected: FAIL, `ImportError: cannot import name 'BATCH_ROWS'`.

- [ ] **Step 3: Implement**

Add to `src/npd_loader/dialect/mssql.py`:

```python
from datetime import datetime, timezone

import pyodbc

from npd_loader.raw_load import RAW_PARENT, NdjsonInput, RawLoadError, RawLoadResult, iter_lines, validate_line
from npd_loader.sqltext import standalone_name
from npd_loader.storage import Storage

BATCH_ROWS = 5000
RAW_COLUMNS = ("release_date", "resource_type", "resource_id", "last_updated", "ndjson_file_id", "zst_file_id",
               "line_number", "resource")


def to_utc_naive(text: str | None) -> datetime | None:
    """FHIR instant (meta.lastUpdated) as a naive UTC datetime for a datetime2 column."""
    if text is None:
        return None
    value = datetime.fromisoformat(text)
    if value.tzinfo is not None:
        value = value.astimezone(timezone.utc).replace(tzinfo=None)
    return value
```

Methods on `MssqlDialect`:

```python
    def _create_standalone(self, conn: Connection, schema: str, parent: str, release: date, run_id: int) -> str:
        """Empty heap shaped like `parent`, PAGE-compressed, CHECKed to one release so SWITCH is metadata only."""
        name = standalone_name(parent, release, run_id, MAX_IDENTIFIER - 3)   # room for the ck_ constraint name
        target = self.q(schema, name)
        conn.exec_driver_sql(f"SELECT TOP 0 * INTO {target} FROM {self.q(schema, parent)}")
        conn.exec_driver_sql(f"ALTER TABLE {target} REBUILD WITH (DATA_COMPRESSION = PAGE)")
        conn.exec_driver_sql(f"ALTER TABLE {target} WITH CHECK ADD CONSTRAINT {self.q('ck_' + name)} "
                             f"CHECK (release_date = '{release.isoformat()}')")
        return name

    def _clone_indexes(self, conn: Connection, schema: str, parent: str, name: str) -> None:
        """Build the parent's indexes on standalone table `name` (same names, columns, uniqueness, compression)."""
        rows = conn.exec_driver_sql(
            "SELECT i.name, i.is_unique, c.name FROM sys.indexes i "
            "JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id "
            "JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id "
            "WHERE i.object_id = OBJECT_ID(?) AND i.index_id > 0 AND ic.is_included_column = 0 "
            "ORDER BY i.index_id, ic.key_ordinal", (f"{schema}.{parent}",)).fetchall()
        indexes: dict[str, tuple[bool, list[str]]] = {}
        for index, unique, column in rows:
            indexes.setdefault(index, (bool(unique), []))[1].append(column)
        for index, (unique, columns) in indexes.items():
            conn.exec_driver_sql(
                f"CREATE {'UNIQUE ' if unique else ''}INDEX {self.q(index)} ON {self.q(schema, name)} "
                f"({', '.join(self.q(c) for c in columns)}) WITH (DATA_COMPRESSION = PAGE)")

    def _load_file(self, raw_conn, table: str, release: date, inp: NdjsonInput, storage: Storage) -> int:
        """Insert one .ndjson in batches of BATCH_ROWS, committing each batch as soon as it lands."""
        cur = raw_conn.cursor()
        cur.fast_executemany = True
        # SPIKE DECISION (docs/profile/2026-10-05-mssql-raw-load-spike.md): keep the next line only if the decision
        # is "fast_executemany+setinputsizes"; delete it for "fast_executemany".
        cur.setinputsizes([None] * 7 + [(pyodbc.SQL_WVARCHAR, 0, 0)])
        insert = (f"INSERT INTO {self.q(self.cfg.raw_schema, table)} ({', '.join(RAW_COLUMNS)}) "
                  f"VALUES ({', '.join('?' * len(RAW_COLUMNS))})")
        lines, batch = 0, []
        try:
            with storage.open_read(inp.rel_path) as f:
                for number, text in iter_lines(f):
                    resource_id, last_updated = validate_line(text, number, inp.resource_type)
                    batch.append((release, inp.resource_type, resource_id, to_utc_naive(last_updated),
                                  inp.file_id, inp.zst_file_id, number, text))
                    if len(batch) == BATCH_ROWS:
                        cur.executemany(insert, batch)
                        raw_conn.commit()
                        lines += len(batch)
                        batch = []
            if batch:
                cur.executemany(insert, batch)
                raw_conn.commit()
                lines += len(batch)
        except RawLoadError as exc:
            raw_conn.rollback()
            raise RawLoadError(f"{inp.name}: {exc}", inp.file_id) from exc
        except pyodbc.Error as exc:
            raw_conn.rollback()
            raise RawLoadError(f"{inp.name}: insert failed: {exc}", inp.file_id) from exc
        except (OSError, ValueError) as exc:
            raw_conn.rollback()
            raise RawLoadError(f"{inp.name}: cannot read {inp.rel_path}: {exc}", inp.file_id) from exc
        count = cur.execute(f"SELECT COUNT_BIG(*) FROM {self.q(self.cfg.raw_schema, table)} WHERE resource_type = ?",
                            inp.resource_type).fetchone()[0]
        if count != lines:
            raise RawLoadError(f"{inp.name}: read {lines} lines but loaded {count} rows", inp.file_id)
        log.info("loaded %d %s resources from %s", lines, inp.resource_type, inp.rel_path)
        return lines

    def load_raw(self, storage: Storage, release: date, run_id: int, inputs: list[NdjsonInput]) -> RawLoadResult:
        raw_schema = self.cfg.raw_schema
        with self._autocommit() as conn:
            table = self._create_standalone(conn, raw_schema, RAW_PARENT, release, run_id)
        raw_conn = self.engine.raw_connection()
        try:
            rows = {inp.resource_type: self._load_file(raw_conn, table, release, inp, storage) for inp in inputs}
        finally:
            raw_conn.close()
        try:
            with self._autocommit() as conn:
                self._clone_indexes(conn, raw_schema, RAW_PARENT, table)
        except DBAPIError as exc:
            if error_number(exc) != 1505:
                raise
            with self.engine.connect() as conn:
                dups = conn.exec_driver_sql(
                    f"SELECT TOP 20 resource_type, resource_id, STRING_AGG(CAST(line_number AS varchar(20)), ',') "
                    f"WITHIN GROUP (ORDER BY line_number) FROM {self.q(raw_schema, table)} "
                    f"GROUP BY resource_type, resource_id HAVING COUNT(*) > 1 ORDER BY 1, 2").fetchall()
            detail = "; ".join(f"{t} {i} at lines {lines}" for t, i, lines in dups)
            file_ids = {inp.resource_type: inp.file_id for inp in inputs}
            raise RawLoadError(f"duplicate resource ids: {detail}", file_ids.get(dups[0][0]) if dups else None) from exc
        return RawLoadResult(table, rows)
```

If the spike decision is `bcp`, replace the body of `_load_file` with a writer that streams the validated rows as
tab-separated UTF-8 into a temporary file under the storage root in chunks of `BATCH_ROWS` lines and runs
`bcp <db>.<raw_schema>.<table> in <file> -S <server> -T -c -C 65001 -t "\t" -b 5000` for each chunk (each `-b`
batch commits); take server and database from `self.engine.url.query["odbc_connect"]` via `urllib.parse`. Keep the
line-count check and the error wrapping. (Only needed if Task 2 decided `bcp`.)

- [ ] **Step 4: Run the tests**

Run: `.\.venv\Scripts\python -m pytest tests/test_mssql_raw_load.py -v`
Expected: 6 passed.

- [ ] **Step 5: Commit**

```bash
git add src/npd_loader/dialect/mssql.py tests/mssql_fixture_load.py tests/test_mssql_raw_load.py tests/pg_helpers.py
git commit -m "feat(mssql): raw load in 5,000-row batches, each committed as it lands"
```

---

### Task 9: T-SQL transforms 010–040 and the transform runner

**Files:**
- Create: `src/npd_loader/sql/mssql/transform/010_practitioner.sql`, `020_organization.sql`, `030_location.sql`, `040_endpoint.sql`
- Modify: `src/npd_loader/dialect/mssql.py`
- Test: `tests/test_mssql_transform.py`

**Interfaces:**
- Consumes: `_create_standalone`, `_clone_indexes` (Task 8), `TransformResult` (Task 6).
- Produces: `MssqlDialect.run_transforms(raw_table, release, run_id) -> TransformResult`. Transform tokens: `<<raw>>`, `<<schema>>`, `<<release>>`, `<<t:<parent>>>` for every parent.

- [ ] **Step 1: Write the failing tests**

`tests/test_mssql_transform.py`:

```python
from datetime import datetime

import pytest

from npd_loader.storage import LocalStorage
from mssql_fixture_load import R, fetch, load_fixture_raw

P1, P2 = "Practitioner-1003000100", "Practitioner-1083687529"


@pytest.fixture
def result(mssql_dialect, tmp_path):
    raw = load_fixture_raw(mssql_dialect, LocalStorage(tmp_path / "data"))
    return mssql_dialect, mssql_dialect.run_transforms(raw.table, R, 7)


def rows(d, res, table, columns, order="resource_id"):
    return [tuple(r) for r in fetch(d, f"SELECT {columns} FROM {d.q(d.cfg.schema, res.tables[table])} ORDER BY {order}")]


def test_standalone_tables_counts_and_not_published(result):
    d, res = result
    assert res.tables["practitioner"] == "practitioner__20260929__r7"
    assert res.counts["practitioner"] == 2 and res.counts["organization"] == 2
    assert all(n > 0 for t, n in res.counts.items() if t in ("location", "endpoint", "practitioner_name"))
    assert fetch(d, f"SELECT count(*) FROM {d.q(d.cfg.schema, 'practitioner')}")[0][0] == 0


def test_practitioner(result):
    d, res = result
    assert rows(d, res, "practitioner", "resource_id, last_updated, npi, active, gender, name_family, name_given, "
                                        "name_prefix, name_suffix, identity_verified") == [
        (P1, datetime(2026, 9, 29, 4, 34, 0, 724000), "1003000100", True, "male", "GOMEZ", "GERARDO", None, None, False),
        (P2, datetime(2026, 9, 29, 4, 35), "1083687529", True, "female", "JONES", "ANNA MARIE", "DR.", "MD", True),
    ]


def test_practitioner_children(result):
    d, res = result
    assert rows(d, res, "practitioner_name", "resource_id, seq, [use], family, period_start", "resource_id, seq") == [
        (P1, 1, "official", "GOMEZ", None), (P2, 1, "maiden", "SMITH", datetime(1990, 1, 1)),
        (P2, 2, "official", "JONES", None)]
    assert rows(d, res, "practitioner_telecom", "resource_id, seq, [value]", "resource_id, seq")[0] == (P1, 1, "2133831280")
    assert rows(d, res, "practitioner_address", "resource_id, seq, line1, city", "resource_id, seq")[0] == \
        (P1, 1, "108 W Victoria St", "Gardena")
    assert len(rows(d, res, "practitioner_qualification", "resource_id, seq, code", "resource_id, seq")) >= 1


def test_organization_location_endpoint(result):
    d, res = result
    org = rows(d, res, "organization", "resource_id, npi, pseudo_ein, active, part_of_organization_id, verification_status")
    assert org[0] == ("Organization-1336200294", "1336200294", "6e5d8b3e-13d3-48be-9ed2-881d08f2c459", True, None, "complete")
    assert org[1][4] == "Organization-1336200294"
    addr = rows(d, res, "organization_address", "line1, line2, extra_lines", "resource_id, seq")
    assert addr == [("2515 Eastbluff Dr", "Suite 100", "Building B")]
    assert rows(d, res, "organization_endpoint", "endpoint_id", "resource_id, seq") == \
        [("Endpoint-000f410c-e1e9-4a78-a988-b6ce47d6a793",)]
    assert res.counts["location"] >= 1 and res.counts["endpoint"] >= 1
```

- [ ] **Step 2: Run to verify failure**

Run: `.\.venv\Scripts\python -m pytest tests/test_mssql_transform.py -v`
Expected: FAIL, `AttributeError: 'MssqlDialect' object has no attribute 'run_transforms'`.

- [ ] **Step 3: Implement the runner**

Add to `MssqlDialect`:

```python
    def run_transforms(self, raw_table: str, release: date, run_id: int) -> TransformResult:
        schema = self.cfg.schema
        with self._autocommit() as conn:
            tables = {p: self._create_standalone(conn, schema, p, release, run_id)
                      for p in self.parent_tables(conn, schema)}
        tokens = {"raw": self.q(self.cfg.raw_schema, raw_table), "release": f"'{release.isoformat()}'",
                  "schema": self.q(schema), **{f"t:{p}": self.q(schema, n) for p, n in tables.items()}}
        for script, text in sql_scripts("mssql", "transform"):
            log.info("running transform %s", script)
            for batch in split_batches(render(text, tokens)):
                with self.engine.begin() as conn:          # each statement commits as soon as it lands
                    conn.exec_driver_sql(batch)
        counts: dict[str, int] = {}
        with self._autocommit() as conn:
            for parent, name in tables.items():
                self._clone_indexes(conn, schema, parent, name)
                counts[parent] = conn.exec_driver_sql(f"SELECT COUNT_BIG(*) FROM {self.q(schema, name)}").scalar()
        return TransformResult(tables, counts)
```

Import `TransformResult` from `npd_loader.dialect`.

- [ ] **Step 4: Write the transforms**

Translation rules used throughout: `x->>'k'` → `JSON_VALUE(x, '$.k')`; `a->'b'->0->>'c'` → `JSON_VALUE(a, '$.b[0].c')`;
array rows → `CROSS APPLY OPENJSON(r.resource, '$.arr') x` with `seq = CAST(x.[key] AS int) + 1` and fields from
`x.value`; `::boolean` → `CAST(... AS bit)`; `::double precision` → `CAST(... AS float)`; inserts use
`WITH (TABLOCK)`; statements separated by `GO`.

`src/npd_loader/sql/mssql/transform/010_practitioner.sql`:

```sql
INSERT INTO <<t:practitioner>> WITH (TABLOCK) (
    release_date, resource_id, ndjson_file_id, zst_file_id, last_updated,
    npi, active, gender, name_family, name_given, name_prefix, name_suffix,
    identity_verified, medicare_enrolled, in_hhs_exclusion_list, aligned_with_data_network)
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, r.last_updated,
       npi.value,
       CAST(JSON_VALUE(r.resource, '$.active') AS bit),
       JSON_VALUE(r.resource, '$.gender'),
       JSON_VALUE(n.e, '$.family'),
       g.txt, pre.txt, suf.txt,
       CAST(JSON_VALUE(e1.ext, '$.valueBoolean') AS bit),
       CAST(JSON_VALUE(e2.ext, '$.valueBoolean') AS bit),
       CAST(JSON_VALUE(e3.ext, '$.valueBoolean') AS bit),
       CAST(JSON_VALUE(e4.ext, '$.valueBoolean') AS bit)
FROM <<raw>> r
OUTER APPLY <<schema>>.identifier_value(r.resource, N'["http://terminology.hl7.org/NamingSystem/npi","http://hl7.org/fhir/sid/us-npi"]') npi
-- the official name, else a name with a use, else the first name (Postgres: (use = 'official') DESC NULLS LAST)
OUTER APPLY (SELECT TOP 1 x.value AS e FROM OPENJSON(r.resource, '$.name') x
             ORDER BY CASE WHEN JSON_VALUE(x.value, '$.use') = 'official' THEN 0
                           WHEN JSON_VALUE(x.value, '$.use') IS NOT NULL THEN 1 ELSE 2 END,
                      CAST(x.[key] AS int)) n
OUTER APPLY <<schema>>.join_text(JSON_QUERY(n.e, '$.given'), N' ', 0) g
OUTER APPLY <<schema>>.join_text(JSON_QUERY(n.e, '$.prefix'), N' ', 0) pre
OUTER APPLY <<schema>>.join_text(JSON_QUERY(n.e, '$.suffix'), N' ', 0) suf
OUTER APPLY <<schema>>.ext(r.resource, N'http://hl7.org/fhir/us/ndh/StructureDefinition/base-ext-cms-identity-verified') e1
OUTER APPLY <<schema>>.ext(r.resource, N'http://hl7.org/fhir/us/ndh/StructureDefinition/base-ext-cms_medicare_enrollment') e2
OUTER APPLY <<schema>>.ext(r.resource, N'http://hl7.org/fhir/us/ndh/StructureDefinition/base-ext-hhs-in-exclusion-list') e3
OUTER APPLY <<schema>>.ext(r.resource, N'http://hl7.org/fhir/us/ndh/StructureDefinition/base-ext-cms_aligned_with_data_network') e4
WHERE r.resource_type = 'Practitioner'
GO
INSERT INTO <<t:practitioner_name>> WITH (TABLOCK) (
    release_date, resource_id, ndjson_file_id, zst_file_id, seq,
    [use], family, given, prefix, suffix, period_start, period_end)
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, CAST(x.[key] AS int) + 1,
       JSON_VALUE(x.value, '$.use'), JSON_VALUE(x.value, '$.family'), g.txt, pre.txt, suf.txt,
       <<schema>>.fhir_ts(JSON_VALUE(x.value, '$.period.start')),
       <<schema>>.fhir_ts(JSON_VALUE(x.value, '$.period.end'))
FROM <<raw>> r
CROSS APPLY OPENJSON(r.resource, '$.name') x
OUTER APPLY <<schema>>.join_text(JSON_QUERY(x.value, '$.given'), N' ', 0) g
OUTER APPLY <<schema>>.join_text(JSON_QUERY(x.value, '$.prefix'), N' ', 0) pre
OUTER APPLY <<schema>>.join_text(JSON_QUERY(x.value, '$.suffix'), N' ', 0) suf
WHERE r.resource_type = 'Practitioner'
GO
INSERT INTO <<t:practitioner_address>> WITH (TABLOCK) (
    release_date, resource_id, ndjson_file_id, zst_file_id, seq,
    [use], [type], line1, line2, extra_lines, city, state, postal_code, country)
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, CAST(x.[key] AS int) + 1,
       JSON_VALUE(x.value, '$.use'), JSON_VALUE(x.value, '$.type'),
       JSON_VALUE(x.value, '$.line[0]'), JSON_VALUE(x.value, '$.line[1]'), extra.txt,
       JSON_VALUE(x.value, '$.city'), JSON_VALUE(x.value, '$.state'),
       JSON_VALUE(x.value, '$.postalCode'), JSON_VALUE(x.value, '$.country')
FROM <<raw>> r
CROSS APPLY OPENJSON(r.resource, '$.address') x
OUTER APPLY <<schema>>.join_text(JSON_QUERY(x.value, '$.line'), N', ', 2) extra
WHERE r.resource_type = 'Practitioner'
GO
INSERT INTO <<t:practitioner_telecom>> WITH (TABLOCK) (
    release_date, resource_id, ndjson_file_id, zst_file_id, seq, [system], [use], [value])
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, CAST(x.[key] AS int) + 1,
       JSON_VALUE(x.value, '$.system'), JSON_VALUE(x.value, '$.use'), JSON_VALUE(x.value, '$.value')
FROM <<raw>> r
CROSS APPLY OPENJSON(r.resource, '$.telecom') x
WHERE r.resource_type = 'Practitioner'
GO
INSERT INTO <<t:practitioner_qualification>> WITH (TABLOCK) (
    release_date, resource_id, ndjson_file_id, zst_file_id, seq,
    code_system, code, code_display, code_text, identifier_value, identifier_type_code, issuer_organization_id)
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, CAST(x.[key] AS int) + 1,
       JSON_VALUE(x.value, '$.code.coding[0].system'),
       JSON_VALUE(x.value, '$.code.coding[0].code'),
       JSON_VALUE(x.value, '$.code.coding[0].display'),
       JSON_VALUE(x.value, '$.code.text'),
       JSON_VALUE(x.value, '$.identifier[0].value'),
       JSON_VALUE(x.value, '$.identifier[0].type.coding[0].code'),
       <<schema>>.ref_id(JSON_VALUE(x.value, '$.issuer.reference'))
FROM <<raw>> r
CROSS APPLY OPENJSON(r.resource, '$.qualification') x
WHERE r.resource_type = 'Practitioner'
```

`src/npd_loader/sql/mssql/transform/020_organization.sql`:

```sql
INSERT INTO <<t:organization>> WITH (TABLOCK) (
    release_date, resource_id, ndjson_file_id, zst_file_id, last_updated,
    npi, pseudo_ein, name, active, type_code, type_display, part_of_organization_id, verification_status)
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, r.last_updated,
       npi.value, ein.value,
       JSON_VALUE(r.resource, '$.name'),
       CAST(JSON_VALUE(r.resource, '$.active') AS bit),
       JSON_VALUE(r.resource, '$.type[0].coding[0].code'),
       JSON_VALUE(r.resource, '$.type[0].coding[0].display'),
       <<schema>>.ref_id(JSON_VALUE(r.resource, '$.partOf.reference')),
       JSON_VALUE(vs.ext, '$.valueCodeableConcept.coding[0].code')
FROM <<raw>> r
OUTER APPLY <<schema>>.identifier_value(r.resource, N'["http://terminology.hl7.org/NamingSystem/npi","http://hl7.org/fhir/sid/us-npi"]') npi
OUTER APPLY <<schema>>.identifier_value(r.resource, N'["https://npd.cms.gov/fhir/sid/us-pseudo-ein"]') ein
OUTER APPLY <<schema>>.ext(r.resource, N'http://hl7.org/fhir/us/ndh/StructureDefinition/base-ext-verification-status') vs
WHERE r.resource_type = 'Organization'
GO
INSERT INTO <<t:organization_address>> WITH (TABLOCK) (
    release_date, resource_id, ndjson_file_id, zst_file_id, seq,
    [use], [type], line1, line2, extra_lines, city, state, postal_code, country)
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, CAST(x.[key] AS int) + 1,
       JSON_VALUE(x.value, '$.use'), JSON_VALUE(x.value, '$.type'),
       JSON_VALUE(x.value, '$.line[0]'), JSON_VALUE(x.value, '$.line[1]'), extra.txt,
       JSON_VALUE(x.value, '$.city'), JSON_VALUE(x.value, '$.state'),
       JSON_VALUE(x.value, '$.postalCode'), JSON_VALUE(x.value, '$.country')
FROM <<raw>> r
CROSS APPLY OPENJSON(r.resource, '$.address') x
OUTER APPLY <<schema>>.join_text(JSON_QUERY(x.value, '$.line'), N', ', 2) extra
WHERE r.resource_type = 'Organization'
GO
INSERT INTO <<t:organization_telecom>> WITH (TABLOCK) (
    release_date, resource_id, ndjson_file_id, zst_file_id, seq, [system], [use], [value])
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, CAST(x.[key] AS int) + 1,
       JSON_VALUE(x.value, '$.system'), JSON_VALUE(x.value, '$.use'), JSON_VALUE(x.value, '$.value')
FROM <<raw>> r
CROSS APPLY OPENJSON(r.resource, '$.telecom') x
WHERE r.resource_type = 'Organization'
GO
INSERT INTO <<t:organization_endpoint>> WITH (TABLOCK) (
    release_date, resource_id, ndjson_file_id, zst_file_id, seq, endpoint_id)
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, CAST(x.[key] AS int) + 1,
       <<schema>>.ref_id(JSON_VALUE(x.value, '$.reference'))
FROM <<raw>> r
CROSS APPLY OPENJSON(r.resource, '$.endpoint') x
WHERE r.resource_type = 'Organization'
```

`src/npd_loader/sql/mssql/transform/030_location.sql`:

```sql
INSERT INTO <<t:location>> WITH (TABLOCK) (
    release_date, resource_id, ndjson_file_id, zst_file_id, last_updated,
    status, name, description, mode, address_use, address_type, line1, line2, extra_lines,
    city, state, postal_code, country, latitude, longitude, managing_organization_id)
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, r.last_updated,
       JSON_VALUE(r.resource, '$.status'), JSON_VALUE(r.resource, '$.name'),
       JSON_VALUE(r.resource, '$.description'), JSON_VALUE(r.resource, '$.mode'),
       JSON_VALUE(r.resource, '$.address.use'), JSON_VALUE(r.resource, '$.address.type'),
       JSON_VALUE(r.resource, '$.address.line[0]'), JSON_VALUE(r.resource, '$.address.line[1]'), extra.txt,
       JSON_VALUE(r.resource, '$.address.city'), JSON_VALUE(r.resource, '$.address.state'),
       JSON_VALUE(r.resource, '$.address.postalCode'), JSON_VALUE(r.resource, '$.address.country'),
       CAST(JSON_VALUE(r.resource, '$.position.latitude') AS float),
       CAST(JSON_VALUE(r.resource, '$.position.longitude') AS float),
       <<schema>>.ref_id(JSON_VALUE(r.resource, '$.managingOrganization.reference'))
FROM <<raw>> r
OUTER APPLY <<schema>>.join_text(JSON_QUERY(r.resource, '$.address.line'), N', ', 2) extra
WHERE r.resource_type = 'Location'
GO
INSERT INTO <<t:location_telecom>> WITH (TABLOCK) (
    release_date, resource_id, ndjson_file_id, zst_file_id, seq, [system], [use], [value])
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, CAST(x.[key] AS int) + 1,
       JSON_VALUE(x.value, '$.system'), JSON_VALUE(x.value, '$.use'), JSON_VALUE(x.value, '$.value')
FROM <<raw>> r
CROSS APPLY OPENJSON(r.resource, '$.telecom') x
WHERE r.resource_type = 'Location'
```

`src/npd_loader/sql/mssql/transform/040_endpoint.sql`:

```sql
INSERT INTO <<t:endpoint>> WITH (TABLOCK) (
    release_date, resource_id, ndjson_file_id, zst_file_id, last_updated,
    status, name, address, connection_type_system, connection_type_code,
    payload_type_system, payload_type_code, managing_organization_id, verification_status)
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, r.last_updated,
       JSON_VALUE(r.resource, '$.status'), JSON_VALUE(r.resource, '$.name'), JSON_VALUE(r.resource, '$.address'),
       JSON_VALUE(r.resource, '$.connectionType.system'), JSON_VALUE(r.resource, '$.connectionType.code'),
       JSON_VALUE(r.resource, '$.payloadType[0].coding[0].system'),
       JSON_VALUE(r.resource, '$.payloadType[0].coding[0].code'),
       <<schema>>.ref_id(JSON_VALUE(r.resource, '$.managingOrganization.reference')),
       JSON_VALUE(vs.ext, '$.valueCodeableConcept.coding[0].code')
FROM <<raw>> r
OUTER APPLY <<schema>>.ext(r.resource, N'http://hl7.org/fhir/us/ndh/StructureDefinition/base-ext-verification-status') vs
WHERE r.resource_type = 'Endpoint'
```

- [ ] **Step 5: Run the tests**

Run: `.\.venv\Scripts\python -m pytest tests/test_mssql_transform.py -v`
Expected: 4 passed. (Scripts 050–090 do not exist yet; their standalone tables are created empty, which these tests allow.)

- [ ] **Step 6: Commit**

```bash
git add src/npd_loader/dialect/mssql.py src/npd_loader/sql/mssql/transform tests/test_mssql_transform.py
git commit -m "feat(mssql): transform runner and T-SQL transforms for practitioner, organization, location, endpoint"
```

---

### Task 10: T-SQL transforms 050–090

**Files:**
- Create: `src/npd_loader/sql/mssql/transform/050_practitioner_role.sql`, `060_organization_affiliation.sql`, `070_healthcare_service.sql`, `080_insurance_plan.sql`, `090_identifier.sql`
- Modify: `tests/test_mssql_transform.py`

**Interfaces:**
- Consumes: `run_transforms` and tokens (Task 9).

- [ ] **Step 1: Write the failing tests** (append to `tests/test_mssql_transform.py`)

```python
def test_every_table_gets_rows(result):
    d, res = result
    empty = sorted(t for t, n in res.counts.items() if n == 0)
    assert empty == [], f"no rows in {empty}"


def test_roles_and_identifier(result):
    d, res = result
    role = rows(d, res, "practitioner_role", "practitioner_id, organization_id")
    assert role and all(p and p.startswith("Practitioner-") for p, _ in role)
    ids = rows(d, res, "identifier", "resource_type, resource_id, seq, [system], [value]",
               "resource_type, resource_id, seq")
    assert ("Organization", "Organization-1336200294", 1, "http://terminology.hl7.org/NamingSystem/npi",
            "1336200294") in ids
```

If the fixture release (`tests/fixture_data.py`) lacks a resource for a table (check `test_every_table_gets_rows`'s
message), that is a fixture gap, not a transform bug: compare with the Postgres expectations in
`tests/test_transform_roles.py` and, if Postgres also produces no rows for that table, add it to an `ALLOWED_EMPTY`
set in the test with a comment naming the missing fixture element.

- [ ] **Step 2: Run to verify failure**

Run: `.\.venv\Scripts\python -m pytest tests/test_mssql_transform.py -v`
Expected: FAIL in `test_every_table_gets_rows` listing `identifier`, `practitioner_role`, ….

- [ ] **Step 3: Write the transforms**

`050_practitioner_role.sql`:

```sql
INSERT INTO <<t:practitioner_role>> WITH (TABLOCK) (
    release_date, resource_id, ndjson_file_id, zst_file_id, last_updated,
    active, practitioner_id, organization_id, period_start, period_end,
    network_organization_id, accepting_patients)
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, r.last_updated,
       CAST(JSON_VALUE(r.resource, '$.active') AS bit),
       <<schema>>.ref_id(JSON_VALUE(r.resource, '$.practitioner.reference')),
       <<schema>>.ref_id(JSON_VALUE(r.resource, '$.organization.reference')),
       <<schema>>.fhir_ts(JSON_VALUE(r.resource, '$.period.start')),
       <<schema>>.fhir_ts(JSON_VALUE(r.resource, '$.period.end')),
       <<schema>>.ref_id(JSON_VALUE(net.ext, '$.valueReference.reference')),
       JSON_VALUE(acc.ext, '$.valueCodeableConcept.coding[0].code')
FROM <<raw>> r
OUTER APPLY <<schema>>.ext(r.resource, N'http://hl7.org/fhir/us/ndh/StructureDefinition/base-ext-network-reference') net
OUTER APPLY <<schema>>.ext(r.resource, N'http://hl7.org/fhir/us/ndh/StructureDefinition/base-ext-newpatients') np
OUTER APPLY <<schema>>.ext(np.ext, N'acceptingPatients') acc
WHERE r.resource_type = 'PractitionerRole'
GO
INSERT INTO <<t:practitioner_role_endpoint>> WITH (TABLOCK) (
    release_date, resource_id, ndjson_file_id, zst_file_id, seq, endpoint_id)
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, CAST(x.[key] AS int) + 1,
       <<schema>>.ref_id(JSON_VALUE(x.value, '$.reference'))
FROM <<raw>> r
CROSS APPLY OPENJSON(r.resource, '$.endpoint') x
WHERE r.resource_type = 'PractitionerRole'
GO
INSERT INTO <<t:practitioner_role_location>> WITH (TABLOCK) (
    release_date, resource_id, ndjson_file_id, zst_file_id, seq, location_id)
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, CAST(x.[key] AS int) + 1,
       <<schema>>.ref_id(JSON_VALUE(x.value, '$.reference'))
FROM <<raw>> r
CROSS APPLY OPENJSON(r.resource, '$.location') x
WHERE r.resource_type = 'PractitionerRole'
GO
INSERT INTO <<t:practitioner_role_specialty>> WITH (TABLOCK) (
    release_date, resource_id, ndjson_file_id, zst_file_id, seq, [system], code, display, [text])
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, CAST(x.[key] AS int) + 1,
       JSON_VALUE(x.value, '$.coding[0].system'), JSON_VALUE(x.value, '$.coding[0].code'),
       JSON_VALUE(x.value, '$.coding[0].display'), JSON_VALUE(x.value, '$.text')
FROM <<raw>> r
CROSS APPLY OPENJSON(r.resource, '$.specialty') x
WHERE r.resource_type = 'PractitionerRole'
GO
INSERT INTO <<t:practitioner_role_code>> WITH (TABLOCK) (
    release_date, resource_id, ndjson_file_id, zst_file_id, seq, [system], code, display, [text])
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, CAST(x.[key] AS int) + 1,
       JSON_VALUE(x.value, '$.coding[0].system'), JSON_VALUE(x.value, '$.coding[0].code'),
       JSON_VALUE(x.value, '$.coding[0].display'), JSON_VALUE(x.value, '$.text')
FROM <<raw>> r
CROSS APPLY OPENJSON(r.resource, '$.code') x
WHERE r.resource_type = 'PractitionerRole'
GO
INSERT INTO <<t:practitioner_role_telecom>> WITH (TABLOCK) (
    release_date, resource_id, ndjson_file_id, zst_file_id, seq, [system], [use], [value])
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, CAST(x.[key] AS int) + 1,
       JSON_VALUE(x.value, '$.system'), JSON_VALUE(x.value, '$.use'), JSON_VALUE(x.value, '$.value')
FROM <<raw>> r
CROSS APPLY OPENJSON(r.resource, '$.telecom') x
WHERE r.resource_type = 'PractitionerRole'
```

`060_organization_affiliation.sql`:

```sql
INSERT INTO <<t:organization_affiliation>> WITH (TABLOCK) (
    release_date, resource_id, ndjson_file_id, zst_file_id, last_updated,
    active, organization_id, participating_organization_id, role_code, role_display, role_text,
    period_start, period_end)
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, r.last_updated,
       CAST(JSON_VALUE(r.resource, '$.active') AS bit),
       <<schema>>.ref_id(JSON_VALUE(r.resource, '$.organization.reference')),
       <<schema>>.ref_id(JSON_VALUE(r.resource, '$.participatingOrganization.reference')),
       JSON_VALUE(r.resource, '$.code[0].coding[0].code'),
       JSON_VALUE(r.resource, '$.code[0].coding[0].display'),
       JSON_VALUE(r.resource, '$.code[0].text'),
       <<schema>>.fhir_ts(JSON_VALUE(r.resource, '$.period.start')),
       <<schema>>.fhir_ts(JSON_VALUE(r.resource, '$.period.end'))
FROM <<raw>> r
WHERE r.resource_type = 'OrganizationAffiliation'
GO
INSERT INTO <<t:organization_affiliation_network>> WITH (TABLOCK) (
    release_date, resource_id, ndjson_file_id, zst_file_id, seq, network_organization_id)
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, CAST(x.[key] AS int) + 1,
       <<schema>>.ref_id(JSON_VALUE(x.value, '$.reference'))
FROM <<raw>> r
CROSS APPLY OPENJSON(r.resource, '$.network') x
WHERE r.resource_type = 'OrganizationAffiliation'
```

`070_healthcare_service.sql`:

```sql
INSERT INTO <<t:healthcare_service>> WITH (TABLOCK) (
    release_date, resource_id, ndjson_file_id, zst_file_id, last_updated,
    active, name, provided_by_organization_id, network_organization_id)
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, r.last_updated,
       CAST(JSON_VALUE(r.resource, '$.active') AS bit),
       JSON_VALUE(r.resource, '$.name'),
       <<schema>>.ref_id(JSON_VALUE(r.resource, '$.providedBy.reference')),
       <<schema>>.ref_id(JSON_VALUE(net.ext, '$.valueReference.reference'))
FROM <<raw>> r
OUTER APPLY <<schema>>.ext(r.resource, N'http://hl7.org/fhir/us/ndh/StructureDefinition/base-ext-network-reference') net
WHERE r.resource_type = 'HealthcareService'
GO
INSERT INTO <<t:healthcare_service_location>> WITH (TABLOCK) (
    release_date, resource_id, ndjson_file_id, zst_file_id, seq, location_id)
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, CAST(x.[key] AS int) + 1,
       <<schema>>.ref_id(JSON_VALUE(x.value, '$.reference'))
FROM <<raw>> r
CROSS APPLY OPENJSON(r.resource, '$.location') x
WHERE r.resource_type = 'HealthcareService'
```

`080_insurance_plan.sql`:

```sql
INSERT INTO <<t:insurance_plan>> WITH (TABLOCK) (
    release_date, resource_id, ndjson_file_id, zst_file_id, last_updated,
    status, name, type_code, type_text, period_start, period_end,
    owned_by_organization_id, administered_by_organization_id)
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, r.last_updated,
       JSON_VALUE(r.resource, '$.status'), JSON_VALUE(r.resource, '$.name'),
       JSON_VALUE(r.resource, '$.type[0].coding[0].code'),
       JSON_VALUE(r.resource, '$.type[0].text'),
       <<schema>>.fhir_ts(JSON_VALUE(r.resource, '$.period.start')),
       <<schema>>.fhir_ts(JSON_VALUE(r.resource, '$.period.end')),
       <<schema>>.ref_id(JSON_VALUE(r.resource, '$.ownedBy.reference')),
       <<schema>>.ref_id(JSON_VALUE(r.resource, '$.administeredBy.reference'))
FROM <<raw>> r
WHERE r.resource_type = 'InsurancePlan'
GO
INSERT INTO <<t:insurance_plan_alias>> WITH (TABLOCK) (
    release_date, resource_id, ndjson_file_id, zst_file_id, seq, alias)
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, CAST(x.[key] AS int) + 1, x.value
FROM <<raw>> r
CROSS APPLY OPENJSON(r.resource, '$.alias') x
WHERE r.resource_type = 'InsurancePlan'
GO
INSERT INTO <<t:insurance_plan_network>> WITH (TABLOCK) (
    release_date, resource_id, ndjson_file_id, zst_file_id, seq, network_organization_id)
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, CAST(x.[key] AS int) + 1,
       <<schema>>.ref_id(JSON_VALUE(x.value, '$.reference'))
FROM <<raw>> r
CROSS APPLY OPENJSON(r.resource, '$.network') x
WHERE r.resource_type = 'InsurancePlan'
```

`090_identifier.sql`:

```sql
INSERT INTO <<t:identifier>> WITH (TABLOCK) (
    release_date, resource_id, ndjson_file_id, zst_file_id, resource_type, seq,
    [system], [value], [use], type_code, type_text, period_start, period_end)
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, r.resource_type, CAST(x.[key] AS int) + 1,
       JSON_VALUE(x.value, '$.system'), JSON_VALUE(x.value, '$.value'), JSON_VALUE(x.value, '$.use'),
       JSON_VALUE(x.value, '$.type.coding[0].code'),
       JSON_VALUE(x.value, '$.type.text'),
       <<schema>>.fhir_ts(JSON_VALUE(x.value, '$.period.start')),
       <<schema>>.fhir_ts(JSON_VALUE(x.value, '$.period.end'))
FROM <<raw>> r
CROSS APPLY OPENJSON(r.resource, '$.identifier') x
```

- [ ] **Step 4: Run the tests**

Run: `.\.venv\Scripts\python -m pytest tests/test_mssql_transform.py -v`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add src/npd_loader/sql/mssql/transform tests/test_mssql_transform.py
git commit -m "feat(mssql): T-SQL transforms for roles, affiliations, services, plans and identifiers"
```

---

### Task 11: Publish, drop release, run lock, orphan cleanup

**Files:**
- Modify: `src/npd_loader/dialect/mssql.py`
- Test: `tests/test_mssql_publish.py`

**Interfaces:**
- Consumes: Tasks 7–10; `LockUnavailable`, `PublishConflict` (Task 6); `RAW_PARENT`.
- Produces: `MssqlDialect.publish(...)`, `drop_release(release) -> list[str]`, `run_lock(stage) -> ContextManager[bool]`, `drop_standalone_tables(run_id=None) -> list[str]`; constants `LOCK_ATTEMPTS = 3`, `LOCK_BACKOFF_SECONDS = 2.0`.

- [ ] **Step 1: Write the failing tests**

`tests/test_mssql_publish.py`:

```python
import threading
from datetime import date

import pytest

from npd_loader.dialect import LockUnavailable, PublishConflict
from npd_loader.raw_load import RawLoadError
from npd_loader.storage import LocalStorage
from mssql_fixture_load import R, fetch, load_fixture_raw
import fixture_data


def import_release(d, tmp_path, release=R, run_id=7, force=False):
    raw = load_fixture_raw(d, LocalStorage(tmp_path / f"data{run_id}"), release=release, run_id=run_id)
    res = d.run_transforms(raw.table, release, run_id)
    d.publish(raw.table, res.tables, release, run_id, force)
    return res


def count(d, schema, table, release=None):
    where = f" WHERE release_date = '{release}'" if release else ""
    return fetch(d, f"SELECT count(*) FROM {d.q(schema, table)}{where}")[0][0]


def test_publish_switches_in_and_records(mssql_dialect, tmp_path):
    d = mssql_dialect
    import_release(d, tmp_path)
    assert count(d, d.cfg.raw_schema, "resource", R) == 12
    assert count(d, d.cfg.schema, "practitioner", R) == 2
    assert count(d, d.cfg.schema, "v_practitioner") == 2
    assert d.published_releases() == [R] and d.partitioned_releases() == {R} and d.is_published(R)
    assert fetch(d, f"SELECT import_run_id FROM {d.q(d.cfg.schema, 'release')}")[0][0] == 7
    assert d.drop_standalone_tables() == []          # standalone tables were consumed by SWITCH


def test_republish_needs_force_and_force_replaces(mssql_dialect, tmp_path):
    d = mssql_dialect
    import_release(d, tmp_path, run_id=7)
    with pytest.raises(PublishConflict, match="already published"):
        import_release(d, tmp_path, run_id=8)
    assert d.drop_standalone_tables(8)               # the failed attempt's tables are cleanable
    import_release(d, tmp_path, run_id=9, force=True)
    assert count(d, d.cfg.raw_schema, "resource", R) == 12
    assert fetch(d, f"SELECT import_run_id FROM {d.q(d.cfg.schema, 'release')}")[0][0] == 9


def test_two_releases_and_drop(mssql_dialect, tmp_path):
    d = mssql_dialect
    r1, r2 = date(2026, 9, 22), R
    import_release(d, tmp_path, release=r2, run_id=7)
    import_release(d, tmp_path, release=r1, run_id=8)          # older release published after a newer one
    assert d.partitioned_releases() == {r1, r2}
    assert count(d, d.cfg.schema, "v_practitioner") == 2 and count(d, d.cfg.schema, "practitioner") == 4
    dropped = d.drop_release(r1)
    assert f"{d.cfg.schema}.practitioner" in dropped
    assert d.partitioned_releases() == {r2} and d.published_releases() == [r2]
    assert count(d, d.cfg.schema, "practitioner") == 2


def test_drop_release_gives_up_on_lock(mssql_dialect, mssql_engine, tmp_path):
    d = mssql_dialect
    import_release(d, tmp_path)
    blocker = mssql_engine.connect()
    tx = blocker.begin()
    blocker.exec_driver_sql(f"SELECT TOP 1 * FROM {d.q(d.cfg.schema, 'practitioner')} WITH (TABLOCKX, HOLDLOCK)")
    try:
        with pytest.raises(LockUnavailable):
            d.drop_release(R)
    finally:
        tx.rollback()
        blocker.close()
    assert d.partitioned_releases() == {R}           # nothing dropped


def test_run_lock_is_exclusive(mssql_dialect):
    d = mssql_dialect
    with d.run_lock("import") as first:
        assert first is True
        result = {}
        t = threading.Thread(target=lambda: result.setdefault("second", d.run_lock("import").__enter__()))
        t.start(); t.join()
        assert result["second"] is False
        with d.run_lock("download") as other:
            assert other is True
    with d.run_lock("import") as again:
        assert again is True


def test_failed_raw_load_leaves_droppable_table(mssql_dialect, tmp_path):
    d = mssql_dialect
    good = (__import__("json").dumps(fixture_data.ORG1) + "\n").encode()
    with pytest.raises(RawLoadError):
        load_fixture_raw(d, LocalStorage(tmp_path / "data"), ndjson={"01-Organization.ndjson": good + b"{nope\n"})
    assert count(d, d.cfg.raw_schema, "resource") == 0
    assert d.drop_standalone_tables(7) == [f"{d.cfg.raw_schema}.resource__20260929__r7"]
```

(The second `run_lock` in the thread returns `False` without a matching `__exit__`; that is fine because a lock that
was not acquired holds nothing — but the connection must still be closed: `run_lock` closes it before yielding
`False`.)

- [ ] **Step 2: Run to verify failure**

Run: `.\.venv\Scripts\python -m pytest tests/test_mssql_publish.py -v`
Expected: FAIL, `AttributeError: ... 'publish'`.

- [ ] **Step 3: Implement**

Add to `MssqlDialect` (import `LockUnavailable`, `PublishConflict` from `npd_loader.dialect`, `RAW_PARENT` already imported):

```python
LOCK_ATTEMPTS = 3
LOCK_BACKOFF_SECONDS = 2.0
STANDALONE_RE = r"__r{run}(__[a-z]+)?$"
```

```python
    def _locked_transaction(self, work: Callable[[Connection], "T"], what: str) -> "T":
        """Run `work` in one transaction with SET LOCK_TIMEOUT; retry the whole transaction LOCK_ATTEMPTS times on
        error 1222 (lock request timeout), then raise LockUnavailable."""
        timeout_ms = max(1, round(self.cfg.lock_timeout_seconds * 1000))
        for attempt in range(1, LOCK_ATTEMPTS + 1):
            try:
                with self.engine.connect() as conn:
                    conn.exec_driver_sql(f"SET XACT_ABORT ON; SET LOCK_TIMEOUT {timeout_ms}")
                    try:
                        with conn.begin():
                            return work(conn)
                    finally:
                        conn.exec_driver_sql("SET LOCK_TIMEOUT -1")
            except DBAPIError as exc:
                if error_number(exc) != 1222:
                    raise
                if attempt == LOCK_ATTEMPTS:
                    raise LockUnavailable(f"{what}: lock request timed out after {self.cfg.lock_timeout_seconds}s "
                                          f"({LOCK_ATTEMPTS} attempts)") from exc
                log.warning("%s: lock request timed out (attempt %d of %d); retrying", what, attempt, LOCK_ATTEMPTS)
                self._sleep(LOCK_BACKOFF_SECONDS * attempt)
        raise AssertionError("unreachable")

    def _partition_rows(self, conn: Connection, schema: str, table: str, release: date) -> int:
        return conn.exec_driver_sql(
            f"SELECT COALESCE(SUM(p.rows), 0) FROM sys.partitions p WHERE p.object_id = OBJECT_ID(?) "
            f"AND p.index_id IN (0, 1) AND p.partition_number = $PARTITION.{self.q(self.pf(schema))}(?)",
            (f"{schema}.{table}", release)).scalar()

    def _partition_number(self, conn: Connection, schema: str, release: date) -> int:
        return conn.exec_driver_sql(f"SELECT $PARTITION.{self.q(self.pf(schema))}(?)", (release,)).scalar()

    def _switch_out_and_drop(self, conn: Connection, schema: str, parent: str, release: date, run_id: int) -> None:
        name = standalone_name(f"{parent}__out", release, run_id, MAX_IDENTIFIER)
        target = self.q(schema, name)
        conn.exec_driver_sql(f"SELECT TOP 0 * INTO {target} FROM {self.q(schema, parent)}")
        conn.exec_driver_sql(f"ALTER TABLE {target} REBUILD WITH (DATA_COMPRESSION = PAGE)")
        self._clone_indexes(conn, schema, parent, name)
        number = self._partition_number(conn, schema, release)
        conn.exec_driver_sql(f"ALTER TABLE {self.q(schema, parent)} SWITCH PARTITION {number} TO {target}")
        conn.exec_driver_sql(f"DROP TABLE {target}")

    def publish(self, raw_table: str, tables: dict[str, str], release: date, run_id: int, force: bool) -> None:
        raw_schema, schema = self.cfg.raw_schema, self.cfg.schema
        targets = [(raw_schema, RAW_PARENT, raw_table)] + [(schema, p, n) for p, n in sorted(tables.items())]
        day = f"'{release.isoformat()}'"

        def work(conn: Connection) -> None:
            for s in (raw_schema, schema):
                if release not in self._boundaries(conn, s):
                    conn.exec_driver_sql(f"ALTER PARTITION SCHEME {self.q(self.ps(s))} NEXT USED [PRIMARY]")
                    conn.exec_driver_sql(f"ALTER PARTITION FUNCTION {self.q(self.pf(s))}() SPLIT RANGE ({day})")
            for s, parent, new in targets:
                if self._partition_rows(conn, s, parent, release):
                    if not force:
                        raise PublishConflict(f"release {release} is already published in {s}.{parent}; "
                                              f"rerun with --force to replace it")
                    self._switch_out_and_drop(conn, s, parent, release, run_id)
                number = self._partition_number(conn, s, release)
                conn.exec_driver_sql(f"ALTER TABLE {self.q(s, new)} SWITCH TO {self.q(s, parent)} PARTITION {number}")
                conn.exec_driver_sql(f"DROP TABLE {self.q(s, new)}")
            conn.exec_driver_sql(
                f"MERGE {self.q(schema, 'release')} AS t USING (SELECT CAST(? AS date) AS release_date, ? AS run_id) AS s "
                f"ON t.release_date = s.release_date "
                f"WHEN MATCHED THEN UPDATE SET import_run_id = s.run_id, published_at = SYSUTCDATETIME() "
                f"WHEN NOT MATCHED THEN INSERT (release_date, import_run_id) VALUES (s.release_date, s.run_id);",
                (release, run_id))

        self._locked_transaction(work, f"publish release {release}")
        log.info("published release %s (%d tables)", release, len(targets))
        with self._autocommit() as conn:
            for s, parent, _ in targets:
                conn.exec_driver_sql(f"UPDATE STATISTICS {self.q(s, parent)}")

    def drop_release(self, release: date) -> list[str]:
        raw_schema, schema = self.cfg.raw_schema, self.cfg.schema

        def work(conn: Connection) -> list[str]:
            dropped: list[str] = []
            for s in (raw_schema, schema):
                if release not in self._boundaries(conn, s):
                    continue
                for parent in self.parent_tables(conn, s):
                    if self._partition_rows(conn, s, parent, release):
                        self._switch_out_and_drop(conn, s, parent, release, 0)
                        dropped.append(f"{s}.{parent}")
                conn.exec_driver_sql(f"ALTER PARTITION FUNCTION {self.q(self.pf(s))}() "
                                     f"MERGE RANGE ('{release.isoformat()}')")
            conn.exec_driver_sql(f"DELETE FROM {self.q(schema, 'release')} WHERE release_date = ?", (release,))
            return dropped

        return self._locked_transaction(work, f"drop release {release}")

    @contextmanager
    def run_lock(self, stage: str) -> Iterator[bool]:
        """Session-owned application lock on its own connection, held for the whole stage."""
        conn = self._autocommit()
        resource = f"npd_loader:{self.cfg.schema}:{stage}"
        try:
            rc = conn.exec_driver_sql(
                "SET NOCOUNT ON; DECLARE @rc int; EXEC @rc = sp_getapplock @Resource = ?, @LockMode = 'Exclusive', "
                "@LockOwner = 'Session', @LockTimeout = 0; SELECT @rc", (resource,)).scalar()
        except Exception:
            conn.close()
            raise
        acquired = rc is not None and rc >= 0
        if not acquired:
            conn.close()
            yield False
            return
        try:
            yield True
        finally:
            try:
                conn.exec_driver_sql("EXEC sp_releaseapplock @Resource = ?, @LockOwner = 'Session'", (resource,))
            finally:
                conn.close()

    def drop_standalone_tables(self, run_id: int | None = None) -> list[str]:
        """Drop unpublished tables of import `run_id` (any run when None). Only safe with the import lock held."""
        pattern = re.compile(STANDALONE_RE.format(run=run_id if run_id is not None else r"\d+"))
        dropped: list[str] = []
        with self._autocommit() as conn:
            for schema in (self.cfg.raw_schema, self.cfg.schema):
                published = set(self.parent_tables(conn, schema))
                names = [r[0] for r in conn.exec_driver_sql(
                    "SELECT name FROM sys.tables WHERE schema_id = SCHEMA_ID(?) ORDER BY name", (schema,))]
                for name in names:
                    if name not in published and pattern.search(name):
                        conn.exec_driver_sql(f"DROP TABLE {self.q(schema, name)}")
                        dropped.append(f"{schema}.{name}")
        return dropped
```

The lock resource includes the data schema, so test schemas (and dev/prod on one server) never block each other.
`release` is `NOT NULL` with a primary key, so the `MERGE` is safe without `HOLDLOCK` inside this single-writer
transaction (the import lock serializes publishers).

- [ ] **Step 4: Run the tests**

Run: `.\.venv\Scripts\python -m pytest tests/test_mssql_publish.py tests/test_mssql_raw_load.py -v`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add src/npd_loader/dialect/mssql.py tests/test_mssql_publish.py
git commit -m "feat(mssql): atomic publish and retention with partition SWITCH, sp_getapplock run lock, orphan cleanup"
```

---

### Task 12: End to end on SQL Server; README

**Files:**
- Create: `tests/test_mssql_e2e.py`
- Modify: `README.md`

**Interfaces:**
- Consumes: everything above; `tests/helpers.config_data`, `write_env_file`, `to_toml`; `test_catalog_mssql.make_catalog_schema`.

- [ ] **Step 1: Write the test**

`tests/test_mssql_e2e.py`:

```python
import copy
from datetime import date, timedelta

from npd_loader.catalog import SqlCatalog
from npd_loader.cli import main
from npd_loader.config import parse_config
import fixture_data
from helpers import config_data, to_toml, write_env_file
from release_builder import build_release
from test_catalog_mssql import make_catalog_schema

BASE = date(2026, 8, 4)


def publish(cms, week: int) -> date:
    release = BASE + timedelta(weeks=week)
    records = copy.deepcopy(fixture_data.RECORDS)
    records["01-Organization.ndjson"][0]["name"] = f"ORG {release}"
    cms.publish(build_release(release.isoformat(), records=records))
    return release


def test_releases_end_to_end_on_sql_server(tmp_path, cms, mssql_doc, mssql_engine, mssql_schemas, capsys):
    raw, data, cat = mssql_schemas("raw", "", "cat")
    cat_cfg = make_catalog_schema(mssql_engine, cat)
    env = write_env_file(tmp_path / "database.env", {"data": mssql_doc, "catalog": mssql_doc})
    cfg_data = config_data(tmp_path / "data", cms.manifest_url, env_file=env, schemas=(raw, data),
                           catalog_tables=(cat_cfg.run_table, cat_cfg.file_table), keep_releases=2)
    config_path = tmp_path / "config.toml"
    config_path.write_text(to_toml(cfg_data))
    catalog = SqlCatalog(mssql_engine, parse_config(cfg_data).catalog)

    def cli(*args):
        return main(["--config", str(config_path), *args])

    def scalar(sql):
        with mssql_engine.connect() as conn:
            return conn.exec_driver_sql(sql).scalar()

    assert cli("init-db") == 0
    assert cli("init-db") == 0

    r1 = publish(cms, 0)
    assert cli("run") == 0
    assert scalar(f"SELECT count(*) FROM [{data}].[v_practitioner]") == 2
    assert len(catalog.get_data_files(r1, "ndjson")) == 8
    assert cli("run") == 0                                   # nothing to do
    assert cli("import", "--force") == 0                     # replaces the partitions
    assert scalar(f"SELECT count(*) FROM [{raw}].[resource]") == 12

    r2 = publish(cms, 1)
    assert cli("run") == 0
    r3 = publish(cms, 2)
    assert cli("run") == 0                                   # keep_releases = 2: r1 is dropped
    assert scalar(f"SELECT count(DISTINCT release_date) FROM [{raw}].[resource]") == 2
    assert scalar(f"SELECT MIN(release_date) FROM [{data}].[release]") == r2
    capsys.readouterr()
    assert cli("status") == 0
    out = capsys.readouterr().out
    assert out.splitlines()[0].split() == ["release", "download", "extract", "import", "published"]
    assert r3.isoformat() in out and "using:" not in out     # DataEngine's import print is suppressed
```

- [ ] **Step 2: Run it**

Run: `.\.venv\Scripts\python -m pytest tests/test_mssql_e2e.py -v`
Expected: PASS. If it fails, fix the cause in the owning module (do not loosen the test) and re-run the whole
SQL Server suite: `.\.venv\Scripts\python -m pytest tests -k mssql -v`.

- [ ] **Step 3: README** — add a section `## SQL Server (Windows)` after the intro, keeping the Postgres sections:

```markdown
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
- Disk: one release used 73 GB on Postgres; page compression reduces that. Plan for several hundred GB at
  `keep_releases = 5`.

### Install

    py -3.12 -m venv C:\npd-loader\.venv
    C:\npd-loader\.venv\Scripts\pip install <path to this repo>
    copy config.example.toml C:\npd-loader\config.toml
    copy database.prd.env C:\npd-loader\database.prd.env
    C:\npd-loader\.venv\Scripts\npd-loader --config C:\npd-loader\config.toml init-db

### Schedule (Task Scheduler)

    schtasks /Create /TN "npd-loader" /SC DAILY /ST 06:00 /RU <DOMAIN\service-account> /RP *
      /TR "C:\npd-loader\.venv\Scripts\npd-loader.exe --config C:\npd-loader\config.toml run"

Logs go to stderr; redirect them in a wrapper `.cmd` if you need a file.

### How it works on SQL Server

Raw lines go into `npd_raw.resource` (`varchar(max)` with a UTF-8 collation), in batches of 5,000 rows, each committed
as it lands, into a standalone table for the release. T-SQL transforms (`OPENJSON`) fill standalone `npd.*` tables.
Publish switches every standalone table into its parent's release partition in one transaction (`SPLIT RANGE`,
`SWITCH`); retention switches old partitions out and `MERGE`s their boundaries. The run lock is `sp_getapplock`.

### Tests

    $env:NPD_TEST_MSSQL_DB = '{"type":"mssql","server":"cssnpi","database":"npd_test","trusted":"yes"}'
    .\.venv\Scripts\python -m pytest

SQL Server tests create and drop their own schemas in `npd_test` and never touch either catalog database.
```

Also update the first README sentence to "Loads the CMS National Provider Directory FHIR bulk release into SQL Server
or Postgres" and replace the old `## Install (192.10.0.7)` config steps that mention users/passwords in `config.toml`
with "connections: see `database.*.env`".

- [ ] **Step 4: Commit**

```bash
git add tests/test_mssql_e2e.py README.md
git commit -m "test(mssql): end-to-end releases through the CLI; README SQL Server section"
```

---

### Task 13: Dev run against `npd_dev` + `HIE_WAREHOUSE_META_DEV` with a real release (manual, with the user)

**Files:**
- Create: `docs/profile/2026-10-XX-mssql-dev-import.md` (use the actual run date)

Prerequisite: `npd_dev` exists; the user confirms before anything is written to `HIE_WAREHOUSE_META_DEV`.

- [ ] **Step 1: Configure** `config.local.toml` (gitignored by the existing `*.local.toml` rule), copied from
`config.example.toml`, with `env_file` pointing at this repo's `database.dev.env` and `[storage] root` at a local
data folder with about 40 GB free.

- [ ] **Step 2: Run**

```powershell
.\.venv\Scripts\npd-loader --config config.local.toml init-db
.\.venv\Scripts\npd-loader --config config.local.toml run
.\.venv\Scripts\npd-loader --config config.local.toml status
```

- [ ] **Step 3: Record** in the profile doc: start/end per stage (from the log timestamps), rows per table
(`SELECT t.name, SUM(p.rows) FROM sys.tables t JOIN sys.partitions p ON p.object_id = t.object_id AND p.index_id IN (0,1) WHERE SCHEMA_NAME(t.schema_id) IN ('npd','npd_raw') GROUP BY t.name`),
database size (`EXEC sp_spaceused`), any failure and its fix. Compare with `docs/profile/2026-09-29-full-import.md`.

- [ ] **Step 4: Commit**

```bash
git add docs/profile/
git commit -m "docs: first full SQL Server dev import"
```

---

### Task 14: Postgres flavor on DataEngine (verification deferred)

**Files:**
- Modify: `src/npd_loader/dialect/postgres.py` (absorbs `db.py`, `raw_load.py` COPY part, `transform.py`, `publish.py`, `schema.py`), `src/npd_loader/dialect/__init__.py`, `src/npd_loader/import_stage.py`, `pyproject.toml`, `tests/conftest.py`, `tests/pg_helpers.py`, Postgres tests
- Delete: `src/npd_loader/db.py`, `src/npd_loader/publish.py`, `src/npd_loader/transform.py`, `src/npd_loader/schema.py`

**Interfaces:**
- Produces: `PostgresDialect(engine: sqlalchemy.Engine, cfg: NpdDbConfig, sleep=time.sleep)`; `dialect_for` passes `conn_obj.engine`. `pg_conninfo` is removed.

- [ ] **Step 1: Port mechanically**, keeping every SQL statement and behavior:
  - connections: `engine.raw_connection()` gives a psycopg2 connection; use it for everything (it keeps the existing transaction pattern). Replace `conn.execute(q, args).fetchall()` with `cur = conn.cursor(); cur.execute(q, args); cur.fetchall()`; `%s` placeholders are unchanged in psycopg2.
  - identifiers: replace `psycopg.sql.Identifier(a, b)` with `self.q(a, b)` built from `engine.dialect.identifier_preparer.quote`; `sql.Literal(release)` with `f"'{release.isoformat()}'"`.
  - token rendering: `npd_loader.sqltext.render` with pre-quoted strings (drop `db.render_sql`).
  - lock timeout: catch `psycopg2.errors.LockNotAvailable` (`pgcode == "55P03"`), raise `LockUnavailable` after the third attempt.
  - advisory lock: `pg_try_advisory_lock` on a dedicated raw connection with `autocommit = True`.
  - raw load: `cur.copy_expert(f"COPY {leaf} ({cols}) FROM STDIN (FORMAT csv)", stream)` once per `BATCH_ROWS` rows followed by `conn.commit()`, where `stream` is an `io.StringIO` holding `csv.writer` output for that batch (release, type, id, last_updated or empty, file ids, line number, JSON text). Keep the leaf-per-file layout, line-count check and duplicate check.
  - `session()` becomes a no-op; each method opens and closes its own raw connection.
- [ ] **Step 2: Dependencies** — remove `psycopg[binary]` from `pyproject.toml`; `testcontainers[postgres]` stays in `test`. Rewrite `tests/conftest.py` Postgres fixtures with `psycopg2` (`psycopg2.connect(dsn)`; `CREATE DATABASE` with `autocommit = True`), and `tests/pg_helpers.py`, `test_schema.py`, `test_raw_load.py`, `test_transform_*.py`, `test_import.py`, `test_retention.py`, `test_e2e.py` to build a `PostgresDialect(create_engine(...), cfg)` and query through `psycopg2`.
- [ ] **Step 3: Run** `.\.venv\Scripts\python -m pytest -q`. Expected: SQL Server and unit tests pass; Postgres tests skip unless `NPD_TEST_PG_DSN` is set. Note in the commit message that the Postgres flavor is not yet verified against a server.
- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "refactor(postgres): Postgres flavor on PgConnectionObject (psycopg2); drop psycopg (unverified against a server)"
```
