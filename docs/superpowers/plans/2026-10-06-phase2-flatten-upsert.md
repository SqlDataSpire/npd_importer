# Phase 2: Python Flattening, Per-Type Staging and Delta Upserts — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the Phase 1 import (raw JSON table, T-SQL transforms, partition-SWITCH publish of per-release
snapshots) with Python flattening from declarative table specs, `bcp` into fixed staging tables, and a one-transaction
delta upsert into a single current dataset in SQL Server.

**Architecture:** Each `.ndjson` is streamed by a worker process that parses every line once with `orjson` (no JSON
ever reaches SQL Server), hashes it and flattens it with the 26 table specs into stage files (field `0x1F`, row
`0x1E`). `MssqlDialect.stage_release` truncates the fixed staging tables `npd_stage.<table>` and bulk loads the files
with `bcp`; `MssqlDialect.apply_delta` classifies every resource as new/changed/unchanged against
`npd.resource_state` and, in one transaction, upserts: replaces the rows of changed resources and inserts new ones.
Resources missing from a release stay live (aging data); `resource_state.last_seen_release` records when each was last
seen. Download, extract, the catalog and the CLI are unchanged; the `.ndjson.zst` originals are the backup.

**Tech Stack:** Python 3.12, orjson, Python-DataEngine (SQLAlchemy + pyodbc), `bcp.exe` (ODBC 17/18 tools), SQL Server
2019, pytest.

**Spec:** `docs/superpowers/specs/2026-10-06-phase2-python-flatten-upsert-design.md` (approved 2026-10-06, revised
2026-10-07: fixed staging tables, no deletions — aging data stays live; SHA-1 of the raw line; SQL Server only; branch
`feature/phase2-flatten`).

## Global Constraints

- Branch `feature/phase2-flatten`, created from `feature/sqlport-v1`. Never commit to `main` or `feature/sqlport-v1`.
- Python `>=3.12`. Dependencies: `Python-DataEngine>=2.4`, `zstandard>=0.22`, `httpx>=0.27`, `orjson>=3.10`.
- SQL Server only (P3): `dialect_for` rejects a Postgres connection with a `ConfigError`; Postgres code and tests are removed on this branch (`main` remains the Postgres loader).
- Windows auth only (`"trusted": "yes"`); no passwords in committed files.
- Tests never touch `HIE_WAREHOUSE_META*`, `npd` or `npd_dev`; SQL Server tests use `NPD_TEST_MSSQL_DB` (`cssnpi.npd_test`) scratch schemas and skip when it is unset.
- Stage files: `bcp -c -C 65001`, field terminator `0x1F`, row terminator `0x1E`, `NULL` = empty field with `-k`; a value containing `\x1f` or `\x1e` is rejected naming file and line.
- `bcp` success is judged by its output (`N rows copied`, no `Error`) and by row counts in the staging table — never by its exit code alone.
- Staging is a fixed set of tables in `NpdDbConfig.stage_schema` (default `npd_stage`): `npd_stage.<table>` for each of the 26 tables plus `npd_stage.resource_hash`, created by `init-db`, truncated at the start of every import. No per-run tables are ever created or dropped.
- The delta apply is exactly one transaction (`SET XACT_ABORT ON`, `LOCK_TIMEOUT`, whole-transaction retry on error 1222 as in Phase 1).
- Nothing is ever deleted because it is missing from a release (P1, user 2026-10-07): those resources stay live; `resource_state.last_seen_release` / `last_seen_run_id` record the last release that contained each resource. Only the rows of a *changed* resource are replaced.
- Change detection hash: SHA-1 hex of the line's UTF-8 bytes without the line terminator (P2).
- Converter semantics follow Phase 1: FHIR dateTime → UTC `datetime2(3)` rounded half-up to milliseconds, partial dates (`YYYY`, `YYYY-MM`, `YYYY-MM-DD`) → midnight; extensions/identifiers/name pick = first match by array position; `location.description` longer than 4000 characters → NULL.
- Commit messages end with a blank line, then `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- Two deliberate simplifications of the spec, recorded here: stage files are written for a whole release first, then loaded (not handed to `bcp` per 500k-row buffer while flattening); `src/npd_loader/sql/mapped_paths.txt` stays a static file (not generated from the specs).

## Review Focus

1. A release older than the newest applied one (e.g. `import --release` of last month) → refused with a clear message unless `--force` (Task 9 test).
2. A truncated or partial CMS file that drops most of a type → nothing is deleted, the missing resources keep their old `last_seen_release`, and the release's `not_seen_resources` count shows the gap (Task 8 test).
3. A value longer than its column (e.g. a 1,500-character city) → the run fails naming the table and the bcp error, nothing applied (Task 7 test).
4. An import killed after loading staging → the next import truncates staging before loading, so stale rows are never applied, and the catalog run is closed as interrupted (Task 9 test).
5. A failure inside the apply transaction → the previous current state is fully intact (Task 8 test).

---

## File Structure

| File | Responsibility |
|---|---|
| `src/npd_loader/flatten/__init__.py` | package marker |
| `src/npd_loader/flatten/convert.py` | value converters (ts, boolean, number, join, ref) |
| `src/npd_loader/flatten/engine.py` | `Table`/`Col` spec types, path getters, `ext`/`identifier`/`official_name` helpers, `flatten_resource`, `columns` |
| `src/npd_loader/flatten/specs.py` | the 26 table specs, `SPECS` (by resource type), `ALL_TABLES`, `tables_for(resource_type)` |
| `src/npd_loader/flatten/stagefiles.py` | per-file streaming worker, chunked stage writers, hash rows, `flatten_files` (process pool), `FlattenError` |
| `src/npd_loader/dialect/bcp.py` | `bcp_target(engine)`, `bcp_in(...)` with output/count checks, `BcpError` |
| `src/npd_loader/dialect/mssql.py` | Phase 2 `MssqlDialect`: init_db (incl. fixed staging tables), run_lock, published_releases, truncate_stage, stage_release, apply_delta |
| `src/npd_loader/dialect/__init__.py` | Phase 2 `Dialect` protocol, `StageResult`, `DeltaResult`, `dialect_for` |
| `src/npd_loader/sql/mssql/init/001_schemas.sql`, `003_tables.sql`, `900_migrations.sql` | Phase 2 schema |
| `src/npd_loader/import_stage.py`, `retention.py`, `config.py`, `cli.py` | rewired import, file-only retention, new config keys |
| `tests/golden/fixture_tables.json`, `tests/make_golden.py` | Phase 1 output for the fixture release (golden values for the specs) |
| `tests/test_flatten_*.py`, `tests/test_mssql_stage.py`, `tests/test_mssql_delta.py`, `tests/test_mssql_import.py`, `tests/test_mssql_e2e.py` | tests |

---

### Task 1: Branch, orjson, and golden output from Phase 1

**Files:**
- Modify: `pyproject.toml`
- Create: `tests/make_golden.py`, `tests/golden/fixture_tables.json`, `tests/golden_values.py`

**Interfaces:**
- Produces: `tests/golden_values.py`: `normalize(v) -> str | None` (None→None, bool→"1"/"0", datetime→`YYYY-MM-DD HH:MM:SS.fff`, date→ISO, float→`repr(v)`, int→`str(v)`, str→itself) and `load_golden() -> dict[str, list[list[str | None]]]` (rows per table, in `columns(table)` order, sorted).
- Produces the file-id assignment used by all golden comparisons: fixture files sorted by name get `ndjson_file_id = 500 + 2*i`, `zst_file_id = 501 + 2*i` (the existing `pg_helpers.write_inputs` rule).

- [ ] **Step 1: Branch and dependency**

```powershell
git switch -c feature/phase2-flatten feature/sqlport-v1
```

In `pyproject.toml` add `"orjson>=3.10",` to `dependencies`; then `.\.venv\Scripts\python -m pip install -e ".[test]"`.

- [ ] **Step 2: Write `tests/golden_values.py`**

```python
"""Golden table values for the fixture release, produced once by the Phase 1 T-SQL path (tests/make_golden.py)."""
from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path

GOLDEN = Path(__file__).resolve().parent / "golden" / "fixture_tables.json"


def normalize(v):
    if v is None:
        return None
    if isinstance(v, bool):
        return "1" if v else "0"
    if isinstance(v, datetime):
        return v.isoformat(sep=" ", timespec="milliseconds")
    if isinstance(v, date):
        return v.isoformat()
    if isinstance(v, float):
        return repr(v)
    return str(v)


def load_golden() -> dict[str, list[list[str | None]]]:
    return json.loads(GOLDEN.read_text(encoding="utf-8"))
```

- [ ] **Step 3: Write `tests/make_golden.py`** (run once now, while the Phase 1 code still exists; kept for the record)

```python
"""One-off: run the Phase 1 T-SQL path on the fixture release in an npd_test scratch schema and save every table's
rows (normalized) to tests/golden/fixture_tables.json. Usage: python tests/make_golden.py (NPD_TEST_MSSQL_DB set)."""
import json
import os
import sys
import tempfile
import uuid
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from golden_values import GOLDEN, normalize                      # noqa: E402
from mssql_fixture_load import load_fixture_raw                  # noqa: E402
from mssql_helpers import drop_schemas, sql_connection_object    # noqa: E402
from npd_loader.config import NpdDbConfig                        # noqa: E402
from npd_loader.dialect.mssql import MssqlDialect                # noqa: E402
from npd_loader.storage import LocalStorage                      # noqa: E402

engine = sql_connection_object("golden", json.loads(os.environ["NPD_TEST_MSSQL_DB"])).engine
base = f"g{uuid.uuid4().hex[:8]}"
raw, data = f"{base}_raw", base
with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as c:
    for s in (raw, data):
        c.exec_driver_sql(f"CREATE SCHEMA [{s}]")
try:
    d = MssqlDialect(engine, NpdDbConfig(connection="data", raw_schema=raw, schema=data), sleep=lambda s: None)
    d.init_db()
    res = load_fixture_raw(d, LocalStorage(Path(tempfile.mkdtemp())))
    out = d.run_transforms(res.table, date(2026, 9, 29), 7)
    golden = {}
    with engine.connect() as c:
        for parent, name in sorted(out.tables.items()):
            cols = [r[0] for r in c.exec_driver_sql(
                "SELECT name FROM sys.columns WHERE object_id = OBJECT_ID(?) ORDER BY column_id", (f"{data}.{name}",))]
            rows = c.exec_driver_sql(f"SELECT * FROM [{data}].[{name}]").fetchall()
            golden[parent] = sorted([[normalize(v) for v in r] for r in rows], key=lambda r: [x or "" for x in r])
            golden[f"{parent}#columns"] = cols
    GOLDEN.parent.mkdir(exist_ok=True)
    GOLDEN.write_text(json.dumps(golden, indent=1, ensure_ascii=False), encoding="utf-8")
    print(f"wrote {GOLDEN}: {sum(len(v) for k, v in golden.items() if not k.endswith('#columns'))} rows")
finally:
    drop_schemas(engine, [data, raw])
```

- [ ] **Step 4: Run it**

Run: `$env:NPD_TEST_MSSQL_DB='{"type":"mssql","server":"cssnpi","database":"npd_test","trusted":"yes"}'; .\.venv\Scripts\python tests\make_golden.py`
Expected: `wrote ...fixture_tables.json: N rows` with N > 0; the JSON has 26 table keys plus 26 `#columns` keys, and
every table key has at least one row (the Phase 1 test `test_every_table_gets_rows` guarantees the fixture fills all).

- [ ] **Step 5: Commit**

```bash
git add pyproject.toml tests/golden_values.py tests/make_golden.py tests/golden/fixture_tables.json
git commit -m "test: golden fixture-release table values from the Phase 1 T-SQL path; add orjson"
```

---

### Task 2: Value converters

**Files:**
- Create: `src/npd_loader/flatten/__init__.py` (empty docstring module), `src/npd_loader/flatten/convert.py`
- Test: `tests/test_flatten_convert.py`

**Interfaces:**
- Produces (`npd_loader.flatten.convert`): `ts(v) -> str | None`, `boolean(v) -> str | None`, `number(v) -> str | None`, `join(v, sep=" ", skip=0) -> str | None`, `ref(v) -> str | None`, `ConvertError(ValueError)`.

- [ ] **Step 1: Write the failing tests**

```python
import pytest

from npd_loader.flatten.convert import ConvertError, boolean, join, number, ref, ts


def test_ts():
    assert ts("2026-09-29T04:34:00.724328Z") == "2026-09-29 04:34:00.724"
    assert ts("2026-09-29T04:34:00.9996Z") == "2026-09-29 04:34:01.000"          # half-up to ms, carries
    assert ts("2026-09-29T01:00:00-05:00") == "2026-09-29 06:00:00.000"
    assert ts("2026-09-29T04:34:00") == "2026-09-29 04:34:00.000"
    assert ts("2020") == "2020-01-01 00:00:00.000"
    assert ts("2020-05") == "2020-05-01 00:00:00.000"
    assert ts("2020-05-07") == "2020-05-07 00:00:00.000"
    assert ts(None) is None and ts("") is None
    with pytest.raises(ConvertError):
        ts("not a date")


def test_boolean_number_join_ref():
    assert (boolean(True), boolean(False), boolean(None)) == ("1", "0", None)
    with pytest.raises(ConvertError):
        boolean("yes")
    assert number(33.5) == "33.5" and number(7) == "7.0" and number(None) is None
    with pytest.raises(ConvertError):
        number(True)
    assert join(["A", "B", "C"]) == "A B C"
    assert join(["a", "b", "c"], ", ", 2) == "c"
    assert join([]) is None and join(None) is None and join(["a", None]) == "a"
    assert ref("Organization/Organization-1") == "Organization-1"
    assert ref("Organization-1") == "Organization-1" and ref(None) is None and ref("Organization/") is None
```

- [ ] **Step 2: Run to verify failure**

Run: `.\.venv\Scripts\python -m pytest tests/test_flatten_convert.py -v`
Expected: FAIL, `ModuleNotFoundError: No module named 'npd_loader.flatten'`.

- [ ] **Step 3: Implement `src/npd_loader/flatten/convert.py`**

```python
"""Value converters for flattened columns; text forms are what bcp -c loads into the SQL Server column types."""
from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from typing import Any

_PARTIAL = re.compile(r"^\d{4}(-\d{2}(-\d{2})?)?$")


class ConvertError(ValueError):
    pass


def ts(v: Any) -> str | None:
    """FHIR date/dateTime/instant -> 'YYYY-MM-DD HH:MM:SS.fff' in UTC (datetime2(3)); partial dates -> midnight."""
    if v is None or v == "":
        return None
    if not isinstance(v, str):
        raise ConvertError(f"not a date/time: {v!r}")
    if _PARTIAL.match(v):
        parts = (v + "-01-01")[:10] if len(v) == 4 else (v + "-01")[:10] if len(v) == 7 else v
        return f"{parts} 00:00:00.000"
    try:
        d = datetime.fromisoformat(v)
    except ValueError as exc:
        raise ConvertError(f"not a date/time: {v!r}") from exc
    if d.tzinfo is not None:
        d = d.astimezone(timezone.utc).replace(tzinfo=None)
    ms, rem = divmod(d.microsecond, 1000)
    d = d.replace(microsecond=0) + timedelta(milliseconds=ms + (1 if rem >= 500 else 0))
    return d.isoformat(sep=" ", timespec="milliseconds")


def boolean(v: Any) -> str | None:
    if v is None:
        return None
    if isinstance(v, bool):
        return "1" if v else "0"
    raise ConvertError(f"not a boolean: {v!r}")


def number(v: Any) -> str | None:
    if v is None:
        return None
    if isinstance(v, bool) or not isinstance(v, (int, float)):
        raise ConvertError(f"not a number: {v!r}")
    return repr(float(v))


def join(v: Any, sep: str = " ", skip: int = 0) -> str | None:
    if not isinstance(v, list):
        return None
    text = sep.join(str(x) for x in v[skip:] if x is not None)
    return text or None


def ref(v: Any) -> str | None:
    if not isinstance(v, str):
        return None
    return v.rsplit("/", 1)[-1] or None
```

- [ ] **Step 4: Run the tests**

Run: `.\.venv\Scripts\python -m pytest tests/test_flatten_convert.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/npd_loader/flatten tests/test_flatten_convert.py
git commit -m "feat(flatten): value converters mirroring the Phase 1 SQL helpers"
```

---

### Task 3: Spec types and the generic flattener

**Files:**
- Create: `src/npd_loader/flatten/engine.py`
- Test: `tests/test_flatten_engine.py`

**Interfaces:**
- Consumes: `npd_loader.flatten.convert`.
- Produces (`npd_loader.flatten.engine`):
  - `path(expr: str) -> Callable[[Any], Any]` — `"a.b[0].c"`; missing steps → None; non-dict/list → None.
  - `R(get, conv=None) -> Col` (value from the resource), `E(get, conv=None) -> Col` (value from the repeating element); `get` is a path string or a callable.
  - `ext(url: str, inner: Callable | None = None) -> Callable` — first element of `obj["extension"]` whose `url` matches (`inner` applied to an intermediate object first, for nested extensions).
  - `identifier(systems: tuple[str, ...]) -> Callable` — value of the first identifier whose system is in `systems`.
  - `official_name(res) -> dict | None` — official name, else first with a `use`, else first.
  - `@dataclass(frozen=True) class Table: name: str; cols: dict[str, Col]; each: str | None = None; with_type: bool = False`.
  - `LINEAGE = ("release_date", "resource_id", "ndjson_file_id", "zst_file_id")`.
  - `columns(t: Table) -> list[str]` — `LINEAGE + ["resource_type"] if with_type + ["seq"] if each + list(cols)`.
  - `flatten_resource(res: dict, tables: list[Table], lineage: tuple) -> Iterator[tuple[str, tuple]]` — `(table name, values in columns() order)`; `lineage` = `(release_date, resource_id, ndjson_file_id, zst_file_id)`; `seq` = 1-based index; non-dict elements are passed to `E` getters as-is.

- [ ] **Step 1: Write the failing tests**

```python
from npd_loader.flatten.convert import join, ref
from npd_loader.flatten.engine import (E, R, Table, columns, ext, flatten_resource, identifier, official_name, path)

RES = {"resourceType": "Practitioner", "id": "P1", "meta": {"lastUpdated": "2026-09-29T04:34:00Z"},
       "extension": [{"url": "a", "valueBoolean": True}, {"url": "n", "extension": [{"url": "x", "valueCode": "y"}]}],
       "identifier": [{"system": "s1", "value": "1"}, {"system": "npi", "value": "2"}, {"system": "npi", "value": "3"}],
       "name": [{"use": "maiden", "family": "S"}, {"use": "official", "family": "J", "given": ["A", "M"]}],
       "telecom": [{"system": "phone", "value": "1"}, "junk", {"value": "2"}],
       "alias": ["one", "two"]}
LIN = ("2026-09-29", "P1", 500, 501)


def test_path_and_helpers():
    assert path("meta.lastUpdated")(RES) == "2026-09-29T04:34:00Z"
    assert path("name[1].given[0]")(RES) == "A" and path("name[9].family")(RES) is None
    assert path("telecom[1].value")(RES) is None
    assert ext("a")(RES)["valueBoolean"] is True and ext("zzz")(RES) is None
    assert ext("x", inner=ext("n"))(RES)["valueCode"] == "y"
    assert identifier(("npi",))(RES) == "2"
    assert official_name(RES)["family"] == "J"
    assert official_name({"name": [{"family": "A"}, {"use": "x", "family": "B"}]})["family"] == "B"


def test_flatten_resource():
    t1 = Table("p", {"family": R(lambda r: (official_name(r) or {}).get("family")),
                     "given": R(lambda r: (official_name(r) or {}).get("given"), join)})
    t2 = Table("p_telecom", {"system": E("system"), "value": E("value")}, each="telecom")
    t3 = Table("p_alias", {"alias": E(lambda e: e)}, each="alias")
    t4 = Table("ident", {"value": E("value")}, each="identifier", with_type=True)
    assert columns(t2) == ["release_date", "resource_id", "ndjson_file_id", "zst_file_id", "seq", "system", "value"]
    assert columns(t4)[4:6] == ["resource_type", "seq"]
    rows = list(flatten_resource(RES, [t1, t2, t3, t4], LIN))
    assert rows[0] == ("p", LIN + ("J", "A M"))
    assert rows[1:4] == [("p_telecom", LIN + (1, "phone", "1")), ("p_telecom", LIN + (2, None, None)),
                         ("p_telecom", LIN + (3, None, "2"))]
    assert ("p_alias", LIN + (2, "two")) in rows
    assert ("ident", LIN + ("Practitioner", 1, "1")) in rows
    assert ref(path("x")({"x": "Org/O-1"})) == "O-1"
```

- [ ] **Step 2: Run to verify failure**

Run: `.\.venv\Scripts\python -m pytest tests/test_flatten_engine.py -v`
Expected: FAIL, `ModuleNotFoundError: No module named 'npd_loader.flatten.engine'`.

- [ ] **Step 3: Implement `src/npd_loader/flatten/engine.py`**

```python
"""Declarative table specs and the generic flattener: one parsed FHIR resource -> rows for every table of its type."""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Callable, Iterator

Getter = Callable[[Any], Any]
_STEP = re.compile(r"([^.\[\]]+)|\[(\d+)\]")
LINEAGE = ("release_date", "resource_id", "ndjson_file_id", "zst_file_id")


def path(expr: str) -> Getter:
    steps = [(name, int(idx)) if idx else (name, None) for name, idx in _STEP.findall(expr)]

    def get(obj: Any) -> Any:
        for name, idx in steps:
            if name:
                obj = obj.get(name) if isinstance(obj, dict) else None
            else:
                obj = obj[idx] if isinstance(obj, list) and len(obj) > idx else None
            if obj is None:
                return None
        return obj
    return get


@dataclass(frozen=True)
class Col:
    source: str                      # "r" (resource) or "e" (repeating element)
    get: Getter
    conv: Callable[[Any], Any] | None = None

    def value(self, res: Any, el: Any) -> Any:
        v = self.get(res if self.source == "r" else el)
        return self.conv(v) if self.conv else v


def _getter(get: str | Getter) -> Getter:
    return path(get) if isinstance(get, str) else get


def R(get: str | Getter, conv: Callable[[Any], Any] | None = None) -> Col:
    return Col("r", _getter(get), conv)


def E(get: str | Getter, conv: Callable[[Any], Any] | None = None) -> Col:
    return Col("e", _getter(get), conv)


def ext(url: str, inner: Getter | None = None) -> Getter:
    """First extension element with `url` on the object (or on `inner(object)` for nested extensions)."""
    def get(obj: Any) -> Any:
        if inner is not None:
            obj = inner(obj)
        for e in (obj.get("extension") or ()) if isinstance(obj, dict) else ():
            if isinstance(e, dict) and e.get("url") == url:
                return e
        return None
    return get


def identifier(systems: tuple[str, ...]) -> Getter:
    def get(res: Any) -> Any:
        for i in (res.get("identifier") or ()) if isinstance(res, dict) else ():
            if isinstance(i, dict) and i.get("system") in systems:
                return i.get("value")
        return None
    return get


def official_name(res: Any) -> dict | None:
    names = [n for n in (res.get("name") or ()) if isinstance(n, dict)] if isinstance(res, dict) else []
    if not names:
        return None
    rank = lambda p: (0 if p[1].get("use") == "official" else 1 if p[1].get("use") is not None else 2, p[0])
    return min(enumerate(names), key=rank)[1]


@dataclass(frozen=True)
class Table:
    name: str
    cols: dict[str, Col] = field(hash=False)
    each: str | None = None
    with_type: bool = False


def columns(t: Table) -> list[str]:
    return list(LINEAGE) + (["resource_type"] if t.with_type else []) + (["seq"] if t.each else []) + list(t.cols)


def flatten_resource(res: dict, tables: list[Table], lineage: tuple) -> Iterator[tuple[str, tuple]]:
    rtype = res.get("resourceType")
    for t in tables:
        head = lineage + ((rtype,) if t.with_type else ())
        if t.each is None:
            yield t.name, head + tuple(c.value(res, None) for c in t.cols.values())
        else:
            items = res.get(t.each)
            for i, el in enumerate(items if isinstance(items, list) else (), start=1):
                yield t.name, head + (i,) + tuple(c.value(res, el) for c in t.cols.values())
```

- [ ] **Step 4: Run the tests**

Run: `.\.venv\Scripts\python -m pytest tests/test_flatten_engine.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/npd_loader/flatten/engine.py tests/test_flatten_engine.py
git commit -m "feat(flatten): declarative table specs and the generic flattener"
```

---

### Task 4: The 26 table specs, checked against the golden values

**Files:**
- Create: `src/npd_loader/flatten/specs.py`
- Test: `tests/test_flatten_specs.py`

**Interfaces:**
- Consumes: Task 2 and Task 3 modules; `tests/golden_values.py` (Task 1).
- Produces (`npd_loader.flatten.specs`): `SPECS: dict[str, list[Table]]` (resource type → its own tables), `IDENTIFIER: Table`, `tables_for(resource_type) -> list[Table]` (its tables + `IDENTIFIER`), `ALL_TABLES: list[Table]` (all 26, each once), `TABLE_TYPES: dict[str, str | None]` (table → resource type; `identifier` → None).

- [ ] **Step 1: Write the failing test**

```python
from golden_values import load_golden, normalize
from npd_loader.flatten.engine import columns, flatten_resource
from npd_loader.flatten.specs import ALL_TABLES, SPECS, TABLE_TYPES, tables_for
from release_builder import build_release
import orjson


def flatten_fixture() -> dict[str, list[list]]:
    out: dict[str, list[list]] = {}
    ndjson = build_release("2026-09-29").ndjson
    for i, (name, data) in enumerate(sorted(ndjson.items())):
        fid, zid = 500 + 2 * i, 501 + 2 * i
        for line in data.splitlines():
            if not line.strip():
                continue
            res = orjson.loads(line)
            for table, values in flatten_resource(res, tables_for(res["resourceType"]),
                                                  ("2026-09-29", res["id"], fid, zid)):
                out.setdefault(table, []).append([normalize(v) for v in values])
    return out


def test_specs_cover_every_table_once():
    assert len(ALL_TABLES) == 26 and len({t.name for t in ALL_TABLES}) == 26
    assert set(SPECS) == {"Practitioner", "Organization", "Location", "Endpoint", "PractitionerRole",
                          "OrganizationAffiliation", "HealthcareService", "InsurancePlan"}
    assert TABLE_TYPES["identifier"] is None and TABLE_TYPES["practitioner_role_code"] == "PractitionerRole"


def test_columns_match_the_phase1_tables():
    golden = load_golden()
    for t in ALL_TABLES:
        assert columns(t) == golden[f"{t.name}#columns"], t.name


def test_flattened_fixture_equals_phase1_output():
    golden, got = load_golden(), flatten_fixture()
    for t in ALL_TABLES:
        rows = sorted(got.get(t.name, []), key=lambda r: [x or "" for x in r])
        assert rows == golden[t.name], t.name
```

- [ ] **Step 2: Run to verify failure**

Run: `.\.venv\Scripts\python -m pytest tests/test_flatten_specs.py -v`
Expected: FAIL, `ModuleNotFoundError: No module named 'npd_loader.flatten.specs'`.

- [ ] **Step 3: Implement `src/npd_loader/flatten/specs.py`**

```python
"""The 26 npd tables as declarative specs (the Phase 1 T-SQL transforms, one for one)."""
from __future__ import annotations

from npd_loader.flatten.convert import boolean, join, number, ref, ts
from npd_loader.flatten.engine import E, R, Table, ext, identifier, official_name, path

NDH = "http://hl7.org/fhir/us/ndh/StructureDefinition/"
NPI = ("http://terminology.hl7.org/NamingSystem/npi", "http://hl7.org/fhir/sid/us-npi")
PSEUDO_EIN = ("https://npd.cms.gov/fhir/sid/us-pseudo-ein",)


def _name(field: str, conv=None):
    get = lambda r: (official_name(r) or {}).get(field)
    return R(get, conv)


def _code(getter):
    return lambda obj: path("valueCodeableConcept.coding[0].code")(getter(obj))


def _long_text_to_null(v):
    return None if isinstance(v, str) and len(v) > 4000 else v


LAST_UPDATED = {"last_updated": R("meta.lastUpdated", ts)}
TELECOM = {"system": E("system"), "use": E("use"), "value": E("value")}
ADDRESS = {"use": E("use"), "type": E("type"), "line1": E("line[0]"), "line2": E("line[1]"),
           "extra_lines": E(lambda e: join(e.get("line") if isinstance(e, dict) else None, ", ", 2)),
           "city": E("city"), "state": E("state"), "postal_code": E("postalCode"), "country": E("country")}
REF = {"reference": E("reference", ref)}
CODING = {"system": E("coding[0].system"), "code": E("coding[0].code"), "display": E("coding[0].display"),
          "text": E("text")}
VERIFICATION = R(_code(ext(NDH + "base-ext-verification-status")))
NETWORK_REF = R(lambda r: ref(path("valueReference.reference")(ext(NDH + "base-ext-network-reference")(r))))


def _ref_table(name: str, each: str, column: str) -> Table:
    return Table(name, {column: E("reference", ref)}, each=each)


SPECS: dict[str, list[Table]] = {
    "Practitioner": [
        Table("practitioner", {
            **LAST_UPDATED,
            "npi": R(identifier(NPI)),
            "active": R("active", boolean),
            "gender": R("gender"),
            "name_family": _name("family"),
            "name_given": _name("given", join),
            "name_prefix": _name("prefix", join),
            "name_suffix": _name("suffix", join),
            "identity_verified": R(lambda r: path("valueBoolean")(ext(NDH + "base-ext-cms-identity-verified")(r)), boolean),
            "medicare_enrolled": R(lambda r: path("valueBoolean")(ext(NDH + "base-ext-cms_medicare_enrollment")(r)), boolean),
            "in_hhs_exclusion_list": R(lambda r: path("valueBoolean")(ext(NDH + "base-ext-hhs-in-exclusion-list")(r)), boolean),
            "aligned_with_data_network": R(lambda r: path("valueBoolean")(ext(NDH + "base-ext-cms_aligned_with_data_network")(r)), boolean),
        }),
        Table("practitioner_name", {
            "use": E("use"), "family": E("family"), "given": E("given", join), "prefix": E("prefix", join),
            "suffix": E("suffix", join), "period_start": E("period.start", ts), "period_end": E("period.end", ts),
        }, each="name"),
        Table("practitioner_address", ADDRESS, each="address"),
        Table("practitioner_telecom", TELECOM, each="telecom"),
        Table("practitioner_qualification", {
            "code_system": E("code.coding[0].system"), "code": E("code.coding[0].code"),
            "code_display": E("code.coding[0].display"), "code_text": E("code.text"),
            "identifier_value": E("identifier[0].value"),
            "identifier_type_code": E("identifier[0].type.coding[0].code"),
            "issuer_organization_id": E("issuer.reference", ref),
        }, each="qualification"),
    ],
    "Organization": [
        Table("organization", {
            **LAST_UPDATED,
            "npi": R(identifier(NPI)), "pseudo_ein": R(identifier(PSEUDO_EIN)),
            "name": R("name"), "active": R("active", boolean),
            "type_code": R("type[0].coding[0].code"), "type_display": R("type[0].coding[0].display"),
            "part_of_organization_id": R("partOf.reference", ref),
            "verification_status": VERIFICATION,
        }),
        Table("organization_address", ADDRESS, each="address"),
        Table("organization_telecom", TELECOM, each="telecom"),
        _ref_table("organization_endpoint", "endpoint", "endpoint_id"),
    ],
    "Location": [
        Table("location", {
            **LAST_UPDATED,
            "status": R("status"), "name": R("name"), "description": R("description", _long_text_to_null),
            "mode": R("mode"),
            "address_use": R("address.use"), "address_type": R("address.type"),
            "line1": R("address.line[0]"), "line2": R("address.line[1]"),
            "extra_lines": R(lambda r: join(path("address.line")(r), ", ", 2)),
            "city": R("address.city"), "state": R("address.state"),
            "postal_code": R("address.postalCode"), "country": R("address.country"),
            "latitude": R("position.latitude", number), "longitude": R("position.longitude", number),
            "managing_organization_id": R("managingOrganization.reference", ref),
        }),
        Table("location_telecom", TELECOM, each="telecom"),
    ],
    "Endpoint": [
        Table("endpoint", {
            **LAST_UPDATED,
            "status": R("status"), "name": R("name"), "address": R("address"),
            "connection_type_system": R("connectionType.system"), "connection_type_code": R("connectionType.code"),
            "payload_type_system": R("payloadType[0].coding[0].system"),
            "payload_type_code": R("payloadType[0].coding[0].code"),
            "managing_organization_id": R("managingOrganization.reference", ref),
            "verification_status": VERIFICATION,
        }),
    ],
    "PractitionerRole": [
        Table("practitioner_role", {
            **LAST_UPDATED,
            "active": R("active", boolean),
            "practitioner_id": R("practitioner.reference", ref),
            "organization_id": R("organization.reference", ref),
            "period_start": R("period.start", ts), "period_end": R("period.end", ts),
            "network_organization_id": NETWORK_REF,
            "accepting_patients": R(_code(ext("acceptingPatients", inner=ext(NDH + "base-ext-newpatients")))),
        }),
        _ref_table("practitioner_role_endpoint", "endpoint", "endpoint_id"),
        _ref_table("practitioner_role_location", "location", "location_id"),
        Table("practitioner_role_specialty", CODING, each="specialty"),
        Table("practitioner_role_code", CODING, each="code"),
        Table("practitioner_role_telecom", TELECOM, each="telecom"),
    ],
    "OrganizationAffiliation": [
        Table("organization_affiliation", {
            **LAST_UPDATED,
            "active": R("active", boolean),
            "organization_id": R("organization.reference", ref),
            "participating_organization_id": R("participatingOrganization.reference", ref),
            "role_code": R("code[0].coding[0].code"), "role_display": R("code[0].coding[0].display"),
            "role_text": R("code[0].text"),
            "period_start": R("period.start", ts), "period_end": R("period.end", ts),
        }),
        _ref_table("organization_affiliation_network", "network", "network_organization_id"),
    ],
    "HealthcareService": [
        Table("healthcare_service", {
            **LAST_UPDATED,
            "active": R("active", boolean), "name": R("name"),
            "provided_by_organization_id": R("providedBy.reference", ref),
            "network_organization_id": NETWORK_REF,
        }),
        _ref_table("healthcare_service_location", "location", "location_id"),
    ],
    "InsurancePlan": [
        Table("insurance_plan", {
            **LAST_UPDATED,
            "status": R("status"), "name": R("name"),
            "type_code": R("type[0].coding[0].code"), "type_text": R("type[0].text"),
            "period_start": R("period.start", ts), "period_end": R("period.end", ts),
            "owned_by_organization_id": R("ownedBy.reference", ref),
            "administered_by_organization_id": R("administeredBy.reference", ref),
        }),
        Table("insurance_plan_alias", {"alias": E(lambda e: e if isinstance(e, str) else None)}, each="alias"),
        _ref_table("insurance_plan_network", "network", "network_organization_id"),
    ],
}

IDENTIFIER = Table("identifier", {
    "system": E("system"), "value": E("value"), "use": E("use"),
    "type_code": E("type.coding[0].code"), "type_text": E("type.text"),
    "period_start": E("period.start", ts), "period_end": E("period.end", ts),
}, each="identifier", with_type=True)

ALL_TABLES: list[Table] = [t for tables in SPECS.values() for t in tables] + [IDENTIFIER]
TABLE_TYPES: dict[str, str | None] = {t.name: rt for rt, tables in SPECS.items() for t in tables} | {"identifier": None}


def tables_for(resource_type: str) -> list[Table]:
    return SPECS.get(resource_type, []) + [IDENTIFIER]
```

If `test_flattened_fixture_equals_phase1_output` shows differences, compare against the Phase 1 script for that table
(`src/npd_loader/sql/mssql/transform/0x0_*.sql` on this branch until Task 10) and fix the spec — the golden file is the
reference. The only allowed deviation is extension lookups taking the first match (Phase 1 Task 15 used MAX); the
fixture has no duplicate extensions, so the golden values agree.

- [ ] **Step 4: Run the tests**

Run: `.\.venv\Scripts\python -m pytest tests/test_flatten_specs.py -v`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add src/npd_loader/flatten/specs.py tests/test_flatten_specs.py
git commit -m "feat(flatten): the 26 table specs, equal to the Phase 1 transform output on the fixture"
```

---

### Task 5: Stage files: streaming worker, chunked writers, hashes

**Files:**
- Create: `src/npd_loader/flatten/stagefiles.py`
- Modify: `src/npd_loader/storage.py` (add `LocalStorage.local_path(rel) -> str` if it does not exist: absolute path of `rel` under the root)
- Test: `tests/test_flatten_stagefiles.py`

**Interfaces:**
- Consumes: `npd_loader.raw_load.NdjsonInput` (file_id, zst_file_id, rel_path, resource_type, name) and `iter_lines`; specs (Task 4).
- Produces (`npd_loader.flatten.stagefiles`):
  - `FIELD = "\x1f"`, `ROW = "\x1e"`, `HASH_TABLE = "resource_hash"`, `HASH_COLUMNS = ("resource_type", "resource_id", "hash", "last_updated", "release_date", "ndjson_file_id", "line_number")`.
  - `class FlattenError(Exception): (message, file_id)`.
  - `@dataclass StagedFile(table: str, path: str, rows: int)`.
  - `@dataclass FlattenResult(files: list[StagedFile], rows: dict[str, int], resources: dict[str, int])` (`rows` per table incl. `resource_hash`; `resources` per resource type).
  - `flatten_file(inp: NdjsonInput, src_path: str, release: str, out_dir: str, chunk_rows: int = 500_000) -> FlattenResult`.
  - `flatten_files(inputs: list[NdjsonInput], storage: LocalStorage, release: date, out_dir: str, workers: int, chunk_rows: int = 500_000) -> FlattenResult` (one process per file, at most `workers`).

- [ ] **Step 1: Write the failing tests**

```python
import orjson
import pytest

from npd_loader.flatten.stagefiles import FIELD, ROW, FlattenError, flatten_file, flatten_files
from npd_loader.raw_load import NdjsonInput
from npd_loader.storage import LocalStorage
from release_builder import build_release
import fixture_data


def write(tmp_path, name, data: bytes, fid=500):
    p = tmp_path / name
    p.write_bytes(data)
    return NdjsonInput(file_id=fid, zst_file_id=fid + 1, rel_path=name, resource_type=name.split("-", 1)[1].split(".")[0],
                       name=name), str(p)


def read_rows(path):
    text = open(path, encoding="utf-8").read()
    return [r.split(FIELD) for r in text.split(ROW) if r]


def test_flatten_file_writes_rows_and_hashes(tmp_path):
    inp, src = write(tmp_path, "06-Practitioner.ndjson", build_release("2026-09-29").ndjson["06-Practitioner.ndjson"])
    res = flatten_file(inp, src, "2026-09-29", str(tmp_path / "out"), chunk_rows=2)
    assert res.resources == {"Practitioner": 2}
    assert res.rows["practitioner"] == 2 and res.rows["resource_hash"] == 2 and res.rows["practitioner_name"] == 3
    chunks = [f for f in res.files if f.table == "practitioner_name"]
    assert [f.rows for f in chunks] == [2, 1]                       # rotates every chunk_rows rows
    h = read_rows([f for f in res.files if f.table == "resource_hash"][0].path)[0]
    assert h[0] == "Practitioner" and len(h[2]) == 40 and h[4] == "2026-09-29" and h[6] == "1"
    p = read_rows([f for f in res.files if f.table == "practitioner"][0].path)[0]
    assert p[1] == "Practitioner-1003000100" and p[6] == "1" and p[10] == ""   # active=1, prefix NULL -> empty


def test_bad_input_names_file_and_line(tmp_path):
    good = orjson.dumps(fixture_data.ORG1) + b"\n"
    for data, msg in [(good + b"{nope\n", "line 2: invalid JSON"),
                      (good + orjson.dumps({**fixture_data.ORG1, "resourceType": "Location"}) + b"\n", "line 2: resourceType"),
                      (orjson.dumps({**fixture_data.ORG1, "name": "A\x1fB"}) + b"\n", "line 1: .*0x1F"),
                      (orjson.dumps({**fixture_data.ORG1, "active": "yes"}) + b"\n", "line 1: .*not a boolean")]:
        inp, src = write(tmp_path, "01-Organization.ndjson", data)
        with pytest.raises(FlattenError, match=msg) as e:
            flatten_file(inp, src, "2026-09-29", str(tmp_path / "out"))
        assert e.value.file_id == 500


def test_flatten_files_in_parallel(tmp_path):
    storage = LocalStorage(tmp_path / "data")
    inputs = []
    for i, (name, data) in enumerate(sorted(build_release("2026-09-29").ndjson.items())):
        with storage.open_write(name) as f:
            f.write(data)
        inputs.append(NdjsonInput(500 + 2 * i, 501 + 2 * i, name, name.split("-", 1)[1].split(".")[0], name))
    from datetime import date
    res = flatten_files(inputs, storage, date(2026, 9, 29), str(tmp_path / "out"), workers=2)
    assert sum(res.resources.values()) == 12 and res.rows["resource_hash"] == 12
    assert res.rows["identifier"] > 0 and res.rows["practitioner_role"] > 0
```

- [ ] **Step 2: Run to verify failure**

Run: `.\.venv\Scripts\python -m pytest tests/test_flatten_stagefiles.py -v`
Expected: FAIL, `ModuleNotFoundError`.

- [ ] **Step 3: Implement `src/npd_loader/flatten/stagefiles.py`**

```python
"""Stream one .ndjson: parse each line once (orjson), validate, hash, flatten with its type's specs, and write
chunked stage files (bcp -c, field 0x1F, row 0x1E) per table plus one resource_hash row per resource."""
from __future__ import annotations

import hashlib
import multiprocessing as mp
import os
from dataclasses import dataclass, field
from datetime import date

import orjson

from npd_loader.flatten.convert import ConvertError, ts
from npd_loader.flatten.engine import flatten_resource
from npd_loader.flatten.specs import tables_for
from npd_loader.raw_load import NdjsonInput, RawLoadError, iter_lines

FIELD, ROW = "\x1f", "\x1e"
HASH_TABLE = "resource_hash"
HASH_COLUMNS = ("resource_type", "resource_id", "hash", "last_updated", "release_date", "ndjson_file_id", "line_number")


class FlattenError(Exception):
    def __init__(self, message: str, file_id: int | None = None):
        super().__init__(message)
        self.file_id = file_id


@dataclass
class StagedFile:
    table: str
    path: str
    rows: int


@dataclass
class FlattenResult:
    files: list[StagedFile] = field(default_factory=list)
    rows: dict[str, int] = field(default_factory=dict)
    resources: dict[str, int] = field(default_factory=dict)

    def merge(self, other: "FlattenResult") -> None:
        self.files += other.files
        for k, v in other.rows.items():
            self.rows[k] = self.rows.get(k, 0) + v
        for k, v in other.resources.items():
            self.resources[k] = self.resources.get(k, 0) + v


def encode(v) -> str:
    if v is None:
        return ""
    s = v if isinstance(v, str) else str(v)
    if FIELD in s or ROW in s:
        raise ConvertError("value contains a 0x1F/0x1E control character")
    return s


class _Writer:
    """One table's stage files: rotates to a new file every chunk_rows rows."""

    def __init__(self, table: str, out_dir: str, prefix: str, chunk_rows: int):
        self.table, self.out_dir, self.prefix, self.chunk_rows = table, out_dir, prefix, chunk_rows
        self.files: list[StagedFile] = []
        self._fh = None
        self._rows = 0

    def write(self, values: tuple) -> None:
        if self._fh is None:
            path = os.path.join(self.out_dir, f"{self.table}.{self.prefix}.{len(self.files):04d}.dat")
            self._fh = open(path, "w", encoding="utf-8", newline="", buffering=1 << 20)
            self.files.append(StagedFile(self.table, path, 0))
            self._rows = 0
        self._fh.write(FIELD.join(encode(v) for v in values))
        self._fh.write(ROW)
        self._rows += 1
        self.files[-1].rows = self._rows
        if self._rows >= self.chunk_rows:
            self.close()

    def close(self) -> None:
        if self._fh is not None:
            self._fh.close()
            self._fh = None


def flatten_file(inp: NdjsonInput, src_path: str, release: str, out_dir: str,
                 chunk_rows: int = 500_000) -> FlattenResult:
    os.makedirs(out_dir, exist_ok=True)
    tables = tables_for(inp.resource_type)
    prefix = f"f{inp.file_id}"
    writers = {t.name: _Writer(t.name, out_dir, prefix, chunk_rows) for t in tables}
    writers[HASH_TABLE] = _Writer(HASH_TABLE, out_dir, prefix, chunk_rows)
    resources = 0
    number = 0
    try:
        with open(src_path, "rb") as f:
            for number, text in iter_lines(f):
                try:
                    res = orjson.loads(text)
                except orjson.JSONDecodeError as exc:
                    raise FlattenError(f"{inp.name}: line {number}: invalid JSON: {exc}", inp.file_id) from exc
                if not isinstance(res, dict):
                    raise FlattenError(f"{inp.name}: line {number}: not a JSON object", inp.file_id)
                if res.get("resourceType") != inp.resource_type:
                    raise FlattenError(f"{inp.name}: line {number}: resourceType {res.get('resourceType')!r}, "
                                       f"expected {inp.resource_type!r}", inp.file_id)
                rid = res.get("id")
                if not isinstance(rid, str) or not rid:
                    raise FlattenError(f"{inp.name}: line {number}: missing id", inp.file_id)
                try:
                    for table, values in flatten_resource(res, tables, (release, rid, inp.file_id, inp.zst_file_id)):
                        writers[table].write(values)
                    writers[HASH_TABLE].write((inp.resource_type, rid, hashlib.sha1(text.encode("utf-8")).hexdigest(),
                                               ts((res.get("meta") or {}).get("lastUpdated")), release,
                                               inp.file_id, number))
                except ConvertError as exc:
                    raise FlattenError(f"{inp.name}: line {number}: {exc}", inp.file_id) from exc
                resources += 1
    except RawLoadError as exc:                        # iter_lines: blank line in the middle, not UTF-8
        raise FlattenError(f"{inp.name}: {exc}", inp.file_id) from exc
    except OSError as exc:
        raise FlattenError(f"{inp.name}: cannot read {src_path}: {exc}", inp.file_id) from exc
    finally:
        for w in writers.values():
            w.close()
    result = FlattenResult(resources={inp.resource_type: resources})
    for name, w in writers.items():
        result.files += [f for f in w.files if f.rows]
        result.rows[name] = sum(f.rows for f in w.files)
    return result


def _job(args):
    return flatten_file(*args)


def flatten_files(inputs: list[NdjsonInput], storage, release: date, out_dir: str, workers: int,
                  chunk_rows: int = 500_000) -> FlattenResult:
    jobs = [(inp, storage.local_path(inp.rel_path), release.isoformat(), out_dir, chunk_rows) for inp in inputs]
    total = FlattenResult()
    if workers <= 1 or len(jobs) == 1:
        for job in jobs:
            total.merge(_job(job))
        return total
    with mp.get_context("spawn").Pool(min(workers, len(jobs))) as pool:
        for result in pool.imap_unordered(_job, jobs):
            total.merge(result)
    return total
```

Note on the error test with `\x1f`: `orjson` escapes control characters, so the parsed value contains a real `\x1f`;
`encode` raises `ConvertError("... 0x1F/0x1E ...")`, re-raised as `FlattenError` with the line — the test's regex
`line 1: .*0x1F` matches.

- [ ] **Step 4: Run the tests**

Run: `.\.venv\Scripts\python -m pytest tests/test_flatten_stagefiles.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/npd_loader/flatten/stagefiles.py src/npd_loader/storage.py tests/test_flatten_stagefiles.py
git commit -m "feat(flatten): streaming per-file worker writing chunked bcp stage files and resource hashes"
```

---

### Task 6: Phase 2 schema and config

**Files:**
- Modify: `src/npd_loader/config.py`, `config.example.toml`, `tests/test_config.py`, `tests/helpers.py`
- Modify: `src/npd_loader/sql/mssql/init/001_schemas.sql`, `src/npd_loader/sql/mssql/init/003_tables.sql`, `src/npd_loader/sql/mssql/init/900_migrations.sql`
- Delete: `src/npd_loader/sql/mssql/init/002_raw.sql`
- Modify: `src/npd_loader/dialect/mssql.py` (`_tokens`, `init_db`)
- Test: `tests/test_mssql_schema.py` (rewrite), `tests/conftest.py` (`mssql_dialect` fixture)

**Interfaces:**
- Produces: `NpdDbConfig(connection: str, schema: str = "npd", stage_schema: str = "npd_stage", lock_timeout_seconds: float = 30.0, flatten_workers: int = 4, bcp_workers: int = 8)`; a config with `raw_schema` raises `ConfigError` mentioning `stage_schema`; `stage_schema == schema` raises `ConfigError`.
- Produces: tables `npd.resource_state` and extended `npd.release` (see SQL below); permanent tables with clustered primary keys and no partitioning; the fixed staging tables `npd_stage.<table>` (same columns as `columns(t)`, heaps) and `npd_stage.resource_hash` (columns `HASH_COLUMNS`); `MssqlDialect._tokens()` = `schema`, `s:schema`, `stage_schema`, `s:stage_schema`; `MssqlDialect.init_db()` runs the init scripts, creates any missing staging table, and creates `v_<table>` pass-through views for every table in `ALL_TABLES`.
- Fixture `mssql_dialect` (tests/conftest.py): fresh `t<hex>` and `t<hex>_stage` schemas, `NpdDbConfig(connection="data", schema=data, stage_schema=stage, lock_timeout_seconds=2, flatten_workers=1, bcp_workers=4)`, `init_db()` run.

- [ ] **Step 1: Config** — in `config.py` replace `NpdDbConfig` with:

```python
@dataclass(frozen=True)
class NpdDbConfig:
    connection: str
    schema: str = "npd"
    stage_schema: str = "npd_stage"
    lock_timeout_seconds: float = 30.0   # how long the delta apply waits for a table lock (3 attempts)
    flatten_workers: int = 4             # parallel .ndjson files being flattened
    bcp_workers: int = 8                 # parallel bcp loads
```

and in `parse_config` build it with
`NpdDbConfig(connection=_req(npd, "npd_db", "connection"), **_optional({k: v for k, v in npd.items() if k != "connection"}, "npd_db", NpdDbConfig))`
after this check: `if "raw_schema" in npd: raise ConfigError("[npd_db] raw_schema was replaced by stage_schema (Phase 2 has no raw table)")`.
Range checks: `lock_timeout_seconds > 0`; `flatten_workers >= 1`; `bcp_workers >= 1`;
`schema != stage_schema` (each with a `ConfigError` naming the key). In `config.example.toml` `[npd_db]` replace
`raw_schema = "npd_raw"` with `stage_schema = "npd_stage"`. In `tests/helpers.py`
`config_data(...)`: `schemas` becomes `("npd_stage", "npd")` → `"npd_db": {"connection": "data", "stage_schema": schemas[0], "schema": schemas[1]}`.
Update `tests/test_config.py`: replace `cfg.npd_db.raw_schema == "npd_raw"` with `cfg.npd_db.stage_schema == "npd_stage"`;
the old `raw_schema == schema` test becomes `stage_schema == schema`; add:

```python
def test_raw_schema_key_is_rejected():
    data = minimal()
    data["npd_db"]["raw_schema"] = "npd_raw"
    with pytest.raises(ConfigError, match="stage_schema"):
        parse_config(data)


@pytest.mark.parametrize("key,value", [("flatten_workers", 0), ("bcp_workers", 0), ("lock_timeout_seconds", 0)])
def test_phase2_ranges(key, value):
    data = minimal()
    data["npd_db"][key] = value
    with pytest.raises(ConfigError, match=key):
        parse_config(data)
```

- [ ] **Step 2: Schema SQL.** `001_schemas.sql` becomes:

```sql
IF SCHEMA_ID(<<s:schema>>) IS NULL
BEGIN
    DECLARE @sql nvarchar(400) = N'CREATE SCHEMA ' + QUOTENAME(<<s:schema>>)
    EXEC (@sql)
END
GO
IF SCHEMA_ID(<<s:stage_schema>>) IS NULL
BEGIN
    DECLARE @sql nvarchar(400) = N'CREATE SCHEMA ' + QUOTENAME(<<s:stage_schema>>)
    EXEC (@sql)
END
GO
-- One row per applied release, written in the delta transaction.
IF OBJECT_ID(<<s:schema>> + N'.release', N'U') IS NULL
    CREATE TABLE <<schema>>.release (
        release_date        date         NOT NULL CONSTRAINT pk_release PRIMARY KEY,
        import_run_id       int          NOT NULL,
        published_at        datetime2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
        new_resources       int          NULL,
        changed_resources   int          NULL,
        unchanged_resources int          NULL,
        not_seen_resources  int          NULL
    )
GO
-- The current version of every resource: its content hash decides new/changed/unchanged. Resources missing from a
-- release are kept (aging data); last_seen_release is the last release that contained them.
IF OBJECT_ID(<<s:schema>> + N'.resource_state', N'U') IS NULL
    CREATE TABLE <<schema>>.resource_state (
        resource_type     varchar(40)  NOT NULL,
        resource_id       varchar(128) NOT NULL,
        hash              char(40)     NOT NULL,
        last_updated      datetime2(3) NULL,
        release_date      date         NOT NULL,   -- release whose content is current
        run_id            int          NOT NULL,
        last_seen_release date         NOT NULL,
        last_seen_run_id  int          NOT NULL,
        CONSTRAINT pk_resource_state PRIMARY KEY CLUSTERED (resource_type, resource_id)
    ) WITH (DATA_COMPRESSION = PAGE)
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(<<s:schema>> + N'.resource_state') AND name = N'resource_state_last_seen')
    CREATE INDEX resource_state_last_seen ON <<schema>>.resource_state (last_seen_release) WITH (DATA_COMPRESSION = PAGE)
GO
-- Fixed staging for resource hashes (the 26 table stages are created by init_db from the specs).
IF OBJECT_ID(<<s:stage_schema>> + N'.resource_hash', N'U') IS NULL
    CREATE TABLE <<stage_schema>>.resource_hash (
        resource_type  varchar(40)  NOT NULL,
        resource_id    varchar(128) NOT NULL,
        hash           char(40)     NOT NULL,
        last_updated   datetime2(3) NULL,
        release_date   date         NOT NULL,
        ndjson_file_id int          NOT NULL,
        line_number    bigint       NOT NULL
    )
GO
-- Phase 1 helper functions are no longer used.
DROP FUNCTION IF EXISTS <<schema>>.ref_id
GO
DROP FUNCTION IF EXISTS <<schema>>.fhir_ts
GO
DROP FUNCTION IF EXISTS <<schema>>.join_text
GO
DROP FUNCTION IF EXISTS <<schema>>.ext
GO
DROP FUNCTION IF EXISTS <<schema>>.identifier_value
```

`900_migrations.sql`: keep its comment block; replace the trailing `SELECT 1` with guarded column adds so a database
initialised by an earlier Phase 2 build gets the release counters:

```sql
IF COL_LENGTH(<<s:schema>> + N'.release', N'new_resources') IS NULL
    ALTER TABLE <<schema>>.release ADD new_resources int NULL, changed_resources int NULL,
        unchanged_resources int NULL, not_seen_resources int NULL
```

Delete `002_raw.sql`. Convert `003_tables.sql` with this one-off script (run once from the repo root, then commit the
result; do not commit the script):

```python
import re
from pathlib import Path

p = Path("src/npd_loader/sql/mssql/init/003_tables.sql")
s = p.read_text(encoding="utf-8")
s = s.replace(") ON <<ps:schema>> (release_date) WITH (DATA_COMPRESSION = PAGE)", ") WITH (DATA_COMPRESSION = PAGE)")


def unique_to_pk(m):
    table, cols = m.group("table"), [c.strip() for c in m.group("cols").split(",") if c.strip() != "release_date"]
    return (f"IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(<<s:schema>> + N'.{table}') "
            f"AND name = N'pk_{table}')\n    ALTER TABLE <<schema>>.{table} ADD CONSTRAINT pk_{table} PRIMARY KEY "
            f"CLUSTERED ({', '.join(cols)}) WITH (DATA_COMPRESSION = PAGE)")


s = re.sub(r"IF NOT EXISTS \(SELECT 1 FROM sys\.indexes WHERE object_id = OBJECT_ID\(<<s:schema>> \+ N'\.(?P<table>\w+)'\) "
           r"AND name = N'\w+_key'\)\n\s+CREATE UNIQUE INDEX \w+_key ON <<schema>>\.\w+ \((?P<cols>[^)]*)\) "
           r"WITH \(DATA_COMPRESSION = PAGE\)", unique_to_pk, s)
s = re.sub(r"(CREATE INDEX \w+ ON <<schema>>\.\w+ \()release_date, ", r"\1", s)
s = s.replace("-- Every table: release_date, resource_id", "-- Phase 2: one current dataset, no partitioning. Every table: release_date (release the row came from), resource_id")
assert "<<ps:" not in s and "_key ON" not in s and "(release_date," not in s
p.write_text(s, encoding="utf-8")
print("converted")
```

- [ ] **Step 3: Dialect init.** In `src/npd_loader/dialect/mssql.py` replace `_tokens` and `init_db`:

```python
    def _tokens(self) -> dict[str, str]:
        return {"schema": self.q(self.cfg.schema), "s:schema": self.lit(self.cfg.schema),
                "stage_schema": self.q(self.cfg.stage_schema), "s:stage_schema": self.lit(self.cfg.stage_schema)}

    def init_db(self) -> None:
        from npd_loader.flatten.engine import columns
        from npd_loader.flatten.specs import ALL_TABLES
        tokens = self._tokens()
        schema, stage = self.cfg.schema, self.cfg.stage_schema
        with self._autocommit() as conn:
            for name, text in sql_scripts("mssql", "init"):
                for batch in split_batches(render(text, tokens)):
                    conn.exec_driver_sql(batch)
            for t in ALL_TABLES:
                # fixed staging heap, same columns (in spec order) as the permanent table; created once, reused
                if conn.exec_driver_sql("SELECT OBJECT_ID(?, 'U')", (f"{stage}.{t.name}",)).scalar() is None:
                    cols = ", ".join(self.q(c) for c in columns(t))
                    conn.exec_driver_sql(f"SELECT TOP 0 {cols} INTO {self.q(stage, t.name)} FROM {self.q(schema, t.name)}")
                conn.exec_driver_sql(f"CREATE OR ALTER VIEW {self.q(schema, 'v_' + t.name)} AS "
                                     f"SELECT * FROM {self.q(schema, t.name)}")
```

A column added later to a permanent table (via `900_migrations.sql`) must be added to its staging table in the same
migration; `test_tables_have_spec_columns_and_primary_keys` checks both.

`published_releases()` stays as it is. (The Phase 1 methods are removed in Task 10.)

- [ ] **Step 4: Rewrite the schema test and fixture.** `tests/conftest.py` `mssql_dialect`:

```python
@pytest.fixture
def mssql_dialect(mssql_engine, mssql_schemas):
    from npd_loader.config import NpdDbConfig
    from npd_loader.dialect.mssql import MssqlDialect
    stage, data = mssql_schemas("stage", "")
    d = MssqlDialect(mssql_engine, NpdDbConfig(connection="data", schema=data, stage_schema=stage,
                                               lock_timeout_seconds=2, flatten_workers=1, bcp_workers=4),
                     sleep=lambda s: None)
    d.init_db()
    return d
```

`tests/test_mssql_schema.py`:

```python
from npd_loader.flatten.engine import columns
from npd_loader.flatten.specs import ALL_TABLES


def scalar(d, sql, *args):
    with d.engine.connect() as conn:
        return conn.exec_driver_sql(sql, args).scalar()


def test_tables_have_spec_columns_and_primary_keys(mssql_dialect):
    d = mssql_dialect
    with d.engine.connect() as conn:
        for t in ALL_TABLES:
            cols = [r[0] for r in conn.exec_driver_sql(
                "SELECT name FROM sys.columns WHERE object_id = OBJECT_ID(?) ORDER BY column_id",
                (f"{d.cfg.schema}.{t.name}",))]
            assert cols == columns(t), t.name
            assert conn.exec_driver_sql("SELECT count(*) FROM sys.key_constraints WHERE parent_object_id = OBJECT_ID(?) "
                                        "AND type = 'PK'", (f"{d.cfg.schema}.{t.name}",)).scalar() == 1, t.name
            stage_cols = [r[0] for r in conn.exec_driver_sql(
                "SELECT name FROM sys.columns WHERE object_id = OBJECT_ID(?) ORDER BY column_id",
                (f"{d.cfg.stage_schema}.{t.name}",))]
            assert stage_cols == columns(t), f"stage {t.name}"
    assert scalar(d, "SELECT count(*) FROM sys.tables WHERE schema_id = SCHEMA_ID(?)", d.cfg.stage_schema) == 27
    assert scalar(d, "SELECT count(*) FROM sys.partition_functions WHERE name LIKE ?", f"pf_{d.cfg.schema}%") == 0
    assert scalar(d, "SELECT count(*) FROM sys.views WHERE schema_id = SCHEMA_ID(?)", d.cfg.schema) == 26
    assert scalar(d, "SELECT count(*) FROM sys.objects WHERE schema_id = SCHEMA_ID(?) AND type IN ('FN','IF')",
                  d.cfg.schema) == 0
    for col in ("hash", "release_date", "run_id", "last_seen_release", "last_seen_run_id"):
        assert scalar(d, "SELECT COL_LENGTH(?, ?)", f"{d.cfg.schema}.resource_state", col) is not None
    assert scalar(d, "SELECT COL_LENGTH(?, 'not_seen_resources')", f"{d.cfg.schema}.release") is not None


def test_init_db_is_idempotent(mssql_dialect):
    mssql_dialect.init_db()
    mssql_dialect.init_db()
```

- [ ] **Step 5: Run the tests**

Run: `.\.venv\Scripts\python -m pytest tests/test_config.py tests/test_mssql_schema.py -v` (with `NPD_TEST_MSSQL_DB` set)
Expected: PASS. (Phase 1 SQL Server tests — raw load, transform, publish, e2e — now fail; they are removed in Task 10.
Run only the listed files in this task.)

- [ ] **Step 6: Commit**

```bash
git add -A src/npd_loader/config.py config.example.toml src/npd_loader/sql/mssql/init src/npd_loader/dialect/mssql.py tests/test_config.py tests/helpers.py tests/conftest.py tests/test_mssql_schema.py
git commit -m "feat(phase2): unpartitioned current-dataset schema, resource_state, fixed staging tables"
```

---

### Task 7: bcp loader and `stage_release` (fixed staging tables)

**Files:**
- Create: `src/npd_loader/dialect/bcp.py`
- Modify: `src/npd_loader/dialect/mssql.py`, `src/npd_loader/dialect/__init__.py`
- Test: `tests/test_mssql_stage.py`

**Interfaces:**
- Consumes: `flatten_files`, `FlattenResult`, `HASH_TABLE`, `FlattenError` (Task 5); `ALL_TABLES` (Task 4); the fixed staging tables from `init_db` (Task 6); `NdjsonInput`.
- Produces (`npd_loader.dialect.bcp`): `class BcpError(Exception)`; `bcp_target(engine) -> tuple[str, str]` (server, database from the ODBC connect string); `bcp_in(server, database, schema, table, path, expected_rows) -> int`.
- Produces (`npd_loader.dialect`): `@dataclass StageResult(rows: dict[str, int], resources: dict[str, int])`.
- Produces on `MssqlDialect`: `truncate_stage() -> None` (truncates all 27 staging tables); `stage_release(storage, release: date, run_id: int, inputs: list[NdjsonInput]) -> StageResult` (truncate, flatten, bcp, verify counts, duplicate check).

- [ ] **Step 1: Write the failing tests**

```python
from datetime import date

import orjson
import pytest

from npd_loader.flatten.stagefiles import FlattenError
from npd_loader.dialect.bcp import BcpError
from npd_loader.raw_load import NdjsonInput
from npd_loader.storage import LocalStorage
from release_builder import build_release
import fixture_data

R = date(2026, 9, 29)


def inputs_for(storage, ndjson):
    out = []
    for i, (name, data) in enumerate(sorted(ndjson.items())):
        with storage.open_write(name) as f:
            f.write(data)
        out.append(NdjsonInput(500 + 2 * i, 501 + 2 * i, name, name.split("-", 1)[1].split(".")[0], name))
    return out


def count(d, table):
    with d.engine.connect() as c:
        return c.exec_driver_sql(f"SELECT COUNT_BIG(*) FROM {d.q(d.cfg.stage_schema, table)}").scalar()


def test_stage_release_loads_every_table(mssql_dialect, tmp_path):
    d = mssql_dialect
    storage = LocalStorage(tmp_path / "data")
    res = d.stage_release(storage, R, 7, inputs_for(storage, build_release("2026-09-29").ndjson))
    assert sum(res.resources.values()) == 12
    assert count(d, "resource_hash") == 12
    assert count(d, "practitioner") == 2 and count(d, "identifier") == res.rows["identifier"]
    assert not list((tmp_path / "data" / "stage").rglob("*.dat"))          # stage files deleted after load


def test_each_import_starts_from_empty_staging(mssql_dialect, tmp_path):
    d = mssql_dialect
    storage = LocalStorage(tmp_path / "data")
    inputs = inputs_for(storage, build_release("2026-09-29").ndjson)
    d.stage_release(storage, R, 7, inputs)
    d.stage_release(storage, R, 8, inputs)                                   # e.g. after a killed run
    assert count(d, "resource_hash") == 12 and count(d, "practitioner") == 2


def test_duplicate_ids_name_the_file(mssql_dialect, tmp_path):
    d = mssql_dialect
    storage = LocalStorage(tmp_path / "data")
    line = orjson.dumps(fixture_data.ORG1) + b"\n"
    with pytest.raises(FlattenError, match="duplicate resource ids: Organization Organization-1336200294") as e:
        d.stage_release(storage, R, 7, inputs_for(storage, {"01-Organization.ndjson": line * 2}))
    assert e.value.file_id == 500


def test_too_long_value_fails_with_the_bcp_error(mssql_dialect, tmp_path):
    d = mssql_dialect
    storage = LocalStorage(tmp_path / "data")
    rec = {**fixture_data.ORG1, "address": [{"city": "x" * 1500}]}         # organization_address.city nvarchar(1000)
    with pytest.raises(BcpError, match="organization_address"):
        d.stage_release(storage, R, 7, inputs_for(storage, {"01-Organization.ndjson": orjson.dumps(rec) + b"\n"}))
```

- [ ] **Step 2: Run to verify failure**

Run: `.\.venv\Scripts\python -m pytest tests/test_mssql_stage.py -v`
Expected: FAIL (`ModuleNotFoundError: npd_loader.dialect.bcp`).

- [ ] **Step 3: Implement `src/npd_loader/dialect/bcp.py`**

```python
"""bcp.exe bulk loads of stage files (Windows auth). Success is judged by the output and row counts, never the exit
code alone: bcp exits 0 even when every row fails."""
from __future__ import annotations

import re
import subprocess
import urllib.parse

from sqlalchemy.engine import Engine

_COPIED = re.compile(r"(\d+) rows copied")


class BcpError(Exception):
    pass


def bcp_target(engine: Engine) -> tuple[str, str]:
    odbc = urllib.parse.unquote_plus(engine.url.query.get("odbc_connect", ""))
    parts = {k.strip().upper(): v.strip() for k, _, v in (p.partition("=") for p in odbc.split(";")) if k}
    return parts["SERVER"], parts["DATABASE"]


def bcp_in(server: str, database: str, schema: str, table: str, path: str, expected_rows: int) -> int:
    cmd = ["bcp", f"[{database}].[{schema}].[{table}]", "in", path, "-S", server, "-T", "-c", "-C", "65001",
           "-t", "0x1f", "-r", "0x1e", "-k", "-m", "1", "-b", "500000", "-h", "TABLOCK"]
    proc = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace")
    out = proc.stdout + proc.stderr
    m = _COPIED.search(out)
    copied = int(m.group(1)) if m else 0
    if proc.returncode != 0 or "Error" in out or copied != expected_rows:
        raise BcpError(f"bcp into {schema}.{table} from {path}: copied {copied} of {expected_rows} rows: {out[-600:].strip()}")
    return copied
```

Hex terminators (`-t 0x1f -r 0x1e`) are documented for bcp, and the spike proved `-r 0x0a` works. If the installed
bcp rejects them anyway, stop and report NEEDS_CONTEXT with the bcp output rather than switching formats.

- [ ] **Step 4: Implement `truncate_stage` and `stage_release` in `MssqlDialect`** (imports: `import shutil`,
`from concurrent.futures import ThreadPoolExecutor`, `from npd_loader.dialect import StageResult`,
`from npd_loader.dialect.bcp import bcp_in, bcp_target`, `from npd_loader.flatten.specs import ALL_TABLES`,
`from npd_loader.flatten.stagefiles import HASH_TABLE, FlattenError, flatten_files`). Add to
`src/npd_loader/dialect/__init__.py`:

```python
@dataclass
class StageResult:
    rows: dict[str, int]          # rows loaded per staging table (incl. resource_hash)
    resources: dict[str, int]     # resources per resource type
```

Methods:

```python
    def _stage_tables(self) -> list[str]:
        return [t.name for t in ALL_TABLES] + [HASH_TABLE]

    def truncate_stage(self) -> None:
        with self._autocommit() as conn:
            for table in self._stage_tables():
                conn.exec_driver_sql(f"TRUNCATE TABLE {self.q(self.cfg.stage_schema, table)}")

    def stage_release(self, storage, release: date, run_id: int, inputs: list) -> StageResult:
        """Empty the fixed staging tables, flatten the release's files in parallel and bcp them in. Holding the import
        lock, this is the only writer of staging; leftovers of a killed run are removed by the truncate."""
        s = self.cfg.stage_schema
        self.truncate_stage()
        out_dir = storage.local_path(f"stage/run_{run_id}")
        try:
            flat = flatten_files(inputs, storage, release, out_dir, self.cfg.flatten_workers)
            server, database = bcp_target(self.engine)
            with ThreadPoolExecutor(self.cfg.bcp_workers) as pool:
                futures = [pool.submit(bcp_in, server, database, s, f.table, f.path, f.rows) for f in flat.files]
                for fut in futures:
                    fut.result()
        finally:
            shutil.rmtree(out_dir, ignore_errors=True)
        with self.engine.connect() as conn:
            for table, expected in flat.rows.items():
                got = conn.exec_driver_sql(f"SELECT COUNT_BIG(*) FROM {self.q(s, table)}").scalar()
                if got != expected:
                    raise FlattenError(f"staging table {table}: flattened {expected} rows but loaded {got}")
            dups = conn.exec_driver_sql(
                f"SELECT TOP 20 resource_type, resource_id, MIN(ndjson_file_id), STRING_AGG(CAST(line_number AS "
                f"varchar(20)), ',') WITHIN GROUP (ORDER BY line_number) FROM {self.q(s, HASH_TABLE)} "
                f"GROUP BY resource_type, resource_id HAVING COUNT(*) > 1 ORDER BY 1, 2").fetchall()
        if dups:
            detail = "; ".join(f"{t} {i} at lines {lines}" for t, i, _, lines in dups)
            raise FlattenError(f"duplicate resource ids: {detail}", dups[0][2])
        return StageResult(flat.rows, flat.resources)
```

- [ ] **Step 5: Run the tests**

Run: `.\.venv\Scripts\python -m pytest tests/test_mssql_stage.py -v`
Expected: PASS (4 tests).

- [ ] **Step 6: Commit**

```bash
git add src/npd_loader/dialect/bcp.py src/npd_loader/dialect/mssql.py src/npd_loader/dialect/__init__.py tests/test_mssql_stage.py
git commit -m "feat(phase2): stage_release — truncate fixed staging, parallel flatten, bcp, count and duplicate checks"
```

---

### Task 8: `apply_delta` (upsert; aging data stays live)

**Files:**
- Modify: `src/npd_loader/dialect/mssql.py`, `src/npd_loader/dialect/__init__.py`
- Test: `tests/test_mssql_delta.py`

**Interfaces:**
- Consumes: `stage_release`, `_locked_transaction` (Phase 1), `ALL_TABLES`, `TABLE_TYPES`, `columns`, `HASH_TABLE`.
- Produces (`npd_loader.dialect`): `@dataclass DeltaResult(kinds: dict[str, dict[str, int]], inserted: dict[str, int], replaced: dict[str, int])` (`kinds[resource_type] = {"new":…, "changed":…, "unchanged":…, "not_seen":…}`; `replaced` = rows removed from a table because their resource changed).
- Produces on `MssqlDialect`: `apply_delta(release: date, run_id: int) -> DeltaResult` — classifies against `resource_state`, upserts in one transaction, updates `last_seen_*` for every resource in the release, writes `npd.release`, updates statistics (best effort). Never deletes a resource that is missing from the release.

- [ ] **Step 1: Write the failing tests**

```python
import copy
from datetime import date

from npd_loader.storage import LocalStorage
from release_builder import build_release
from test_mssql_stage import inputs_for
import fixture_data

R1, R2 = date(2026, 9, 29), date(2026, 10, 6)


def rows(d, sql, *args):
    with d.engine.connect() as c:
        return [tuple(r) for r in c.exec_driver_sql(sql, args).fetchall()]


def load(d, tmp_path, release, run_id, records=None):
    storage = LocalStorage(tmp_path / f"data{run_id}")
    rel = build_release(release.isoformat(), records=records)
    d.stage_release(storage, release, run_id, inputs_for(storage, rel.ndjson))
    return d.apply_delta(release, run_id)


def test_first_load_inserts_everything(mssql_dialect, tmp_path):
    d = mssql_dialect
    res = load(d, tmp_path, R1, 7)
    assert sum(k["new"] for k in res.kinds.values()) == 12
    assert rows(d, f"SELECT count(*) FROM {d.q(d.cfg.schema, 'resource_state')}") == [(12,)]
    assert rows(d, f"SELECT count(*) FROM {d.q(d.cfg.schema, 'practitioner')}") == [(2,)]
    assert rows(d, f"SELECT import_run_id, new_resources, not_seen_resources FROM {d.q(d.cfg.schema, 'release')}") == [(7, 12, 0)]


def test_second_release_upserts_and_keeps_aging_data(mssql_dialect, tmp_path):
    d = mssql_dialect
    load(d, tmp_path, R1, 7)
    records = copy.deepcopy(fixture_data.RECORDS)
    p = records["06-Practitioner.ndjson"][0]
    p["name"][0]["family"] = "GOMEZ-CHANGED"
    p["telecom"] = p["telecom"][:1]                                       # child rows shrink
    missing = records["08-OrganizationAffiliation.ndjson"].pop()          # not in the new release
    res = load(d, tmp_path, R2, 8, records)
    assert res.kinds["Practitioner"] == {"new": 0, "changed": 1, "unchanged": 1, "not_seen": 0}
    assert res.kinds["OrganizationAffiliation"]["not_seen"] == 1
    pid, sch = p["id"], d.cfg.schema
    assert rows(d, f"SELECT name_family FROM {d.q(sch, 'practitioner')} WHERE resource_id = ?", pid) == [("GOMEZ-CHANGED",)]
    assert rows(d, f"SELECT count(*) FROM {d.q(sch, 'practitioner_telecom')} WHERE resource_id = ?", pid) == [(1,)]
    # aging data stays live, with its last-seen release
    assert rows(d, f"SELECT count(*) FROM {d.q(sch, 'organization_affiliation')} WHERE resource_id = ?", missing["id"]) == [(1,)]
    assert rows(d, f"SELECT last_seen_release FROM {d.q(sch, 'resource_state')} WHERE resource_id = ?", missing["id"]) == [(R1,)]
    # content release vs last seen
    assert rows(d, f"SELECT release_date, last_seen_release FROM {d.q(sch, 'resource_state')} WHERE resource_id = ?",
                pid) == [(R2, R2)]
    unchanged = records["06-Practitioner.ndjson"][1]["id"]
    assert rows(d, f"SELECT release_date, last_seen_release FROM {d.q(sch, 'resource_state')} WHERE resource_id = ?",
                unchanged) == [(R1, R2)]
    assert rows(d, f"SELECT not_seen_resources FROM {d.q(sch, 'release')} WHERE release_date = ?", R2) == [(1,)]


def test_partial_file_deletes_nothing(mssql_dialect, tmp_path):
    d = mssql_dialect
    load(d, tmp_path, R1, 7)
    records = copy.deepcopy(fixture_data.RECORDS)
    records["01-Organization.ndjson"] = records["01-Organization.ndjson"][:1]      # truncated file
    res = load(d, tmp_path, R2, 8, records)
    assert res.kinds["Organization"]["not_seen"] == 1
    assert rows(d, f"SELECT count(*) FROM {d.q(d.cfg.schema, 'organization')}") == [(2,)]


def test_failure_inside_apply_rolls_back(mssql_dialect, tmp_path):
    d = mssql_dialect
    load(d, tmp_path, R1, 7)
    records = copy.deepcopy(fixture_data.RECORDS)
    records["06-Practitioner.ndjson"][0]["gender"] = "unknown"
    storage = LocalStorage(tmp_path / "data8")
    d.stage_release(storage, R2, 8, inputs_for(storage, build_release(R2.isoformat(), records=records).ndjson))
    with d._autocommit() as c:                                            # sabotage one staging table
        c.exec_driver_sql(f"EXEC sp_rename '{d.cfg.stage_schema}.practitioner_role', 'practitioner_role_x'")
    try:
        try:
            d.apply_delta(R2, 8)
            raise AssertionError("apply_delta should have failed")
        except Exception as exc:
            assert "practitioner_role" in str(exc)
        assert rows(d, f"SELECT gender FROM {d.q(d.cfg.schema, 'practitioner')} ORDER BY resource_id")[0] == ("male",)
        assert [r[0] for r in rows(d, f"SELECT release_date FROM {d.q(d.cfg.schema, 'release')}")] == [R1]
    finally:
        with d._autocommit() as c:
            c.exec_driver_sql(f"EXEC sp_rename '{d.cfg.stage_schema}.practitioner_role_x', 'practitioner_role'")
```

- [ ] **Step 2: Run to verify failure**

Run: `.\.venv\Scripts\python -m pytest tests/test_mssql_delta.py -v`
Expected: FAIL (`AttributeError: ... 'apply_delta'`).

- [ ] **Step 3: Implement.** Add to `src/npd_loader/dialect/__init__.py`:

```python
@dataclass
class DeltaResult:
    kinds: dict[str, dict[str, int]]   # resource type -> {"new", "changed", "unchanged", "not_seen"}
    inserted: dict[str, int]           # rows inserted per table
    replaced: dict[str, int]           # rows removed per table because their resource changed
```

In `MssqlDialect` (imports: `from npd_loader.dialect import DeltaResult`,
`from npd_loader.flatten.engine import columns`, `from npd_loader.flatten.specs import TABLE_TYPES`):

```python
    KINDS = {"N": "new", "C": "changed", "U": "unchanged"}

    def apply_delta(self, release: date, run_id: int) -> DeltaResult:
        """Upsert the staged release: replace the rows of changed resources, insert new ones, mark every resource of
        the release as seen. Resources missing from the release are kept (aging data)."""
        s, schema = self.cfg.stage_schema, self.cfg.schema
        hashes, state = self.q(s, HASH_TABLE), self.q(schema, "resource_state")
        delta = self.q(s, "delta")
        with self._autocommit() as conn:
            conn.exec_driver_sql(f"DROP TABLE IF EXISTS {delta}")
            conn.exec_driver_sql(
                f"SELECT h.resource_type, h.resource_id, h.hash, h.last_updated, "
                f"CAST(CASE WHEN st.resource_id IS NULL THEN 'N' WHEN st.hash <> h.hash THEN 'C' ELSE 'U' END AS char(1)) AS kind "
                f"INTO {delta} FROM {hashes} h LEFT JOIN {state} st "
                f"ON st.resource_type = h.resource_type AND st.resource_id = h.resource_id")
            conn.exec_driver_sql(f"CREATE UNIQUE CLUSTERED INDEX ux_delta ON {delta} (resource_type, resource_id)")
            kinds: dict[str, dict[str, int]] = {}
            for rtype, kind, n in conn.exec_driver_sql(
                    f"SELECT resource_type, kind, COUNT_BIG(*) FROM {delta} GROUP BY resource_type, kind"):
                kinds.setdefault(rtype, {"new": 0, "changed": 0, "unchanged": 0, "not_seen": 0})[self.KINDS[kind]] = n
            for rtype, n in conn.exec_driver_sql(
                    f"SELECT st.resource_type, COUNT_BIG(*) FROM {state} st WHERE NOT EXISTS (SELECT 1 FROM {delta} d "
                    f"WHERE d.resource_type = st.resource_type AND d.resource_id = st.resource_id) "
                    f"GROUP BY st.resource_type"):
                kinds.setdefault(rtype, {"new": 0, "changed": 0, "unchanged": 0, "not_seen": 0})["not_seen"] = n

        def work(conn) -> tuple[dict[str, int], dict[str, int]]:
            inserted, replaced = {}, {}
            for t in ALL_TABLES:
                target, staged = self.q(schema, t.name), self.q(s, t.name)
                rtype = TABLE_TYPES[t.name]
                match = "d.resource_type = x.resource_type" if rtype is None else f"d.resource_type = '{rtype}'"
                replaced[t.name] = conn.exec_driver_sql(
                    f"DELETE x FROM {target} x JOIN {delta} d ON d.resource_id = x.resource_id AND {match} "
                    f"AND d.kind = 'C'").rowcount
                cols = ", ".join(self.q(c) for c in columns(t))
                inserted[t.name] = conn.exec_driver_sql(
                    f"INSERT INTO {target} WITH (TABLOCK) ({cols}) SELECT {cols} FROM {staged} x WHERE EXISTS "
                    f"(SELECT 1 FROM {delta} d WHERE d.resource_id = x.resource_id AND {match} "
                    f"AND d.kind IN ('N', 'C'))").rowcount
            conn.exec_driver_sql(
                f"MERGE {state} AS st USING {delta} AS d "
                f"ON st.resource_type = d.resource_type AND st.resource_id = d.resource_id "
                f"WHEN MATCHED AND d.kind = 'C' THEN UPDATE SET hash = d.hash, last_updated = d.last_updated, "
                f"release_date = ?, run_id = ?, last_seen_release = ?, last_seen_run_id = ? "
                f"WHEN MATCHED THEN UPDATE SET last_seen_release = ?, last_seen_run_id = ? "
                f"WHEN NOT MATCHED THEN INSERT (resource_type, resource_id, hash, last_updated, release_date, run_id, "
                f"last_seen_release, last_seen_run_id) VALUES (d.resource_type, d.resource_id, d.hash, d.last_updated, "
                f"?, ?, ?, ?);",
                (release, run_id, release, run_id, release, run_id, release, run_id, release, run_id))
            totals = {k: sum(v[k] for v in kinds.values()) for k in ("new", "changed", "unchanged", "not_seen")}
            conn.exec_driver_sql(
                f"MERGE {self.q(schema, 'release')} AS t USING (SELECT CAST(? AS date) AS release_date) AS s "
                f"ON t.release_date = s.release_date "
                f"WHEN MATCHED THEN UPDATE SET import_run_id = ?, published_at = SYSUTCDATETIME(), new_resources = ?, "
                f"changed_resources = ?, unchanged_resources = ?, not_seen_resources = ? "
                f"WHEN NOT MATCHED THEN INSERT (release_date, import_run_id, new_resources, changed_resources, "
                f"unchanged_resources, not_seen_resources) VALUES (s.release_date, ?, ?, ?, ?, ?);",
                (release, run_id, totals["new"], totals["changed"], totals["unchanged"], totals["not_seen"],
                 run_id, totals["new"], totals["changed"], totals["unchanged"], totals["not_seen"]))
            return inserted, replaced

        inserted, replaced = self._locked_transaction(work, f"apply release {release}")
        log.info("applied release %s: %s", release, {t: k for t, k in sorted(kinds.items())})
        with self._autocommit() as conn:            # best effort: the delta is already committed
            for t in ALL_TABLES:
                if inserted.get(t.name) or replaced.get(t.name):
                    try:
                        conn.exec_driver_sql(f"UPDATE STATISTICS {self.q(schema, t.name)}")
                    except Exception as exc:
                        log.warning("UPDATE STATISTICS %s.%s failed: %s", schema, t.name, exc)
        return DeltaResult(kinds, inserted, replaced)
```

`npd_stage.delta` is a scratch table rebuilt by each apply (it is not one of the 27 staging tables `truncate_stage`
handles, and `init_db` does not create it).

- [ ] **Step 4: Run the tests**

Run: `.\.venv\Scripts\python -m pytest tests/test_mssql_delta.py tests/test_mssql_stage.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/npd_loader/dialect/mssql.py src/npd_loader/dialect/__init__.py tests/test_mssql_delta.py
git commit -m "feat(phase2): apply_delta — classify by hash, one-transaction upsert, aging data kept with last_seen"
```

---

### Task 9: Rewire the import, retention and CLI; Phase 2 Dialect protocol

**Files:**
- Modify: `src/npd_loader/import_stage.py`, `src/npd_loader/retention.py`, `src/npd_loader/dialect/__init__.py`, `src/npd_loader/cli.py`
- Test: `tests/test_mssql_import.py` (new), `tests/test_retention.py` (rewrite), `tests/test_dialect_factory.py` (update)

**Interfaces:**
- Consumes: `stage_release`, `apply_delta`, `published_releases`, `StageResult`, `DeltaResult`, `FlattenError`.
- Produces: Phase 2 `Dialect` protocol:

```python
class Dialect(Protocol):
    name: str
    cfg: NpdDbConfig

    def init_db(self) -> None: ...
    def run_lock(self, stage: str) -> ContextManager[bool]: ...
    def published_releases(self) -> list[date]: ...
    def stage_release(self, storage: "Storage", release: date, run_id: int, inputs: "list[NdjsonInput]") -> StageResult: ...
    def apply_delta(self, release: date, run_id: int) -> DeltaResult: ...
```

`dialect_for`: a `postgresql` engine raises `ConfigError("Phase 2 supports SQL Server only; the Postgres loader is on main")`
(import `ConfigError` from `npd_loader.config`). Remove `PublishConflict`, `TransformResult`, `LockUnavailable` stays (used by `_locked_transaction`).

- [ ] **Step 1: Write the failing tests**

`tests/test_mssql_import.py`:

```python
import copy
from datetime import date

import pytest

from npd_loader.download import run_download
from npd_loader.import_stage import run_import
from npd_loader.stages import Outcome, StageFailed
from fakes import FakeCatalog
from helpers import make_ctx
from release_builder import build_release
import fixture_data


@pytest.fixture
def ctx(tmp_path, cms, mssql_dialect):
    return make_ctx(tmp_path, cms, catalog=FakeCatalog(), dialect=mssql_dialect)


def test_import_flattens_and_applies(ctx, cms):
    cms.publish(build_release("2026-09-29"))
    run_download(ctx)
    assert run_import(ctx) is Outcome.SUCCESS
    imp = [r for r in ctx.catalog.runs.values() if r["run_class"] == "IMPORT"][0]
    assert '<delta resource_type="Practitioner" new="2"' in imp["output_xml"]
    assert ctx.dialect.published_releases() == [date(2026, 9, 29)]
    assert run_import(ctx) is Outcome.SKIPPED


def test_older_release_is_refused_without_force(ctx, cms):
    cms.publish(build_release("2026-10-06"))
    run_download(ctx)
    run_import(ctx)
    cms.publish(build_release("2026-09-29"))
    run_download(ctx)
    with pytest.raises(StageFailed, match="older than the current release 2026-10-06"):
        run_import(ctx, release=date(2026, 9, 29))
    assert run_import(ctx, release=date(2026, 9, 29), force=True) is Outcome.SUCCESS


def test_killed_run_leftovers_are_never_applied_and_run_closed(ctx, cms):
    cms.publish(build_release("2026-09-29"))
    run_download(ctx)
    d = ctx.dialect
    with d._autocommit() as c:                                     # a killed run left a stale staged practitioner
        c.exec_driver_sql(f"INSERT INTO {d.q(d.cfg.stage_schema, 'practitioner')} (release_date, resource_id, "
                          f"ndjson_file_id, zst_file_id) VALUES ('2026-01-01', 'Practitioner-STALE', 1, 2)")
    open_run = ctx.catalog.start_run("IMPORT", "killed", "<WAREHOUSE_RUN_CONFIG />")
    assert run_import(ctx) is Outcome.SUCCESS
    with d.engine.connect() as c:
        assert c.exec_driver_sql(f"SELECT count(*) FROM {d.q(d.cfg.schema, 'practitioner')} "
                                 f"WHERE resource_id = 'Practitioner-STALE'").scalar() == 0
    assert ctx.catalog.runs[open_run.id]["status"] == "Failed"


def test_flatten_error_is_recorded_on_the_file(ctx, cms):
    bad = build_release("2026-09-29", raw_ndjson={"01-Organization.ndjson": b'{"resourceType": "Organization"}\n'})
    cms.publish(bad)
    run_download(ctx)
    with pytest.raises(StageFailed, match="missing id"):
        run_import(ctx)
    org = [f for f in ctx.catalog.files.values() if (f.get("file_name") or "").endswith("01-Organization.ndjson")]
    assert org and "missing id" in (org[0].get("exceptions") or "")
    assert ctx.dialect.published_releases() == []
```

(`make_ctx` gained `dialect=` in Phase 1; FakeCatalog stores files under `.files` with the keys used above — check
`tests/fakes.py` and adapt the two lookups to its actual structure if they differ.)

`tests/test_retention.py` (replace the Postgres-based file):

```python
from datetime import date, timedelta

from npd_loader.retention import apply_retention
from fakes import FakeCatalog
from helpers import make_ctx

BASE = date(2026, 8, 4)


def seed(ctx, release):
    run = ctx.catalog.add_successful_run("EXTRACT", release)
    rel = f"run_{release}/file_06-Practitioner.ndjson"
    ctx.catalog.add_data_file(run, file_type="ndjson", source_version_num=release.isoformat(), file_rel_path=rel,
                              file_hash="x")
    with ctx.storage.open_write(rel) as f:
        f.write(b"data")
    return rel


def test_keeps_ndjson_of_newest_releases_only(tmp_path, cms):
    ctx = make_ctx(tmp_path, cms, catalog=FakeCatalog(), keep_releases=2)
    rels = {BASE + timedelta(weeks=k): seed(ctx, BASE + timedelta(weeks=k)) for k in range(4)}
    newest = max(rels)
    assert apply_retention(ctx, newest) == []
    kept = {r for r, p in rels.items() if ctx.storage.exists(p)}
    assert kept == set(sorted(rels)[-2:])
```

- [ ] **Step 2: Run to verify failure**

Run: `.\.venv\Scripts\python -m pytest tests/test_mssql_import.py tests/test_retention.py -v`
Expected: FAIL.

- [ ] **Step 3: Implement.** `src/npd_loader/import_stage.py` — replace imports of `PublishConflict`, `RAW_PARENT`,
`RawLoadError` with `from npd_loader.dialect import DeltaResult, StageResult` and
`from npd_loader.flatten.stagefiles import FlattenError`; keep `NdjsonInput` from `raw_load`. Delete `_drop_orphans`
and its call in `run_import` (staging is truncated by `stage_release`, so there is nothing to clean up after a killed
run; `close_interrupted_runs` stays). Add `_summary`, replace `_import` and the error handling:

```python
def _summary(staged: StageResult, delta: DeltaResult, schema: str) -> list[dict]:
    items: list[dict] = [{"resource_type": t, **k} for t, k in sorted(delta.kinds.items())]
    items += [{"table": f"{schema}.{t}", "inserted": delta.inserted.get(t, 0), "replaced": delta.replaced.get(t, 0),
               "staged": staged.rows.get(t, 0)} for t in sorted(delta.inserted)]
    return items


def _import(ctx: Context, run: Run, release: date, inputs: list[NdjsonInput], force: bool) -> list[dict]:
    d = ctx.dialect
    newest = max(d.published_releases(), default=None)
    if newest is not None and release < newest and not force:
        raise StageFailed(f"release {release} is older than the current release {newest}; applying it would roll "
                          f"the data back. Rerun with --force to apply it anyway")
    staged = d.stage_release(ctx.storage, release, run.id, inputs)
    delta = d.apply_delta(release, run.id)
    return _summary(staged, delta, ctx.config.npd_db.schema)
```

The run output has two item kinds (`delta` per resource type, `table` per table). Let `runxml.build_output_xml`
take an optional per-item `"_tag"` key:

```python
def build_output_xml(items: list[dict[str, object]], item_tag: str) -> str:
    root = ET.Element("WAREHOUSE_RUN_OUTPUT")
    for item in items:
        attrs = {k: str(v) for k, v in item.items() if v is not None and k != "_tag"}
        ET.SubElement(root, str(item.get("_tag", item_tag)), attrs)
    return ET.tostring(root, encoding="unicode")
```

and `_finish`:

```python
def _finish(ctx: Context, run: Run, release: date, summary: list[dict]) -> None:
    warnings = apply_retention(ctx, release)
    items = [{"_tag": "delta", **i} if "resource_type" in i else i for i in summary]
    ctx.catalog.finish_run(run, SUCCESS, result="; ".join(warnings) or None,
                           output_xml=build_output_xml(items, item_tag="table"))
```

Add to `tests/test_runxml.py`:

```python
def test_output_xml_item_tags():
    xml = build_output_xml([{"_tag": "delta", "resource_type": "Practitioner", "new": 2}, {"table": "npd.p", "rows": 3}],
                           item_tag="table")
    assert '<delta resource_type="Practitioner" new="2" />' in xml and '<table table="npd.p" rows="3" />' in xml
```

In `run_import`'s `except` block replace the `RawLoadError` check and the standalone cleanup with:

```python
        except Exception as exc:
            if isinstance(exc, FlattenError) and exc.file_id is not None:
                ctx.catalog.update_data_file(exc.file_id, exceptions=str(exc))
            fail_run(ctx, run, exc)
            raise StageFailed(f"import of release {release} failed: {exc}") from exc
```

`src/npd_loader/retention.py` (file-only; docstring updated):

```python
"""Keep the .ndjson files of the newest keep_releases releases (extracted or imported per the catalog); delete older
ones. The data itself is one current dataset (no releases to drop). Never touches .zst/manifest files or catalog rows."""
from __future__ import annotations

import logging
from datetime import date

from npd_loader.stages import Context

log = logging.getLogger(__name__)


def _delete_ndjson(ctx: Context, release: date) -> None:
    for row in ctx.catalog.get_data_files(release, ctx.config.catalog.file_type_ndjson):
        if not row.file_rel_path:
            continue
        for rel in (row.file_rel_path, row.file_rel_path + ".part"):
            if ctx.storage.exists(rel):
                ctx.storage.delete(rel)
                log.info("retention deleted %s", rel)


def apply_retention(ctx: Context, just_imported: date) -> list[str]:
    cfg = ctx.config
    warnings: list[str] = []
    try:
        releases = set(ctx.catalog.successful_releases(cfg.catalog.run_class_extract)) \
            | set(ctx.catalog.successful_releases(cfg.catalog.run_class_import)) | {just_imported}
        keep = set(sorted(releases, reverse=True)[:cfg.retention.keep_releases]) | {just_imported}
        for release in sorted(releases - keep):
            try:
                _delete_ndjson(ctx, release)
            except Exception as exc:
                warnings.append(f"retention of release {release}: {exc}")
    except Exception as exc:
        warnings.append(f"retention: {exc}")
    for warning in warnings:
        log.warning(warning)
    return warnings
```

`src/npd_loader/dialect/__init__.py`: replace the protocol with the Phase 2 one above; remove `PublishConflict` and
`TransformResult`; `dialect_for` postgres branch → `raise ConfigError(...)`. `tests/test_dialect_factory.py`: the
Postgres test becomes `pytest.raises(ConfigError, match="SQL Server only")`.

- [ ] **Step 4: Run the tests**

Run: `.\.venv\Scripts\python -m pytest tests/test_mssql_import.py tests/test_retention.py tests/test_runxml.py tests/test_dialect_factory.py tests/test_cli.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add -A src/npd_loader tests/test_mssql_import.py tests/test_retention.py tests/test_runxml.py tests/test_dialect_factory.py
git commit -m "feat(phase2): import = flatten + stage + delta apply; file-only retention; SQL Server only"
```

---

### Task 10: Remove Phase 1 and Postgres code

**Files:**
- Delete: `src/npd_loader/sql/mssql/transform/`, `src/npd_loader/sql/postgres/`, `src/npd_loader/dialect/postgres.py`
- Modify: `src/npd_loader/dialect/mssql.py` (remove `load_raw`, `_load_file`, `_check_row_counts`, `_chunks`, `vlit`, `run_transforms`, `publish`, `drop_release`, `_switch_out_and_drop`, `_partition_rows`, `_partition_number`, `_boundaries`, `partitioned_releases`, `is_published`, `session`, `pf`, `ps`, `parent_tables`, `_create_standalone`, `_clone_indexes`, `drop_standalone_tables`, `to_utc_naive`, `BATCH_ROWS`, `RAW_COLUMNS`, `CHUNK_*`, `STANDALONE_RE`; keep `error_number`, `q`, `lit`, `_tokens`, `_autocommit`, `init_db`, `published_releases`, `_locked_transaction`, `run_lock`, Phase 2 methods)
- Modify: `src/npd_loader/raw_load.py` (keep only `NdjsonInput`, `RawLoadError`, `iter_lines`; remove `RAW_PARENT`, `RawLoadResult`, `validate_line`, `NUL_ESCAPE_RE`)
- Modify: `pyproject.toml` (remove `testcontainers[postgres]` from `test`; package-data `sql/*/init/*.sql`, `sql/mapped_paths.txt`)
- Delete tests: `test_mssql_raw_load.py`, `test_mssql_transform.py`, `test_mssql_publish.py`, `test_raw_load.py`, `test_schema.py`, `test_transform_organization.py`, `test_transform_practitioner.py`, `test_transform_roles.py`, `test_catalog_pg.py`, `test_e2e.py`, `test_import.py`, `pg_helpers.py`, `mssql_fixture_load.py`, `mssql_bench.py`
- Modify: `tests/conftest.py` (remove `pg_server`, `make_db`, `catalog_db`, `npd_db` fixtures and the psycopg2/testcontainers imports), `tests/helpers.py` (remove `pg_dialect`), `tests/make_golden.py` (header note: "needs the Phase 1 code at commit <Task 1 commit>; kept for the record, not runnable on this branch")
- Modify: `README.md` (Phase 2 "How it works", remove Postgres install/flavor sections, keep a pointer to `main` for Postgres; DBA prerequisite: run once per npd database, with no other connections, `ALTER DATABASE [npd] SET READ_COMMITTED_SNAPSHOT ON` so readers see the last committed state while a delta applies), `config.example.toml` (comments)

- [ ] **Step 1: Delete and trim** per the list above.
- [ ] **Step 2: Grep for leftovers**

Run: `git grep -nE "raw_schema|RAW_PARENT|run_transforms|load_raw|partition|psycopg|PgConnectionObject|drop_standalone|PublishConflict|TransformResult|_chunks" -- src tests README.md config.example.toml`
Expected: only intentional mentions (README history note, `connections.py` building `PgConnectionObject` for a `type: postgres` entry, which `dialect_for` then rejects). Remove everything else.

- [ ] **Step 3: Full suite and collection**

Run: `.\.venv\Scripts\python -m pytest --collect-only -q` then `.\.venv\Scripts\python -m pytest -q` (with `NPD_TEST_MSSQL_DB`)
Expected: collection clean; all tests pass; no Postgres skips remain.

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "refactor(phase2): remove the Phase 1 raw/transform/partition path and the Postgres flavor"
```

---

### Task 11: End to end through the CLI on SQL Server (two releases)

**Files:**
- Modify: `tests/test_mssql_e2e.py` (rewrite for Phase 2)

**Interfaces:**
- Consumes: CLI `main`, fixtures `cms`, `mssql_doc`, `mssql_engine`, `mssql_schemas`; `helpers.config_data` (Task 6 shape), `write_env_file`, `to_toml`; `test_catalog_mssql.make_catalog_schema`.

- [ ] **Step 1: Write the test**

```python
import copy
from datetime import date

from npd_loader.catalog import SqlCatalog
from npd_loader.cli import main
from npd_loader.config import parse_config
import fixture_data
from helpers import config_data, to_toml, write_env_file
from release_builder import build_release
from test_catalog_mssql import make_catalog_schema


def test_two_releases_end_to_end(tmp_path, cms, mssql_doc, mssql_engine, mssql_schemas, capsys):
    stage, data, cat = mssql_schemas("stage", "", "cat")
    cat_cfg = make_catalog_schema(mssql_engine, cat)
    env = write_env_file(tmp_path / "database.env", {"data": mssql_doc, "catalog": mssql_doc})
    cfg = config_data(tmp_path / "data", cms.manifest_url, env_file=env, schemas=(stage, data),
                      catalog_tables=(cat_cfg.run_table, cat_cfg.file_table), keep_releases=1)
    (tmp_path / "config.toml").write_text(to_toml(cfg))
    cli = lambda *a: main(["--config", str(tmp_path / "config.toml"), *a])
    scalar = lambda q: mssql_engine.connect().exec_driver_sql(q).scalar()

    assert cli("init-db") == 0 and cli("init-db") == 0
    cms.publish(build_release("2026-09-29"))
    assert cli("run") == 0
    assert scalar(f"SELECT count(*) FROM [{data}].[practitioner]") == 2
    assert scalar(f"SELECT count(*) FROM [{data}].[resource_state]") == 12
    assert cli("run") == 0                                                   # nothing to do

    records = copy.deepcopy(fixture_data.RECORDS)
    records["06-Practitioner.ndjson"][0]["gender"] = "female"
    aging = records["08-OrganizationAffiliation.ndjson"].pop()
    cms.publish(build_release("2026-10-06", records=records))
    assert cli("run") == 0
    assert scalar(f"SELECT count(*) FROM [{data}].[practitioner] WHERE gender = 'female'") == 2
    assert scalar(f"SELECT count(*) FROM [{data}].[organization_affiliation] WHERE resource_id = '{aging['id']}'") == 1
    assert scalar(f"SELECT changed_resources FROM [{data}].[release] WHERE release_date = '2026-10-06'") == 1
    assert scalar(f"SELECT not_seen_resources FROM [{data}].[release] WHERE release_date = '2026-10-06'") == 1
    assert scalar(f"SELECT CAST(last_seen_release AS varchar(10)) FROM [{data}].[resource_state] "
                  f"WHERE resource_id = '{aging['id']}'") == "2026-09-29"
    assert not list((tmp_path / "data").rglob("*2026-09-29*/*.ndjson"))      # keep_releases = 1: old .ndjson gone
    capsys.readouterr()
    assert cli("status") == 0
    out = capsys.readouterr().out
    assert "2026-10-06" in out and "2026-09-29" in out
```

(Adjust the `.ndjson` retention assertion to the run-folder naming in `stages.run_folder` if the glob does not match:
run folders are `run_<id>_<timestamp>`, so check instead that no `.ndjson` file whose catalog row has
`source_version_num = '2026-09-29'` still exists, via `SqlCatalog(...).get_data_files(date(2026, 9, 29), "ndjson")`.)

- [ ] **Step 2: Run it**

Run: `.\.venv\Scripts\python -m pytest tests/test_mssql_e2e.py -v`
Expected: PASS. Fix failures in the owning module, not the test.

- [ ] **Step 3: Commit**

```bash
git add tests/test_mssql_e2e.py
git commit -m "test(phase2): two releases end to end through the CLI (first load, then upsert; aging data kept)"
```

---

## Surrogate integer keys (Tasks 13–15, executed before Task 12)

Spec: section "Revision 2026-10-07: surrogate integer keys" and "References to missing data". Summary of the contract
these three tasks share:

- **Staging stays text-shaped** (`columns(t)`, unchanged names): `resource_id` and every reference column `<name>_id`
  hold the natural id **without** its `Type-` prefix (`Practitioner-1003000100` → `1003000100`).
- **Permanent tables are key-shaped** (`key_columns(t)`): `resource_id` → `resource_key int`, `<name>_id` →
  `<name>_key int`, `identifier.resource_type` dropped.
- `npd.resource_type(resource_type_id tinyint, name)` seeded with the 8 types; `npd.resource_state` is the key registry
  (`resource_key int IDENTITY`). An id that has only been referenced has a row with `hash`, `last_updated`,
  `release_date`, `run_id`, `last_seen_*` all NULL. No flag.
- Reference columns declare their target type in the specs (`target="Organization"`); a reference naming a different
  type is a `ConvertError` (the import fails loudly, naming file and line).

Between Task 14 and Task 15 the delta/import/e2e SQL Server tests fail (schema is key-shaped, `apply_delta` not yet) —
Task 14 runs only the flatten and schema tests (same pattern as ruling R1).

### Task 13: Ids without the type prefix; reference columns declare their target type

**Files:**
- Modify: `src/npd_loader/flatten/convert.py` (replace `ref` with `strip_id` + `ref_to`)
- Modify: `src/npd_loader/flatten/engine.py` (`Col.target`, `R/E(..., target=)`, `key_columns`, `ref_columns`)
- Modify: `src/npd_loader/flatten/specs.py` (every reference column gets `target=`)
- Modify: `src/npd_loader/flatten/stagefiles.py` (strip the resource id)
- Test: `tests/test_flatten_convert.py`, `tests/test_flatten_engine.py`, `tests/test_flatten_specs.py`,
  `tests/test_flatten_stagefiles.py`

**Interfaces:**
- Produces: `convert.strip_id(rtype: str, rid: str) -> str`; `convert.ref_to(target: str) -> Callable[[Any], str | None]`;
  `engine.Col.target: str | None`; `engine.R(get, conv=None, *, target=None)`, `engine.E(...)` likewise;
  `engine.KEY_LINEAGE = ("release_date", "resource_key", "ndjson_file_id", "zst_file_id")`;
  `engine.key_columns(t: Table) -> list[str]`; `engine.ref_columns(t: Table) -> dict[str, str]` (staging column →
  target type, spec order). `columns(t)` is unchanged.

- [ ] **Step 1: Write the failing tests**

In `tests/test_flatten_convert.py`, change the import line to import `ref_to, strip_id` instead of `ref`, replace the
two `ref(...)` asserts in `test_boolean_number_join_ref` with nothing, and add:

```python
def test_strip_id_and_ref_to():
    assert strip_id("Practitioner", "Practitioner-1003000100") == "1003000100"
    assert strip_id("Organization", "Organization-ea579d05-454e") == "ea579d05-454e"
    assert strip_id("Organization", "1902099112") == "1902099112"            # no prefix: kept
    assert strip_id("Organization", "Organization-") == "Organization-"      # nothing left: kept
    assert strip_id("Location", "Organization-1") == "Organization-1"        # another type's prefix: kept
    org = ref_to("Organization")
    assert org("Organization/Organization-1336200294") == "1336200294"
    assert org("Organization-1336200294") == "1336200294"                    # bare id
    assert org(None) is None and org("Organization/") is None and org(7) is None
    with pytest.raises(ConvertError, match="Practitioner/Practitioner-1 is not a Organization reference"):
        org("Practitioner/Practitioner-1")
```

In `tests/test_flatten_engine.py`, change `ref` in the imports to `ref_to`, replace the last assert of
`test_path_and_helpers` (`assert ref(path("x")(...)) == "O-1"`) with
`assert ref_to("Org")(path("x")({"x": "Org/Org-1"})) == "1"`, and add:

```python
def test_reference_columns_and_key_columns():
    t = Table("p_role", {"active": R("active"), "practitioner_id": R("practitioner.reference", target="Practitioner"),
                         "endpoint_id": E("reference", target="Endpoint")}, each="endpoint")
    i = Table("ident", {"value": E("value")}, each="identifier", with_type=True)
    assert ref_columns(t) == {"practitioner_id": "Practitioner", "endpoint_id": "Endpoint"}
    assert columns(t) == ["release_date", "resource_id", "ndjson_file_id", "zst_file_id", "seq",
                          "active", "practitioner_id", "endpoint_id"]
    assert key_columns(t) == ["release_date", "resource_key", "ndjson_file_id", "zst_file_id", "seq",
                              "active", "practitioner_key", "endpoint_key"]
    assert key_columns(i) == ["release_date", "resource_key", "ndjson_file_id", "zst_file_id", "seq", "value"]
    res = {"practitioner": {"reference": "Practitioner/Practitioner-9"}, "endpoint": [{"reference": "Endpoint/Endpoint-e1"}]}
    assert list(flatten_resource(res, [t], LIN)) == [("p_role", LIN + (1, None, "9", "e1"))]
    with pytest.raises(ValueError, match="must end in _id"):
        Table("bad", {"practitioner": R("practitioner.reference", target="Practitioner")})
```

(add `import pytest` and `key_columns, ref_columns` to the engine imports if missing.)

In `tests/test_flatten_specs.py`, replace `flatten_fixture` and `test_flattened_fixture_equals_phase1_output`, and add
a coverage test:

```python
from npd_loader.flatten.convert import strip_id
from npd_loader.flatten.engine import columns, flatten_resource, ref_columns


def flatten_fixture() -> dict[str, list[list]]:
    out: dict[str, list[list]] = {}
    ndjson = build_release("2026-09-29").ndjson
    for i, (name, data) in enumerate(sorted(ndjson.items())):
        fid, zid = 500 + 2 * i, 501 + 2 * i
        for line in data.splitlines():
            if not line.strip():
                continue
            res = orjson.loads(line)
            rid = strip_id(res["resourceType"], res["id"])
            for table, values in flatten_resource(res, tables_for(res["resourceType"]),
                                                  ("2026-09-29", rid, fid, zid)):
                out.setdefault(table, []).append([normalize(v) for v in values])
    return out


def golden_without_prefixes(t, rows):
    """The Phase 1 golden values keep 'Type-' prefixes; Phase 2 strips them from resource ids and references."""
    cols = columns(t)
    rid, refs = cols.index("resource_id"), {cols.index(c): target for c, target in ref_columns(t).items()}
    out = []
    for row in rows:
        row = list(row)
        rtype = row[cols.index("resource_type")] if t.with_type else TABLE_TYPES[t.name]
        row[rid] = strip_id(rtype, row[rid])
        for i, target in refs.items():
            row[i] = None if row[i] is None else strip_id(target, row[i])
        out.append(row)
    return sorted(out, key=lambda r: [x or "" for x in r])


def test_flattened_fixture_equals_phase1_output():
    golden, got = load_golden(), flatten_fixture()
    for t in ALL_TABLES:
        rows = sorted(got.get(t.name, []), key=lambda r: [x or "" for x in r])
        assert rows == golden_without_prefixes(t, golden[t.name]), t.name


def test_every_reference_column_has_a_target():
    for t in ALL_TABLES:
        for name, col in t.cols.items():
            assert name.endswith("_id") == (col.target is not None), f"{t.name}.{name}"
            assert col.target is None or col.target in SPECS, f"{t.name}.{name}"
    assert sum(len(ref_columns(t)) for t in ALL_TABLES) == 18
```

In `tests/test_flatten_stagefiles.py` `test_flatten_file_writes_rows_and_hashes`, change the expected practitioner id:
`assert h[1] == "1003000100"` (add to the `h` assert line) and `assert p[1] == "1003000100" and ...` (was
`"Practitioner-1003000100"`).

- [ ] **Step 2: Run the tests to verify they fail**

Run: `.venv/Scripts/python -m pytest tests/test_flatten_convert.py tests/test_flatten_engine.py tests/test_flatten_specs.py tests/test_flatten_stagefiles.py -q`
Expected: FAIL / collection errors (`strip_id`, `ref_to`, `key_columns`, `ref_columns` don't exist).

- [ ] **Step 3: Implement**

`src/npd_loader/flatten/convert.py` — replace `ref` with:

```python
def strip_id(rtype: str, rid: str) -> str:
    """'Practitioner-1003000100' -> '1003000100'. Ids without their own type's prefix (or with nothing after it) are
    kept whole."""
    prefix = rtype + "-"
    return rid[len(prefix):] if rid.startswith(prefix) and len(rid) > len(prefix) else rid


def ref_to(target: str):
    """Converter for a reference to a `target` resource: 'Organization/Organization-1' -> '1'. A reference naming
    another resource type is an error: the column holds keys of `target` only."""
    def conv(v: Any) -> str | None:
        if not isinstance(v, str):
            return None
        rtype, _, rid = v.rpartition("/")
        if rtype and rtype.rsplit("/", 1)[-1] != target:
            raise ConvertError(f"{v} is not a {target} reference")
        return strip_id(target, rid) if rid else None
    return conv
```

`src/npd_loader/flatten/engine.py`:

```python
from npd_loader.flatten.convert import ref_to

KEY_LINEAGE = ("release_date", "resource_key", "ndjson_file_id", "zst_file_id")


@dataclass(frozen=True)
class Col:
    source: str                      # "r" (resource) or "e" (repeating element)
    get: Getter
    conv: Callable[[Any], Any] | None = None
    target: str | None = None        # reference column: the resource type it points to

    def value(self, res: Any, el: Any) -> Any:
        v = self.get(res if self.source == "r" else el)
        return self.conv(v) if self.conv else v


def R(get: str | Getter, conv: Callable[[Any], Any] | None = None, *, target: str | None = None) -> Col:
    return Col("r", _getter(get), ref_to(target) if target else conv, target)


def E(get: str | Getter, conv: Callable[[Any], Any] | None = None, *, target: str | None = None) -> Col:
    return Col("e", _getter(get), ref_to(target) if target else conv, target)
```

Add to `Table`:

```python
    def __post_init__(self):
        for name, col in self.cols.items():
            if col.target and not name.endswith("_id"):
                raise ValueError(f"{self.name}.{name}: a reference column's name must end in _id")
```

and after `columns`:

```python
def ref_columns(t: Table) -> dict[str, str]:
    """Staging reference columns (text ids) -> the resource type they point to."""
    return {name: c.target for name, c in t.cols.items() if c.target}


def key_columns(t: Table) -> list[str]:
    """Permanent table columns: resource_key for resource_id, <name>_key for each reference column <name>_id, no
    resource_type (the key implies it)."""
    return (list(KEY_LINEAGE) + (["seq"] if t.each else [])
            + [name[:-3] + "_key" if c.target else name for name, c in t.cols.items()])
```

`src/npd_loader/flatten/specs.py` — drop `ref` from the convert import, and declare targets (17 columns):

```python
NETWORK_REF = R(lambda r: path("valueReference.reference")(ext(NDH + "base-ext-network-reference")(r)),
                target="Organization")


def _ref_table(name: str, each: str, column: str, target: str) -> Table:
    return Table(name, {column: E("reference", target=target)}, each=each)
```

Delete the unused `REF = {...}` constant. Then each reference column (18):

| table | column | becomes |
|---|---|---|
| practitioner_qualification | issuer_organization_id | `E("issuer.reference", target="Organization")` |
| organization | part_of_organization_id | `R("partOf.reference", target="Organization")` |
| organization_endpoint | endpoint_id | `_ref_table("organization_endpoint", "endpoint", "endpoint_id", "Endpoint")` |
| location | managing_organization_id | `R("managingOrganization.reference", target="Organization")` |
| endpoint | managing_organization_id | `R("managingOrganization.reference", target="Organization")` |
| practitioner_role | practitioner_id | `R("practitioner.reference", target="Practitioner")` |
| practitioner_role | organization_id | `R("organization.reference", target="Organization")` |
| practitioner_role | network_organization_id | `NETWORK_REF` |
| practitioner_role_endpoint | endpoint_id | `_ref_table(..., "endpoint_id", "Endpoint")` |
| practitioner_role_location | location_id | `_ref_table(..., "location_id", "Location")` |
| organization_affiliation | organization_id | `R("organization.reference", target="Organization")` |
| organization_affiliation | participating_organization_id | `R("participatingOrganization.reference", target="Organization")` |
| organization_affiliation_network | network_organization_id | `_ref_table(..., "network_organization_id", "Organization")` |
| healthcare_service | provided_by_organization_id | `R("providedBy.reference", target="Organization")` |
| healthcare_service | network_organization_id | `NETWORK_REF` |
| healthcare_service_location | location_id | `_ref_table(..., "location_id", "Location")` |
| insurance_plan | owned_by_organization_id | `R("ownedBy.reference", target="Organization")` |
| insurance_plan | administered_by_organization_id | `R("administeredBy.reference", target="Organization")` |

`src/npd_loader/flatten/stagefiles.py` — in `flatten_file`, right after the `missing id` check:

```python
                rid = strip_id(inp.resource_type, rid)
```

(import `strip_id` from `npd_loader.flatten.convert`). The hash still covers the raw line, so it is unchanged.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `.venv/Scripts/python -m pytest tests/test_flatten_convert.py tests/test_flatten_engine.py tests/test_flatten_specs.py tests/test_flatten_stagefiles.py -q`
Expected: PASS. `tests/test_flatten_specs.py::test_columns_match_the_phase1_tables` still passes (staging column names
are unchanged).

- [ ] **Step 5: Commit**

```bash
git add src/npd_loader/flatten tests/test_flatten_convert.py tests/test_flatten_engine.py tests/test_flatten_specs.py tests/test_flatten_stagefiles.py
git commit -m "feat(phase2): strip type prefixes from ids; reference columns declare their target type"
```

### Task 14: Key schema: resource_type, resource_state key registry, key-shaped tables, text-shaped staging

**Files:**
- Modify: `src/npd_loader/sql/mssql/init/001_schemas.sql`
- Modify: `src/npd_loader/sql/mssql/init/003_tables.sql` (via the one-off script below; not committed)
- Modify: `src/npd_loader/sql/mssql/init/900_migrations.sql` (comment only)
- Modify: `src/npd_loader/dialect/mssql.py` (`init_db` staging DDL)
- Test: `tests/test_mssql_schema.py`

**Interfaces:**
- Consumes: `columns`, `key_columns`, `ref_columns` (Task 13).
- Produces: tables `npd.resource_type`, `npd.resource_state` (key registry), 26 key-shaped permanent tables, 26
  text-shaped staging heaps; `MssqlDialect._stage_select(t) -> str`.

The schema has never been deployed with data (npd_dev and npd are empty; tests use scratch schemas), so the CREATE
statements are edited in place rather than migrated. `001_schemas.sql` refuses to run over a pre-key `resource_state`.

- [ ] **Step 1: Write the failing tests** — in `tests/test_mssql_schema.py`, change the import to
`from npd_loader.flatten.engine import columns, key_columns` and in `test_tables_have_spec_columns_and_primary_keys`
change `assert cols == columns(t), t.name` to `assert cols == key_columns(t), t.name` (staging keeps `columns(t)`).
Replace the `for col in ("hash", ...)` loop with the block below and add two tests:

```python
    for col in ("resource_key", "resource_type_id", "resource_id", "hash", "release_date", "run_id",
                "last_seen_release", "last_seen_run_id"):
        assert scalar(d, "SELECT COL_LENGTH(?, ?)", f"{d.cfg.schema}.resource_state", col) is not None, col
    assert scalar(d, "SELECT COL_LENGTH(?, 'resource_type')", f"{d.cfg.schema}.resource_state") is None
    assert scalar(d, "SELECT COL_LENGTH(?, 'not_seen_resources')", f"{d.cfg.schema}.release") is not None


def test_resource_types_are_seeded_and_keys_are_ints(mssql_dialect):
    d = mssql_dialect
    with d.engine.connect() as conn:
        types = dict(conn.exec_driver_sql(f"SELECT name, resource_type_id FROM {d.q(d.cfg.schema, 'resource_type')}"))
        assert set(types) == set(SPECS) and sorted(types.values()) == list(range(1, 9))
        assert conn.exec_driver_sql(
            "SELECT TYPE_NAME(system_type_id) + ':' + CAST(is_identity AS varchar(1)) FROM sys.columns "
            "WHERE object_id = OBJECT_ID(?) AND name = 'resource_key'", (f"{d.cfg.schema}.resource_state",)).scalar() == "int:1"
        assert conn.exec_driver_sql(
            "SELECT TYPE_NAME(system_type_id) FROM sys.columns WHERE object_id = OBJECT_ID(?) AND name = 'hash'",
            (f"{d.cfg.schema}.resource_state",)).scalar() == "binary"
        assert conn.exec_driver_sql(
            "SELECT count(*) FROM sys.indexes WHERE object_id = OBJECT_ID(?) AND is_unique = 1 AND is_primary_key = 0",
            (f"{d.cfg.schema}.resource_state",)).scalar() == 1
        for t in ALL_TABLES:                         # every *_key column is int; staging id columns are varchar
            kinds = dict(conn.exec_driver_sql(
                "SELECT name, TYPE_NAME(system_type_id) FROM sys.columns WHERE object_id = OBJECT_ID(?)",
                (f"{d.cfg.schema}.{t.name}",)))
            assert all(v == "int" for k, v in kinds.items() if k.endswith("_key")), t.name
            stage = dict(conn.exec_driver_sql(
                "SELECT name, TYPE_NAME(system_type_id) FROM sys.columns WHERE object_id = OBJECT_ID(?)",
                (f"{d.cfg.stage_schema}.{t.name}",)))
            assert stage["resource_id"] == "varchar" and all(stage[c] == "varchar" for c in ref_columns(t)), t.name


def test_init_db_refuses_a_pre_key_resource_state(mssql_dialect):
    d = mssql_dialect
    st = d.q(d.cfg.schema, "resource_state")
    with d.engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        conn.exec_driver_sql(f"DROP TABLE {st}")
        conn.exec_driver_sql(f"CREATE TABLE {st} (resource_type varchar(40) NOT NULL, resource_id varchar(128) NOT NULL)")
    with pytest.raises(Exception, match="predates surrogate keys"):
        d.init_db()
```

(imports: `import pytest`, `from npd_loader.flatten.engine import columns, key_columns, ref_columns`,
`from npd_loader.flatten.specs import ALL_TABLES, SPECS`.)

- [ ] **Step 2: Run to verify failure**

Run: `.venv/Scripts/python -m pytest tests/test_mssql_schema.py -q` (needs `NPD_TEST_MSSQL_DB`, see ledger)
Expected: FAIL (permanent columns are `resource_id`; no `resource_type` table).

- [ ] **Step 3: Rewrite the schema**

`001_schemas.sql` — replace the `resource_state` block and its `resource_state_last_seen` index block with:

```sql
-- Refuse to run over the pre-key schema (text resource ids): drop and re-create that schema instead.
IF COL_LENGTH(<<s:schema>> + N'.resource_state', N'resource_type') IS NOT NULL
    THROW 50001, 'resource_state predates surrogate keys: drop the data schema and run init-db again', 1
GO
IF OBJECT_ID(<<s:schema>> + N'.resource_type', N'U') IS NULL
    CREATE TABLE <<schema>>.resource_type (
        resource_type_id tinyint     NOT NULL CONSTRAINT pk_resource_type PRIMARY KEY,
        name             varchar(40) NOT NULL CONSTRAINT ux_resource_type_name UNIQUE
    )
GO
INSERT INTO <<schema>>.resource_type (resource_type_id, name)
SELECT v.id, v.name FROM (VALUES (1, 'Practitioner'), (2, 'Organization'), (3, 'Location'), (4, 'Endpoint'),
    (5, 'PractitionerRole'), (6, 'OrganizationAffiliation'), (7, 'HealthcareService'), (8, 'InsurancePlan')) v (id, name)
WHERE NOT EXISTS (SELECT 1 FROM <<schema>>.resource_type t WHERE t.resource_type_id = v.id)
GO
-- Key registry and current state of every resource. A key is assigned the first time an id is seen, as a resource or
-- as a reference target, and never changes or gets reused. An id that has only been referenced has NULL hash,
-- last_updated, release_date, run_id and last_seen_* (no data rows). Resources missing from a release are kept (aging
-- data); last_seen_release is the last release that contained them.
IF OBJECT_ID(<<s:schema>> + N'.resource_state', N'U') IS NULL
    CREATE TABLE <<schema>>.resource_state (
        resource_key      int          IDENTITY(1, 1) NOT NULL CONSTRAINT pk_resource_state PRIMARY KEY CLUSTERED,
        resource_type_id  tinyint      NOT NULL,
        resource_id       varchar(128) NOT NULL,   -- natural id without its 'Type-' prefix
        hash              binary(20)   NULL,       -- SHA-1 of SPEC_VERSION + the ndjson line
        last_updated      datetime2(3) NULL,
        release_date      date         NULL,       -- release whose content is current
        run_id            int          NULL,
        last_seen_release date         NULL,
        last_seen_run_id  int          NULL
    ) WITH (DATA_COMPRESSION = PAGE)
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(<<s:schema>> + N'.resource_state') AND name = N'ux_resource_state_id')
    CREATE UNIQUE INDEX ux_resource_state_id ON <<schema>>.resource_state (resource_type_id, resource_id) WITH (DATA_COMPRESSION = PAGE)
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(<<s:schema>> + N'.resource_state') AND name = N'resource_state_last_seen')
    CREATE INDEX resource_state_last_seen ON <<schema>>.resource_state (last_seen_release) WITH (DATA_COMPRESSION = PAGE)
GO
```

The `THROW` must come before anything else touches `resource_state`; keep it right after the `release` table block.
The staging `resource_hash` block is unchanged (text id, `hash char(40)` hex).

`003_tables.sql` — run this one-off script from the repo root (scratchpad file, not committed), then review the diff:

```python
import re, pathlib
from npd_loader.flatten.specs import ALL_TABLES
from npd_loader.flatten.engine import ref_columns
p = pathlib.Path("src/npd_loader/sql/mssql/init/003_tables.sql")
s = p.read_text(encoding="utf-8")
refs = sorted({c for t in ALL_TABLES for c in ref_columns(t)})
s = s.replace("resource_id varchar(128) NOT NULL", "resource_key int NOT NULL")
s = s.replace("CLUSTERED (resource_type, resource_id, seq)", "CLUSTERED (resource_key, seq)")
s = s.replace("CLUSTERED (resource_id", "CLUSTERED (resource_key")
s = re.sub(r"\n\s*resource_type varchar\(128\) NOT NULL,", "", s)
for c in refs:
    s = re.sub(rf"\b{c} varchar\(128\)", f"{c[:-3]}_key int", s)
    s = re.sub(rf"\(({c})\)", f"({c[:-3]}_key)", s)          # nonclustered indexes on reference columns
s = s.replace("resource_id, ndjson_file_id, zst_file_id first", "resource_key, ndjson_file_id, zst_file_id first")
p.write_text(s, encoding="utf-8")
assert "resource_id" not in s and "_id varchar" not in s and "resource_type" not in s, "leftover text ids"
```

Expected diff: 26 `resource_key int NOT NULL`, 26 PKs on `resource_key`, 18 `<name>_key int`, the three reference
indexes (`location_managing_organization`, `practitioner_role_practitioner`, `practitioner_role_organization`) on
`_key` columns, identifier without `resource_type`. Index names stay.

`900_migrations.sql` — add one line to the header comment: `-- 2026-10-07: surrogate keys were introduced by editing
001/003 in place (nothing deployed yet); from now on follow the convention above.`

`src/npd_loader/dialect/mssql.py` — import `ref_columns` from the engine, add:

```python
    def _stage_select(self, t) -> str:
        """Select list that shapes a staging heap from its permanent table: text ids where the table has keys."""
        text_ids = {"resource_id": "varchar(128)", "resource_type": "varchar(40)"} | {c: "varchar(128)" for c in ref_columns(t)}
        return ", ".join(f"CAST(NULL AS {text_ids[c]}) AS {self.q(c)}" if c in text_ids else f"x.{self.q(c)}"
                         for c in columns(t))
```

and in `init_db` replace the staging `SELECT TOP 0 {cols} INTO ... FROM {permanent}` with
`f"SELECT TOP 0 {self._stage_select(t)} INTO {self.q(stage, t.name)} FROM {self.q(schema, t.name)} x"` (drop the
now-unused `cols` variable). The resync check above it (`have != columns(t)`) stays as is.

- [ ] **Step 4: Run the tests**

Run: `.venv/Scripts/python -m pytest tests/test_mssql_schema.py tests/test_flatten_*.py -q`
Expected: PASS. (`test_mssql_delta/import/e2e/stage` fail until Task 15 — don't run them here.)

- [ ] **Step 5: Commit**

```bash
git add src/npd_loader/sql/mssql/init src/npd_loader/dialect/mssql.py tests/test_mssql_schema.py
git commit -m "feat(phase2): surrogate int keys: resource_type, key registry, key-shaped tables, text staging"
```

### Task 15: `apply_delta` assigns keys and translates text ids

**Files:**
- Modify: `src/npd_loader/dialect/mssql.py` (`apply_delta`)
- Test: `tests/test_mssql_delta.py`, `tests/test_mssql_import.py`, `tests/test_mssql_e2e.py`
- Modify: `README.md` (the `resource_state` paragraph near line 98: keys, referenced-only ids)

**Interfaces:**
- Consumes: `key_columns`, `ref_columns`, `TABLE_TYPES`, `ALL_TABLES`; schema from Task 14.
- Produces: unchanged `DeltaResult(kinds, inserted, replaced)`; `kinds` per type name as before. `not_seen` counts only
  resources with data (`hash IS NOT NULL`).

- [ ] **Step 1: Write the failing tests** — rewrite `tests/test_mssql_delta.py` (keep `rows`, `load`, imports):

```python
def key(d, rtype, rid):
    found = rows(d, f"SELECT s.resource_key FROM {d.q(d.cfg.schema, 'resource_state')} s JOIN "
                    f"{d.q(d.cfg.schema, 'resource_type')} t ON t.resource_type_id = s.resource_type_id "
                    f"WHERE t.name = ? AND s.resource_id = ?", rtype, rid)
    return found[0][0] if found else None


def test_first_load_inserts_everything(mssql_dialect, tmp_path):
    d, sch = mssql_dialect, mssql_dialect.cfg.schema
    res = load(d, tmp_path, R1, 7)
    assert sum(k["new"] for k in res.kinds.values()) == 12
    assert res.inserted["practitioner"] == 2 and res.replaced["practitioner"] == 0      # real rowcounts
    assert rows(d, f"SELECT count(*) FROM {d.q(sch, 'resource_state')} WHERE hash IS NOT NULL") == [(12,)]
    assert rows(d, f"SELECT count(*) FROM {d.q(sch, 'practitioner')}") == [(2,)]
    assert rows(d, f"SELECT import_run_id, new_resources, not_seen_resources FROM {d.q(sch, 'release')}") == [(7, 12, 0)]
    # references translate to the target's key
    role, prac = key(d, "PractitionerRole", "0f00aa11"), key(d, "Practitioner", "1003000100")
    assert role and prac
    assert rows(d, f"SELECT practitioner_key FROM {d.q(sch, 'practitioner_role')} WHERE resource_key = ?", role) == [(prac,)]
    # identifiers are keyed by their resource
    assert rows(d, f"SELECT count(*) FROM {d.q(sch, 'identifier')} WHERE resource_key = ?", prac)[0][0] >= 1


def test_reference_to_missing_data_gets_a_key_without_state(mssql_dialect, tmp_path):
    d, sch = mssql_dialect, mssql_dialect.cfg.schema
    load(d, tmp_path, R1, 7)
    org = key(d, "Organization", "1295596195")              # HealthcareService.providedBy; no such Organization
    assert org is not None
    assert rows(d, f"SELECT hash, last_updated, release_date, run_id, last_seen_release, last_seen_run_id "
                   f"FROM {d.q(sch, 'resource_state')} WHERE resource_key = ?", org) == [(None,) * 6]
    assert rows(d, f"SELECT provided_by_organization_key FROM {d.q(sch, 'healthcare_service')}") == [(org,)]
    assert rows(d, f"SELECT count(*) FROM {d.q(sch, 'organization')} WHERE resource_key = ?", org) == [(0,)]
    # the data arrives in the next release: same key, counted as new, state filled in
    records = copy.deepcopy(fixture_data.RECORDS)
    late = copy.deepcopy(records["01-Organization.ndjson"][0])
    late["id"] = "Organization-1295596195"
    records["01-Organization.ndjson"].append(late)
    res = load(d, tmp_path, R2, 8, records)
    assert res.kinds["Organization"]["new"] == 1
    assert key(d, "Organization", "1295596195") == org
    assert rows(d, f"SELECT release_date, last_seen_release FROM {d.q(sch, 'resource_state')} WHERE resource_key = ?",
                org) == [(R2, R2)]
    assert rows(d, f"SELECT count(*) FROM {d.q(sch, 'organization')} WHERE resource_key = ?", org) == [(1,)]
    assert rows(d, f"SELECT not_seen_resources FROM {d.q(sch, 'release')} WHERE release_date = ?", R2) == [(0,)]


def test_second_release_upserts_and_keeps_aging_data(mssql_dialect, tmp_path):
    d = mssql_dialect
    load(d, tmp_path, R1, 7)
    records = copy.deepcopy(fixture_data.RECORDS)
    p = records["06-Practitioner.ndjson"][0]
    p["name"][0]["family"] = "GOMEZ-CHANGED"
    p["telecom"] = p["telecom"][:1]                                       # child rows shrink
    missing = records["08-OrganizationAffiliation.ndjson"].pop()          # not in the new release
    pkey = key(d, "Practitioner", p["id"].split("-", 1)[1])
    mkey = key(d, "OrganizationAffiliation", missing["id"].split("-", 1)[1])
    res = load(d, tmp_path, R2, 8, records)
    assert res.kinds["Practitioner"] == {"new": 0, "changed": 1, "unchanged": 1, "not_seen": 0}
    assert res.kinds["OrganizationAffiliation"]["not_seen"] == 1
    assert res.inserted["practitioner"] == 1 and res.replaced["practitioner"] == 1
    sch = d.cfg.schema
    assert key(d, "Practitioner", p["id"].split("-", 1)[1]) == pkey                   # keys never change
    assert rows(d, f"SELECT name_family FROM {d.q(sch, 'practitioner')} WHERE resource_key = ?", pkey) == [("GOMEZ-CHANGED",)]
    assert rows(d, f"SELECT count(*) FROM {d.q(sch, 'practitioner_telecom')} WHERE resource_key = ?", pkey) == [(1,)]
    # aging data stays live, with its last-seen release
    assert rows(d, f"SELECT count(*) FROM {d.q(sch, 'organization_affiliation')} WHERE resource_key = ?", mkey) == [(1,)]
    assert rows(d, f"SELECT last_seen_release FROM {d.q(sch, 'resource_state')} WHERE resource_key = ?", mkey) == [(R1,)]
    # content release vs last seen
    assert rows(d, f"SELECT release_date, last_seen_release FROM {d.q(sch, 'resource_state')} WHERE resource_key = ?",
                pkey) == [(R2, R2)]
    unchanged = key(d, "Practitioner", records["06-Practitioner.ndjson"][1]["id"].split("-", 1)[1])
    assert rows(d, f"SELECT release_date, last_seen_release FROM {d.q(sch, 'resource_state')} WHERE resource_key = ?",
                unchanged) == [(R1, R2)]
    assert rows(d, f"SELECT not_seen_resources FROM {d.q(sch, 'release')} WHERE release_date = ?", R2) == [(1,)]
```

Keep `test_partial_file_deletes_nothing` as is. In `test_failure_inside_apply_rolls_back`: change
`ORDER BY resource_id` to `ORDER BY resource_key`; add a new resource to `records` so the failed apply would have
assigned keys (`records["06-Practitioner.ndjson"].append({**records["06-Practitioner.ndjson"][1], "id": "Practitioner-1999999999"})`);
right before the `try:` record `before = rows(d, f"SELECT count(*), max(resource_key) FROM {d.q(d.cfg.schema, 'resource_state')}")`;
and add after the release assert:
`assert rows(d, f"SELECT count(*), max(resource_key) FROM {d.q(d.cfg.schema, 'resource_state')}") == before` (key
assignment rolls back with everything else; IDENTITY values consumed by the rollback are simply skipped later).

`tests/test_mssql_import.py` `test_killed_run_leftovers_are_never_applied_and_run_closed`: stage the stale row as
`VALUES ('2026-01-01', 'STALE', 1, 2)` and replace the final count with

```python
        assert c.exec_driver_sql(f"SELECT count(*) FROM {d.q(d.cfg.schema, 'resource_state')} "
                                 f"WHERE resource_id = 'STALE'").scalar() == 0      # never gets a key or a row
```

`tests/test_mssql_e2e.py` lines 40 and 43–44: compare by id through `resource_state`:

```python
    aging_id = aging["id"].split("-", 1)[1]
    assert scalar(f"SELECT count(*) FROM [{data}].[organization_affiliation] a JOIN [{data}].[resource_state] s "
                  f"ON s.resource_key = a.resource_key WHERE s.resource_id = '{aging_id}'") == 1
    ...
    assert scalar(f"SELECT CAST(last_seen_release AS varchar(10)) FROM [{data}].[resource_state] "
                  f"WHERE resource_id = '{aging_id}'") == "2026-09-29"
```

- [ ] **Step 2: Run to verify failure**

Run: `.venv/Scripts/python -m pytest tests/test_mssql_delta.py -q`
Expected: FAIL (apply_delta still references `resource_type`/text ids in the permanent tables).

- [ ] **Step 3: Implement** — replace `apply_delta` in `src/npd_loader/dialect/mssql.py` (imports: add `key_columns`,
`ref_columns` from `npd_loader.flatten.engine`):

```python
    def apply_delta(self, release: date, run_id: int) -> DeltaResult:
        """Upsert the staged release: give every new id (resource or reference target) a key, replace the rows of
        changed resources, insert new ones with text ids translated to keys, mark every resource of the release as
        seen. Resources missing from the release are kept (aging data)."""
        s, schema = self.cfg.stage_schema, self.cfg.schema
        hashes, state = self.q(s, HASH_TABLE), self.q(schema, "resource_state")
        types = self.q(schema, "resource_type")
        delta = self.q(s, "delta")      # scratch table rebuilt per apply, not one of the fixed staging tables
        # Classifying outside the transaction is safe: the import lock is held, so nothing else changes resource_state.
        with self._autocommit() as conn:
            type_ids = {n: i for n, i in conn.exec_driver_sql(f"SELECT name, resource_type_id FROM {types}")}
            unknown = [r[0] for r in conn.exec_driver_sql(
                f"SELECT DISTINCT resource_type FROM {hashes} h WHERE NOT EXISTS "
                f"(SELECT 1 FROM {types} t WHERE t.name = h.resource_type)")]
            if unknown:
                raise ValueError(f"staged resource types missing from {schema}.resource_type: {unknown}")
            conn.exec_driver_sql(f"DROP TABLE IF EXISTS {delta}")
            conn.exec_driver_sql(
                f"SELECT t.resource_type_id, h.resource_type, h.resource_id, CONVERT(binary(20), h.hash, 2) AS hash, "
                f"h.last_updated, CAST(st.resource_key AS int) AS resource_key, "
                f"CAST(CASE WHEN st.hash IS NULL THEN 'N' WHEN st.hash <> CONVERT(binary(20), h.hash, 2) THEN 'C' "
                f"ELSE 'U' END AS char(1)) AS kind "
                f"INTO {delta} FROM {hashes} h JOIN {types} t ON t.name = h.resource_type "
                f"LEFT JOIN {state} st ON st.resource_type_id = t.resource_type_id AND st.resource_id = h.resource_id")
            conn.exec_driver_sql(f"CREATE UNIQUE CLUSTERED INDEX ux_delta ON {delta} (resource_type_id, resource_id)")
            kinds: dict[str, dict[str, int]] = {}
            for rtype, kind, n in conn.exec_driver_sql(
                    f"SELECT resource_type, kind, COUNT_BIG(*) FROM {delta} GROUP BY resource_type, kind"):
                kinds.setdefault(rtype, {"new": 0, "changed": 0, "unchanged": 0, "not_seen": 0})[self.KINDS[kind]] = n
            for rtype, n in conn.exec_driver_sql(
                    f"SELECT t.name, COUNT_BIG(*) FROM {state} st JOIN {types} t ON t.resource_type_id = st.resource_type_id "
                    f"WHERE st.hash IS NOT NULL AND NOT EXISTS (SELECT 1 FROM {delta} d "
                    f"WHERE d.resource_type_id = st.resource_type_id AND d.resource_id = st.resource_id) GROUP BY t.name"):
                kinds.setdefault(rtype, {"new": 0, "changed": 0, "unchanged": 0, "not_seen": 0})["not_seen"] = n

        def work(conn) -> tuple[dict[str, int], dict[str, int]]:
            # keys: first the resources of this release, then every id their rows reference
            conn.exec_driver_sql(f"INSERT INTO {state} (resource_type_id, resource_id) "
                                 f"SELECT resource_type_id, resource_id FROM {delta} WHERE resource_key IS NULL")
            conn.exec_driver_sql(f"UPDATE d SET resource_key = st.resource_key FROM {delta} d JOIN {state} st "
                                 f"ON st.resource_type_id = d.resource_type_id AND st.resource_id = d.resource_id "
                                 f"WHERE d.resource_key IS NULL")
            conn.exec_driver_sql(f"CREATE UNIQUE INDEX ux_delta_key ON {delta} (resource_key) INCLUDE (kind)")
            for t in ALL_TABLES:
                for col, target in ref_columns(t).items():
                    c = self.q(col)
                    conn.exec_driver_sql(
                        f"INSERT INTO {state} (resource_type_id, resource_id) SELECT DISTINCT {type_ids[target]}, x.{c} "
                        f"FROM {self.q(s, t.name)} x WHERE x.{c} IS NOT NULL AND NOT EXISTS (SELECT 1 FROM {state} st "
                        f"WHERE st.resource_type_id = {type_ids[target]} AND st.resource_id = x.{c})")
            inserted, replaced = {}, {}
            for t in ALL_TABLES:
                target, staged = self.q(schema, t.name), self.q(s, t.name)
                rtype = TABLE_TYPES[t.name]
                replaced[t.name] = conn.exec_driver_sql(
                    f"DELETE x FROM {target} x JOIN {delta} d ON d.resource_key = x.resource_key "
                    f"WHERE d.kind = 'C'").rowcount
                select, joins = ["x.release_date", "d.resource_key", "x.ndjson_file_id", "x.zst_file_id"], []
                if t.each:
                    select.append("x.seq")
                for i, (name, col) in enumerate(t.cols.items()):
                    if col.target:
                        a = f"k{i}"
                        select.append(f"{a}.resource_key")
                        joins.append(f"LEFT JOIN {state} {a} ON {a}.resource_type_id = {type_ids[col.target]} "
                                     f"AND {a}.resource_id = x.{self.q(name)}")
                    else:
                        select.append(f"x.{self.q(name)}")
                match = ("d.resource_type = x.resource_type" if rtype is None
                         else f"d.resource_type_id = {type_ids[rtype]}")
                inserted[t.name] = conn.exec_driver_sql(
                    f"INSERT INTO {target} WITH (TABLOCK) ({', '.join(self.q(c) for c in key_columns(t))}) "
                    f"SELECT {', '.join(select)} FROM {staged} x JOIN {delta} d ON d.resource_id = x.resource_id "
                    f"AND {match} AND d.kind IN ('N', 'C') {' '.join(joins)}").rowcount
            conn.exec_driver_sql(
                f"UPDATE st SET "
                f"hash = CASE WHEN d.kind = 'U' THEN st.hash ELSE d.hash END, "
                f"last_updated = CASE WHEN d.kind = 'U' THEN st.last_updated ELSE d.last_updated END, "
                f"release_date = CASE WHEN d.kind = 'U' THEN st.release_date ELSE CAST(? AS date) END, "
                f"run_id = CASE WHEN d.kind = 'U' THEN st.run_id ELSE ? END, "
                f"last_seen_release = ?, last_seen_run_id = ? "
                f"FROM {state} st JOIN {delta} d ON d.resource_key = st.resource_key",
                (release, run_id, release, run_id))
            totals = {k: sum(v[k] for v in kinds.values()) for k in ("new", "changed", "unchanged", "not_seen")}
            conn.exec_driver_sql(
                f"MERGE {self.q(schema, 'release')} AS t USING (SELECT CAST(? AS date) AS release_date) AS s "
                f"ON t.release_date = s.release_date "
                f"WHEN MATCHED THEN UPDATE SET import_run_id = ?, published_at = SYSUTCDATETIME(), new_resources = ?, "
                f"changed_resources = ?, unchanged_resources = ?, not_seen_resources = ? "
                f"WHEN NOT MATCHED THEN INSERT (release_date, import_run_id, new_resources, changed_resources, "
                f"unchanged_resources, not_seen_resources) VALUES (s.release_date, ?, ?, ?, ?, ?);",
                (release, run_id, totals["new"], totals["changed"], totals["unchanged"], totals["not_seen"],
                 run_id, totals["new"], totals["changed"], totals["unchanged"], totals["not_seen"]))
            return inserted, replaced
```

The rest of `apply_delta` (the `_locked_transaction` call, log line, return) is unchanged. In the `UPDATE STATISTICS` loop afterwards also update
`resource_state` when anything was inserted (`if any(inserted.values()): ... UPDATE STATISTICS {state}` with the same
best-effort try/except).

Why `kind = 'U'` tests in the UPDATE: an `N` row may be a referenced-only id that already had a key; it must get its
hash and content release like a changed one.

README: in the paragraph that introduces `npd.resource_state`, say that it is the key registry (`resource_key`, `int`,
assigned the first time an id is seen as a resource or as a reference, never reused); that data tables use
`resource_key` and reference columns `<name>_key`; that ids are stored without their `Type-` prefix; and that a key
whose `hash` is NULL is a referenced id with no data yet (find such references with a `LEFT JOIN`).

- [ ] **Step 4: Run the SQL Server suite**

Run: `.venv/Scripts/python -m pytest -q` (whole suite, `NPD_TEST_MSSQL_DB` set)
Expected: all pass (≈155 tests).

- [ ] **Step 5: Commit**

```bash
git add src/npd_loader/dialect/mssql.py tests/test_mssql_delta.py tests/test_mssql_import.py tests/test_mssql_e2e.py README.md
git commit -m "feat(phase2): apply_delta assigns surrogate keys and translates text ids by join"
```

### Task 12: Acceptance against Phase 1 and performance in npd_dev (manual, with the user)

**Files:**
- Create: `docs/profile/2026-10-XX-phase2-dev-import.md` (actual date)

Prerequisites (ask the user before starting): writing a new IMPORT run to `HIE_WAREHOUSE_META_DEV` is approved. Since
2026-10-07 the Phase 1 result for 2026-09-29 lives in the database `npd_proof` (the former `npd_dev`, renamed and kept
for investigation), and `npd_dev` is a new empty database with SIMPLE recovery and `READ_COMMITTED_SNAPSHOT ON`.

- [ ] **Step 1: Configure.** `config.local.toml` already points at `database.dev.env` (`npd_dev` +
`HIE_WAREHOUSE_META_DEV`); update its `[npd_db]` section to the Phase 2 keys (`schema = "npd"`,
`stage_schema = "npd_stage"`, no `raw_schema`).
- [ ] **Step 2: Run** `npd-loader --config config.local.toml init-db`, then
`npd-loader --config config.local.toml import --release 2026-09-29 --force` (`--force` because dev run 7009 already
recorded 2026-09-29 as imported; the .ndjson files are on `E:`). Record start/end per phase from the log (flatten, bcp,
apply).
- [ ] **Step 3: Compare with Phase 1** for each of the 26 tables (read-only). Phase 2 stores keys, so the Phase 2
side is rebuilt with text ids (`Type-` prefix put back) before `EXCEPT`. Generate the 26 queries with this scratch
script (not committed) and run them with `sqlcmd -S cssnpi -E -i compare.sql`:

```python
from npd_loader.flatten.engine import columns, ref_columns
from npd_loader.flatten.specs import ALL_TABLES, TABLE_TYPES
out = []
for t in ALL_TABLES:
    rtype, refs, sel, joins = TABLE_TYPES[t.name], ref_columns(t), [], []
    for c in columns(t):
        if c == "resource_id":
            sel.append("rt.name + '-' + s.resource_id AS resource_id")
        elif c == "resource_type":
            sel.append("rt.name AS resource_type")
        elif c in refs:
            a = f"k_{c}"
            sel.append(f"'{refs[c]}-' + {a}.resource_id AS [{c}]")
            joins.append(f"LEFT JOIN npd_dev.npd.resource_state {a} ON {a}.resource_key = x.[{c[:-3]}_key]")
        else:
            sel.append(f"x.[{c}]")
    p2 = (f"SELECT {', '.join(sel)} FROM npd_dev.npd.[{t.name}] x JOIN npd_dev.npd.resource_state s "
          f"ON s.resource_key = x.resource_key JOIN npd_dev.npd.resource_type rt ON rt.resource_type_id = "
          f"s.resource_type_id {' '.join(joins)}")
    p1 = f"SELECT {', '.join(f'[{c}]' for c in columns(t))} FROM npd_proof.npd.[{t.name}] WHERE release_date = '2026-09-29'"
    out.append(f"SELECT '{t.name}' AS t, (SELECT COUNT_BIG(*) FROM ({p1}) a) AS phase1, "
               f"(SELECT COUNT_BIG(*) FROM ({p2}) b) AS phase2, "
               f"(SELECT COUNT_BIG(*) FROM ({p1} EXCEPT {p2}) c) AS only_phase1, "
               f"(SELECT COUNT_BIG(*) FROM ({p2} EXCEPT {p1}) d) AS only_phase2;")
open("compare.sql", "w").write("SET NOCOUNT ON;
" + "
".join(out) + "
")
```

Also record: `SELECT COUNT(*) FROM npd_dev.npd.resource_state WHERE hash IS NULL` (references to missing data; 0
expected for 2026-09-29) and the size of every table (`sp_spaceused`) next to its `npd_proof` size.

Record the results; explain every non-zero difference (expected sources: extension first-match vs Phase 1 MAX;
millisecond rounding at exact .0005 boundaries).
- [ ] **Step 4: Second release** if a newer CMS release is available: `npd-loader --config config.phase2.local.toml run`
and record the delta counts and duration.
- [ ] **Step 5: Commit** the profile doc.

```bash
git add docs/profile/
git commit -m "docs: Phase 2 dev import — timings and comparison with Phase 1"
```
