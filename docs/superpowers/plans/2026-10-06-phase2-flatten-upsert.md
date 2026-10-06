# Phase 2: Python Flattening, Per-Type Staging and Delta Upserts — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the Phase 1 import (raw JSON table, T-SQL transforms, partition-SWITCH publish of per-release
snapshots) with Python flattening from declarative table specs, `bcp` into per-run staging tables, and a one-transaction
delta upsert into a single current dataset in SQL Server.

**Architecture:** Each `.ndjson` is streamed by a worker process that parses every line once with `orjson`, hashes it
and flattens it with the 26 table specs into stage files (field `0x1F`, row `0x1E`). `MssqlDialect.stage_release` bulk
loads those files with `bcp` into `npd_stage.<table>__r<run>` heaps; `MssqlDialect.apply_delta` classifies every
resource as new/changed/unchanged/deleted against `npd.resource_state` and, in one transaction, replaces the rows of
changed/deleted resources and inserts new ones. Download, extract, the catalog and the CLI are unchanged.

**Tech Stack:** Python 3.12, orjson, Python-DataEngine (SQLAlchemy + pyodbc), `bcp.exe` (ODBC 17/18 tools), SQL Server
2019, pytest.

**Spec:** `docs/superpowers/specs/2026-10-06-phase2-python-flatten-upsert-design.md` (approved 2026-10-06; proposed
decisions P1–P4 accepted as proposed: hard delete, SHA-1 of the raw line, SQL Server only, branch
`feature/phase2-flatten`).

## Global Constraints

- Branch `feature/phase2-flatten`, created from `feature/sqlport-v1`. Never commit to `main` or `feature/sqlport-v1`.
- Python `>=3.12`. Dependencies: `Python-DataEngine>=2.4`, `zstandard>=0.22`, `httpx>=0.27`, `orjson>=3.10`.
- SQL Server only (P3): `dialect_for` rejects a Postgres connection with a `ConfigError`; Postgres code and tests are removed on this branch (`main` remains the Postgres loader).
- Windows auth only (`"trusted": "yes"`); no passwords in committed files.
- Tests never touch `HIE_WAREHOUSE_META*`, `npd` or `npd_dev`; SQL Server tests use `NPD_TEST_MSSQL_DB` (`cssnpi.npd_test`) scratch schemas and skip when it is unset.
- Stage files: `bcp -c -C 65001`, field terminator `0x1F`, row terminator `0x1E`, `NULL` = empty field with `-k`; a value containing `\x1f` or `\x1e` is rejected naming file and line.
- `bcp` success is judged by its output (`N rows copied`, no `Error`) and by row counts in the staging table — never by its exit code alone.
- Every staging table name ends in `__r<run_id>` and lives in `NpdDbConfig.stage_schema` (default `npd_stage`).
- The delta apply is exactly one transaction (`SET XACT_ABORT ON`, `LOCK_TIMEOUT`, whole-transaction retry on error 1222 as in Phase 1).
- Deleted resources are hard-deleted (P1); only resource types present in the release can have deletions; more than `max_delete_share` (default 0.2) of a type's current rows marked deleted fails the run before applying.
- Change detection hash: SHA-1 hex of the line's UTF-8 bytes without the line terminator (P2).
- Converter semantics follow Phase 1: FHIR dateTime → UTC `datetime2(3)` rounded half-up to milliseconds, partial dates (`YYYY`, `YYYY-MM`, `YYYY-MM-DD`) → midnight; extensions/identifiers/name pick = first match by array position; `location.description` longer than 4000 characters → NULL.
- Commit messages end with a blank line, then `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- Two deliberate simplifications of the spec, recorded here: stage files are written for a whole release first, then loaded (not handed to `bcp` per 500k-row buffer while flattening); `src/npd_loader/sql/mapped_paths.txt` stays a static file (not generated from the specs).

## Review Focus

1. A release older than the newest applied one (e.g. `import --release` of last month) → refused with a clear message unless `--force` (Task 9 test).
2. A truncated or partial CMS file that drops most of a type → the deletion safety check fails the run and nothing changes (Task 8 test).
3. A value longer than its column (e.g. a 300-character city) → the run fails naming the table and the bcp error, staging dropped, nothing applied (Task 7 test).
4. An import killed during staging → the next import drops its `__r<run>` stage tables and the catalog run is closed as interrupted (Task 9 test).
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
| `src/npd_loader/dialect/mssql.py` | Phase 2 `MssqlDialect`: init_db, run_lock, published_releases, stage_release, apply_delta, drop_stage_tables |
| `src/npd_loader/dialect/__init__.py` | Phase 2 `Dialect` protocol, `StageResult`, `DeltaResult`, `DeltaRejected`, `dialect_for` |
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
- Produces: `NpdDbConfig(connection: str, schema: str = "npd", stage_schema: str = "npd_stage", lock_timeout_seconds: float = 30.0, max_delete_share: float = 0.2, flatten_workers: int = 4, bcp_workers: int = 8)`; a config with `raw_schema` raises `ConfigError` mentioning `stage_schema`; `stage_schema == schema` raises `ConfigError`.
- Produces: tables `npd.resource_state` and extended `npd.release` (see SQL below); permanent tables with clustered primary keys and no partitioning; `MssqlDialect._tokens()` = `schema`, `s:schema`, `stage_schema`, `s:stage_schema`; `MssqlDialect.init_db()` runs the init scripts and creates `v_<table>` pass-through views for every table in `ALL_TABLES`.
- Fixture `mssql_dialect` (tests/conftest.py): fresh `t<hex>` and `t<hex>_stage` schemas, `NpdDbConfig(connection="data", schema=data, stage_schema=stage, lock_timeout_seconds=2, flatten_workers=1, bcp_workers=4)`, `init_db()` run.

- [ ] **Step 1: Config** — in `config.py` replace `NpdDbConfig` with:

```python
@dataclass(frozen=True)
class NpdDbConfig:
    connection: str
    schema: str = "npd"
    stage_schema: str = "npd_stage"
    lock_timeout_seconds: float = 30.0   # how long the delta apply waits for a table lock (3 attempts)
    max_delete_share: float = 0.2        # fail an import that would delete more than this share of a type's rows
    flatten_workers: int = 4             # parallel .ndjson files being flattened
    bcp_workers: int = 8                 # parallel bcp loads
```

and in `parse_config` build it with
`NpdDbConfig(connection=_req(npd, "npd_db", "connection"), **_optional({k: v for k, v in npd.items() if k != "connection"}, "npd_db", NpdDbConfig))`
after this check: `if "raw_schema" in npd: raise ConfigError("[npd_db] raw_schema was replaced by stage_schema (Phase 2 has no raw table)")`.
Range checks: `lock_timeout_seconds > 0`; `0 < max_delete_share <= 1`; `flatten_workers >= 1`; `bcp_workers >= 1`;
`schema != stage_schema` (each with a `ConfigError` naming the key). In `config.example.toml` `[npd_db]` replace
`raw_schema = "npd_raw"` with `stage_schema = "npd_stage"` and add `max_delete_share = 0.2`. In `tests/helpers.py`
`config_data(...)`: `schemas` becomes `("npd_stage", "npd")` → `"npd_db": {"connection": "data", "stage_schema": schemas[0], "schema": schemas[1]}`.
Update `tests/test_config.py`: replace `cfg.npd_db.raw_schema == "npd_raw"` with `cfg.npd_db.stage_schema == "npd_stage"`;
the old `raw_schema == schema` test becomes `stage_schema == schema`; add:

```python
def test_raw_schema_key_is_rejected():
    data = minimal()
    data["npd_db"]["raw_schema"] = "npd_raw"
    with pytest.raises(ConfigError, match="stage_schema"):
        parse_config(data)


@pytest.mark.parametrize("key,value", [("max_delete_share", 0), ("max_delete_share", 1.5), ("flatten_workers", 0)])
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
        deleted_resources   int          NULL
    )
GO
-- The current version of every resource: its content hash decides new/changed/unchanged/deleted.
IF OBJECT_ID(<<s:schema>> + N'.resource_state', N'U') IS NULL
    CREATE TABLE <<schema>>.resource_state (
        resource_type varchar(40)  NOT NULL,
        resource_id   varchar(128) NOT NULL,
        hash          char(40)     NOT NULL,
        last_updated  datetime2(3) NULL,
        release_date  date         NOT NULL,
        run_id        int          NOT NULL,
        CONSTRAINT pk_resource_state PRIMARY KEY CLUSTERED (resource_type, resource_id)
    ) WITH (DATA_COMPRESSION = PAGE)
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
        unchanged_resources int NULL, deleted_resources int NULL
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
        from npd_loader.flatten.specs import ALL_TABLES
        tokens = self._tokens()
        with self._autocommit() as conn:
            for name, text in sql_scripts("mssql", "init"):
                for batch in split_batches(render(text, tokens)):
                    conn.exec_driver_sql(batch)
            for t in ALL_TABLES:
                conn.exec_driver_sql(f"CREATE OR ALTER VIEW {self.q(self.cfg.schema, 'v_' + t.name)} AS "
                                     f"SELECT * FROM {self.q(self.cfg.schema, t.name)}")
```

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
    assert scalar(d, "SELECT count(*) FROM sys.partition_functions WHERE name LIKE ?", f"pf_{d.cfg.schema}%") == 0
    assert scalar(d, "SELECT count(*) FROM sys.views WHERE schema_id = SCHEMA_ID(?)", d.cfg.schema) == 26
    assert scalar(d, "SELECT count(*) FROM sys.objects WHERE schema_id = SCHEMA_ID(?) AND type IN ('FN','IF')",
                  d.cfg.schema) == 0
    for col in ("hash", "release_date", "run_id"):
        assert scalar(d, "SELECT COL_LENGTH(?, ?)", f"{d.cfg.schema}.resource_state", col) is not None
    assert scalar(d, "SELECT COL_LENGTH(?, 'deleted_resources')", f"{d.cfg.schema}.release") is not None


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
git commit -m "feat(phase2): unpartitioned current-dataset schema, resource_state, stage schema config"
```

---

### Task 7: bcp loader and `stage_release`

**Files:**
- Create: `src/npd_loader/dialect/bcp.py`
- Modify: `src/npd_loader/dialect/mssql.py`
- Test: `tests/test_mssql_stage.py`

**Interfaces:**
- Consumes: `flatten_files`, `FlattenResult`, `HASH_TABLE`, `HASH_COLUMNS`, `FlattenError` (Task 5); `ALL_TABLES`, `columns` (Tasks 3–4); `NdjsonInput`.
- Produces (`npd_loader.dialect.bcp`): `class BcpError(Exception)`; `bcp_target(engine) -> tuple[str, str]` (server, database from the ODBC connect string); `bcp_in(server, database, schema, table, path, expected_rows) -> int`.
- Produces on `MssqlDialect`: `stage_name(table, run_id) -> str` (`f"{table}__r{run_id}"`); `stage_release(storage, release: date, run_id: int, inputs: list[NdjsonInput]) -> StageResult` (StageResult defined here in `npd_loader.dialect`: `@dataclass StageResult(rows: dict[str, int], resources: dict[str, int])`); `drop_stage_tables(run_id: int | None = None) -> list[str]`.

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
    assert count(d, "resource_hash__r7") == 12
    assert count(d, "practitioner__r7") == 2 and count(d, "identifier__r7") == res.rows["identifier"]
    assert not list((tmp_path / "data" / "stage").rglob("*.dat"))          # stage files deleted after load


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


def test_drop_stage_tables(mssql_dialect, tmp_path):
    d = mssql_dialect
    storage = LocalStorage(tmp_path / "data")
    d.stage_release(storage, R, 7, inputs_for(storage, build_release("2026-09-29").ndjson))
    dropped = d.drop_stage_tables(7)
    assert f"{d.cfg.stage_schema}.resource_hash__r7" in dropped and len(dropped) == 27
    assert d.drop_stage_tables() == []
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

- [ ] **Step 4: Implement `stage_release` and `drop_stage_tables` in `MssqlDialect`** (add imports: `import os`,
`import shutil`, `from concurrent.futures import ThreadPoolExecutor`, `from npd_loader.dialect import StageResult`,
`from npd_loader.dialect.bcp import bcp_in, bcp_target`, `from npd_loader.flatten.engine import columns`,
`from npd_loader.flatten.specs import ALL_TABLES`,
`from npd_loader.flatten.stagefiles import HASH_COLUMNS, HASH_TABLE, FlattenError, flatten_files`). Add to
`src/npd_loader/dialect/__init__.py`:

```python
@dataclass
class StageResult:
    rows: dict[str, int]          # rows loaded per stage table (incl. resource_hash)
    resources: dict[str, int]     # resources per resource type
```

Methods:

```python
    STAGE_RE = r"__r{run}$"

    @staticmethod
    def stage_name(table: str, run_id: int) -> str:
        return f"{table}__r{run_id}"

    def _create_stage_tables(self, run_id: int) -> None:
        s = self.cfg.stage_schema
        with self._autocommit() as conn:
            for t in ALL_TABLES:
                cols = ", ".join(self.q(c) for c in columns(t))
                conn.exec_driver_sql(f"SELECT TOP 0 {cols} INTO {self.q(s, self.stage_name(t.name, run_id))} "
                                     f"FROM {self.q(self.cfg.schema, t.name)}")
            conn.exec_driver_sql(
                f"CREATE TABLE {self.q(s, self.stage_name(HASH_TABLE, run_id))} (resource_type varchar(40) NOT NULL, "
                f"resource_id varchar(128) NOT NULL, hash char(40) NOT NULL, last_updated datetime2(3) NULL, "
                f"release_date date NOT NULL, ndjson_file_id int NOT NULL, line_number bigint NOT NULL)")

    def stage_release(self, storage, release: date, run_id: int, inputs: list) -> StageResult:
        s = self.cfg.stage_schema
        self._create_stage_tables(run_id)
        out_dir = storage.local_path(f"stage/run_{run_id}")
        try:
            flat = flatten_files(inputs, storage, release, out_dir, self.cfg.flatten_workers)
            server, database = bcp_target(self.engine)
            with ThreadPoolExecutor(self.cfg.bcp_workers) as pool:
                futures = [pool.submit(bcp_in, server, database, s, self.stage_name(f.table, run_id), f.path, f.rows)
                           for f in flat.files]
                for fut in futures:
                    fut.result()
        finally:
            shutil.rmtree(out_dir, ignore_errors=True)
        with self.engine.connect() as conn:
            for table, expected in flat.rows.items():
                got = conn.exec_driver_sql(f"SELECT COUNT_BIG(*) FROM {self.q(s, self.stage_name(table, run_id))}").scalar()
                if got != expected:
                    raise FlattenError(f"stage table {table}: flattened {expected} rows but loaded {got}")
        self._check_duplicates(run_id, inputs)
        return StageResult(flat.rows, flat.resources)

    def _check_duplicates(self, run_id: int, inputs: list) -> None:
        hashes = self.q(self.cfg.stage_schema, self.stage_name(HASH_TABLE, run_id))
        try:
            with self._autocommit() as conn:
                conn.exec_driver_sql(f"CREATE UNIQUE CLUSTERED INDEX ux_hash ON {hashes} (resource_type, resource_id)")
        except DBAPIError as exc:
            if error_number(exc) != 1505:
                raise
            with self.engine.connect() as conn:
                dups = conn.exec_driver_sql(
                    f"SELECT TOP 20 resource_type, resource_id, MIN(ndjson_file_id), STRING_AGG(CAST(line_number AS "
                    f"varchar(20)), ',') WITHIN GROUP (ORDER BY line_number) FROM {hashes} "
                    f"GROUP BY resource_type, resource_id HAVING COUNT(*) > 1 ORDER BY 1, 2").fetchall()
            detail = "; ".join(f"{t} {i} at lines {lines}" for t, i, _, lines in dups)
            raise FlattenError(f"duplicate resource ids: {detail}", dups[0][2] if dups else None) from exc

    def drop_stage_tables(self, run_id: int | None = None) -> list[str]:
        """Drop the stage tables of import `run_id` (any run when None). Only safe with the import lock held."""
        pattern = re.compile(self.STAGE_RE.format(run=run_id if run_id is not None else r"\d+"))
        dropped: list[str] = []
        with self._autocommit() as conn:
            names = [r[0] for r in conn.exec_driver_sql(
                "SELECT name FROM sys.tables WHERE schema_id = SCHEMA_ID(?) ORDER BY name", (self.cfg.stage_schema,))]
            for name in names:
                if pattern.search(name):
                    conn.exec_driver_sql(f"DROP TABLE {self.q(self.cfg.stage_schema, name)}")
                    dropped.append(f"{self.cfg.stage_schema}.{name}")
        return dropped
```

Hex terminators (`-t 0x1f -r 0x1e`) are documented for bcp, and the spike proved `-r 0x0a` works. If the installed
bcp rejects them anyway, stop and report NEEDS_CONTEXT with the bcp output rather than switching formats.

- [ ] **Step 5: Run the tests**

Run: `.\.venv\Scripts\python -m pytest tests/test_mssql_stage.py -v`
Expected: PASS (4 tests).

- [ ] **Step 6: Commit**

```bash
git add src/npd_loader/dialect/bcp.py src/npd_loader/dialect/mssql.py src/npd_loader/dialect/__init__.py tests/test_mssql_stage.py
git commit -m "feat(phase2): stage_release — parallel flatten, bcp into per-run stage heaps, count and duplicate checks"
```

---

### Task 8: `apply_delta`

**Files:**
- Modify: `src/npd_loader/dialect/mssql.py`, `src/npd_loader/dialect/__init__.py`
- Test: `tests/test_mssql_delta.py`

**Interfaces:**
- Consumes: `stage_release`, `stage_name`, `_locked_transaction`, `drop_stage_tables` (Task 7); `SPECS`, `TABLE_TYPES`, `ALL_TABLES`, `columns`.
- Produces (`npd_loader.dialect`): `class DeltaRejected(Exception)`; `@dataclass DeltaResult(kinds: dict[str, dict[str, int]], inserted: dict[str, int], removed: dict[str, int])` (`kinds[resource_type] = {"new":…, "changed":…, "unchanged":…, "deleted":…}`).
- Produces on `MssqlDialect`: `apply_delta(release: date, run_id: int) -> DeltaResult` — classifies, checks `max_delete_share`, applies in one transaction, writes `npd.release`, drops the run's stage tables, updates statistics (best effort).

- [ ] **Step 1: Write the failing tests**

```python
import copy
import dataclasses
from datetime import date

import pytest

from npd_loader.dialect import DeltaRejected
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
    assert rows(d, f"SELECT import_run_id, new_resources, deleted_resources FROM {d.q(d.cfg.schema, 'release')}") == [(7, 12, 0)]
    assert d.drop_stage_tables() == []                                   # stage dropped after apply


def test_second_release_applies_only_the_delta(mssql_dialect, tmp_path):
    d = mssql_dialect
    load(d, tmp_path, R1, 7)
    records = copy.deepcopy(fixture_data.RECORDS)
    p = records["06-Practitioner.ndjson"][0]
    p["name"][0]["family"] = "GOMEZ-CHANGED"
    p["telecom"] = p["telecom"][:1]                                       # child rows shrink
    removed = records["08-OrganizationAffiliation.ndjson"].pop()          # one resource deleted
    d.cfg = dataclasses.replace(d.cfg, max_delete_share=1.0)
    res = load(d, tmp_path, R2, 8, records)
    assert res.kinds["Practitioner"] == {"new": 0, "changed": 1, "unchanged": 1, "deleted": 0}
    assert res.kinds["OrganizationAffiliation"]["deleted"] == 1
    pid = p["id"]
    assert rows(d, f"SELECT name_family FROM {d.q(d.cfg.schema, 'practitioner')} WHERE resource_id = ?", pid) == [("GOMEZ-CHANGED",)]
    assert rows(d, f"SELECT count(*) FROM {d.q(d.cfg.schema, 'practitioner_telecom')} WHERE resource_id = ?", pid) == [(1,)]
    assert rows(d, f"SELECT count(*) FROM {d.q(d.cfg.schema, 'organization_affiliation')} WHERE resource_id = ?",
                removed["id"]) == [(0,)]
    assert rows(d, f"SELECT count(*) FROM {d.q(d.cfg.schema, 'identifier')} WHERE resource_id = ?", removed["id"]) == [(0,)]
    assert rows(d, f"SELECT release_date FROM {d.q(d.cfg.schema, 'resource_state')} WHERE resource_id = ?", pid) == [(R2,)]
    assert [r[0] for r in rows(d, f"SELECT release_date FROM {d.q(d.cfg.schema, 'release')} ORDER BY 1")] == [R1, R2]


def test_too_many_deletions_are_rejected(mssql_dialect, tmp_path):
    d = mssql_dialect
    load(d, tmp_path, R1, 7)
    records = copy.deepcopy(fixture_data.RECORDS)
    records["01-Organization.ndjson"] = records["01-Organization.ndjson"][:1]      # 1 of 2 orgs gone = 50%
    with pytest.raises(DeltaRejected, match="Organization"):
        load(d, tmp_path, R2, 8, records)
    assert rows(d, f"SELECT count(*) FROM {d.q(d.cfg.schema, 'organization')}") == [(2,)]   # nothing applied


def test_failure_inside_apply_rolls_back(mssql_dialect, tmp_path, monkeypatch):
    d = mssql_dialect
    load(d, tmp_path, R1, 7)
    records = copy.deepcopy(fixture_data.RECORDS)
    records["06-Practitioner.ndjson"][0]["gender"] = "unknown"
    storage = LocalStorage(tmp_path / "data8")
    d.stage_release(storage, R2, 8, inputs_for(storage, build_release(R2.isoformat(), records=records).ndjson))
    with d._autocommit() as c:                                            # sabotage one stage table
        c.exec_driver_sql(f"DROP TABLE {d.q(d.cfg.stage_schema, 'practitioner_role__r8')}")
    with pytest.raises(Exception):
        d.apply_delta(R2, 8)
    assert rows(d, f"SELECT gender FROM {d.q(d.cfg.schema, 'practitioner')} ORDER BY resource_id")[0] == ("male",)
    assert [r[0] for r in rows(d, f"SELECT release_date FROM {d.q(d.cfg.schema, 'release')}")] == [R1]
```

- [ ] **Step 2: Run to verify failure**

Run: `.\.venv\Scripts\python -m pytest tests/test_mssql_delta.py -v`
Expected: FAIL (`ImportError: DeltaRejected`).

- [ ] **Step 3: Implement.** Add to `src/npd_loader/dialect/__init__.py`:

```python
class DeltaRejected(Exception):
    """The release would delete more than max_delete_share of a resource type's current rows."""


@dataclass
class DeltaResult:
    kinds: dict[str, dict[str, int]]   # resource type -> {"new", "changed", "unchanged", "deleted"}
    inserted: dict[str, int]           # rows inserted per table
    removed: dict[str, int]            # rows deleted per table
```

In `MssqlDialect` (imports: `from npd_loader.dialect import DeltaRejected, DeltaResult`,
`from npd_loader.flatten.specs import SPECS, TABLE_TYPES`):

```python
    KINDS = {"N": "new", "C": "changed", "U": "unchanged", "D": "deleted"}

    def apply_delta(self, release: date, run_id: int) -> DeltaResult:
        s, schema = self.cfg.stage_schema, self.cfg.schema
        hashes = self.q(s, self.stage_name(HASH_TABLE, run_id))
        delta = self.q(s, self.stage_name("delta", run_id))
        state = self.q(schema, "resource_state")
        with self._autocommit() as conn:
            conn.exec_driver_sql(f"DROP TABLE IF EXISTS {delta}")
            conn.exec_driver_sql(
                f"SELECT h.resource_type, h.resource_id, h.hash, h.last_updated, "
                f"CAST(CASE WHEN st.resource_id IS NULL THEN 'N' WHEN st.hash <> h.hash THEN 'C' ELSE 'U' END AS char(1)) AS kind "
                f"INTO {delta} FROM {hashes} h LEFT JOIN {state} st "
                f"ON st.resource_type = h.resource_type AND st.resource_id = h.resource_id")
            conn.exec_driver_sql(
                f"INSERT INTO {delta} (resource_type, resource_id, hash, last_updated, kind) "
                f"SELECT st.resource_type, st.resource_id, st.hash, st.last_updated, 'D' FROM {state} st "
                f"WHERE st.resource_type IN (SELECT DISTINCT resource_type FROM {hashes}) AND NOT EXISTS "
                f"(SELECT 1 FROM {hashes} h WHERE h.resource_type = st.resource_type AND h.resource_id = st.resource_id)")
            conn.exec_driver_sql(f"CREATE UNIQUE CLUSTERED INDEX ux_delta ON {delta} (resource_type, resource_id)")
            kinds: dict[str, dict[str, int]] = {}
            for rtype, kind, n in conn.exec_driver_sql(
                    f"SELECT resource_type, kind, COUNT_BIG(*) FROM {delta} GROUP BY resource_type, kind"):
                kinds.setdefault(rtype, dict.fromkeys(self.KINDS.values(), 0))[self.KINDS[kind]] = n
            current = dict(conn.exec_driver_sql(f"SELECT resource_type, COUNT_BIG(*) FROM {state} GROUP BY resource_type"))
        too_many = [f"{t}: {k['deleted']} of {current[t]}" for t, k in kinds.items()
                    if current.get(t) and k["deleted"] / current[t] > self.cfg.max_delete_share]
        if too_many:
            raise DeltaRejected(f"release {release} would delete more than {self.cfg.max_delete_share:.0%} of "
                                f"current rows ({'; '.join(too_many)}); check the CMS files or raise "
                                f"[npd_db] max_delete_share")

        def work(conn) -> tuple[dict[str, int], dict[str, int]]:
            inserted, removed = {}, {}
            for t in ALL_TABLES:
                target, staged = self.q(schema, t.name), self.q(s, self.stage_name(t.name, run_id))
                type_match = "d.resource_type = x.resource_type" if TABLE_TYPES[t.name] is None \
                    else f"d.resource_type = '{TABLE_TYPES[t.name]}'"
                removed[t.name] = conn.exec_driver_sql(
                    f"DELETE x FROM {target} x JOIN {delta} d ON d.resource_id = x.resource_id AND {type_match} "
                    f"AND d.kind IN ('C', 'D')").rowcount
                cols = ", ".join(self.q(c) for c in columns(t))
                inserted[t.name] = conn.exec_driver_sql(
                    f"INSERT INTO {target} WITH (TABLOCK) ({cols}) SELECT {cols} FROM {staged} x WHERE EXISTS "
                    f"(SELECT 1 FROM {delta} d WHERE d.resource_id = x.resource_id AND {type_match} "
                    f"AND d.kind IN ('N', 'C'))").rowcount
            conn.exec_driver_sql(f"DELETE st FROM {state} st JOIN {delta} d ON d.resource_type = st.resource_type "
                                 f"AND d.resource_id = st.resource_id AND d.kind IN ('C', 'D')")
            conn.exec_driver_sql(f"INSERT INTO {state} (resource_type, resource_id, hash, last_updated, release_date, "
                                 f"run_id) SELECT resource_type, resource_id, hash, last_updated, ?, ? FROM {delta} "
                                 f"WHERE kind IN ('N', 'C')", (release, run_id))
            totals = {k: sum(v[k] for v in kinds.values()) for k in self.KINDS.values()}
            conn.exec_driver_sql(
                f"MERGE {self.q(schema, 'release')} AS t USING (SELECT CAST(? AS date) AS release_date) AS s "
                f"ON t.release_date = s.release_date "
                f"WHEN MATCHED THEN UPDATE SET import_run_id = ?, published_at = SYSUTCDATETIME(), new_resources = ?, "
                f"changed_resources = ?, unchanged_resources = ?, deleted_resources = ? "
                f"WHEN NOT MATCHED THEN INSERT (release_date, import_run_id, new_resources, changed_resources, "
                f"unchanged_resources, deleted_resources) VALUES (s.release_date, ?, ?, ?, ?, ?);",
                (release, run_id, totals["new"], totals["changed"], totals["unchanged"], totals["deleted"],
                 run_id, totals["new"], totals["changed"], totals["unchanged"], totals["deleted"]))
            return inserted, removed

        inserted, removed = self._locked_transaction(work, f"apply release {release}")
        log.info("applied release %s: %s", release, {t: k for t, k in sorted(kinds.items())})
        self.drop_stage_tables(run_id)
        with self._autocommit() as conn:            # best effort: the delta is already committed
            for t in ALL_TABLES:
                if inserted.get(t.name) or removed.get(t.name):
                    try:
                        conn.exec_driver_sql(f"UPDATE STATISTICS {self.q(schema, t.name)}")
                    except Exception as exc:
                        log.warning("UPDATE STATISTICS %s.%s failed: %s", schema, t.name, exc)
        return DeltaResult(kinds, inserted, removed)
```

Note: `kind` for resource types with only unchanged/new rows still gets all four keys (the `dict.fromkeys` default).
A resource type that has no file in the release has no rows in `hashes`, so it gets no deletions.

- [ ] **Step 4: Run the tests**

Run: `.\.venv\Scripts\python -m pytest tests/test_mssql_delta.py tests/test_mssql_stage.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/npd_loader/dialect/mssql.py src/npd_loader/dialect/__init__.py tests/test_mssql_delta.py
git commit -m "feat(phase2): apply_delta — classify by hash, deletion safety check, one-transaction upsert"
```

---

### Task 9: Rewire the import, retention and CLI; Phase 2 Dialect protocol

**Files:**
- Modify: `src/npd_loader/import_stage.py`, `src/npd_loader/retention.py`, `src/npd_loader/dialect/__init__.py`, `src/npd_loader/cli.py`
- Test: `tests/test_mssql_import.py` (new), `tests/test_retention.py` (rewrite), `tests/test_dialect_factory.py` (update)

**Interfaces:**
- Consumes: `stage_release`, `apply_delta`, `drop_stage_tables`, `published_releases`, `StageResult`, `DeltaResult`, `DeltaRejected`, `FlattenError`.
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
    def drop_stage_tables(self, run_id: int | None = None) -> list[str]: ...
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


def test_killed_run_stage_tables_are_dropped_and_run_closed(ctx, cms):
    cms.publish(build_release("2026-09-29"))
    run_download(ctx)
    d = ctx.dialect
    with d._autocommit() as c:                                     # leftovers of a killed run 99
        c.exec_driver_sql(f"CREATE TABLE {d.q(d.cfg.stage_schema, 'practitioner__r99')} (x int)")
    open_run = ctx.catalog.start_run("IMPORT", "killed", "<WAREHOUSE_RUN_CONFIG />")
    assert run_import(ctx) is Outcome.SUCCESS
    assert d.drop_stage_tables() == []
    assert ctx.catalog.runs[open_run.id]["status"] == "Failed"


def test_flatten_error_is_recorded_on_the_file(ctx, cms):
    bad = build_release("2026-09-29", raw_ndjson={"01-Organization.ndjson": b'{"resourceType": "Organization"}\n'})
    cms.publish(bad)
    run_download(ctx)
    with pytest.raises(StageFailed, match="missing id"):
        run_import(ctx)
    org = [f for f in ctx.catalog.files.values() if (f.get("file_name") or "").endswith("01-Organization.ndjson")]
    assert org and "missing id" in (org[0].get("exceptions") or "")
    assert ctx.dialect.drop_stage_tables() == []
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
`from npd_loader.flatten.stagefiles import FlattenError`; keep `NdjsonInput` from `raw_load`. Replace `_drop_orphans`,
`_import` and the error handling:

```python
def _drop_orphans(ctx: Context) -> None:
    """With the import lock held no import is running, so stage tables of an earlier run are orphans."""
    try:
        dropped = ctx.dialect.drop_stage_tables()
    except Exception:
        log.exception("could not drop orphaned stage tables")
        return
    if dropped:
        log.warning("dropped %d stage tables of earlier interrupted imports: %s", len(dropped), ", ".join(dropped))


def _summary(staged: StageResult, delta: DeltaResult, schema: str) -> list[dict]:
    items: list[dict] = [{"resource_type": t, **k} for t, k in sorted(delta.kinds.items())]
    items += [{"table": f"{schema}.{t}", "inserted": delta.inserted.get(t, 0), "deleted": delta.removed.get(t, 0),
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
            try:
                ctx.dialect.drop_stage_tables(run.id)
            except Exception:
                log.exception("could not drop stage tables of run %s", run.id)
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
    cfg["npd_db"]["max_delete_share"] = 1.0
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
    gone = records["08-OrganizationAffiliation.ndjson"].pop()
    cms.publish(build_release("2026-10-06", records=records))
    assert cli("run") == 0
    assert scalar(f"SELECT count(*) FROM [{data}].[practitioner] WHERE gender = 'female'") == 2
    assert scalar(f"SELECT count(*) FROM [{data}].[organization_affiliation] WHERE resource_id = '{gone['id']}'") == 0
    assert scalar(f"SELECT changed_resources FROM [{data}].[release] WHERE release_date = '2026-10-06'") == 1
    assert scalar(f"SELECT deleted_resources FROM [{data}].[release] WHERE release_date = '2026-10-06'") == 1
    assert scalar(f"SELECT count(*) FROM sys.tables WHERE schema_id = SCHEMA_ID('{stage}')") == 0
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
git commit -m "test(phase2): two releases end to end through the CLI (first load, then delta with change and delete)"
```

---

### Task 12: Acceptance against Phase 1 and performance in npd_dev (manual, with the user)

**Files:**
- Create: `docs/profile/2026-10-XX-phase2-dev-import.md` (actual date)

Prerequisites (ask the user before starting): the Phase 1 tables for 2026-09-29 exist in `npd_dev.npd` (the SSMS
finish script ran); writing a new IMPORT run to `HIE_WAREHOUSE_META_DEV` is approved.

- [ ] **Step 1: Configure** `config.phase2.local.toml` (gitignored by `*.local.toml`) from `config.local.toml` with
`[npd_db] schema = "npd2"`, `stage_schema = "npd2_stage"`, `max_delete_share = 0.2`. Ask the user whether to enable
`READ_COMMITTED_SNAPSHOT` on `npd_dev` now (`ALTER DATABASE [npd_dev] SET READ_COMMITTED_SNAPSHOT ON`, needs a moment
with no other connections); the run works either way, readers just block during the apply without it.
- [ ] **Step 2: Run** `npd-loader --config config.phase2.local.toml init-db`, then
`npd-loader --config config.phase2.local.toml import --release 2026-09-29 --force` (the .ndjson files are on `E:`).
Record start/end per phase from the log (flatten, bcp, apply).
- [ ] **Step 3: Compare with Phase 1** for each of the 26 tables (read-only):

```sql
-- per table T: counts and differences both ways (identical column lists)
SELECT (SELECT COUNT_BIG(*) FROM npd_dev.npd.T WHERE release_date = '2026-09-29') AS phase1,
       (SELECT COUNT_BIG(*) FROM npd_dev.npd2.T) AS phase2,
       (SELECT COUNT_BIG(*) FROM (SELECT * FROM npd_dev.npd.T WHERE release_date = '2026-09-29'
                                  EXCEPT SELECT * FROM npd_dev.npd2.T) x) AS only_phase1,
       (SELECT COUNT_BIG(*) FROM (SELECT * FROM npd_dev.npd2.T
                                  EXCEPT SELECT * FROM npd_dev.npd.T WHERE release_date = '2026-09-29') x) AS only_phase2;
```

Record the results; explain every non-zero difference (expected sources: extension first-match vs Phase 1 MAX;
millisecond rounding at exact .0005 boundaries).
- [ ] **Step 4: Second release** if a newer CMS release is available: `npd-loader --config config.phase2.local.toml run`
and record the delta counts and duration.
- [ ] **Step 5: Commit** the profile doc.

```bash
git add docs/profile/
git commit -m "docs: Phase 2 dev import — timings and comparison with Phase 1"
```
