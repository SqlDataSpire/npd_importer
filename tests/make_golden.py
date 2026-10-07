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
