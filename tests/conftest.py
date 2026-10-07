import json as _json
import os
import uuid

import pytest


@pytest.fixture
def cms():
    from fake_cms import FakeCms
    server = FakeCms().start()
    yield server
    server.stop()


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
                created.append(name)
        return names

    yield factory
    drop_schemas(mssql_engine, created)


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
