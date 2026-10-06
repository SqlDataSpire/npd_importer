import os
import uuid
from pathlib import Path

import psycopg
import pytest
from psycopg.conninfo import conninfo_to_dict, make_conninfo

PG_IMAGE = os.environ.get("NPD_TEST_PG_IMAGE", "postgres:16")
TESTS = Path(__file__).resolve().parent


def _with_dbname(admin_dsn: str, dbname: str) -> str:
    """Build a conninfo for `dbname` on the same server as `admin_dsn`, changing only dbname.

    Shared by both the external-server path (R1) and the testcontainers path, so neither has to
    know how the other addresses the server.
    """
    info = conninfo_to_dict(admin_dsn)
    info["dbname"] = dbname
    return make_conninfo(**info)


@pytest.fixture(scope="session")
def pg_server():
    """Yields an admin conninfo string (dbname=postgres) for a running Postgres server.

    If NPD_TEST_PG_DSN is set, that DSN is used as-is and no container is started (R1: for
    environments without Docker where a Postgres server is already available). Otherwise a
    disposable Postgres container is started via testcontainers, as in the original plan.
    """
    env_dsn = os.environ.get("NPD_TEST_PG_DSN")
    if env_dsn:
        yield env_dsn
        return

    try:
        from testcontainers.postgres import PostgresContainer
        container = PostgresContainer(PG_IMAGE, username="test", password="test", dbname="postgres")
        container.start()
    except Exception as exc:  # Docker not running / not installed
        pytest.skip(f"Docker Postgres unavailable: {exc}")
    try:
        yield make_conninfo(host=container.get_container_host_ip(), port=int(container.get_exposed_port(5432)),
                             user="test", password="test", dbname="postgres")
    finally:
        container.stop()


@pytest.fixture
def make_db(pg_server):
    admin_dsn = pg_server
    created: list[str] = []

    def factory() -> str:
        name = f"t_{uuid.uuid4().hex[:12]}"
        with psycopg.connect(admin_dsn, autocommit=True) as conn:
            conn.execute(f'CREATE DATABASE "{name}"')
        created.append(name)
        return _with_dbname(admin_dsn, name)

    yield factory
    with psycopg.connect(admin_dsn, autocommit=True) as conn:
        for name in created:
            conn.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')


@pytest.fixture
def catalog_db(make_db) -> str:
    info = make_db()
    with psycopg.connect(info, autocommit=True) as conn:
        conn.execute((TESTS / "sql" / "css_catalog_schema.sql").read_text())
    return info


@pytest.fixture
def cms():
    from fake_cms import FakeCms
    server = FakeCms().start()
    yield server
    server.stop()


@pytest.fixture
def npd_db(make_db) -> str:
    from npd_loader.schema import init_db
    info = make_db()
    init_db(info, "npd_raw", "npd")
    return info


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
