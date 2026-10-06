import os
import uuid
from pathlib import Path

import psycopg2
import pytest
from psycopg2.extensions import make_dsn

PG_IMAGE = os.environ.get("NPD_TEST_PG_IMAGE", "postgres:16")
TESTS = Path(__file__).resolve().parent


def _admin_execute(dsn: str, statement: str) -> None:
    """Run `statement` on its own autocommit psycopg2 connection (CREATE/DROP DATABASE need one)."""
    conn = psycopg2.connect(dsn)
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(statement)
    finally:
        conn.close()


@pytest.fixture(scope="session")
def pg_server():
    """Yields an admin libpq DSN (dbname=postgres) for a running Postgres server.

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
        yield make_dsn(host=container.get_container_host_ip(), port=int(container.get_exposed_port(5432)),
                       user="test", password="test", dbname="postgres")
    finally:
        container.stop()


@pytest.fixture
def make_db(pg_server):
    """Factory: a new empty database on the test server, as a DSN; all dropped after the test."""
    from pg_helpers import with_dbname
    admin_dsn = pg_server
    created: list[str] = []

    def factory() -> str:
        name = f"t_{uuid.uuid4().hex[:12]}"
        _admin_execute(admin_dsn, f'CREATE DATABASE "{name}"')
        created.append(name)
        return with_dbname(admin_dsn, name)

    yield factory
    for name in created:
        _admin_execute(admin_dsn, f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')


@pytest.fixture
def catalog_db(make_db) -> str:
    dsn = make_db()
    _admin_execute(dsn, (TESTS / "sql" / "css_catalog_schema.sql").read_text())
    return dsn


@pytest.fixture
def cms():
    from fake_cms import FakeCms
    server = FakeCms().start()
    yield server
    server.stop()


@pytest.fixture
def npd_db(make_db) -> str:
    """DSN of a new database with init-db applied (schemas npd_raw and npd)."""
    from helpers import pg_dialect
    dsn = make_db()
    d = pg_dialect(dsn)
    d.init_db()
    d.engine.dispose()
    return dsn


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
                created.append(name)
        return names

    yield factory
    drop_schemas(mssql_engine, created)


@pytest.fixture
def mssql_dialect(mssql_engine, mssql_schemas):
    from npd_loader.config import NpdDbConfig
    from npd_loader.dialect.mssql import MssqlDialect
    raw, data = mssql_schemas("raw", "")
    d = MssqlDialect(mssql_engine, NpdDbConfig(connection="data", raw_schema=raw, schema=data,
                                               lock_timeout_seconds=2), sleep=lambda s: None)
    d.init_db()
    return d
