"""Postgres test helpers: psycopg2 connections to a test database (given as a libpq DSN), SQLAlchemy engines for
the dialect and the catalog, and fixture-release loading through PostgresDialect."""
from __future__ import annotations

from contextlib import contextmanager
from datetime import date
from typing import Iterator

import psycopg2
from psycopg2.extensions import make_dsn, parse_dsn
from sqlalchemy import create_engine
from sqlalchemy.engine import URL, Engine

from npd_loader.manifest import resource_type_for
from npd_loader.raw_load import NdjsonInput
from release_builder import build_release

R = date(2026, 9, 29)


def with_dbname(dsn: str, dbname: str) -> str:
    """A DSN for `dbname` on the same server as `dsn`, changing only dbname."""
    return make_dsn(dsn, dbname=dbname)


def dsn_parts(dsn: str) -> dict:
    """host, port, user, password and dbname of a DSN (port defaults to 5432)."""
    info = parse_dsn(dsn)
    return {"host": info.get("host", "localhost"), "port": int(info.get("port", 5432)),
            "user": info.get("user", ""), "password": info.get("password", ""), "dbname": info.get("dbname", "")}


def pg_engine(dsn: str) -> Engine:
    p = dsn_parts(dsn)
    return create_engine(URL.create("postgresql+psycopg2", username=p["user"], password=p["password"],
                                    host=p["host"], port=p["port"], database=p["dbname"]))


def pg_doc(dsn: str) -> dict:
    """database.env document for a DSN (what DataEngine's PgConnectionObject is built from)."""
    p = dsn_parts(dsn)
    return {"type": "postgres", "server": f"{p['host']}:{p['port']}", "database": p["dbname"],
            "UN": p["user"], "PW": p["password"]}


class PgConn:
    """A psycopg2 connection with psycopg-style execute(): returns the cursor, so .fetchone()/.fetchall() chain."""

    def __init__(self, raw):
        self.raw = raw

    def execute(self, query: str, args: tuple | list | None = None):
        cur = self.raw.cursor()
        cur.execute(query, args or None)
        return cur

    def commit(self) -> None:
        self.raw.commit()

    def rollback(self) -> None:
        self.raw.rollback()

    def close(self) -> None:
        self.raw.close()


def open_conn(dsn: str, autocommit: bool = False) -> PgConn:
    raw = psycopg2.connect(dsn)
    raw.autocommit = autocommit
    return PgConn(raw)


@contextmanager
def connect(dsn: str, autocommit: bool = False) -> Iterator[PgConn]:
    """Commit at the end of the block (roll back on an error), then close."""
    conn = open_conn(dsn, autocommit)
    try:
        yield conn
        if not autocommit:
            conn.commit()
    except BaseException:
        if not autocommit:
            conn.rollback()
        raise
    finally:
        conn.close()


def one(dsn: str, query: str, *args):
    with connect(dsn) as conn:
        return conn.execute(query, args).fetchone()[0]


def write_inputs(storage, ndjson: dict[str, bytes], first_id: int = 500) -> list[NdjsonInput]:
    inputs = []
    for i, (name, data) in enumerate(sorted(ndjson.items())):
        file_id = first_id + 2 * i
        rel = f"run_1_2026-09-29-000000/file_{file_id}_{name}"
        with storage.open_write(rel) as f:
            f.write(data)
        inputs.append(NdjsonInput(file_id=file_id, zst_file_id=file_id + 1, rel_path=rel,
                                  resource_type=resource_type_for(name), name=name))
    return inputs


def load_fixture_raw(dialect, storage, ndjson=None, release=R, run_id=7):
    ndjson = ndjson if ndjson is not None else build_release(release.isoformat()).ndjson
    return dialect.load_raw(storage, release, run_id, write_inputs(storage, ndjson))


def transformed(dialect, storage, run_id=7):
    raw = load_fixture_raw(dialect, storage, run_id=run_id)
    return dialect.run_transforms(raw.table, R, run_id)


def rows(conn: PgConn, result, table: str, columns: str) -> list[tuple]:
    has_seq = conn.execute("SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'npd' "
                           "AND table_name = %s AND column_name = 'seq')", (table,)).fetchone()[0]
    order = "resource_id, seq" if has_seq else "resource_id"
    return conn.execute(f'SELECT {columns} FROM "npd"."{result.tables[table]}" ORDER BY {order}').fetchall()
