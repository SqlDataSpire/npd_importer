"""Postgres helpers for the npd database."""
from __future__ import annotations

import hashlib
import re
from contextlib import contextmanager
from datetime import date
from typing import Iterator, Mapping

import psycopg
from psycopg import sql

TOKEN_RE = re.compile(r"<<([a-z_]+(?::[a-z_]+)?)>>")
BOUND_RE = re.compile(r"FOR VALUES IN \('(\d{4}-\d{2}-\d{2})'\)")
MAX_IDENTIFIER = 63


def render_sql(text: str, tokens: Mapping[str, sql.Composable], conn: psycopg.Connection) -> str:
    def substitute(match: re.Match) -> str:
        key = match.group(1)
        if key not in tokens:
            raise KeyError(f"unknown SQL token <<{key}>>")
        return tokens[key].as_string(conn)
    return TOKEN_RE.sub(substitute, text)


def standalone_name(table: str, release: date, run_id: int) -> str:
    name = f"{table}__{release:%Y%m%d}__r{run_id}"
    if len(name) > MAX_IDENTIFIER:
        raise ValueError(f"table name {name!r} is longer than {MAX_IDENTIFIER} characters")
    return name


def list_parent_tables(conn: psycopg.Connection, schema: str) -> list[str]:
    rows = conn.execute(
        "SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname = %s AND c.relkind = 'p' AND NOT c.relispartition ORDER BY c.relname", (schema,)).fetchall()
    return [r[0] for r in rows]


def list_release_partitions(conn: psycopg.Connection, schema: str, parent: str) -> dict[date, str]:
    rows = conn.execute(
        "SELECT c.relname, pg_get_expr(c.relpartbound, c.oid) FROM pg_inherits i "
        "JOIN pg_class c ON c.oid = i.inhrelid JOIN pg_class p ON p.oid = i.inhparent "
        "JOIN pg_namespace n ON n.oid = p.relnamespace WHERE n.nspname = %s AND p.relname = %s",
        (schema, parent)).fetchall()
    found = {}
    for name, bound in rows:
        m = BOUND_RE.search(bound or "")
        if m:
            found[date.fromisoformat(m.group(1))] = name
    return found


def clone_parent_indexes(conn: psycopg.Connection, schema: str, parent: str, target: sql.Composable) -> None:
    """Build the parent's indexes on a standalone table so ATTACH PARTITION reuses them instead of building
    them while holding the parent lock."""
    rows = conn.execute(
        "SELECT pg_get_indexdef(i.indexrelid), i.indisunique FROM pg_index i "
        "JOIN pg_class c ON c.oid = i.indrelid JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname = %s AND c.relname = %s ORDER BY i.indexrelid", (schema, parent)).fetchall()
    for indexdef, unique in rows:
        tail = indexdef[indexdef.index(" USING "):]
        conn.execute(sql.SQL("CREATE {}INDEX ON {}{}").format(
            sql.SQL("UNIQUE " if unique else ""), target, sql.SQL(tail)))


def _lock_key(name: str) -> int:
    return int.from_bytes(hashlib.sha256(f"npd_loader:{name}".encode()).digest()[:8], "big", signed=True)


@contextmanager
def advisory_lock(conninfo: str, name: str) -> Iterator[bool]:
    """Session-level advisory lock held on its own connection for the whole stage."""
    key = _lock_key(name)
    with psycopg.connect(conninfo, autocommit=True) as conn:
        acquired = conn.execute("SELECT pg_try_advisory_lock(%s)", (key,)).fetchone()[0]
        try:
            yield acquired
        finally:
            if acquired:
                conn.execute("SELECT pg_advisory_unlock(%s)", (key,))


def published_releases(conninfo: str, schema: str) -> list[date]:
    with psycopg.connect(conninfo) as conn:
        rows = conn.execute(sql.SQL("SELECT release_date FROM {}.release ORDER BY release_date")
                            .format(sql.Identifier(schema))).fetchall()
    return [r[0] for r in rows]
