"""IMPORT, table part: run sql/postgres/transform/*.sql (in name order) into standalone tables for one release."""
from __future__ import annotations

import logging
from datetime import date
from importlib import resources

import psycopg
from psycopg import sql

from npd_loader.db import clone_parent_indexes, list_parent_tables, render_sql, standalone_name
from npd_loader.dialect import TransformResult

log = logging.getLogger(__name__)


def _scripts() -> list[tuple[str, str]]:
    folder = resources.files("npd_loader") / "sql" / "postgres" / "transform"
    return [(p.name, p.read_text(encoding="utf-8")) for p in sorted(folder.iterdir(), key=lambda p: p.name)
            if p.name.endswith(".sql")]


def run_transforms(conn: psycopg.Connection, raw_schema: str, raw_table: str, schema: str, release: date,
                   run_id: int) -> TransformResult:
    conn.execute("SET TIME ZONE 'UTC'")
    tables: dict[str, str] = {}
    for parent in list_parent_tables(conn, schema):
        name = standalone_name(parent, release, run_id)
        target = sql.Identifier(schema, name)
        conn.execute(sql.SQL("CREATE TABLE {} (LIKE {} INCLUDING DEFAULTS)").format(
            target, sql.Identifier(schema, parent)))
        conn.execute(sql.SQL("ALTER TABLE {} ADD CHECK (release_date = {})").format(target, sql.Literal(release)))
        tables[parent] = name
    conn.commit()

    tokens: dict[str, sql.Composable] = {
        "raw": sql.Identifier(raw_schema, raw_table),
        "release": sql.Literal(release),
        "schema": sql.Identifier(schema),
        **{f"t:{parent}": sql.Identifier(schema, name) for parent, name in tables.items()},
    }
    for script_name, text in _scripts():
        log.info("running transform %s", script_name)
        conn.execute(render_sql(text, tokens, conn))
        conn.commit()

    for parent, name in tables.items():
        clone_parent_indexes(conn, schema, parent, sql.Identifier(schema, name))
    conn.commit()
    counts = {parent: conn.execute(sql.SQL("SELECT count(*) FROM {}").format(sql.Identifier(schema, name)))
              .fetchone()[0] for parent, name in tables.items()}
    return TransformResult(tables, counts)
