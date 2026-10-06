"""init-db: create schemas, parent tables, helper functions and latest-release views. Safe to re-run."""
from __future__ import annotations

from importlib import resources
from typing import Sequence

import psycopg
from psycopg import sql

from npd_loader.db import list_parent_tables, render_sql


def _init_scripts() -> list[tuple[str, str]]:
    """(name, text) of every init script in name order; 900_migrations.sql runs last."""
    folder = resources.files("npd_loader") / "sql" / "postgres" / "init"
    return [(p.name, p.read_text(encoding="utf-8")) for p in sorted(folder.iterdir(), key=lambda p: p.name)
            if p.name.endswith(".sql")]


def _create_view(conn: psycopg.Connection, view_schema: str, table: str, release_schema: str) -> None:
    conn.execute(sql.SQL(
        "CREATE OR REPLACE VIEW {view} AS SELECT * FROM {table} "
        "WHERE release_date = (SELECT max(release_date) FROM {release})").format(
        view=sql.Identifier(view_schema, f"v_{table}"), table=sql.Identifier(view_schema, table),
        release=sql.Identifier(release_schema, "release")))


def init_db(conninfo: str, raw_schema: str, schema: str, extra_scripts: Sequence[str] = ()) -> None:
    """Run the init scripts, then `extra_scripts` (tests use this to exercise a migration without shipping one),
    then recreate the views so they pick up any new column."""
    with psycopg.connect(conninfo) as conn:
        tokens = {"raw_schema": sql.Identifier(raw_schema), "schema": sql.Identifier(schema)}
        for text in [text for _, text in _init_scripts()] + list(extra_scripts):
            conn.execute(render_sql(text, tokens, conn))
        for s in (raw_schema, schema):
            for table in list_parent_tables(conn, s):
                _create_view(conn, s, table, schema)
