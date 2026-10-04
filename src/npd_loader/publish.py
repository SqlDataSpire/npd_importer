"""Make a release visible (or invisible) in one transaction."""
from __future__ import annotations

import logging
from datetime import date

import psycopg
from psycopg import sql

from npd_loader.db import list_parent_tables, list_release_partitions
from npd_loader.raw_load import RAW_PARENT

log = logging.getLogger(__name__)


class PublishConflict(Exception):
    pass


def _detach_and_drop(conn: psycopg.Connection, schema: str, parent: str, name: str) -> None:
    conn.execute(sql.SQL("ALTER TABLE {} DETACH PARTITION {}").format(
        sql.Identifier(schema, parent), sql.Identifier(schema, name)))
    conn.execute(sql.SQL("DROP TABLE {}").format(sql.Identifier(schema, name)))


def publish_release(conn: psycopg.Connection, raw_schema: str, raw_table: str, schema: str,
                    tables: dict[str, str], release: date, run_id: int, force: bool) -> None:
    targets = [(raw_schema, RAW_PARENT, raw_table)] + [(schema, p, n) for p, n in sorted(tables.items())]
    conn.commit()
    with conn.transaction():
        for s, parent, new in targets:
            existing = list_release_partitions(conn, s, parent).get(release)
            if existing is not None:
                if not force:
                    raise PublishConflict(f"release {release} is already published in {s}.{parent}; "
                                          f"rerun with --force to replace it")
                _detach_and_drop(conn, s, parent, existing)
            conn.execute(sql.SQL("ALTER TABLE {} ATTACH PARTITION {} FOR VALUES IN ({})").format(
                sql.Identifier(s, parent), sql.Identifier(s, new), sql.Literal(release)))
        conn.execute(sql.SQL(
            "INSERT INTO {} (release_date, import_run_id, published_at) VALUES (%s, %s, now()) "
            "ON CONFLICT (release_date) DO UPDATE SET import_run_id = EXCLUDED.import_run_id, published_at = now()"
        ).format(sql.Identifier(schema, "release")), (release, run_id))
    log.info("published release %s (%d tables)", release, len(targets))
    for s, _, new in targets:
        conn.execute(sql.SQL("ANALYZE {}").format(sql.Identifier(s, new)))
    conn.commit()


def drop_release(conn: psycopg.Connection, raw_schema: str, schema: str, release: date) -> list[str]:
    """Detach and drop every partition of `release`, in one transaction. Checks what actually exists first."""
    dropped: list[str] = []
    conn.commit()
    with conn.transaction():
        for s in (raw_schema, schema):
            for parent in list_parent_tables(conn, s):
                name = list_release_partitions(conn, s, parent).get(release)
                if name is not None:
                    _detach_and_drop(conn, s, parent, name)
                    dropped.append(f"{s}.{name}")
        conn.execute(sql.SQL("DELETE FROM {} WHERE release_date = %s").format(
            sql.Identifier(schema, "release")), (release,))
    return dropped
