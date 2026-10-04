"""IMPORT, raw part: stream each .ndjson into a standalone raw table with COPY, strictly validated.

Layout built here (not visible to readers until publish.py attaches it):
  {raw}.resource__YYYYMMDD__rRUN                 PARTITION BY LIST (resource_type)
  {raw}.resource__YYYYMMDD__rRUN__practitioner   one leaf per file, CHECKed on release_date and resource_type
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import date
from typing import BinaryIO, Iterator

import psycopg
from psycopg import sql

from npd_loader.db import MAX_IDENTIFIER, clone_parent_indexes, standalone_name
from npd_loader.storage import Storage

log = logging.getLogger(__name__)
RAW_PARENT = "resource"
COLUMNS = ("release_date", "resource_type", "resource_id", "last_updated", "ndjson_file_id", "zst_file_id",
           "line_number", "resource")


class RawLoadError(Exception):
    def __init__(self, message: str, file_id: int | None = None):
        super().__init__(message)
        self.file_id = file_id


@dataclass(frozen=True)
class NdjsonInput:
    file_id: int
    zst_file_id: int
    rel_path: str
    resource_type: str
    name: str


@dataclass
class RawLoadResult:
    table: str
    rows: dict[str, int]


def iter_lines(f: BinaryIO) -> Iterator[tuple[int, str]]:
    """Yield (line number, text). Accepts LF or CRLF and a missing final newline. Blank lines are only
    allowed at the very end of the file."""
    blank_at: int | None = None
    for number, raw in enumerate(f, start=1):
        line = raw.rstrip(b"\r\n")
        if not line.strip():
            blank_at = blank_at or number
            continue
        if blank_at is not None:
            raise RawLoadError(f"line {blank_at}: blank line")
        try:
            yield number, line.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise RawLoadError(f"line {number}: not UTF-8: {exc}") from exc


def validate_line(text: str, number: int, expected_type: str) -> tuple[str, str | None]:
    if "\\u0000" in text:
        raise RawLoadError(f"line {number}: contains \\u0000, which Postgres jsonb cannot store")
    try:
        obj = json.loads(text)
    except json.JSONDecodeError as exc:
        raise RawLoadError(f"line {number}: invalid JSON: {exc}") from exc
    if not isinstance(obj, dict):
        raise RawLoadError(f"line {number}: not a JSON object")
    if obj.get("resourceType") != expected_type:
        raise RawLoadError(f"line {number}: resourceType {obj.get('resourceType')!r}, expected {expected_type!r}")
    resource_id = obj.get("id")
    if not isinstance(resource_id, str) or not resource_id:
        raise RawLoadError(f"line {number}: missing id")
    meta = obj.get("meta")
    last_updated = meta.get("lastUpdated") if isinstance(meta, dict) else None
    return resource_id, last_updated if isinstance(last_updated, str) else None


def _create_release_table(conn: psycopg.Connection, raw_schema: str, release: date, run_id: int) -> str:
    name = standalone_name(RAW_PARENT, release, run_id)
    conn.execute(sql.SQL("CREATE TABLE {} (LIKE {} INCLUDING DEFAULTS) PARTITION BY LIST (resource_type)").format(
        sql.Identifier(raw_schema, name), sql.Identifier(raw_schema, RAW_PARENT)))
    conn.commit()
    return name


def _load_file(conn: psycopg.Connection, storage: Storage, raw_schema: str, table: str, release: date,
               inp: NdjsonInput) -> int:
    leaf = f"{table}__{inp.resource_type.lower()}"
    if len(leaf) > MAX_IDENTIFIER:
        raise RawLoadError(f"table name {leaf!r} is too long", inp.file_id)
    leaf_id = sql.Identifier(raw_schema, leaf)
    conn.execute(sql.SQL("CREATE TABLE {} (LIKE {} INCLUDING DEFAULTS)").format(
        leaf_id, sql.Identifier(raw_schema, RAW_PARENT)))
    conn.execute(sql.SQL("ALTER TABLE {} ADD CHECK (release_date = {} AND resource_type = {})").format(
        leaf_id, sql.Literal(release), sql.Literal(inp.resource_type)))
    lines = 0
    copy_sql = sql.SQL("COPY {} ({}) FROM STDIN").format(leaf_id, sql.SQL(", ").join(map(sql.Identifier, COLUMNS)))
    try:
        with storage.open_read(inp.rel_path) as f, conn.cursor() as cur, cur.copy(copy_sql) as copy:
            for number, text in iter_lines(f):
                resource_id, last_updated = validate_line(text, number, inp.resource_type)
                copy.write_row((release, inp.resource_type, resource_id, last_updated, inp.file_id,
                                inp.zst_file_id, number, text))
                lines += 1
    except RawLoadError as exc:
        conn.rollback()
        raise RawLoadError(f"{inp.name}: {exc}", inp.file_id) from exc
    except psycopg.Error as exc:
        conn.rollback()
        raise RawLoadError(f"{inp.name}: COPY failed: {exc}", inp.file_id) from exc
    except OSError as exc:
        conn.rollback()
        raise RawLoadError(f"{inp.name}: cannot read {inp.rel_path}: {exc}", inp.file_id) from exc
    count = conn.execute(sql.SQL("SELECT count(*) FROM {}").format(leaf_id)).fetchone()[0]
    if count != lines:
        conn.rollback()
        raise RawLoadError(f"{inp.name}: read {lines} lines but loaded {count} rows", inp.file_id)
    conn.execute(sql.SQL("ALTER TABLE {} ATTACH PARTITION {} FOR VALUES IN ({})").format(
        sql.Identifier(raw_schema, table), leaf_id, sql.Literal(inp.resource_type)))
    conn.commit()
    log.info("loaded %d %s resources from %s", lines, inp.resource_type, inp.rel_path)
    return lines


def _index_and_check_duplicates(conn: psycopg.Connection, raw_schema: str, table: str) -> None:
    target = sql.Identifier(raw_schema, table)
    try:
        clone_parent_indexes(conn, raw_schema, RAW_PARENT, target)
        conn.commit()
    except psycopg.errors.UniqueViolation:
        conn.rollback()
        dups = conn.execute(sql.SQL(
            "SELECT resource_type, resource_id, array_agg(line_number ORDER BY line_number) FROM {} "
            "GROUP BY 1, 2 HAVING count(*) > 1 ORDER BY 1, 2 LIMIT 20").format(target)).fetchall()
        detail = "; ".join(f"{t} {i} at lines {lines}" for t, i, lines in dups)
        raise RawLoadError(f"duplicate resource ids: {detail}")


def load_raw(conn: psycopg.Connection, storage: Storage, raw_schema: str, release: date, run_id: int,
             inputs: list[NdjsonInput]) -> RawLoadResult:
    table = _create_release_table(conn, raw_schema, release, run_id)
    rows = {inp.resource_type: _load_file(conn, storage, raw_schema, table, release, inp) for inp in inputs}
    _index_and_check_duplicates(conn, raw_schema, table)
    return RawLoadResult(table, rows)
