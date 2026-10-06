"""SQL Server flavor: every npd-database operation, on a SqlConnectionObject's engine (pyodbc)."""
from __future__ import annotations

import logging
import re
import time
from contextlib import contextmanager
from datetime import date, datetime, timezone
from typing import Callable, Iterator

import pyodbc
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import DBAPIError

from npd_loader.config import NpdDbConfig
from npd_loader.raw_load import RAW_PARENT, NdjsonInput, RawLoadError, RawLoadResult, iter_lines, validate_line
from npd_loader.sqltext import render, split_batches, sql_scripts, standalone_name
from npd_loader.storage import Storage

log = logging.getLogger(__name__)
MAX_IDENTIFIER = 128
BATCH_ROWS = 5000
RAW_COLUMNS = ("release_date", "resource_type", "resource_id", "last_updated", "ndjson_file_id", "zst_file_id",
               "line_number", "resource")
ERROR_NUMBER_RE = re.compile(r"\((\d+)\)")


def error_number(exc: BaseException) -> int | None:
    """SQL Server native error number from a pyodbc error message, e.g. '... exceeded. (1222) (SQLExecDirectW)'."""
    match = ERROR_NUMBER_RE.search(str(getattr(exc, "orig", exc)))
    return int(match.group(1)) if match else None


def to_utc_naive(text: str | None) -> datetime | None:
    """FHIR instant (meta.lastUpdated) as a naive UTC datetime for a datetime2 column."""
    if text is None:
        return None
    value = datetime.fromisoformat(text)
    if value.tzinfo is not None:
        value = value.astimezone(timezone.utc).replace(tzinfo=None)
    return value


class MssqlDialect:
    name = "mssql"

    def __init__(self, engine: Engine, cfg: NpdDbConfig, sleep: Callable[[float], None] = time.sleep):
        self.engine = engine
        self.cfg = cfg
        self._sleep = sleep

    # -- naming -------------------------------------------------------------------------------------------
    @staticmethod
    def q(*parts: str) -> str:
        return ".".join("[" + p.replace("]", "]]") + "]" for p in parts)

    @staticmethod
    def pf(schema: str) -> str:
        return f"pf_{schema}_release"

    @staticmethod
    def ps(schema: str) -> str:
        return f"ps_{schema}_release"

    @staticmethod
    def lit(value: str) -> str:
        return "N'" + value.replace("'", "''") + "'"

    def _tokens(self) -> dict[str, str]:
        tokens: dict[str, str] = {}
        for key, schema in (("schema", self.cfg.schema), ("raw_schema", self.cfg.raw_schema)):
            tokens[key] = self.q(schema)
            tokens[f"s:{key}"] = self.lit(schema)
            tokens[f"pf:{key}"] = self.q(self.pf(schema))
            tokens[f"ps:{key}"] = self.q(self.ps(schema))
            tokens[f"pfname:{key}"] = self.lit(self.pf(schema))
            tokens[f"psname:{key}"] = self.lit(self.ps(schema))
        return tokens

    def _autocommit(self) -> Connection:
        return self.engine.connect().execution_options(isolation_level="AUTOCOMMIT")

    # -- catalog views ------------------------------------------------------------------------------------
    @staticmethod
    def parent_tables(conn: Connection, schema: str) -> list[str]:
        """Partitioned (published) tables of `schema`; standalone tables are never partitioned."""
        rows = conn.exec_driver_sql(
            "SELECT t.name FROM sys.tables t "
            "JOIN sys.indexes i ON i.object_id = t.object_id AND i.index_id IN (0, 1) "
            "JOIN sys.partition_schemes s ON s.data_space_id = i.data_space_id "
            "WHERE t.schema_id = SCHEMA_ID(?) ORDER BY t.name", (schema,)).fetchall()
        return [r[0] for r in rows]

    def _boundaries(self, conn: Connection, schema: str) -> set[date]:
        rows = conn.exec_driver_sql(
            "SELECT CAST(v.value AS date) FROM sys.partition_range_values v "
            "JOIN sys.partition_functions f ON f.function_id = v.function_id WHERE f.name = ?",
            (self.pf(schema),)).fetchall()
        return {r[0] for r in rows}

    # -- Dialect --------------------------------------------------------------------------------------------
    def init_db(self) -> None:
        tokens = self._tokens()
        with self._autocommit() as conn:
            for name, text in sql_scripts("mssql", "init"):
                for batch in split_batches(render(text, tokens)):
                    conn.exec_driver_sql(batch)
            for schema in (self.cfg.raw_schema, self.cfg.schema):
                for table in self.parent_tables(conn, schema):
                    conn.exec_driver_sql(
                        f"CREATE OR ALTER VIEW {self.q(schema, 'v_' + table)} AS SELECT * FROM {self.q(schema, table)} "
                        f"WHERE release_date = (SELECT MAX(release_date) FROM {self.q(self.cfg.schema, 'release')})")

    def published_releases(self) -> list[date]:
        with self.engine.connect() as conn:
            rows = conn.exec_driver_sql(
                f"SELECT release_date FROM {self.q(self.cfg.schema, 'release')} ORDER BY release_date").fetchall()
        return [r[0] for r in rows]

    def partitioned_releases(self) -> set[date]:
        """A release is published exactly when its boundary exists: publish SPLITs it in, retention MERGEs it out."""
        with self.engine.connect() as conn:
            return self._boundaries(conn, self.cfg.raw_schema) | self._boundaries(conn, self.cfg.schema)

    def is_published(self, release: date) -> bool:
        with self.engine.connect() as conn:
            return release in self._boundaries(conn, self.cfg.raw_schema)

    @contextmanager
    def session(self) -> Iterator[None]:
        yield

    # -- raw load -----------------------------------------------------------------------------------------
    def _create_standalone(self, conn: Connection, schema: str, parent: str, release: date, run_id: int) -> str:
        """Empty heap shaped like `parent`, PAGE-compressed, CHECKed to one release so SWITCH is metadata only."""
        name = standalone_name(parent, release, run_id, MAX_IDENTIFIER - 3)   # room for the ck_ constraint name
        target = self.q(schema, name)
        conn.exec_driver_sql(f"SELECT TOP 0 * INTO {target} FROM {self.q(schema, parent)}")
        conn.exec_driver_sql(f"ALTER TABLE {target} REBUILD WITH (DATA_COMPRESSION = PAGE)")
        conn.exec_driver_sql(f"ALTER TABLE {target} WITH CHECK ADD CONSTRAINT {self.q('ck_' + name)} "
                             f"CHECK (release_date = '{release.isoformat()}')")
        return name

    def _clone_indexes(self, conn: Connection, schema: str, parent: str, name: str) -> None:
        """Build the parent's indexes on standalone table `name` (same names, columns, uniqueness, compression)."""
        rows = conn.exec_driver_sql(
            "SELECT i.name, i.is_unique, c.name FROM sys.indexes i "
            "JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id "
            "JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id "
            "WHERE i.object_id = OBJECT_ID(?) AND i.index_id > 0 AND ic.is_included_column = 0 "
            "ORDER BY i.index_id, ic.key_ordinal", (f"{schema}.{parent}",)).fetchall()
        indexes: dict[str, tuple[bool, list[str]]] = {}
        for index, unique, column in rows:
            indexes.setdefault(index, (bool(unique), []))[1].append(column)
        for index, (unique, columns) in indexes.items():
            conn.exec_driver_sql(
                f"CREATE {'UNIQUE ' if unique else ''}INDEX {self.q(index)} ON {self.q(schema, name)} "
                f"({', '.join(self.q(c) for c in columns)}) WITH (DATA_COMPRESSION = PAGE)")

    def _load_file(self, raw_conn, table: str, release: date, inp: NdjsonInput, storage: Storage) -> int:
        """Insert one .ndjson in batches of BATCH_ROWS, committing each batch as soon as it lands."""
        cur = raw_conn.cursor()
        cur.fast_executemany = True
        insert = (f"INSERT INTO {self.q(self.cfg.raw_schema, table)} ({', '.join(RAW_COLUMNS)}) "
                  f"VALUES ({', '.join('?' * len(RAW_COLUMNS))})")
        lines, batch = 0, []
        try:
            with storage.open_read(inp.rel_path) as f:
                for number, text in iter_lines(f):
                    resource_id, last_updated = validate_line(text, number, inp.resource_type)
                    batch.append((release, inp.resource_type, resource_id, to_utc_naive(last_updated),
                                  inp.file_id, inp.zst_file_id, number, text))
                    if len(batch) == BATCH_ROWS:
                        cur.executemany(insert, batch)
                        raw_conn.commit()
                        lines += len(batch)
                        batch = []
            if batch:
                cur.executemany(insert, batch)
                raw_conn.commit()
                lines += len(batch)
        except RawLoadError as exc:
            raw_conn.rollback()
            raise RawLoadError(f"{inp.name}: {exc}", inp.file_id) from exc
        except pyodbc.Error as exc:
            raw_conn.rollback()
            raise RawLoadError(f"{inp.name}: insert failed: {exc}", inp.file_id) from exc
        except (OSError, ValueError) as exc:
            raw_conn.rollback()
            raise RawLoadError(f"{inp.name}: cannot read {inp.rel_path}: {exc}", inp.file_id) from exc
        count = cur.execute(f"SELECT COUNT_BIG(*) FROM {self.q(self.cfg.raw_schema, table)} WHERE resource_type = ?",
                            inp.resource_type).fetchone()[0]
        if count != lines:
            raise RawLoadError(f"{inp.name}: read {lines} lines but loaded {count} rows", inp.file_id)
        log.info("loaded %d %s resources from %s", lines, inp.resource_type, inp.rel_path)
        return lines

    def load_raw(self, storage: Storage, release: date, run_id: int, inputs: list[NdjsonInput]) -> RawLoadResult:
        raw_schema = self.cfg.raw_schema
        with self._autocommit() as conn:
            table = self._create_standalone(conn, raw_schema, RAW_PARENT, release, run_id)
        raw_conn = self.engine.raw_connection()
        try:
            rows = {inp.resource_type: self._load_file(raw_conn, table, release, inp, storage) for inp in inputs}
        finally:
            raw_conn.close()
        try:
            with self._autocommit() as conn:
                self._clone_indexes(conn, raw_schema, RAW_PARENT, table)
        except DBAPIError as exc:
            if error_number(exc) != 1505:
                raise
            with self.engine.connect() as conn:
                dups = conn.exec_driver_sql(
                    f"SELECT TOP 20 resource_type, resource_id, STRING_AGG(CAST(line_number AS varchar(20)), ',') "
                    f"WITHIN GROUP (ORDER BY line_number) FROM {self.q(raw_schema, table)} "
                    f"GROUP BY resource_type, resource_id HAVING COUNT(*) > 1 ORDER BY 1, 2").fetchall()
            detail = "; ".join(f"{t} {i} at lines {lines}" for t, i, lines in dups)
            file_ids = {inp.resource_type: inp.file_id for inp in inputs}
            raise RawLoadError(f"duplicate resource ids: {detail}", file_ids.get(dups[0][0]) if dups else None) from exc
        return RawLoadResult(table, rows)
