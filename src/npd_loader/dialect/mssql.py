"""SQL Server flavor: every npd-database operation, on a SqlConnectionObject's engine (pyodbc)."""
from __future__ import annotations

import logging
import re
import time
from contextlib import contextmanager
from datetime import date
from typing import Callable, Iterator

from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import DBAPIError

from npd_loader.config import NpdDbConfig
from npd_loader.sqltext import render, split_batches, sql_scripts

log = logging.getLogger(__name__)
MAX_IDENTIFIER = 128
ERROR_NUMBER_RE = re.compile(r"\((\d+)\)")


def error_number(exc: BaseException) -> int | None:
    """SQL Server native error number from a pyodbc error message, e.g. '... exceeded. (1222) (SQLExecDirectW)'."""
    match = ERROR_NUMBER_RE.search(str(getattr(exc, "orig", exc)))
    return int(match.group(1)) if match else None


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
