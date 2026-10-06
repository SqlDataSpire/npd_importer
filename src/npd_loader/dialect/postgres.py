"""Postgres flavor. Wraps the psycopg implementation until it moves onto PgConnectionObject's engine (Task 14)."""
from __future__ import annotations

import time
from contextlib import contextmanager
from datetime import date
from typing import Callable, Iterator

import psycopg

from npd_loader.config import NpdDbConfig
from npd_loader.db import advisory_lock, list_parent_tables, list_release_partitions
from npd_loader.db import published_releases as _published_releases
from npd_loader.dialect import LockUnavailable, TransformResult
from npd_loader.publish import drop_release as _drop_release
from npd_loader.publish import publish_release
from npd_loader.raw_load import RAW_PARENT, NdjsonInput, RawLoadResult, load_raw
from npd_loader.schema import init_db
from npd_loader.storage import Storage
from npd_loader.transform import run_transforms


class PostgresDialect:
    name = "postgres"

    def __init__(self, conninfo: str, cfg: NpdDbConfig, sleep: Callable[[float], None] = time.sleep):
        self.conninfo = conninfo
        self.cfg = cfg
        self._sleep = sleep
        self._conn: psycopg.Connection | None = None

    def init_db(self) -> None:
        init_db(self.conninfo, self.cfg.raw_schema, self.cfg.schema)

    @contextmanager
    def run_lock(self, stage: str) -> Iterator[bool]:
        with advisory_lock(self.conninfo, stage) as acquired:
            yield acquired

    def published_releases(self) -> list[date]:
        return _published_releases(self.conninfo, self.cfg.schema)

    def partitioned_releases(self) -> set[date]:
        found: set[date] = set()
        with psycopg.connect(self.conninfo) as conn:
            for schema in (self.cfg.raw_schema, self.cfg.schema):
                for parent in list_parent_tables(conn, schema):
                    found |= set(list_release_partitions(conn, schema, parent))
        return found

    def is_published(self, release: date) -> bool:
        with psycopg.connect(self.conninfo) as conn:
            return release in list_release_partitions(conn, self.cfg.raw_schema, RAW_PARENT)

    @contextmanager
    def session(self) -> Iterator[None]:
        """One connection shared by load_raw, run_transforms and publish of one import, as before."""
        with psycopg.connect(self.conninfo) as conn:
            self._conn = conn
            try:
                yield
            finally:
                self._conn = None

    def load_raw(self, storage: Storage, release: date, run_id: int, inputs: list[NdjsonInput]) -> RawLoadResult:
        return load_raw(self._conn, storage, self.cfg.raw_schema, release, run_id, inputs)

    def run_transforms(self, raw_table: str, release: date, run_id: int) -> TransformResult:
        return run_transforms(self._conn, self.cfg.raw_schema, raw_table, self.cfg.schema, release, run_id)

    def publish(self, raw_table: str, tables: dict[str, str], release: date, run_id: int, force: bool) -> None:
        publish_release(self._conn, self.cfg.raw_schema, raw_table, self.cfg.schema, tables, release, run_id,
                        force, self.cfg.lock_timeout_seconds, self._sleep)

    def drop_release(self, release: date) -> list[str]:
        with psycopg.connect(self.conninfo) as conn:
            try:
                return _drop_release(conn, self.cfg.raw_schema, self.cfg.schema, release,
                                     self.cfg.lock_timeout_seconds, self._sleep)
            except psycopg.errors.LockNotAvailable as exc:
                raise LockUnavailable(str(exc).strip()) from exc

    def drop_standalone_tables(self, run_id: int | None = None) -> list[str]:
        from npd_loader.import_stage import drop_standalone_tables
        return drop_standalone_tables(self.conninfo, [self.cfg.raw_schema, self.cfg.schema], run_id)
