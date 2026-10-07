"""Engine-specific npd-database operations. The stages call only this protocol."""
from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import date
from typing import TYPE_CHECKING, Callable, ContextManager, Protocol

from npd_loader.config import NpdDbConfig

if TYPE_CHECKING:
    from npd_loader.raw_load import NdjsonInput, RawLoadResult
    from npd_loader.storage import Storage


class LockUnavailable(Exception):
    """A table lock was not granted within lock_timeout_seconds on every attempt."""


class PublishConflict(Exception):
    pass


@dataclass
class TransformResult:
    tables: dict[str, str]
    counts: dict[str, int]


@dataclass
class StageResult:
    rows: dict[str, int]          # rows loaded per staging table (incl. resource_hash)
    resources: dict[str, int]     # resources per resource type


@dataclass
class DeltaResult:
    kinds: dict[str, dict[str, int]]   # resource type -> {"new", "changed", "unchanged", "not_seen"}
    inserted: dict[str, int]           # rows inserted per table
    replaced: dict[str, int]           # rows removed per table because their resource changed


class Dialect(Protocol):
    name: str
    cfg: NpdDbConfig

    def init_db(self) -> None: ...
    def run_lock(self, stage: str) -> ContextManager[bool]: ...
    def published_releases(self) -> list[date]: ...
    def partitioned_releases(self) -> set[date]: ...
    def is_published(self, release: date) -> bool: ...
    def session(self) -> ContextManager[None]: ...
    def load_raw(self, storage: "Storage", release: date, run_id: int,
                 inputs: "list[NdjsonInput]") -> "RawLoadResult": ...
    def run_transforms(self, raw_table: str, release: date, run_id: int) -> TransformResult: ...
    def publish(self, raw_table: str, tables: dict[str, str], release: date, run_id: int, force: bool) -> None: ...
    def drop_release(self, release: date) -> list[str]: ...
    def drop_standalone_tables(self, run_id: int | None = None) -> list[str]: ...


def dialect_for(conn_obj: object, cfg: NpdDbConfig, sleep: Callable[[float], None] = time.sleep) -> Dialect:
    name = conn_obj.engine.dialect.name
    if name == "mssql":
        from npd_loader.dialect.mssql import MssqlDialect
        return MssqlDialect(conn_obj.engine, cfg, sleep)
    if name == "postgresql":
        from npd_loader.dialect.postgres import PostgresDialect
        return PostgresDialect(conn_obj.engine, cfg, sleep)
    raise ValueError(f"unsupported database engine {name!r}")
