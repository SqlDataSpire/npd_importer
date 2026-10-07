"""Engine-specific npd-database operations. The stages call only this protocol."""
from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import date
from typing import TYPE_CHECKING, Callable, ContextManager, Protocol

from npd_loader.config import ConfigError, NpdDbConfig

if TYPE_CHECKING:
    from npd_loader.raw_load import NdjsonInput
    from npd_loader.storage import Storage


class LockUnavailable(Exception):
    """A table lock was not granted within lock_timeout_seconds on every attempt."""


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
    def stage_release(self, storage: "Storage", release: date, run_id: int,
                      inputs: "list[NdjsonInput]") -> StageResult: ...
    def apply_delta(self, release: date, run_id: int) -> DeltaResult: ...


def dialect_for(conn_obj: object, cfg: NpdDbConfig, sleep: Callable[[float], None] = time.sleep) -> Dialect:
    name = conn_obj.engine.dialect.name
    if name == "mssql":
        from npd_loader.dialect.mssql import MssqlDialect
        return MssqlDialect(conn_obj.engine, cfg, sleep)
    if name == "postgresql":
        raise ConfigError("Phase 2 supports SQL Server only; the Postgres loader is on main")
    raise ValueError(f"unsupported database engine {name!r}")
