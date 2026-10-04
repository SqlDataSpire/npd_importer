"""Shared plumbing for the DOWNLOAD / EXTRACT / IMPORT stages."""
from __future__ import annotations

import hashlib
import logging
import re
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import date, datetime
from enum import StrEnum
from typing import Callable, ContextManager, Iterator, Sequence

import httpx

from npd_loader.catalog import FAILED, Catalog, DataFile, Run
from npd_loader.config import Config
from npd_loader.runxml import build_config_xml
from npd_loader.storage import Storage

log = logging.getLogger(__name__)


class StageFailed(Exception):
    pass


class Outcome(StrEnum):
    SUCCESS = "success"
    SKIPPED = "skipped"
    LOCKED = "locked"


@dataclass
class Context:
    config: Config
    catalog: Catalog
    storage: Storage
    http: httpx.Client
    lock: Callable[[str], ContextManager[bool]]
    npd_conninfo: str | None = None
    sleep: Callable[[float], None] = field(default=time.sleep)


@contextmanager
def no_lock(stage: str) -> Iterator[bool]:
    yield True


def now() -> datetime:
    return datetime.now().replace(microsecond=0)


def run_folder(run: Run) -> str:
    return f"run_{run.id}_{run.started_at:%Y-%m-%d-%H%M%S}"


def prefixed_name(file_id: int, original: str) -> str:
    return f"file_{file_id}_{original}"


def original_name(file_name: str) -> str:
    return re.sub(r"^file_\d+_", "", file_name)


def describe(ctx: Context, stage_label: str, release: date) -> str:
    return ctx.config.catalog.description_template.format(stage=stage_label, release=release.isoformat())


def config_xml(ctx: Context, description: str, release: date, **options: object) -> str:
    cat = ctx.config.catalog
    return build_config_xml({"run_type": cat.run_type, "file_set": cat.file_set,
                             "data_store_base_path": ctx.storage.uri(), "run_description": description,
                             "release_date": release.isoformat(), **options})


def fail_run(ctx: Context, run: Run, exc: BaseException) -> None:
    message = f"{type(exc).__name__}: {exc}"
    log.error("%s run %s failed: %s", run.run_class, run.id, message)
    try:
        ctx.catalog.finish_run(run, FAILED, result=message)
    except Exception:
        log.exception("could not record the failure of run %s in the catalog", run.id)


def sha256_of(storage: Storage, rel_path: str, chunk: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with storage.open_read(rel_path) as f:
        while block := f.read(chunk):
            h.update(block)
    return h.hexdigest()


def read_bytes(storage: Storage, rel_path: str) -> bytes:
    with storage.open_read(rel_path) as f:
        return f.read()


def completed_child(parent_id: int, rows: Sequence[DataFile]) -> DataFile | None:
    """Newest row derived from `parent_id` that finished (has a hash). Rows from failed runs have no hash."""
    done = [r for r in rows if r.parent_file == parent_id and r.file_hash]
    return max(done, key=lambda r: r.id, default=None)
