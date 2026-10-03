"""Run and file tracking. `Catalog` is the interface; CssCatalogPg (below) writes css_catalog_local."""
from __future__ import annotations

from dataclasses import dataclass, fields as dc_fields
from datetime import date, datetime
from typing import Iterable, Protocol

SUCCESS = "Success"
FAILED = "Failed"
MAX_TEXT = 8000


def truncate(text: str | None) -> str | None:
    return None if text is None else text[:MAX_TEXT]


@dataclass(frozen=True)
class Run:
    id: int
    run_class: str
    description: str
    config_xml: str
    started_at: datetime
    release_date: date | None
    status: str | None = None


@dataclass(frozen=True)
class DataFile:
    id: int
    run_id: int
    file_type: str | None
    source_uri: str | None
    source_version_num: str | None
    file_name: str | None
    file_rel_path: str | None
    run_type_root_dir: str | None
    parent_file: int | None
    file_size: int | None
    file_hash: str | None
    date_modified: datetime | None
    date_created: datetime | None
    date_loaded: datetime | None
    exceptions: str | None


DATA_FILE_FIELDS: tuple[str, ...] = tuple(f.name for f in dc_fields(DataFile) if f.name not in ("id", "run_id"))


def clean_fields(fields: dict[str, object]) -> dict[str, object]:
    unknown = set(fields) - set(DATA_FILE_FIELDS)
    if unknown:
        raise TypeError(f"unknown data_file fields: {sorted(unknown)}")
    if isinstance(fields.get("exceptions"), str):
        fields = {**fields, "exceptions": truncate(fields["exceptions"])}
    return fields


def newest_run(runs: Iterable[Run], release: date | None) -> Run | None:
    candidates = [r for r in runs if r.release_date is not None and (release is None or r.release_date == release)]
    return max(candidates, key=lambda r: (r.release_date, r.id), default=None)


def releases_of(runs: Iterable[Run]) -> list[date]:
    return sorted({r.release_date for r in runs if r.release_date is not None})


class Catalog(Protocol):
    def start_run(self, run_class: str, description: str, config_xml: str) -> Run: ...
    def finish_run(self, run: Run, status: str, result: str | None = None, output_xml: str | None = None) -> None: ...
    def add_data_file(self, run: Run, **fields: object) -> int: ...
    def update_data_file(self, file_id: int, **fields: object) -> None: ...
    def get_data_files(self, release: date, file_type: str, run_id: int | None = None) -> list[DataFile]: ...
    def last_successful_run(self, run_class: str, release: date | None = None) -> Run | None: ...
    def successful_releases(self, run_class: str) -> list[date]: ...
