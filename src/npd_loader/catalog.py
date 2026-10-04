"""Run and file tracking. `Catalog` is the interface; CssCatalogPg (below) writes css_catalog_local."""
from __future__ import annotations

from dataclasses import dataclass, fields as dc_fields
from datetime import date, datetime
from typing import Iterable, Protocol

import psycopg
from psycopg import sql
from psycopg.rows import dict_row

from npd_loader.config import CatalogConfig
from npd_loader.runxml import parse_release

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


def _qualified(name: str) -> sql.Identifier:
    return sql.Identifier(*name.split(".", 1))


class CssCatalogPg:
    """Catalog in css_catalog_local. Each call uses its own short autocommit connection, so catalog writes
    commit independently of data loads and survive their failures."""

    def __init__(self, conninfo: str, cfg: CatalogConfig):
        self._conninfo = conninfo
        self._cfg = cfg
        self._runs = _qualified(cfg.run_table)
        self._files = _qualified(cfg.file_table)

    def _connect(self) -> psycopg.Connection:
        return psycopg.connect(self._conninfo, autocommit=True)

    def start_run(self, run_class: str, description: str, config_xml: str) -> Run:
        started = datetime.now().replace(microsecond=0)
        query = sql.SQL("INSERT INTO {} (project, run_type, run_class, run_description, xml_config, date_started) "
                        "VALUES (%s, %s, %s, %s, %s, %s) RETURNING run_id").format(self._runs)
        with self._connect() as conn:
            run_id = conn.execute(query, (self._cfg.project, self._cfg.run_type, run_class, description,
                                          config_xml, started)).fetchone()[0]
        return Run(run_id, run_class, description, config_xml, started, parse_release(config_xml))

    def finish_run(self, run: Run, status: str, result: str | None = None, output_xml: str | None = None) -> None:
        label = {SUCCESS: self._cfg.status_success, FAILED: self._cfg.status_failed}[status]
        query = sql.SQL("UPDATE {} SET completion_status = %s, date_completed = %s, result = %s, xml_output = %s "
                        "WHERE run_id = %s").format(self._runs)
        with self._connect() as conn:
            conn.execute(query, (label, datetime.now().replace(microsecond=0), truncate(result), output_xml, run.id))

    def add_data_file(self, run: Run, **fields: object) -> int:
        fields = clean_fields(fields)
        cols = ["run_id", "file_set", "source_version_name", *fields]
        vals = [run.id, self._cfg.file_set, self._cfg.source_version_name, *fields.values()]
        query = sql.SQL("INSERT INTO {} ({}) VALUES ({}) RETURNING id").format(
            self._files, sql.SQL(", ").join(map(sql.Identifier, cols)),
            sql.SQL(", ").join([sql.Placeholder()] * len(cols)))
        with self._connect() as conn:
            return conn.execute(query, vals).fetchone()[0]

    def update_data_file(self, file_id: int, **fields: object) -> None:
        fields = clean_fields(fields)
        if not fields:
            return
        sets = sql.SQL(", ").join(sql.SQL("{} = %s").format(sql.Identifier(k)) for k in fields)
        query = sql.SQL("UPDATE {} SET {} WHERE id = %s").format(self._files, sets)
        with self._connect() as conn:
            conn.execute(query, [*fields.values(), file_id])

    def get_data_files(self, release: date, file_type: str, run_id: int | None = None) -> list[DataFile]:
        cols = sql.SQL(", ").join(map(sql.Identifier, ["id", "run_id", *DATA_FILE_FIELDS]))
        query = sql.SQL("SELECT {} FROM {} WHERE file_set = %s AND source_version_num = %s AND file_type = %s "
                        "AND (%s::integer IS NULL OR run_id = %s) ORDER BY id").format(cols, self._files)
        with self._connect() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query, (self._cfg.file_set, release.isoformat(), file_type, run_id, run_id))
            return [DataFile(**row) for row in cur.fetchall()]

    def _successful_runs(self, run_class: str) -> list[Run]:
        query = sql.SQL("SELECT run_id, run_class, run_description, xml_config::text, date_started, "
                        "completion_status FROM {} WHERE project = %s AND run_type = %s AND run_class = %s "
                        "AND completion_status = %s").format(self._runs)
        with self._connect() as conn:
            rows = conn.execute(query, (self._cfg.project, self._cfg.run_type, run_class,
                                        self._cfg.status_success)).fetchall()
        return [Run(r[0], r[1], r[2] or "", r[3] or "", r[4], parse_release(r[3]), SUCCESS) for r in rows]

    def last_successful_run(self, run_class: str, release: date | None = None) -> Run | None:
        return newest_run(self._successful_runs(run_class), release)

    def successful_releases(self, run_class: str) -> list[date]:
        return releases_of(self._successful_runs(run_class))
