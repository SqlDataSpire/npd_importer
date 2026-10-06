"""Run and file tracking. `Catalog` is the interface; SqlCatalog writes it through SQLAlchemy."""
from __future__ import annotations

from dataclasses import dataclass, fields as dc_fields
from datetime import date, datetime
from typing import Iterable, Protocol

from sqlalchemy import BigInteger, Column, DateTime, Integer, MetaData, String, Table, Text, and_, insert, select, update
from sqlalchemy.engine import Engine

from npd_loader.config import CatalogConfig
from npd_loader.runxml import parse_release

SUCCESS = "Success"
FAILED = "Failed"
MAX_RESULT = 2000  # master_warehouse_run.result is varchar(2000)
MAX_EXCEPTIONS = 8000  # data_file.exceptions is varchar(8000)


def truncate(text: str | None, limit: int) -> str | None:
    return None if text is None else text[:limit]


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
        fields = {**fields, "exceptions": truncate(fields["exceptions"], MAX_EXCEPTIONS)}
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


def _table(md: MetaData, qualified: str, *columns: Column) -> Table:
    schema, _, name = qualified.rpartition(".")
    return Table(name, md, *columns, schema=schema or None)


class SqlCatalog:
    """Catalog in any SQLAlchemy engine (css_catalog_local on Postgres, HIE_WAREHOUSE_META on SQL Server). Each call
    runs in its own short transaction, so catalog writes commit independently of data loads and survive their
    failures. Column names are lowercase; SQL Server's case-insensitive collation matches the uppercase columns."""

    def __init__(self, engine: Engine, cfg: CatalogConfig):
        self._engine = engine
        self._cfg = cfg
        md = MetaData()
        self._runs = _table(md, cfg.run_table,
                            Column("id", Integer, primary_key=True), Column("project", String),
                            Column("run_type", String), Column("run_class", String),
                            Column("run_description", String), Column("xml_config", Text),
                            Column("xml_output", Text), Column("date_started", DateTime),
                            Column("date_completed", DateTime), Column("completion_status", String),
                            Column("result", String))
        self._files = _table(md, cfg.file_table,
                             Column("id", Integer, primary_key=True), Column("run_id", Integer),
                             Column("file_set", String), Column("source_version_name", String),
                             Column("file_type", String), Column("source_uri", String),
                             Column("source_version_num", String), Column("file_name", String),
                             Column("file_rel_path", String), Column("run_type_root_dir", String),
                             Column("parent_file", Integer), Column("file_size", BigInteger),
                             Column("file_hash", String), Column("date_modified", DateTime),
                             Column("date_created", DateTime), Column("date_loaded", DateTime),
                             Column("exceptions", String))

    def start_run(self, run_class: str, description: str, config_xml: str) -> Run:
        started = datetime.now().replace(microsecond=0)
        stmt = insert(self._runs).values(project=self._cfg.project, run_type=self._cfg.run_type,
                                         run_class=run_class, run_description=description,
                                         xml_config=config_xml, date_started=started).returning(self._runs.c.id)
        with self._engine.begin() as conn:
            run_id = conn.execute(stmt).scalar_one()
        return Run(run_id, run_class, description, config_xml, started, parse_release(config_xml))

    def finish_run(self, run: Run, status: str, result: str | None = None, output_xml: str | None = None) -> None:
        label = {SUCCESS: self._cfg.status_success, FAILED: self._cfg.status_failed}[status]
        stmt = update(self._runs).where(self._runs.c.id == run.id).values(
            completion_status=label, date_completed=datetime.now().replace(microsecond=0),
            result=truncate(result, MAX_RESULT), xml_output=output_xml)
        with self._engine.begin() as conn:
            conn.execute(stmt)

    def add_data_file(self, run: Run, **fields: object) -> int:
        fields = clean_fields(fields)
        stmt = insert(self._files).values(run_id=run.id, file_set=self._cfg.file_set,
                                          source_version_name=self._cfg.source_version_name,
                                          **fields).returning(self._files.c.id)
        with self._engine.begin() as conn:
            return conn.execute(stmt).scalar_one()

    def update_data_file(self, file_id: int, **fields: object) -> None:
        fields = clean_fields(fields)
        if not fields:
            return
        with self._engine.begin() as conn:
            conn.execute(update(self._files).where(self._files.c.id == file_id).values(**fields))

    def get_data_files(self, release: date, file_type: str, run_id: int | None = None) -> list[DataFile]:
        f = self._files.c
        cond = and_(f.file_set == self._cfg.file_set, f.source_version_num == release.isoformat(),
                    f.file_type == file_type)
        if run_id is not None:
            cond = and_(cond, f.run_id == run_id)
        cols = [f.id, f.run_id, *(f[name] for name in DATA_FILE_FIELDS)]
        with self._engine.connect() as conn:
            rows = conn.execute(select(*cols).where(cond).order_by(f.id)).mappings().all()
        return [DataFile(**row) for row in rows]

    def _successful_runs(self, run_class: str) -> list[Run]:
        r = self._runs.c
        stmt = select(r.id, r.run_class, r.run_description, r.xml_config, r.date_started).where(
            r.project == self._cfg.project, r.run_type == self._cfg.run_type, r.run_class == run_class,
            r.completion_status == self._cfg.status_success)
        with self._engine.connect() as conn:
            rows = conn.execute(stmt).all()
        return [Run(row[0], row[1], row[2] or "", row[3] or "", row[4], parse_release(row[3]), SUCCESS)
                for row in rows]

    def last_successful_run(self, run_class: str, release: date | None = None) -> Run | None:
        return newest_run(self._successful_runs(run_class), release)

    def successful_releases(self, run_class: str) -> list[date]:
        return releases_of(self._successful_runs(run_class))
