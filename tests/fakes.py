"""In-memory Catalog for stage unit tests."""
from __future__ import annotations

import itertools
from datetime import date, datetime

from npd_loader.catalog import (DATA_FILE_FIELDS, SUCCESS, DataFile, Run, clean_fields, newest_run,
                                releases_of, truncate)
from npd_loader.runxml import build_config_xml, parse_release


class FakeCatalog:
    def __init__(self) -> None:
        self.runs: dict[int, dict] = {}
        self.files: dict[int, dict] = {}
        self._run_ids = itertools.count(62000)
        self._file_ids = itertools.count(12000)

    def _run(self, run_id: int) -> Run:
        r = self.runs[run_id]
        return Run(id=run_id, run_class=r["run_class"], description=r["description"], config_xml=r["config_xml"],
                   started_at=r["started_at"], release_date=parse_release(r["config_xml"]), status=r["status"])

    def start_run(self, run_class: str, description: str, config_xml: str) -> Run:
        run_id = next(self._run_ids)
        self.runs[run_id] = {"run_class": run_class, "description": description, "config_xml": config_xml,
                             "started_at": datetime.now().replace(microsecond=0), "status": None,
                             "result": None, "output_xml": None}
        return self._run(run_id)

    def finish_run(self, run: Run, status: str, result: str | None = None, output_xml: str | None = None) -> None:
        self.runs[run.id].update(status=status, result=truncate(result), output_xml=output_xml)

    def add_data_file(self, run: Run, **fields: object) -> int:
        fields = clean_fields(fields)
        file_id = next(self._file_ids)
        self.files[file_id] = {name: None for name in DATA_FILE_FIELDS} | fields | {"id": file_id, "run_id": run.id}
        return file_id

    def update_data_file(self, file_id: int, **fields: object) -> None:
        self.files[file_id].update(clean_fields(fields))

    def get_data_files(self, release: date, file_type: str, run_id: int | None = None) -> list[DataFile]:
        rows = [f for f in self.files.values()
                if f["source_version_num"] == release.isoformat() and f["file_type"] == file_type
                and (run_id is None or f["run_id"] == run_id)]
        return [DataFile(**row) for row in sorted(rows, key=lambda f: f["id"])]

    def _successful(self, run_class: str) -> list[Run]:
        return [self._run(i) for i, r in self.runs.items() if r["run_class"] == run_class and r["status"] == SUCCESS]

    def last_successful_run(self, run_class: str, release: date | None = None) -> Run | None:
        return newest_run(self._successful(run_class), release)

    def successful_releases(self, run_class: str) -> list[date]:
        return releases_of(self._successful(run_class))

    # ---- test helpers -------------------------------------------------
    def add_successful_run(self, run_class: str, release: date) -> Run:
        run = self.start_run(run_class, "test", build_config_xml({"release_date": release.isoformat()}))
        self.finish_run(run, SUCCESS)
        return run
