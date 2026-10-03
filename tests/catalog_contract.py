"""Behavior every Catalog implementation must have. Subclass and provide a `catalog` fixture."""
from datetime import date

import pytest

from npd_loader.catalog import FAILED, SUCCESS
from npd_loader.runxml import build_config_xml

R1 = date(2026, 9, 22)
R2 = date(2026, 9, 29)


def cfg(release: date) -> str:
    return build_config_xml({"run_type": "National Provider Directory", "release_date": release.isoformat()})


class CatalogContract:
    def test_start_run_parses_release(self, catalog):
        run = catalog.start_run("DOWNLOAD", "NPD FHIR Download 2026-09-29", cfg(R2))
        assert run.release_date == R2
        assert run.run_class == "DOWNLOAD"
        assert run.started_at.microsecond == 0
        second = catalog.start_run("DOWNLOAD", "again", cfg(R2))
        assert second.id > run.id

    def test_last_successful_run_ignores_failed_and_unfinished(self, catalog):
        failed = catalog.start_run("DOWNLOAD", "d", cfg(R2))
        catalog.finish_run(failed, FAILED, result="boom")
        catalog.start_run("DOWNLOAD", "d", cfg(R2))  # never finished
        assert catalog.last_successful_run("DOWNLOAD", R2) is None
        ok = catalog.start_run("DOWNLOAD", "d", cfg(R2))
        catalog.finish_run(ok, SUCCESS, output_xml="<WAREHOUSE_RUN_OUTPUT />")
        found = catalog.last_successful_run("DOWNLOAD", R2)
        assert found is not None and found.id == ok.id
        assert catalog.last_successful_run("IMPORT", R2) is None

    def test_newest_release_when_release_not_given(self, catalog):
        for release in (R2, R1):
            run = catalog.start_run("DOWNLOAD", "d", cfg(release))
            catalog.finish_run(run, SUCCESS)
        assert catalog.last_successful_run("DOWNLOAD").release_date == R2
        assert catalog.last_successful_run("DOWNLOAD", R1).release_date == R1
        assert catalog.successful_releases("DOWNLOAD") == [R1, R2]

    def test_data_files_filter_and_update(self, catalog):
        run1 = catalog.start_run("DOWNLOAD", "d", cfg(R2))
        run2 = catalog.start_run("DOWNLOAD", "d", cfg(R2))
        a = catalog.add_data_file(run1, file_type="ndjson.zst", source_version_num=R2.isoformat(),
                                  source_uri="https://x/01-Organization.ndjson.zst")
        b = catalog.add_data_file(run2, file_type="ndjson.zst", source_version_num=R2.isoformat(), parent_file=None)
        catalog.add_data_file(run2, file_type="ndjson", source_version_num=R2.isoformat(), parent_file=b)
        catalog.add_data_file(run2, file_type="ndjson.zst", source_version_num=R1.isoformat())
        assert [f.id for f in catalog.get_data_files(R2, "ndjson.zst")] == [a, b]
        assert [f.id for f in catalog.get_data_files(R2, "ndjson.zst", run_id=run2.id)] == [b]
        catalog.update_data_file(a, file_size=42, file_hash="ab" * 32, file_name="file_1_x")
        row = catalog.get_data_files(R2, "ndjson.zst", run_id=run1.id)[0]
        assert (row.file_size, row.file_hash, row.file_name, row.run_id) == (42, "ab" * 32, "file_1_x", run1.id)
        assert row.source_uri == "https://x/01-Organization.ndjson.zst"

    def test_unknown_field_rejected(self, catalog):
        run = catalog.start_run("DOWNLOAD", "d", cfg(R2))
        with pytest.raises(TypeError, match="unknown data_file fields"):
            catalog.add_data_file(run, file_type="x", bogus=1)

    def test_exceptions_truncated(self, catalog):
        run = catalog.start_run("DOWNLOAD", "d", cfg(R2))
        fid = catalog.add_data_file(run, file_type="ndjson", source_version_num=R2.isoformat())
        catalog.update_data_file(fid, exceptions="x" * 9000)
        assert len(catalog.get_data_files(R2, "ndjson")[0].exceptions) == 8000
