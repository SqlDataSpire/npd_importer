from datetime import date

import psycopg
import pytest

from catalog_contract import CatalogContract, cfg
from npd_loader.catalog import FAILED, SUCCESS, CssCatalogPg
from npd_loader.config import CatalogConfig


def catalog_config() -> CatalogConfig:
    return CatalogConfig(backend="css_catalog_pg", host="unused", port=0, dbname="unused", user=None,
                         password=None, project="NPD", run_type="National Provider Directory", file_set="NPD_FHIR")


class TestCssCatalogPg(CatalogContract):
    @pytest.fixture
    def catalog(self, catalog_db):
        return CssCatalogPg(catalog_db, catalog_config())


def test_writes_configured_labels(catalog_db):
    catalog = CssCatalogPg(catalog_db, catalog_config())
    run = catalog.start_run("DOWNLOAD", "NPD FHIR Download 2026-09-29", cfg(date(2026, 9, 29)))
    fid = catalog.add_data_file(run, file_type="manifest", source_version_num="2026-09-29")
    catalog.finish_run(run, SUCCESS, result=None, output_xml="<WAREHOUSE_RUN_OUTPUT />")
    failed = catalog.start_run("IMPORT", "x", cfg(date(2026, 9, 29)))
    catalog.finish_run(failed, FAILED, result="y" * 9000)
    with psycopg.connect(catalog_db) as conn:
        row = conn.execute("SELECT project, run_type, run_class, run_description, completion_status, "
                           "date_completed IS NOT NULL, parent_run_id FROM master_warehouse_run WHERE run_id = %s",
                           (run.id,)).fetchone()
        assert row == ("NPD", "National Provider Directory", "DOWNLOAD", "NPD FHIR Download 2026-09-29",
                       "Success", True, None)
        assert conn.execute("SELECT completion_status, length(result) FROM master_warehouse_run WHERE run_id = %s",
                            (failed.id,)).fetchone() == ("Failed", 8000)
        assert conn.execute("SELECT file_set, source_version_name, run_id FROM data_file WHERE id = %s",
                            (fid,)).fetchone() == ("NPD_FHIR", "NPD release", run.id)


def test_other_projects_runs_are_ignored(catalog_db):
    with psycopg.connect(catalog_db) as conn:
        conn.execute("INSERT INTO master_warehouse_run (project, run_type, run_class, xml_config, completion_status) "
                     "VALUES ('OTHER', 'National Provider Directory', 'DOWNLOAD', %s, 'Success')",
                     (cfg(date(2026, 9, 29)),))
    catalog = CssCatalogPg(catalog_db, catalog_config())
    assert catalog.last_successful_run("DOWNLOAD") is None
