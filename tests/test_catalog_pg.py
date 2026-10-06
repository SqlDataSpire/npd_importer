from datetime import date

import psycopg
import pytest

from catalog_contract import CatalogContract, cfg
from sqlalchemy import create_engine

from npd_loader.catalog import FAILED, SUCCESS, SqlCatalog
from npd_loader.config import CatalogConfig


def pg_engine(conninfo: str):
    from psycopg.conninfo import conninfo_to_dict
    d = conninfo_to_dict(conninfo)
    return create_engine(f"postgresql+psycopg2://{d['user']}:{d['password']}@{d['host']}:{d['port']}/{d['dbname']}")


def catalog_config() -> CatalogConfig:
    return CatalogConfig(connection="catalog", project="NPD", run_type="National Provider Directory",
                         file_set="NPD_FHIR")


class TestSqlCatalogPg(CatalogContract):
    @pytest.fixture
    def catalog(self, catalog_db):
        return SqlCatalog(pg_engine(catalog_db), catalog_config())


def test_writes_configured_labels(catalog_db):
    catalog = SqlCatalog(pg_engine(catalog_db), catalog_config())
    run = catalog.start_run("DOWNLOAD", "NPD FHIR Download 2026-09-29", cfg(date(2026, 9, 29)))
    fid = catalog.add_data_file(run, file_type="manifest", source_version_num="2026-09-29")
    catalog.finish_run(run, SUCCESS, result=None, output_xml="<WAREHOUSE_RUN_OUTPUT />")
    failed = catalog.start_run("IMPORT", "x", cfg(date(2026, 9, 29)))
    catalog.finish_run(failed, FAILED, result="y" * 9000)
    with psycopg.connect(catalog_db) as conn:
        row = conn.execute("SELECT project, run_type, run_class, run_description, completion_status, "
                           "date_completed IS NOT NULL, parent_run_id FROM master_warehouse_run WHERE id = %s",
                           (run.id,)).fetchone()
        assert row == ("NPD", "National Provider Directory", "DOWNLOAD", "NPD FHIR Download 2026-09-29",
                       "Success", True, None)
        assert conn.execute("SELECT completion_status, length(result) FROM master_warehouse_run WHERE id = %s",
                            (failed.id,)).fetchone() == ("Failed", 2000)
        assert conn.execute("SELECT file_set, source_version_name, run_id FROM data_file WHERE id = %s",
                            (fid,)).fetchone() == ("NPD_FHIR", "NPD release", run.id)


def test_other_projects_runs_are_ignored(catalog_db):
    with psycopg.connect(catalog_db) as conn:
        conn.execute("INSERT INTO master_warehouse_run (project, run_type, run_class, xml_config, completion_status) "
                     "VALUES ('OTHER', 'National Provider Directory', 'DOWNLOAD', %s, 'Success')",
                     (cfg(date(2026, 9, 29)),))
    catalog = SqlCatalog(pg_engine(catalog_db), catalog_config())
    assert catalog.last_successful_run("DOWNLOAD") is None
