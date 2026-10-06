from datetime import date

import pytest

from catalog_contract import CatalogContract, cfg
from npd_loader.catalog import FAILED, SUCCESS, SqlCatalog
from npd_loader.config import CatalogConfig
from npd_loader.sqltext import render, split_batches
from tests_paths import TESTS


def make_catalog_schema(engine, schema: str) -> CatalogConfig:
    text = render((TESTS / "sql" / "hie_catalog_schema.sql").read_text(),
                  {"schema": f"[{schema}]", "name": schema})
    with engine.begin() as conn:
        for batch in split_batches(text):
            conn.exec_driver_sql(batch)
    return CatalogConfig(connection="catalog", project="NPD", run_type="National Provider Directory",
                         file_set="NPD_FHIR", run_table=f"{schema}.MASTER_WAREHOUSE_RUN",
                         file_table=f"{schema}.DATA_FILE")


@pytest.fixture
def catalog_cfg(mssql_engine, mssql_schemas):
    (schema,) = mssql_schemas("cat")
    return make_catalog_schema(mssql_engine, schema)


class TestSqlCatalogMssql(CatalogContract):
    @pytest.fixture
    def catalog(self, mssql_engine, catalog_cfg):
        return SqlCatalog(mssql_engine, catalog_cfg)


def test_writes_hie_columns(mssql_engine, catalog_cfg):
    catalog = SqlCatalog(mssql_engine, catalog_cfg)
    run = catalog.start_run("DOWNLOAD", "NPD FHIR Download 2026-09-29", cfg(date(2026, 9, 29)))
    fid = catalog.add_data_file(run, file_type="manifest", source_version_num="2026-09-29",
                                file_name="x" * 1500)
    catalog.finish_run(run, FAILED, result="y" * 9000)
    schema = catalog_cfg.run_table.split(".")[0]
    with mssql_engine.connect() as conn:
        row = conn.exec_driver_sql(
            f"SELECT PROJECT, RUN_TYPE, RUN_CLASS, COMPLETION_STATUS, LEN(RESULT), [USER], RUN_ID "
            f"FROM [{schema}].MASTER_WAREHOUSE_RUN WHERE ID = ?", (run.id,)).one()
        assert row[:5] == ("NPD", "National Provider Directory", "DOWNLOAD", "Failed", 2000)
        assert row[5] and row[6]                      # server defaults still fill USER and the GUID RUN_ID
        assert conn.exec_driver_sql(f"SELECT LEN(FILE_NAME), FILE_SET FROM [{schema}].DATA_FILE WHERE ID = ?",
                                    (fid,)).one() == (1500, "NPD_FHIR")
