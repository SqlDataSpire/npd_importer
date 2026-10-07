import copy
from datetime import date

from npd_loader.catalog import SqlCatalog
from npd_loader.cli import main
from npd_loader.config import parse_config
import fixture_data
from helpers import config_data, to_toml, write_env_file
from release_builder import build_release
from test_catalog_mssql import make_catalog_schema


def test_two_releases_end_to_end(tmp_path, cms, mssql_doc, mssql_engine, mssql_schemas, capsys):
    stage, data, cat = mssql_schemas("stage", "", "cat")
    cat_cfg = make_catalog_schema(mssql_engine, cat)
    env = write_env_file(tmp_path / "database.env", {"data": mssql_doc, "catalog": mssql_doc})
    cfg = config_data(tmp_path / "data", cms.manifest_url, env_file=env, schemas=(stage, data),
                      catalog_tables=(cat_cfg.run_table, cat_cfg.file_table), keep_releases=1)
    (tmp_path / "config.toml").write_text(to_toml(cfg))
    catalog = SqlCatalog(mssql_engine, parse_config(cfg).catalog)
    cli = lambda *a: main(["--config", str(tmp_path / "config.toml"), *a])

    def scalar(q):
        with mssql_engine.connect() as conn:
            return conn.exec_driver_sql(q).scalar()

    assert cli("init-db") == 0 and cli("init-db") == 0
    cms.publish(build_release("2026-09-29"))
    assert cli("run") == 0
    assert scalar(f"SELECT count(*) FROM [{data}].[practitioner]") == 2
    assert scalar(f"SELECT count(*) FROM [{data}].[resource_state] WHERE hash IS NOT NULL") == 12
    assert cli("run") == 0                                                   # nothing to do

    records = copy.deepcopy(fixture_data.RECORDS)
    records["06-Practitioner.ndjson"][0]["gender"] = "female"
    aging = records["08-OrganizationAffiliation.ndjson"].pop()
    cms.publish(build_release("2026-10-06", records=records))
    assert cli("run") == 0
    assert scalar(f"SELECT count(*) FROM [{data}].[practitioner] WHERE gender = 'female'") == 2
    aging_id = aging["id"].split("-", 1)[1]
    assert scalar(f"SELECT count(*) FROM [{data}].[organization_affiliation] a JOIN [{data}].[resource_state] s "
                  f"ON s.resource_key = a.resource_key WHERE s.resource_id = '{aging_id}'") == 1
    assert scalar(f"SELECT changed_resources FROM [{data}].[release] WHERE release_date = '2026-10-06'") == 1
    assert scalar(f"SELECT not_seen_resources FROM [{data}].[release] WHERE release_date = '2026-10-06'") == 1
    assert scalar(f"SELECT CAST(last_seen_release AS varchar(10)) FROM [{data}].[resource_state] "
                  f"WHERE resource_id = '{aging_id}'") == "2026-09-29"
    # keep_releases = 1: the old release's .ndjson files are gone from storage
    assert all(not row.file_rel_path or not (tmp_path / "data" / row.file_rel_path).exists()
               for row in catalog.get_data_files(date(2026, 9, 29), "ndjson"))
    assert any((tmp_path / "data" / row.file_rel_path).exists()
               for row in catalog.get_data_files(date(2026, 10, 6), "ndjson"))
    capsys.readouterr()
    assert cli("status") == 0
    out = capsys.readouterr().out
    assert "2026-10-06" in out and "2026-09-29" in out
