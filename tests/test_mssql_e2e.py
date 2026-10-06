import copy
from datetime import date, timedelta

from npd_loader.catalog import SqlCatalog
from npd_loader.cli import main
from npd_loader.config import parse_config
import fixture_data
from helpers import config_data, to_toml, write_env_file
from release_builder import build_release
from test_catalog_mssql import make_catalog_schema

BASE = date(2026, 8, 4)


def publish(cms, week: int) -> date:
    release = BASE + timedelta(weeks=week)
    records = copy.deepcopy(fixture_data.RECORDS)
    records["01-Organization.ndjson"][0]["name"] = f"ORG {release}"
    cms.publish(build_release(release.isoformat(), records=records))
    return release


def test_releases_end_to_end_on_sql_server(tmp_path, cms, mssql_doc, mssql_engine, mssql_schemas, capsys):
    raw, data, cat = mssql_schemas("raw", "", "cat")
    cat_cfg = make_catalog_schema(mssql_engine, cat)
    env = write_env_file(tmp_path / "database.env", {"data": mssql_doc, "catalog": mssql_doc})
    cfg_data = config_data(tmp_path / "data", cms.manifest_url, env_file=env, schemas=(raw, data),
                           catalog_tables=(cat_cfg.run_table, cat_cfg.file_table), keep_releases=2)
    config_path = tmp_path / "config.toml"
    config_path.write_text(to_toml(cfg_data))
    catalog = SqlCatalog(mssql_engine, parse_config(cfg_data).catalog)

    def cli(*args):
        return main(["--config", str(config_path), *args])

    def scalar(sql):
        with mssql_engine.connect() as conn:
            return conn.exec_driver_sql(sql).scalar()

    assert cli("init-db") == 0
    assert cli("init-db") == 0

    r1 = publish(cms, 0)
    assert cli("run") == 0
    assert scalar(f"SELECT count(*) FROM [{data}].[v_practitioner]") == 2
    assert len(catalog.get_data_files(r1, "ndjson")) == 8
    assert cli("run") == 0                                   # nothing to do
    assert cli("import", "--force") == 0                     # replaces the partitions
    assert scalar(f"SELECT count(*) FROM [{raw}].[resource]") == 12

    r2 = publish(cms, 1)
    assert cli("run") == 0
    r3 = publish(cms, 2)
    assert cli("run") == 0                                   # keep_releases = 2: r1 is dropped
    assert scalar(f"SELECT count(DISTINCT release_date) FROM [{raw}].[resource]") == 2
    assert scalar(f"SELECT MIN(release_date) FROM [{data}].[release]") == r2
    capsys.readouterr()
    assert cli("status") == 0
    out = capsys.readouterr().out
    assert out.splitlines()[0].split() == ["release", "download", "extract", "import", "published"]
    assert r3.isoformat() in out and "using:" not in out     # DataEngine's import print is suppressed
