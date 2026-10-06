import copy
from datetime import date, timedelta

import psycopg

from npd_loader.catalog import SqlCatalog
from npd_loader.cli import main
from npd_loader.config import parse_config
from npd_loader.db import list_release_partitions
from npd_loader.storage import LocalStorage
import fixture_data
from helpers import config_data, to_toml
from test_catalog_pg import pg_engine
from release_builder import build_release

BASE = date(2026, 8, 4)
ORG1 = fixture_data.ORG1["id"]


def publish(cms, week: int) -> date:
    release = BASE + timedelta(weeks=week)
    records = copy.deepcopy(fixture_data.RECORDS)
    records["01-Organization.ndjson"][0]["name"] = f"ORG {release}"
    cms.publish(build_release(release.isoformat(), records=records))
    return release


def test_six_releases_end_to_end(tmp_path, cms, make_db, catalog_db):
    npd = make_db()
    data = config_data(tmp_path / "data", cms.manifest_url, npd_conninfo=npd, catalog_conninfo=catalog_db,
                       keep_releases=5)
    config_path = tmp_path / "config.toml"
    config_path.write_text(to_toml(data))
    catalog = SqlCatalog(pg_engine(catalog_db), parse_config(data).catalog)
    storage = LocalStorage(tmp_path / "data")

    def cli(*args):
        return main(["--config", str(config_path), *args])

    def raw_pairs():
        with psycopg.connect(npd) as conn:
            return set(conn.execute("SELECT DISTINCT ndjson_file_id, zst_file_id FROM npd_raw.resource "
                                    "WHERE release_date = %s", (r1,)).fetchall())

    def run_count():
        with psycopg.connect(catalog_db) as conn:
            return conn.execute("SELECT count(*) FROM master_warehouse_run").fetchone()[0]

    assert cli("init-db") == 0

    # release 1: download -> extract -> import, lineage matches the catalog
    r1 = publish(cms, 0)
    assert cli("run") == 0
    assert run_count() == 3
    ndjson = catalog.get_data_files(r1, "ndjson")
    zst = catalog.get_data_files(r1, "ndjson.zst")
    assert len(ndjson) == 8 and {n.parent_file for n in ndjson} == {z.id for z in zst}
    assert raw_pairs() == {(n.id, n.parent_file) for n in ndjson}

    # nothing to do the second time
    assert cli("run") == 0
    assert run_count() == 3

    # forced import replaces the partitions
    assert cli("import", "--force") == 0
    with psycopg.connect(npd) as conn:
        assert list(list_release_partitions(conn, "npd", "practitioner")) == [r1]

    # deleted .ndjson is re-extracted in place with the same data_file id
    victim = ndjson[0]
    storage.delete(victim.file_rel_path)
    assert cli("import", "--release", r1.isoformat(), "--force") == 0
    assert [n.id for n in catalog.get_data_files(r1, "ndjson")] == [n.id for n in ndjson]
    assert storage.exists(victim.file_rel_path)
    assert raw_pairs() == {(n.id, n.parent_file) for n in ndjson}

    # releases 2..6: release 1 falls out of the newest 5
    releases = [r1] + [None] * 5
    for week in range(1, 6):
        releases[week] = publish(cms, week)
        assert cli("run") == 0
    with psycopg.connect(npd) as conn:
        assert sorted(list_release_partitions(conn, "npd_raw", "resource")) == releases[1:]
        assert sorted(list_release_partitions(conn, "npd", "practitioner")) == releases[1:]
        assert [r[0] for r in conn.execute("SELECT release_date FROM npd.release ORDER BY 1")] == releases[1:]
        assert conn.execute("SELECT name FROM npd.v_organization WHERE resource_id = %s",
                            (ORG1,)).fetchone()[0] == f"ORG {releases[-1]}"
    old_ndjson = catalog.get_data_files(r1, "ndjson")
    assert old_ndjson and not any(storage.exists(n.file_rel_path) for n in old_ndjson)
    assert all(storage.exists(z.file_rel_path) for z in catalog.get_data_files(r1, "ndjson.zst"))
    assert all(storage.exists(m.file_rel_path) for m in catalog.get_data_files(r1, "manifest"))
    for release in releases[1:]:
        assert all(storage.exists(n.file_rel_path) for n in catalog.get_data_files(release, "ndjson"))

    assert cli("status") == 0
