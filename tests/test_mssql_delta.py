import copy
from datetime import date

from npd_loader.storage import LocalStorage
from release_builder import build_release
from test_mssql_stage import inputs_for
import fixture_data

R1, R2 = date(2026, 9, 29), date(2026, 10, 6)


def rows(d, sql, *args):
    with d.engine.connect() as c:
        return [tuple(r) for r in c.exec_driver_sql(sql, args).fetchall()]


def load(d, tmp_path, release, run_id, records=None):
    storage = LocalStorage(tmp_path / f"data{run_id}")
    rel = build_release(release.isoformat(), records=records)
    d.stage_release(storage, release, run_id, inputs_for(storage, rel.ndjson))
    return d.apply_delta(release, run_id)


def test_first_load_inserts_everything(mssql_dialect, tmp_path):
    d = mssql_dialect
    res = load(d, tmp_path, R1, 7)
    assert sum(k["new"] for k in res.kinds.values()) == 12
    assert res.inserted["practitioner"] == 2 and res.replaced["practitioner"] == 0      # real rowcounts
    assert rows(d, f"SELECT count(*) FROM {d.q(d.cfg.schema, 'resource_state')}") == [(12,)]
    assert rows(d, f"SELECT count(*) FROM {d.q(d.cfg.schema, 'practitioner')}") == [(2,)]
    assert rows(d, f"SELECT import_run_id, new_resources, not_seen_resources FROM {d.q(d.cfg.schema, 'release')}") == [(7, 12, 0)]


def test_second_release_upserts_and_keeps_aging_data(mssql_dialect, tmp_path):
    d = mssql_dialect
    load(d, tmp_path, R1, 7)
    records = copy.deepcopy(fixture_data.RECORDS)
    p = records["06-Practitioner.ndjson"][0]
    p["name"][0]["family"] = "GOMEZ-CHANGED"
    p["telecom"] = p["telecom"][:1]                                       # child rows shrink
    missing = records["08-OrganizationAffiliation.ndjson"].pop()          # not in the new release
    res = load(d, tmp_path, R2, 8, records)
    assert res.kinds["Practitioner"] == {"new": 0, "changed": 1, "unchanged": 1, "not_seen": 0}
    assert res.kinds["OrganizationAffiliation"]["not_seen"] == 1
    assert res.inserted["practitioner"] == 1 and res.replaced["practitioner"] == 1
    pid, sch = p["id"], d.cfg.schema
    assert rows(d, f"SELECT name_family FROM {d.q(sch, 'practitioner')} WHERE resource_id = ?", pid) == [("GOMEZ-CHANGED",)]
    assert rows(d, f"SELECT count(*) FROM {d.q(sch, 'practitioner_telecom')} WHERE resource_id = ?", pid) == [(1,)]
    # aging data stays live, with its last-seen release
    assert rows(d, f"SELECT count(*) FROM {d.q(sch, 'organization_affiliation')} WHERE resource_id = ?", missing["id"]) == [(1,)]
    assert rows(d, f"SELECT last_seen_release FROM {d.q(sch, 'resource_state')} WHERE resource_id = ?", missing["id"]) == [(R1,)]
    # content release vs last seen
    assert rows(d, f"SELECT release_date, last_seen_release FROM {d.q(sch, 'resource_state')} WHERE resource_id = ?",
                pid) == [(R2, R2)]
    unchanged = records["06-Practitioner.ndjson"][1]["id"]
    assert rows(d, f"SELECT release_date, last_seen_release FROM {d.q(sch, 'resource_state')} WHERE resource_id = ?",
                unchanged) == [(R1, R2)]
    assert rows(d, f"SELECT not_seen_resources FROM {d.q(sch, 'release')} WHERE release_date = ?", R2) == [(1,)]


def test_partial_file_deletes_nothing(mssql_dialect, tmp_path):
    d = mssql_dialect
    load(d, tmp_path, R1, 7)
    records = copy.deepcopy(fixture_data.RECORDS)
    records["01-Organization.ndjson"] = records["01-Organization.ndjson"][:1]      # truncated file
    res = load(d, tmp_path, R2, 8, records)
    assert res.kinds["Organization"]["not_seen"] == 1
    assert rows(d, f"SELECT count(*) FROM {d.q(d.cfg.schema, 'organization')}") == [(2,)]


def test_failure_inside_apply_rolls_back(mssql_dialect, tmp_path):
    d = mssql_dialect
    load(d, tmp_path, R1, 7)
    records = copy.deepcopy(fixture_data.RECORDS)
    records["06-Practitioner.ndjson"][0]["gender"] = "unknown"
    storage = LocalStorage(tmp_path / "data8")
    d.stage_release(storage, R2, 8, inputs_for(storage, build_release(R2.isoformat(), records=records).ndjson))
    with d._autocommit() as c:                                            # sabotage one staging table
        c.exec_driver_sql(f"EXEC sp_rename '{d.cfg.stage_schema}.practitioner_role', 'practitioner_role_x'")
    try:
        try:
            d.apply_delta(R2, 8)
            raise AssertionError("apply_delta should have failed")
        except Exception as exc:
            assert "practitioner_role" in str(exc)
        assert rows(d, f"SELECT gender FROM {d.q(d.cfg.schema, 'practitioner')} ORDER BY resource_id")[0] == ("male",)
        assert [r[0] for r in rows(d, f"SELECT release_date FROM {d.q(d.cfg.schema, 'release')}")] == [R1]
    finally:
        with d._autocommit() as c:
            c.exec_driver_sql(f"EXEC sp_rename '{d.cfg.stage_schema}.practitioner_role_x', 'practitioner_role'")
