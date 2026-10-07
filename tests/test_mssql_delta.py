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


def key(d, rtype, rid):
    found = rows(d, f"SELECT s.resource_key FROM {d.q(d.cfg.schema, 'resource_state')} s JOIN "
                    f"{d.q(d.cfg.schema, 'resource_type')} t ON t.resource_type_id = s.resource_type_id "
                    f"WHERE t.name = ? AND s.resource_id = ?", rtype, rid)
    return found[0][0] if found else None


def test_first_load_inserts_everything(mssql_dialect, tmp_path):
    d, sch = mssql_dialect, mssql_dialect.cfg.schema
    res = load(d, tmp_path, R1, 7)
    assert sum(k["new"] for k in res.kinds.values()) == 12
    assert res.inserted["practitioner"] == 2 and res.replaced["practitioner"] == 0      # real rowcounts
    assert rows(d, f"SELECT count(*) FROM {d.q(sch, 'resource_state')} WHERE hash IS NOT NULL") == [(12,)]
    assert rows(d, f"SELECT count(*) FROM {d.q(sch, 'practitioner')}") == [(2,)]
    assert rows(d, f"SELECT import_run_id, new_resources, not_seen_resources FROM {d.q(sch, 'release')}") == [(7, 12, 0)]
    # references translate to the target's key
    role, prac = key(d, "PractitionerRole", "0f00aa11"), key(d, "Practitioner", "1003000100")
    assert role and prac
    assert rows(d, f"SELECT practitioner_key FROM {d.q(sch, 'practitioner_role')} WHERE resource_key = ?", role) == [(prac,)]
    # identifiers are keyed by their resource
    assert rows(d, f"SELECT count(*) FROM {d.q(sch, 'identifier')} WHERE resource_key = ?", prac)[0][0] >= 1


def test_reference_to_missing_data_gets_a_key_without_state(mssql_dialect, tmp_path):
    d, sch = mssql_dialect, mssql_dialect.cfg.schema
    load(d, tmp_path, R1, 7)
    org = key(d, "Organization", "1295596195")              # HealthcareService.providedBy; no such Organization
    assert org is not None
    assert rows(d, f"SELECT hash, last_updated, release_date, run_id, last_seen_release, last_seen_run_id "
                   f"FROM {d.q(sch, 'resource_state')} WHERE resource_key = ?", org) == [(None,) * 6]
    assert rows(d, f"SELECT provided_by_organization_key FROM {d.q(sch, 'healthcare_service')}") == [(org,)]
    assert rows(d, f"SELECT count(*) FROM {d.q(sch, 'organization')} WHERE resource_key = ?", org) == [(0,)]
    # the data arrives in the next release: same key, counted as new, state filled in
    records = copy.deepcopy(fixture_data.RECORDS)
    late = copy.deepcopy(records["01-Organization.ndjson"][0])
    late["id"] = "Organization-1295596195"
    records["01-Organization.ndjson"].append(late)
    res = load(d, tmp_path, R2, 8, records)
    assert res.kinds["Organization"]["new"] == 1
    assert key(d, "Organization", "1295596195") == org
    assert rows(d, f"SELECT release_date, last_seen_release FROM {d.q(sch, 'resource_state')} WHERE resource_key = ?",
                org) == [(R2, R2)]
    assert rows(d, f"SELECT count(*) FROM {d.q(sch, 'organization')} WHERE resource_key = ?", org) == [(1,)]
    assert rows(d, f"SELECT not_seen_resources FROM {d.q(sch, 'release')} WHERE release_date = ?", R2) == [(0,)]


def test_second_release_upserts_and_keeps_aging_data(mssql_dialect, tmp_path):
    d = mssql_dialect
    load(d, tmp_path, R1, 7)
    records = copy.deepcopy(fixture_data.RECORDS)
    p = records["06-Practitioner.ndjson"][0]
    p["name"][0]["family"] = "GOMEZ-CHANGED"
    p["telecom"] = p["telecom"][:1]                                       # child rows shrink
    missing = records["08-OrganizationAffiliation.ndjson"].pop()          # not in the new release
    pkey = key(d, "Practitioner", p["id"].split("-", 1)[1])
    mkey = key(d, "OrganizationAffiliation", missing["id"].split("-", 1)[1])
    res = load(d, tmp_path, R2, 8, records)
    assert res.kinds["Practitioner"] == {"new": 0, "changed": 1, "unchanged": 1, "not_seen": 0}
    assert res.kinds["OrganizationAffiliation"]["not_seen"] == 1
    assert res.inserted["practitioner"] == 1 and res.replaced["practitioner"] == 1
    sch = d.cfg.schema
    assert key(d, "Practitioner", p["id"].split("-", 1)[1]) == pkey                   # keys never change
    assert rows(d, f"SELECT name_family FROM {d.q(sch, 'practitioner')} WHERE resource_key = ?", pkey) == [("GOMEZ-CHANGED",)]
    assert rows(d, f"SELECT count(*) FROM {d.q(sch, 'practitioner_telecom')} WHERE resource_key = ?", pkey) == [(1,)]
    # aging data stays live, with its last-seen release
    assert rows(d, f"SELECT count(*) FROM {d.q(sch, 'organization_affiliation')} WHERE resource_key = ?", mkey) == [(1,)]
    assert rows(d, f"SELECT last_seen_release FROM {d.q(sch, 'resource_state')} WHERE resource_key = ?", mkey) == [(R1,)]
    # content release vs last seen
    assert rows(d, f"SELECT release_date, last_seen_release FROM {d.q(sch, 'resource_state')} WHERE resource_key = ?",
                pkey) == [(R2, R2)]
    unchanged = key(d, "Practitioner", records["06-Practitioner.ndjson"][1]["id"].split("-", 1)[1])
    assert rows(d, f"SELECT release_date, last_seen_release FROM {d.q(sch, 'resource_state')} WHERE resource_key = ?",
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
    records["06-Practitioner.ndjson"].append({**records["06-Practitioner.ndjson"][1], "id": "Practitioner-1999999999"})
    storage = LocalStorage(tmp_path / "data8")
    d.stage_release(storage, R2, 8, inputs_for(storage, build_release(R2.isoformat(), records=records).ndjson))
    with d._autocommit() as c:                                            # sabotage one staging table
        c.exec_driver_sql(f"EXEC sp_rename '{d.cfg.stage_schema}.practitioner_role', 'practitioner_role_x'")
    before = rows(d, f"SELECT count(*), max(resource_key) FROM {d.q(d.cfg.schema, 'resource_state')}")
    try:
        try:
            d.apply_delta(R2, 8)
            raise AssertionError("apply_delta should have failed")
        except Exception as exc:
            assert "practitioner_role" in str(exc)
        assert rows(d, f"SELECT gender FROM {d.q(d.cfg.schema, 'practitioner')} ORDER BY resource_key")[0] == ("male",)
        assert [r[0] for r in rows(d, f"SELECT release_date FROM {d.q(d.cfg.schema, 'release')}")] == [R1]
        assert rows(d, f"SELECT count(*), max(resource_key) FROM {d.q(d.cfg.schema, 'resource_state')}") == before
    finally:
        with d._autocommit() as c:
            c.exec_driver_sql(f"EXEC sp_rename '{d.cfg.stage_schema}.practitioner_role_x', 'practitioner_role'")
