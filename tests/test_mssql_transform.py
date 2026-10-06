from datetime import datetime

import pytest

from npd_loader.storage import LocalStorage
from mssql_fixture_load import R, fetch, load_fixture_raw

P1, P2 = "Practitioner-1003000100", "Practitioner-1083687529"


@pytest.fixture
def result(mssql_dialect, tmp_path):
    raw = load_fixture_raw(mssql_dialect, LocalStorage(tmp_path / "data"))
    return mssql_dialect, mssql_dialect.run_transforms(raw.table, R, 7)


def rows(d, res, table, columns, order="resource_id"):
    return [tuple(r) for r in fetch(d, f"SELECT {columns} FROM {d.q(d.cfg.schema, res.tables[table])} ORDER BY {order}")]


def test_standalone_tables_counts_and_not_published(result):
    d, res = result
    assert res.tables["practitioner"] == "practitioner__20260929__r7"
    assert res.counts["practitioner"] == 2 and res.counts["organization"] == 2
    assert all(n > 0 for t, n in res.counts.items() if t in ("location", "endpoint", "practitioner_name"))
    assert fetch(d, f"SELECT count(*) FROM {d.q(d.cfg.schema, 'practitioner')}")[0][0] == 0


def test_practitioner(result):
    d, res = result
    assert rows(d, res, "practitioner", "resource_id, last_updated, npi, active, gender, name_family, name_given, "
                                        "name_prefix, name_suffix, identity_verified") == [
        (P1, datetime(2026, 9, 29, 4, 34, 0, 724000), "1003000100", True, "male", "GOMEZ", "GERARDO", None, None, False),
        (P2, datetime(2026, 9, 29, 4, 35), "1083687529", True, "female", "JONES", "ANNA MARIE", "DR.", "MD", True),
    ]


def test_practitioner_children(result):
    d, res = result
    assert rows(d, res, "practitioner_name", "resource_id, seq, [use], family, period_start", "resource_id, seq") == [
        (P1, 1, "official", "GOMEZ", None), (P2, 1, "maiden", "SMITH", datetime(1990, 1, 1)),
        (P2, 2, "official", "JONES", None)]
    assert rows(d, res, "practitioner_telecom", "resource_id, seq, [value]", "resource_id, seq")[0] == (P1, 1, "2133831280")
    assert rows(d, res, "practitioner_address", "resource_id, seq, line1, city", "resource_id, seq")[0] == \
        (P1, 1, "108 W Victoria St", "Gardena")
    assert len(rows(d, res, "practitioner_qualification", "resource_id, seq, code", "resource_id, seq")) >= 1


def test_organization_location_endpoint(result):
    d, res = result
    org = rows(d, res, "organization", "resource_id, npi, pseudo_ein, active, part_of_organization_id, verification_status")
    assert org[0] == ("Organization-1336200294", "1336200294", "6e5d8b3e-13d3-48be-9ed2-881d08f2c459", True, None, "complete")
    assert org[1][4] == "Organization-1336200294"
    addr = rows(d, res, "organization_address", "line1, line2, extra_lines", "resource_id, seq")
    assert addr == [("2515 Eastbluff Dr", "Suite 100", "Building B")]
    assert rows(d, res, "organization_endpoint", "endpoint_id", "resource_id, seq") == \
        [("Endpoint-000f410c-e1e9-4a78-a988-b6ce47d6a793",)]
    assert res.counts["location"] >= 1 and res.counts["endpoint"] >= 1


def test_every_table_gets_rows(result):
    d, res = result
    empty = sorted(t for t, n in res.counts.items() if n == 0)
    assert empty == [], f"no rows in {empty}"


def test_roles_and_identifier(result):
    d, res = result
    role = rows(d, res, "practitioner_role", "practitioner_id, organization_id")
    assert role and all(p and p.startswith("Practitioner-") for p, _ in role)
    ids = rows(d, res, "identifier", "resource_type, resource_id, seq, [system], [value]",
               "resource_type, resource_id, seq")
    assert ("Organization", "Organization-1336200294", 1, "http://terminology.hl7.org/NamingSystem/npi",
            "1336200294") in ids


def test_chunked_run_matches_one_pass(result):
    """Chunks of one resource each (run 8) give the same rows as one pass per script (run 7)."""
    d, res = result
    d.chunk_rows = 1
    chunked = d.run_transforms(f"resource__{R:%Y%m%d}__r7", R, 8)
    assert chunked.counts == res.counts
    for parent, name in res.tables.items():
        a, b = d.q(d.cfg.schema, name), d.q(d.cfg.schema, chunked.tables[parent])
        assert fetch(d, f"SELECT count(*) FROM (SELECT * FROM {a} EXCEPT SELECT * FROM {b}) x")[0][0] == 0, parent
    stages = fetch(d, "SELECT count(*) FROM sys.tables WHERE schema_id = SCHEMA_ID(?) AND name LIKE 'stage[_]%'",
                   d.cfg.schema)
    assert stages[0][0] == 0


def test_chunked_directive_without_token_is_an_error(result, monkeypatch):
    d, res = result
    monkeypatch.setattr("npd_loader.dialect.mssql.sql_scripts",
                        lambda flavor, kind: [("099_bad.sql", "-- chunked: Practitioner\nSELECT 1\n")])
    with pytest.raises(ValueError, match="099_bad.sql.*<<chunk>>"):
        d.run_transforms(f"resource__{R:%Y%m%d}__r7", R, 9)
