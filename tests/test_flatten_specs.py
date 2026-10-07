from golden_values import load_golden, normalize
from npd_loader.flatten.engine import columns, flatten_resource
from npd_loader.flatten.specs import ALL_TABLES, SPECS, TABLE_TYPES, tables_for
from release_builder import build_release
import orjson


def flatten_fixture() -> dict[str, list[list]]:
    out: dict[str, list[list]] = {}
    ndjson = build_release("2026-09-29").ndjson
    for i, (name, data) in enumerate(sorted(ndjson.items())):
        fid, zid = 500 + 2 * i, 501 + 2 * i
        for line in data.splitlines():
            if not line.strip():
                continue
            res = orjson.loads(line)
            for table, values in flatten_resource(res, tables_for(res["resourceType"]),
                                                  ("2026-09-29", res["id"], fid, zid)):
                out.setdefault(table, []).append([normalize(v) for v in values])
    return out


def test_specs_cover_every_table_once():
    assert len(ALL_TABLES) == 26 and len({t.name for t in ALL_TABLES}) == 26
    assert set(SPECS) == {"Practitioner", "Organization", "Location", "Endpoint", "PractitionerRole",
                          "OrganizationAffiliation", "HealthcareService", "InsurancePlan"}
    assert TABLE_TYPES["identifier"] is None and TABLE_TYPES["practitioner_role_code"] == "PractitionerRole"


def test_columns_match_the_phase1_tables():
    golden = load_golden()
    for t in ALL_TABLES:
        assert columns(t) == golden[f"{t.name}#columns"], t.name


def test_flattened_fixture_equals_phase1_output():
    golden, got = load_golden(), flatten_fixture()
    for t in ALL_TABLES:
        rows = sorted(got.get(t.name, []), key=lambda r: [x or "" for x in r])
        assert rows == golden[t.name], t.name
