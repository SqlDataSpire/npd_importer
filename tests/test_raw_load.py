import io
import json
from datetime import datetime, timezone

import pytest

from npd_loader.raw_load import NdjsonInput, RawLoadError, iter_lines
from npd_loader.storage import LocalStorage
from fixture_data import ORG1, PRAC1, PRAC2, ndjson_bytes
from helpers import pg_dialect
from pg_helpers import R, connect, load_fixture_raw


@pytest.fixture
def storage(tmp_path):
    return LocalStorage(tmp_path / "data")


def test_loads_every_type_with_lineage(npd_db, storage):
    result = load_fixture_raw(pg_dialect(npd_db), storage)
    assert result.table == "resource__20260929__r7"
    assert result.rows == {"Endpoint": 1, "HealthcareService": 1, "InsurancePlan": 1, "Location": 1,
                           "Organization": 2, "OrganizationAffiliation": 2, "Practitioner": 2,
                           "PractitionerRole": 2}
    with connect(npd_db) as conn:
        row = conn.execute(
            "SELECT release_date, resource_type, last_updated, ndjson_file_id, zst_file_id, line_number, resource "
            "FROM npd_raw.resource__20260929__r7 WHERE resource_id = %s", (PRAC2["id"],)).fetchone()
        assert row[:2] == (R, "Practitioner")
        assert row[2] == datetime(2026, 9, 29, 4, 35, tzinfo=timezone.utc)
        assert row[5] == 2
        assert row[6] == PRAC2
        assert row[4] == row[3] + 1
        # leaves are attached to the standalone release table, which is NOT yet attached to npd_raw.resource
        leaves = conn.execute("SELECT count(*) FROM pg_inherits i JOIN pg_class p ON p.oid = i.inhparent "
                              "WHERE p.relname = 'resource__20260929__r7'").fetchone()[0]
        assert leaves == 8
        assert conn.execute("SELECT count(*) FROM npd_raw.resource").fetchone()[0] == 0


def test_rows_land_in_batches(npd_db, storage, monkeypatch):
    import npd_loader.dialect.postgres as pg
    monkeypatch.setattr(pg, "BATCH_ROWS", 2)
    pracs = [{**PRAC1, "id": f"Practitioner-{i}", "meta": {}} for i in range(5)]
    result = load_fixture_raw(pg_dialect(npd_db), storage, {"06-Practitioner.ndjson": ndjson_bytes(pracs)})
    assert result.rows == {"Practitioner": 5}
    with connect(npd_db) as conn:
        got = conn.execute("SELECT line_number, resource_id, last_updated FROM npd_raw.resource__20260929__r7 "
                           "ORDER BY line_number").fetchall()
    assert got == [(i + 1, f"Practitioner-{i}", None) for i in range(5)]


def test_crlf_and_missing_final_newline(npd_db, storage):
    data = (json.dumps(PRAC1) + "\r\n" + json.dumps(PRAC2)).encode()
    result = load_fixture_raw(pg_dialect(npd_db), storage, {"06-Practitioner.ndjson": data})
    assert result.rows == {"Practitioner": 2}


def test_iter_lines_blank_lines():
    assert list(iter_lines(io.BytesIO(b'{"a":1}\n\n\n'))) == [(1, '{"a":1}')]
    with pytest.raises(RawLoadError, match="line 2: blank line"):
        list(iter_lines(io.BytesIO(b'{"a":1}\n\n{"b":2}\n')))


@pytest.mark.parametrize("lines,message", [
    ([json.dumps(PRAC1), "{not json"], "line 2: invalid JSON"),
    ([json.dumps(PRAC1), json.dumps(ORG1)], "line 2: resourceType 'Organization', expected 'Practitioner'"),
    ([json.dumps({**PRAC1, "id": ""})], "line 1: missing id"),
    ([json.dumps(PRAC1), json.dumps(PRAC1)], "duplicate"),
    ([json.dumps({**PRAC1, "gender": "x\u0000y"})], "line 1: contains \\\\u0000"),
    ([json.dumps({**PRAC1, "gender": "x\\\u0000y"})], "line 1: contains \\\\u0000"),  # backslash, then NUL
])
def test_strict_validation(npd_db, storage, lines, message):
    data = ("\n".join(lines) + "\n").encode()
    with pytest.raises(RawLoadError, match=message) as info:
        load_fixture_raw(pg_dialect(npd_db), storage, {"06-Practitioner.ndjson": data})
    assert info.value.file_id == 500  # the only input (write_inputs numbers files from 500)


def test_escaped_backslash_before_u0000_loads(npd_db, storage):
    prac = {**PRAC1, "gender": "x\\u0000y"}  # a literal backslash followed by the text "u0000", no NUL
    assert r"x\\u0000y" in json.dumps(prac)
    result = load_fixture_raw(pg_dialect(npd_db), storage, {"06-Practitioner.ndjson": ndjson_bytes([prac])})
    assert result.rows == {"Practitioner": 1}
    with connect(npd_db) as conn:
        gender = conn.execute("SELECT resource->>'gender' FROM npd_raw.resource__20260929__r7").fetchone()[0]
    assert gender == "x\\u0000y"


def test_duplicate_error_names_the_id(npd_db, storage):
    ndjson = {"03-Organization.ndjson": ndjson_bytes([ORG1]),
              "06-Practitioner.ndjson": ndjson_bytes([PRAC1, PRAC2, PRAC1])}
    with pytest.raises(RawLoadError, match="Practitioner-1003000100 .*lines \\[1, 3\\]") as info:
        load_fixture_raw(pg_dialect(npd_db), storage, ndjson)
    assert info.value.file_id == 502  # the Practitioner file (Organization is 500), so data_file.exceptions is set


def test_unknown_resource_type_loads_raw(npd_db, storage):
    med = {"resourceType": "Medication", "id": "Medication-1", "meta": {"lastUpdated": "2026-09-29T00:00:00Z"}}
    ndjson = {"06-Practitioner.ndjson": ndjson_bytes([PRAC1]), "09-Medication.ndjson": ndjson_bytes([med])}
    result = load_fixture_raw(pg_dialect(npd_db), storage, ndjson)
    assert result.rows == {"Medication": 1, "Practitioner": 1}
    with connect(npd_db) as conn:
        assert conn.execute("SELECT resource_id FROM npd_raw.resource__20260929__r7 "
                            "WHERE resource_type = 'Medication'").fetchone()[0] == "Medication-1"


def test_unreadable_file_reports_raw_load_error(npd_db, storage):
    inp = NdjsonInput(file_id=900, zst_file_id=901, rel_path="run_1/missing-06-Practitioner.ndjson",
                      resource_type="Practitioner", name="06-Practitioner.ndjson")
    with pytest.raises(RawLoadError, match="06-Practitioner.ndjson: cannot read") as info:
        pg_dialect(npd_db).load_raw(storage, R, 7, [inp])
    assert info.value.file_id == 900
