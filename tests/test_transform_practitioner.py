from datetime import datetime, timezone

import psycopg
import pytest

from npd_loader.storage import LocalStorage
from pg_helpers import R, rows, transformed

NPI = "http://terminology.hl7.org/NamingSystem/npi"
TAX = "http://hl7.org/fhir/us/ndh/ValueSet/HealthcareIndividualTaxonomyVS"
P1, P2 = "Practitioner-1003000100", "Practitioner-1083687529"


def utc(*args):
    return datetime(*args, tzinfo=timezone.utc)


@pytest.fixture
def result(npd_db, tmp_path):
    conn = psycopg.connect(npd_db)
    res = transformed(conn, LocalStorage(tmp_path / "data"))
    yield conn, res
    conn.close()


def test_standalone_tables_and_lineage(result):
    conn, res = result
    assert res.tables["practitioner"] == "practitioner__20260929__r7"
    assert res.counts["practitioner"] == 2
    lineage = conn.execute(
        "SELECT p.release_date, p.ndjson_file_id = r.ndjson_file_id, p.zst_file_id = r.zst_file_id "
        "FROM npd.practitioner__20260929__r7 p JOIN npd_raw.resource__20260929__r7 r "
        "ON r.resource_type = 'Practitioner' AND r.resource_id = p.resource_id").fetchall()
    assert lineage == [(R, True, True), (R, True, True)]
    # not yet visible through the parent
    assert conn.execute("SELECT count(*) FROM npd.practitioner").fetchone()[0] == 0


def test_practitioner(result):
    conn, res = result
    assert rows(conn, res, "practitioner",
                "resource_id, last_updated, npi, active, gender, name_family, name_given, name_prefix, name_suffix, "
                "identity_verified, medicare_enrolled, in_hhs_exclusion_list, aligned_with_data_network") == [
        (P1, utc(2026, 9, 29, 4, 34, 0, 724328), "1003000100", True, "male", "GOMEZ", "GERARDO", None, None,
         False, False, False, False),
        (P2, utc(2026, 9, 29, 4, 35), "1083687529", True, "female", "JONES", "ANNA MARIE", "DR.", "MD",
         True, True, False, True),
    ]


def test_practitioner_name(result):
    conn, res = result
    assert rows(conn, res, "practitioner_name",
                "resource_id, seq, use, family, given, prefix, suffix, period_start, period_end") == [
        (P1, 1, "official", "GOMEZ", "GERARDO", None, None, None, None),
        (P2, 1, "maiden", "SMITH", "ANNA", None, None, utc(1990, 1, 1), None),
        (P2, 2, "official", "JONES", "ANNA MARIE", "DR.", "MD", None, None),
    ]


def test_practitioner_address_and_telecom(result):
    conn, res = result
    assert rows(conn, res, "practitioner_address",
                "resource_id, seq, use, type, line1, line2, extra_lines, city, state, postal_code, country") == [
        (P1, 1, "work", "physical", "108 W Victoria St", None, None, "Gardena", "CA", "90248", "US"),
        (P1, 2, "billing", "postal", "680 S Wilton Pl", None, None, "Los Angeles", "CA", "90005", "US"),
    ]
    assert rows(conn, res, "practitioner_telecom", "resource_id, seq, system, use, value") == [
        (P1, 1, "fax", "work", "2133831280"), (P1, 2, "phone", "work", "2133657400"),
        (P1, 3, "phone", "work", "3107152020"), (P2, 1, "phone", "work", "2125551212"),
    ]


def test_practitioner_qualification(result):
    conn, res = result
    assert rows(conn, res, "practitioner_qualification",
                "resource_id, seq, code_system, code, code_display, code_text, identifier_value, "
                "identifier_type_code, issuer_organization_id") == [
        (P1, 1, TAX, "171M00000X", "Case Manager/Care Coordinator", "Case Manager/Care Coordinator",
         None, None, None),
        (P1, 2, TAX, "225400000X", "Rehabilitation Practitioner", "Rehabilitation Practitioner", None, None, None),
        (P2, 1, TAX, "207R00000X", "Internal Medicine Physician", "Internal Medicine Physician",
         "A12345", "MD", "Organization-NY-STATE-BOARD"),
    ]


def test_identifier_covers_all_types(result):
    conn, res = result
    got = conn.execute(
        "SELECT resource_type, resource_id, seq, system, value, use, type_code, type_text, period_start "
        "FROM npd.identifier__20260929__r7 ORDER BY resource_type, resource_id, seq").fetchall()
    assert got == [
        ("Organization", "Organization-1336200294", 1, NPI, "1336200294", "official", "PRN", "NPI", utc(2006, 12, 13)),
        ("Organization", "Organization-1336200294", 2, "https://npd.cms.gov/fhir/sid/us-pseudo-ein",
         "6e5d8b3e-13d3-48be-9ed2-881d08f2c459", "official", "TAX", None, None),
        ("Organization", "Organization-1902099112", 1, NPI, "1902099112", "official", "PRN", "NPI", None),
        ("Practitioner", P1, 1, NPI, "1003000100", "official", "PRN", "NPI", utc(2007, 8, 31)),
        ("Practitioner", P2, 1, NPI, "1083687529", "official", "PRN", "NPI", utc(2005, 5, 23)),
    ]


def test_indexes_cloned_onto_standalone(result):
    conn, _ = result
    defs = [r[0] for r in conn.execute(
        "SELECT indexdef FROM pg_indexes WHERE schemaname = 'npd' AND tablename = 'practitioner__20260929__r7'")]
    assert any("UNIQUE" in d and "(release_date, resource_id)" in d for d in defs)
    assert any("(release_date, npi)" in d for d in defs)
