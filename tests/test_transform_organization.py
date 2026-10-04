from datetime import datetime, timezone

import psycopg
import pytest

from npd_loader.storage import LocalStorage
from pg_helpers import rows, transformed

O1, O2 = "Organization-1336200294", "Organization-1902099112"
L1 = "Location-00027861-c380-4866-b677-4c28e4ceaf6b"
E1 = "Endpoint-000f410c-e1e9-4a78-a988-b6ce47d6a793"


@pytest.fixture
def result(npd_db, tmp_path):
    conn = psycopg.connect(npd_db)
    res = transformed(conn, LocalStorage(tmp_path / "data"))
    yield conn, res
    conn.close()


def test_organization(result):
    conn, res = result
    assert rows(conn, res, "organization",
                "resource_id, last_updated, npi, pseudo_ein, name, active, type_code, type_display, "
                "part_of_organization_id, verification_status") == [
        (O1, datetime(2026, 9, 29, 4, 29, 5, 411440, tzinfo=timezone.utc), "1336200294",
         "6e5d8b3e-13d3-48be-9ed2-881d08f2c459", "NEW MEXICO STATE UNIVERSITY STUDENT HEALTH CENTER", True,
         "prov", "Healthcare Provider", None, "complete"),
        (O2, datetime(2026, 9, 29, 4, 30, tzinfo=timezone.utc), "1902099112", None, "EASTBLUFF MEDICAL GROUP", True,
         "prov", "Healthcare Provider", O1, "complete"),
    ]


def test_organization_children(result):
    conn, res = result
    assert rows(conn, res, "organization_address",
                "resource_id, seq, use, type, line1, line2, extra_lines, city, state, postal_code, country") == [
        (O2, 1, "work", "physical", "2515 Eastbluff Dr", "Suite 100", "Building B", "Newport Beach", "CA",
         "92660", "US"),
    ]
    assert rows(conn, res, "organization_telecom", "resource_id, seq, system, use, value") == [
        (O1, 1, "fax", "work", "5056462692"), (O1, 2, "phone", "work", "5056461512"),
        (O2, 1, "phone", "work", "9496405050"),
    ]
    assert rows(conn, res, "organization_endpoint", "resource_id, seq, endpoint_id") == [(O2, 1, E1)]


def test_location(result):
    conn, res = result
    assert rows(conn, res, "location",
                "resource_id, status, name, description, mode, address_use, address_type, line1, line2, "
                "extra_lines, city, state, postal_code, country, latitude, longitude, managing_organization_id") == [
        (L1, "active", "2515 Eastbluff Dr", "2515 Eastbluff Dr", "instance", None, "physical", "2515 Eastbluff Dr",
         None, None, "Newport Beach", "CA", "92660", "US", 33.6399, -117.87532, O2),
    ]
    assert rows(conn, res, "location_telecom", "resource_id, seq, system, use, value") == [
        (L1, 1, "fax", "work", "9496405051"), (L1, 2, "phone", "work", "9496405050"),
    ]


def test_endpoint(result):
    conn, res = result
    assert rows(conn, res, "endpoint",
                "resource_id, status, name, address, connection_type_system, connection_type_code, "
                "payload_type_system, payload_type_code, managing_organization_id, verification_status") == [
        (E1, "active", "Direct Messaging Address", "heather.bruneau.1@29651.direct.athenahealth.com",
         "http://terminology.hl7.org/CodeSystem/endpoint-connection-type", "direct-project",
         "http://terminology.hl7.org/CodeSystem/data-absent-reason", "not-applicable", None, "complete"),
    ]
