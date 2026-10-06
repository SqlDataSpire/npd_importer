from datetime import datetime, timezone

import pytest

from npd_loader.storage import LocalStorage
from helpers import pg_dialect
from pg_helpers import open_conn, rows, transformed

PR1 = "PractitionerRole-00000990-37aa-428a-a1fd-d91bed7c789d"
PR2 = "PractitionerRole-0f00aa11"
HS1 = "HealthcareService-cd52e7a4-79df-47ef-aa81-a89142e37ffe"
IP1 = "InsurancePlan-0be2a43c-0c13-41fd-b97f-7f43394ec1fd"
OA1 = "OrganizationAffiliation-00111700-8fc3-4ea1-a966-4c3d59b41921"
OA2 = "OrganizationAffiliation-oa00002"


def utc(*args):
    return datetime(*args, tzinfo=timezone.utc)


@pytest.fixture
def result(npd_db, tmp_path):
    res = transformed(pg_dialect(npd_db), LocalStorage(tmp_path / "data"))
    conn = open_conn(npd_db)
    yield conn, res
    conn.close()


def test_practitioner_role(result):
    conn, res = result
    assert rows(conn, res, "practitioner_role",
                "resource_id, active, practitioner_id, organization_id, period_start, period_end, "
                "network_organization_id, accepting_patients") == [
        (PR1, True, "Practitioner-1083687529", None, None, None, None, None),
        (PR2, True, "Practitioner-1003000100", "Organization-1902099112", utc(2020, 1, 1), None,
         "Organization-ea579d05-454e-4359-8751-900c940a599a", "newpt"),
    ]
    assert rows(conn, res, "practitioner_role_endpoint", "resource_id, seq, endpoint_id") == [
        (PR1, 1, "Endpoint-00000990-37aa-428a-a1fd-d91bed7c789d")]
    assert rows(conn, res, "practitioner_role_location", "resource_id, seq, location_id") == [
        (PR2, 1, "Location-00027861-c380-4866-b677-4c28e4ceaf6b")]
    assert rows(conn, res, "practitioner_role_specialty", "resource_id, seq, system, code, display, text") == [
        (PR2, 1, "http://nucc.org/provider-taxonomy", "207R00000X", "Internal Medicine Physician",
         "Internal Medicine")]
    assert rows(conn, res, "practitioner_role_code", "resource_id, seq, system, code, display, text") == [
        (PR2, 1, "http://hl7.org/fhir/us/ndh/CodeSystem/IndividualAndGroupSpecialtiesCS", "ph", "Physician", None)]
    assert rows(conn, res, "practitioner_role_telecom", "resource_id, seq, system, use, value") == [
        (PR2, 1, "phone", "work", "5551234567")]


def test_organization_affiliation(result):
    conn, res = result
    assert rows(conn, res, "organization_affiliation",
                "resource_id, active, organization_id, participating_organization_id, role_code, role_display, "
                "role_text, period_start, period_end") == [
        (OA1, True, "Organization-c618f893-235a-48ae-bbaa-1d60d5ac7ee9", "Organization-1407192586", "bt",
         "Member Of", "Member Of", None, None),
        (OA2, True, "Organization-1336200294", "Organization-1902099112", None, None, None, None, None)]
    assert rows(conn, res, "organization_affiliation_network", "resource_id, seq, network_organization_id") == [
        (OA2, 1, "Organization-c7d4aa30-a4c0-4733-aa2c-c8086e29159b")]


def test_healthcare_service(result):
    conn, res = result
    assert rows(conn, res, "healthcare_service",
                "resource_id, active, name, provided_by_organization_id, network_organization_id") == [
        (HS1, True, None, "Organization-1295596195", "Organization-ea579d05-454e-4359-8751-900c940a599a")]
    assert rows(conn, res, "healthcare_service_location", "resource_id, seq, location_id") == [
        (HS1, 1, "Location-a1ab5e31-a038-4ec0-9439-ee9613d63e11")]


def test_insurance_plan(result):
    conn, res = result
    assert rows(conn, res, "insurance_plan",
                "resource_id, status, name, type_code, type_text, period_start, period_end, "
                "owned_by_organization_id, administered_by_organization_id") == [
        (IP1, "active", "DEVOTED CHOICE GIVEBACK 002 SC (PPO)", None, "Medicare Advantage PPO Plan",
         utc(2026, 1, 1), utc(2026, 12, 31), "Organization-242574ec-a550-43f6-80ae-592aa53c17c8", None)]
    assert rows(conn, res, "insurance_plan_alias", "resource_id, seq, alias") == [
        (IP1, 1, "H7028-002-000"), (IP1, 2, "H7028")]
    assert rows(conn, res, "insurance_plan_network", "resource_id, seq, network_organization_id") == [
        (IP1, 1, "Organization-c7d4aa30-a4c0-4733-aa2c-c8086e29159b")]


def test_every_table_has_rows(result):
    _, res = result
    assert [t for t, n in res.counts.items() if n == 0] == []
