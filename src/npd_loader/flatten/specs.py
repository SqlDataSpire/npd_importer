"""The 26 npd tables as declarative specs (the Phase 1 T-SQL transforms, one for one)."""
from __future__ import annotations

import re

from npd_loader.flatten.convert import boolean, join, number, ref, ts
from npd_loader.flatten.engine import E, R, Table, ext, identifier, official_name, path

NDH = "http://hl7.org/fhir/us/ndh/StructureDefinition/"
NPI = ("http://terminology.hl7.org/NamingSystem/npi", "http://hl7.org/fhir/sid/us-npi")
PSEUDO_EIN = ("https://npd.cms.gov/fhir/sid/us-pseudo-ein",)


def _name(field: str, conv=None):
    get = lambda r: (official_name(r) or {}).get(field)
    return R(get, conv)


def _code(getter):
    return lambda obj: path("valueCodeableConcept.coding[0].code")(getter(obj))


def _long_text_to_null(v):
    return None if isinstance(v, str) and len(v) > 4000 else v


def _instant(v):
    """meta.lastUpdated: Phase 1 loaded it into the raw datetime2(3) column from a Python datetime (the driver
    truncates microseconds), unlike fhir_ts which rounds; the golden values keep that truncation."""
    return ts(re.sub(r"(\.\d{3})\d+", lambda m: m.group(1), v)) if isinstance(v, str) else ts(v)


LAST_UPDATED = {"last_updated": R("meta.lastUpdated", _instant)}
TELECOM = {"system": E("system"), "use": E("use"), "value": E("value")}
ADDRESS = {"use": E("use"), "type": E("type"), "line1": E("line[0]"), "line2": E("line[1]"),
           "extra_lines": E(lambda e: join(e.get("line") if isinstance(e, dict) else None, ", ", 2)),
           "city": E("city"), "state": E("state"), "postal_code": E("postalCode"), "country": E("country")}
REF = {"reference": E("reference", ref)}
CODING = {"system": E("coding[0].system"), "code": E("coding[0].code"), "display": E("coding[0].display"),
          "text": E("text")}
VERIFICATION = R(_code(ext(NDH + "base-ext-verification-status")))
NETWORK_REF = R(lambda r: ref(path("valueReference.reference")(ext(NDH + "base-ext-network-reference")(r))))


def _ref_table(name: str, each: str, column: str) -> Table:
    return Table(name, {column: E("reference", ref)}, each=each)


SPECS: dict[str, list[Table]] = {
    "Practitioner": [
        Table("practitioner", {
            **LAST_UPDATED,
            "npi": R(identifier(NPI)),
            "active": R("active", boolean),
            "gender": R("gender"),
            "name_family": _name("family"),
            "name_given": _name("given", join),
            "name_prefix": _name("prefix", join),
            "name_suffix": _name("suffix", join),
            "identity_verified": R(lambda r: path("valueBoolean")(ext(NDH + "base-ext-cms-identity-verified")(r)), boolean),
            "medicare_enrolled": R(lambda r: path("valueBoolean")(ext(NDH + "base-ext-cms_medicare_enrollment")(r)), boolean),
            "in_hhs_exclusion_list": R(lambda r: path("valueBoolean")(ext(NDH + "base-ext-hhs-in-exclusion-list")(r)), boolean),
            "aligned_with_data_network": R(lambda r: path("valueBoolean")(ext(NDH + "base-ext-cms_aligned_with_data_network")(r)), boolean),
        }),
        Table("practitioner_name", {
            "use": E("use"), "family": E("family"), "given": E("given", join), "prefix": E("prefix", join),
            "suffix": E("suffix", join), "period_start": E("period.start", ts), "period_end": E("period.end", ts),
        }, each="name"),
        Table("practitioner_address", ADDRESS, each="address"),
        Table("practitioner_telecom", TELECOM, each="telecom"),
        Table("practitioner_qualification", {
            "code_system": E("code.coding[0].system"), "code": E("code.coding[0].code"),
            "code_display": E("code.coding[0].display"), "code_text": E("code.text"),
            "identifier_value": E("identifier[0].value"),
            "identifier_type_code": E("identifier[0].type.coding[0].code"),
            "issuer_organization_id": E("issuer.reference", ref),
        }, each="qualification"),
    ],
    "Organization": [
        Table("organization", {
            **LAST_UPDATED,
            "npi": R(identifier(NPI)), "pseudo_ein": R(identifier(PSEUDO_EIN)),
            "name": R("name"), "active": R("active", boolean),
            "type_code": R("type[0].coding[0].code"), "type_display": R("type[0].coding[0].display"),
            "part_of_organization_id": R("partOf.reference", ref),
            "verification_status": VERIFICATION,
        }),
        Table("organization_address", ADDRESS, each="address"),
        Table("organization_telecom", TELECOM, each="telecom"),
        _ref_table("organization_endpoint", "endpoint", "endpoint_id"),
    ],
    "Location": [
        Table("location", {
            **LAST_UPDATED,
            "status": R("status"), "name": R("name"), "description": R("description", _long_text_to_null),
            "mode": R("mode"),
            "address_use": R("address.use"), "address_type": R("address.type"),
            "line1": R("address.line[0]"), "line2": R("address.line[1]"),
            "extra_lines": R(lambda r: join(path("address.line")(r), ", ", 2)),
            "city": R("address.city"), "state": R("address.state"),
            "postal_code": R("address.postalCode"), "country": R("address.country"),
            "latitude": R("position.latitude", number), "longitude": R("position.longitude", number),
            "managing_organization_id": R("managingOrganization.reference", ref),
        }),
        Table("location_telecom", TELECOM, each="telecom"),
    ],
    "Endpoint": [
        Table("endpoint", {
            **LAST_UPDATED,
            "status": R("status"), "name": R("name"), "address": R("address"),
            "connection_type_system": R("connectionType.system"), "connection_type_code": R("connectionType.code"),
            "payload_type_system": R("payloadType[0].coding[0].system"),
            "payload_type_code": R("payloadType[0].coding[0].code"),
            "managing_organization_id": R("managingOrganization.reference", ref),
            "verification_status": VERIFICATION,
        }),
    ],
    "PractitionerRole": [
        Table("practitioner_role", {
            **LAST_UPDATED,
            "active": R("active", boolean),
            "practitioner_id": R("practitioner.reference", ref),
            "organization_id": R("organization.reference", ref),
            "period_start": R("period.start", ts), "period_end": R("period.end", ts),
            "network_organization_id": NETWORK_REF,
            "accepting_patients": R(_code(ext("acceptingPatients", inner=ext(NDH + "base-ext-newpatients")))),
        }),
        _ref_table("practitioner_role_endpoint", "endpoint", "endpoint_id"),
        _ref_table("practitioner_role_location", "location", "location_id"),
        Table("practitioner_role_specialty", CODING, each="specialty"),
        Table("practitioner_role_code", CODING, each="code"),
        Table("practitioner_role_telecom", TELECOM, each="telecom"),
    ],
    "OrganizationAffiliation": [
        Table("organization_affiliation", {
            **LAST_UPDATED,
            "active": R("active", boolean),
            "organization_id": R("organization.reference", ref),
            "participating_organization_id": R("participatingOrganization.reference", ref),
            "role_code": R("code[0].coding[0].code"), "role_display": R("code[0].coding[0].display"),
            "role_text": R("code[0].text"),
            "period_start": R("period.start", ts), "period_end": R("period.end", ts),
        }),
        _ref_table("organization_affiliation_network", "network", "network_organization_id"),
    ],
    "HealthcareService": [
        Table("healthcare_service", {
            **LAST_UPDATED,
            "active": R("active", boolean), "name": R("name"),
            "provided_by_organization_id": R("providedBy.reference", ref),
            "network_organization_id": NETWORK_REF,
        }),
        _ref_table("healthcare_service_location", "location", "location_id"),
    ],
    "InsurancePlan": [
        Table("insurance_plan", {
            **LAST_UPDATED,
            "status": R("status"), "name": R("name"),
            "type_code": R("type[0].coding[0].code"), "type_text": R("type[0].text"),
            "period_start": R("period.start", ts), "period_end": R("period.end", ts),
            "owned_by_organization_id": R("ownedBy.reference", ref),
            "administered_by_organization_id": R("administeredBy.reference", ref),
        }),
        Table("insurance_plan_alias", {"alias": E(lambda e: e if isinstance(e, str) else None)}, each="alias"),
        _ref_table("insurance_plan_network", "network", "network_organization_id"),
    ],
}

IDENTIFIER = Table("identifier", {
    "system": E("system"), "value": E("value"), "use": E("use"),
    "type_code": E("type.coding[0].code"), "type_text": E("type.text"),
    "period_start": E("period.start", ts), "period_end": E("period.end", ts),
}, each="identifier", with_type=True)

ALL_TABLES: list[Table] = [t for tables in SPECS.values() for t in tables] + [IDENTIFIER]
TABLE_TYPES: dict[str, str | None] = {t.name: rt for rt, tables in SPECS.items() for t in tables} | {"identifier": None}


def tables_for(resource_type: str) -> list[Table]:
    return SPECS.get(resource_type, []) + [IDENTIFIER]
