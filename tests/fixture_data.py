"""FHIR sample records. Real ones are verbatim first records of release 2026-09-29; synthetic ones fill gaps."""
import json

NPI = "http://terminology.hl7.org/NamingSystem/npi"
V2 = "http://terminology.hl7.org/CodeSystem/v2-0203"
NDH = "http://hl7.org/fhir/us/ndh/StructureDefinition/"
VERIFIED = {"url": NDH + "base-ext-verification-status",
            "valueCodeableConcept": {"coding": [{"system": "http://hl7.org/fhir/us/ndh/CodeSystem/NdhVerificationStatusCS",
                                                 "code": "complete", "display": "Complete"}]}}
NPI_TYPE = {"coding": [{"system": V2, "code": "PRN", "display": "Provider number"}], "text": "NPI"}

ORG1 = {"resourceType": "Organization", "id": "Organization-1336200294",
        "meta": {"lastUpdated": "2026-09-29T04:29:05.411440Z"}, "extension": [VERIFIED],
        "identifier": [{"type": NPI_TYPE, "system": NPI, "use": "official", "value": "1336200294",
                        "period": {"start": "2006-12-13T00:00:00Z"}},
                       {"type": {"coding": [{"system": V2, "code": "TAX", "display": "Tax ID number"}]},
                        "system": "https://npd.cms.gov/fhir/sid/us-pseudo-ein", "use": "official",
                        "value": "6e5d8b3e-13d3-48be-9ed2-881d08f2c459"}],
        "active": True,
        "type": [{"coding": [{"system": "http://terminology.hl7.org/CodeSystem/organization-type", "code": "prov",
                              "display": "Healthcare Provider"}], "text": "Healthcare Provider"}],
        "name": "NEW MEXICO STATE UNIVERSITY STUDENT HEALTH CENTER",
        "telecom": [{"system": "fax", "value": "5056462692", "use": "work"},
                    {"system": "phone", "value": "5056461512", "use": "work"}]}

ORG2 = {"resourceType": "Organization", "id": "Organization-1902099112",
        "meta": {"lastUpdated": "2026-09-29T04:30:00Z"}, "extension": [VERIFIED],
        "identifier": [{"type": NPI_TYPE, "system": NPI, "use": "official", "value": "1902099112"}],
        "active": True,
        "type": [{"coding": [{"system": "http://terminology.hl7.org/CodeSystem/organization-type", "code": "prov",
                              "display": "Healthcare Provider"}], "text": "Healthcare Provider"}],
        "name": "EASTBLUFF MEDICAL GROUP",
        "telecom": [{"system": "phone", "value": "9496405050", "use": "work"}],
        "address": [{"line": ["2515 Eastbluff Dr", "Suite 100", "Building B"], "city": "Newport Beach",
                     "state": "CA", "postalCode": "92660", "country": "US", "type": "physical", "use": "work"}],
        "partOf": {"reference": "Organization/Organization-1336200294"},
        "endpoint": [{"reference": "Endpoint/Endpoint-000f410c-e1e9-4a78-a988-b6ce47d6a793"}]}

LOC1 = {"resourceType": "Location", "id": "Location-00027861-c380-4866-b677-4c28e4ceaf6b",
        "meta": {"lastUpdated": "2026-09-29T04:29:55.568276Z"}, "status": "active", "name": "2515 Eastbluff Dr",
        "description": "2515 Eastbluff Dr", "mode": "instance",
        "telecom": [{"system": "fax", "value": "9496405051", "use": "work"},
                    {"system": "phone", "value": "9496405050", "use": "work"}],
        "address": {"line": ["2515 Eastbluff Dr"], "city": "Newport Beach", "state": "CA", "postalCode": "92660",
                    "country": "US", "type": "physical"},
        "position": {"longitude": -117.87532, "latitude": 33.6399},
        "managingOrganization": {"reference": "Organization/Organization-1902099112"}}

ENDP1 = {"resourceType": "Endpoint", "id": "Endpoint-000f410c-e1e9-4a78-a988-b6ce47d6a793",
         "meta": {"lastUpdated": "2026-09-29T04:10:10.922997Z"}, "extension": [VERIFIED], "status": "active",
         "connectionType": {"system": "http://terminology.hl7.org/CodeSystem/endpoint-connection-type",
                            "code": "direct-project", "display": "Direct Project"},
         "name": "Direct Messaging Address", "address": "heather.bruneau.1@29651.direct.athenahealth.com",
         "payloadType": [{"coding": [{"system": "http://terminology.hl7.org/CodeSystem/data-absent-reason",
                                      "version": "1.0.0", "code": "not-applicable"}]}]}

HCS1 = {"resourceType": "HealthcareService", "id": "HealthcareService-cd52e7a4-79df-47ef-aa81-a89142e37ffe",
        "meta": {"lastUpdated": "2026-09-29T04:29:07.620230Z"}, "active": True,
        "providedBy": {"reference": "Organization/Organization-1295596195"},
        "location": [{"reference": "Location/Location-a1ab5e31-a038-4ec0-9439-ee9613d63e11"}],
        "extension": [{"url": NDH + "base-ext-network-reference",
                       "valueReference": {"reference": "Organization/Organization-ea579d05-454e-4359-8751-900c940a599a"}}]}

IP1 = {"resourceType": "InsurancePlan", "id": "InsurancePlan-0be2a43c-0c13-41fd-b97f-7f43394ec1fd",
       "meta": {"lastUpdated": "2026-09-29T04:03:43.071628Z"}, "status": "active",
       "name": "DEVOTED CHOICE GIVEBACK 002 SC (PPO)", "type": [{"text": "Medicare Advantage PPO Plan"}],
       "alias": ["H7028-002-000", "H7028"],
       "period": {"start": "2026-01-01T00:00:00Z", "end": "2026-12-31T00:00:00Z"},
       "ownedBy": {"reference": "Organization/Organization-242574ec-a550-43f6-80ae-592aa53c17c8"},
       "network": [{"reference": "Organization/Organization-c7d4aa30-a4c0-4733-aa2c-c8086e29159b"}]}

TAXONOMY = "http://hl7.org/fhir/us/ndh/ValueSet/HealthcareIndividualTaxonomyVS"

PRAC1 = {"resourceType": "Practitioner", "id": "Practitioner-1003000100",
         "meta": {"lastUpdated": "2026-09-29T04:34:00.724328Z"},
         "extension": [{"url": NDH + "base-ext-cms-identity-verified", "valueBoolean": False},
                       {"url": NDH + "base-ext-cms_medicare_enrollment", "valueBoolean": False},
                       {"url": NDH + "base-ext-hhs-in-exclusion-list", "valueBoolean": False},
                       {"url": NDH + "base-ext-cms_aligned_with_data_network", "valueBoolean": False}],
         "identifier": [{"type": NPI_TYPE, "system": NPI, "use": "official", "value": "1003000100",
                         "period": {"start": "2007-08-31T00:00:00Z"}}],
         "active": True, "name": [{"given": ["GERARDO"], "family": "GOMEZ", "use": "official"}],
         "telecom": [{"system": "fax", "value": "2133831280", "use": "work"},
                     {"system": "phone", "value": "2133657400", "use": "work"},
                     {"system": "phone", "value": "3107152020", "use": "work"}],
         "address": [{"line": ["108 W Victoria St"], "city": "Gardena", "state": "CA", "postalCode": "90248",
                      "country": "US", "type": "physical", "use": "work"},
                     {"line": ["680 S Wilton Pl"], "city": "Los Angeles", "state": "CA", "postalCode": "90005",
                      "country": "US", "type": "postal", "use": "billing"}],
         "gender": "male",
         "qualification": [{"code": {"coding": [{"system": TAXONOMY, "code": "171M00000X",
                                                 "display": "Case Manager/Care Coordinator"}],
                                     "text": "Case Manager/Care Coordinator"}},
                           {"code": {"coding": [{"system": TAXONOMY, "code": "225400000X",
                                                 "display": "Rehabilitation Practitioner"}],
                                     "text": "Rehabilitation Practitioner"}}]}

PRAC2 = {"resourceType": "Practitioner", "id": "Practitioner-1083687529",
         "meta": {"lastUpdated": "2026-09-29T04:35:00Z"},
         "extension": [{"url": NDH + "base-ext-cms-identity-verified", "valueBoolean": True},
                       {"url": NDH + "base-ext-cms_medicare_enrollment", "valueBoolean": True},
                       {"url": NDH + "base-ext-hhs-in-exclusion-list", "valueBoolean": False},
                       {"url": NDH + "base-ext-cms_aligned_with_data_network", "valueBoolean": True}],
         "identifier": [{"type": NPI_TYPE, "system": NPI, "use": "official", "value": "1083687529",
                         "period": {"start": "2005-05-23T00:00:00Z"}}],
         "active": True,
         "name": [{"family": "SMITH", "given": ["ANNA"], "use": "maiden", "period": {"start": "1990-01-01T00:00:00Z"}},
                  {"family": "JONES", "given": ["ANNA", "MARIE"], "prefix": ["DR."], "suffix": ["MD"],
                   "use": "official"}],
         "telecom": [{"system": "phone", "value": "2125551212", "use": "work"}],
         "gender": "female",
         "qualification": [{"identifier": [{"type": {"coding": [{"system": V2, "code": "MD",
                                                                 "display": "Medical License number"}]},
                                            "use": "official", "value": "A12345"}],
                            "code": {"coding": [{"system": TAXONOMY, "code": "207R00000X",
                                                 "display": "Internal Medicine Physician"}],
                                     "text": "Internal Medicine Physician"},
                            "issuer": {"reference": "Organization/Organization-NY-STATE-BOARD"}}]}

PR1 = {"resourceType": "PractitionerRole", "id": "PractitionerRole-00000990-37aa-428a-a1fd-d91bed7c789d",
       "meta": {"lastUpdated": "2026-09-29T04:29:22.708739Z"}, "active": True,
       "practitioner": {"reference": "Practitioner/Practitioner-1083687529"},
       "endpoint": [{"reference": "Endpoint/Endpoint-00000990-37aa-428a-a1fd-d91bed7c789d"}]}

PR2 = {"resourceType": "PractitionerRole", "id": "PractitionerRole-0f00aa11",
       "meta": {"lastUpdated": "2026-09-29T04:40:00Z"}, "active": True,
       "practitioner": {"reference": "Practitioner/Practitioner-1003000100"},
       "organization": {"reference": "Organization/Organization-1902099112"},
       "location": [{"reference": "Location/Location-00027861-c380-4866-b677-4c28e4ceaf6b"}],
       "code": [{"coding": [{"system": "http://hl7.org/fhir/us/ndh/CodeSystem/IndividualAndGroupSpecialtiesCS",
                             "code": "ph", "display": "Physician"}]}],
       "specialty": [{"coding": [{"system": "http://nucc.org/provider-taxonomy", "code": "207R00000X",
                                  "display": "Internal Medicine Physician"}], "text": "Internal Medicine"}],
       "period": {"start": "2020-01-01T00:00:00Z"},
       "telecom": [{"system": "phone", "value": "5551234567", "use": "work"}],
       "extension": [{"url": NDH + "base-ext-network-reference",
                      "valueReference": {"reference":
                                         "Organization/Organization-ea579d05-454e-4359-8751-900c940a599a"}},
                    {"url": NDH + "base-ext-newpatients",
                     "extension": [{"url": "acceptingPatients",
                                    "valueCodeableConcept": {
                                        "coding": [{"system": "http://hl7.org/fhir/us/ndh/CodeSystem/AcceptingPatientsCS",
                                                   "code": "newpt", "display": "Accepting"}]}}]}]}

OA1 = {"resourceType": "OrganizationAffiliation", "id": "OrganizationAffiliation-00111700-8fc3-4ea1-a966-4c3d59b41921",
       "meta": {"lastUpdated": "2026-09-29T04:11:32.072722Z"}, "active": True,
       "organization": {"reference": "Organization/Organization-c618f893-235a-48ae-bbaa-1d60d5ac7ee9"},
       "participatingOrganization": {"reference": "Organization/Organization-1407192586"},
       "code": [{"coding": [{"system": "http://terminology.hl7.org/CodeSystem/organization-affiliation-role",
                             "code": "bt", "display": "Member Of"}], "text": "Member Of"}]}

OA2 = {"resourceType": "OrganizationAffiliation", "id": "OrganizationAffiliation-oa00002",
       "meta": {"lastUpdated": "2026-09-29T04:12:00Z"}, "active": True,
       "organization": {"reference": "Organization/Organization-1336200294"},
       "participatingOrganization": {"reference": "Organization/Organization-1902099112"},
       "network": [{"reference": "Organization/Organization-c7d4aa30-a4c0-4733-aa2c-c8086e29159b"}]}

RECORDS: dict[str, list[dict]] = {
    "01-Organization.ndjson": [ORG1, ORG2],
    "02-Location.ndjson": [LOC1],
    "03-Endpoint.ndjson": [ENDP1],
    "04-HealthcareService.ndjson": [HCS1],
    "05-InsurancePlan.ndjson": [IP1],
    "06-Practitioner.ndjson": [PRAC1, PRAC2],
    "07-PractitionerRole.ndjson": [PR1, PR2],
    "08-OrganizationAffiliation.ndjson": [OA1, OA2],
}


def ndjson_bytes(records: list[dict]) -> bytes:
    return b"".join(json.dumps(r).encode() + b"\n" for r in records)


def all_records() -> list[dict]:
    return [r for recs in RECORDS.values() for r in recs]
