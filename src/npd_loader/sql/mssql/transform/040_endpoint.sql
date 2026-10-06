INSERT INTO <<t:endpoint>> WITH (TABLOCK) (
    release_date, resource_id, ndjson_file_id, zst_file_id, last_updated,
    status, name, address, connection_type_system, connection_type_code,
    payload_type_system, payload_type_code, managing_organization_id, verification_status)
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, r.last_updated,
       JSON_VALUE(r.resource, '$.status'), JSON_VALUE(r.resource, '$.name'), JSON_VALUE(r.resource, '$.address'),
       JSON_VALUE(r.resource, '$.connectionType.system'), JSON_VALUE(r.resource, '$.connectionType.code'),
       JSON_VALUE(r.resource, '$.payloadType[0].coding[0].system'),
       JSON_VALUE(r.resource, '$.payloadType[0].coding[0].code'),
       <<schema>>.ref_id(JSON_VALUE(r.resource, '$.managingOrganization.reference')),
       JSON_VALUE(vs.ext, '$.valueCodeableConcept.coding[0].code')
FROM <<raw>> r
OUTER APPLY <<schema>>.ext(r.resource, N'http://hl7.org/fhir/us/ndh/StructureDefinition/base-ext-verification-status') vs
WHERE r.resource_type = 'Endpoint'
