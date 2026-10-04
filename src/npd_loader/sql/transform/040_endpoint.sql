INSERT INTO <<t:endpoint>> (
    release_date, resource_id, ndjson_file_id, zst_file_id, last_updated,
    status, name, address, connection_type_system, connection_type_code,
    payload_type_system, payload_type_code, managing_organization_id, verification_status)
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, r.last_updated,
       r.resource->>'status', r.resource->>'name', r.resource->>'address',
       r.resource->'connectionType'->>'system', r.resource->'connectionType'->>'code',
       r.resource->'payloadType'->0->'coding'->0->>'system',
       r.resource->'payloadType'->0->'coding'->0->>'code',
       <<schema>>.ref_id(r.resource->'managingOrganization'->>'reference'),
       <<schema>>.ext(r.resource, 'http://hl7.org/fhir/us/ndh/StructureDefinition/base-ext-verification-status')
           ->'valueCodeableConcept'->'coding'->0->>'code'
FROM <<raw>> r
WHERE r.resource_type = 'Endpoint';
