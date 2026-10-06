INSERT INTO <<t:healthcare_service>> WITH (TABLOCK) (
    release_date, resource_id, ndjson_file_id, zst_file_id, last_updated,
    active, name, provided_by_organization_id, network_organization_id)
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, r.last_updated,
       CAST(JSON_VALUE(r.resource, '$.active') AS bit),
       JSON_VALUE(r.resource, '$.name'),
       <<schema>>.ref_id(JSON_VALUE(r.resource, '$.providedBy.reference')),
       <<schema>>.ref_id(JSON_VALUE(net.ext, '$.valueReference.reference'))
FROM <<raw>> r
OUTER APPLY <<schema>>.ext(r.resource, N'http://hl7.org/fhir/us/ndh/StructureDefinition/base-ext-network-reference') net
WHERE r.resource_type = 'HealthcareService'
GO
INSERT INTO <<t:healthcare_service_location>> WITH (TABLOCK) (
    release_date, resource_id, ndjson_file_id, zst_file_id, seq, location_id)
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, CAST(x.[key] AS int) + 1,
       <<schema>>.ref_id(JSON_VALUE(x.value, '$.reference'))
FROM <<raw>> r
CROSS APPLY OPENJSON(r.resource, '$.location') x
WHERE r.resource_type = 'HealthcareService'
