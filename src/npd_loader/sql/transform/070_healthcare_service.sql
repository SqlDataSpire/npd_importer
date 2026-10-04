INSERT INTO <<t:healthcare_service>> (
    release_date, resource_id, ndjson_file_id, zst_file_id, last_updated,
    active, name, provided_by_organization_id, network_organization_id)
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, r.last_updated,
       (r.resource->>'active')::boolean,
       r.resource->>'name',
       <<schema>>.ref_id(r.resource->'providedBy'->>'reference'),
       <<schema>>.ref_id(<<schema>>.ext(r.resource,
           'http://hl7.org/fhir/us/ndh/StructureDefinition/base-ext-network-reference')->'valueReference'->>'reference')
FROM <<raw>> r
WHERE r.resource_type = 'HealthcareService';

INSERT INTO <<t:healthcare_service_location>> (
    release_date, resource_id, ndjson_file_id, zst_file_id, seq, location_id)
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, x.ord, <<schema>>.ref_id(x.e->>'reference')
FROM <<raw>> r
CROSS JOIN LATERAL jsonb_array_elements(coalesce(r.resource->'location', '[]'::jsonb)) WITH ORDINALITY AS x(e, ord)
WHERE r.resource_type = 'HealthcareService';
