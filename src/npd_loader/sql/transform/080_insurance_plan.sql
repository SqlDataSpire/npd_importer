INSERT INTO <<t:insurance_plan>> (
    release_date, resource_id, ndjson_file_id, zst_file_id, last_updated,
    status, name, type_code, type_text, period_start, period_end,
    owned_by_organization_id, administered_by_organization_id)
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, r.last_updated,
       r.resource->>'status', r.resource->>'name',
       r.resource->'type'->0->'coding'->0->>'code',
       r.resource->'type'->0->>'text',
       <<schema>>.fhir_ts(r.resource->'period'->>'start'),
       <<schema>>.fhir_ts(r.resource->'period'->>'end'),
       <<schema>>.ref_id(r.resource->'ownedBy'->>'reference'),
       <<schema>>.ref_id(r.resource->'administeredBy'->>'reference')
FROM <<raw>> r
WHERE r.resource_type = 'InsurancePlan';

INSERT INTO <<t:insurance_plan_alias>> (
    release_date, resource_id, ndjson_file_id, zst_file_id, seq, alias)
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, x.ord, x.v
FROM <<raw>> r
CROSS JOIN LATERAL jsonb_array_elements_text(coalesce(r.resource->'alias', '[]'::jsonb)) WITH ORDINALITY AS x(v, ord)
WHERE r.resource_type = 'InsurancePlan';

INSERT INTO <<t:insurance_plan_network>> (
    release_date, resource_id, ndjson_file_id, zst_file_id, seq, network_organization_id)
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, x.ord, <<schema>>.ref_id(x.e->>'reference')
FROM <<raw>> r
CROSS JOIN LATERAL jsonb_array_elements(coalesce(r.resource->'network', '[]'::jsonb)) WITH ORDINALITY AS x(e, ord)
WHERE r.resource_type = 'InsurancePlan';
