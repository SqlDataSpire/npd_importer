INSERT INTO <<t:organization_affiliation>> (
    release_date, resource_id, ndjson_file_id, zst_file_id, last_updated,
    active, organization_id, participating_organization_id, role_code, role_display, role_text,
    period_start, period_end)
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, r.last_updated,
       (r.resource->>'active')::boolean,
       <<schema>>.ref_id(r.resource->'organization'->>'reference'),
       <<schema>>.ref_id(r.resource->'participatingOrganization'->>'reference'),
       r.resource->'code'->0->'coding'->0->>'code',
       r.resource->'code'->0->'coding'->0->>'display',
       r.resource->'code'->0->>'text',
       <<schema>>.fhir_ts(r.resource->'period'->>'start'),
       <<schema>>.fhir_ts(r.resource->'period'->>'end')
FROM <<raw>> r
WHERE r.resource_type = 'OrganizationAffiliation';
