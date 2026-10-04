INSERT INTO <<t:practitioner_role>> (
    release_date, resource_id, ndjson_file_id, zst_file_id, last_updated,
    active, practitioner_id, organization_id, period_start, period_end)
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, r.last_updated,
       (r.resource->>'active')::boolean,
       <<schema>>.ref_id(r.resource->'practitioner'->>'reference'),
       <<schema>>.ref_id(r.resource->'organization'->>'reference'),
       <<schema>>.fhir_ts(r.resource->'period'->>'start'),
       <<schema>>.fhir_ts(r.resource->'period'->>'end')
FROM <<raw>> r
WHERE r.resource_type = 'PractitionerRole';

INSERT INTO <<t:practitioner_role_endpoint>> (
    release_date, resource_id, ndjson_file_id, zst_file_id, seq, endpoint_id)
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, x.ord, <<schema>>.ref_id(x.e->>'reference')
FROM <<raw>> r
CROSS JOIN LATERAL jsonb_array_elements(coalesce(r.resource->'endpoint', '[]'::jsonb)) WITH ORDINALITY AS x(e, ord)
WHERE r.resource_type = 'PractitionerRole';

INSERT INTO <<t:practitioner_role_location>> (
    release_date, resource_id, ndjson_file_id, zst_file_id, seq, location_id)
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, x.ord, <<schema>>.ref_id(x.e->>'reference')
FROM <<raw>> r
CROSS JOIN LATERAL jsonb_array_elements(coalesce(r.resource->'location', '[]'::jsonb)) WITH ORDINALITY AS x(e, ord)
WHERE r.resource_type = 'PractitionerRole';

INSERT INTO <<t:practitioner_role_specialty>> (
    release_date, resource_id, ndjson_file_id, zst_file_id, seq, system, code, display, text)
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, x.ord,
       x.e->'coding'->0->>'system', x.e->'coding'->0->>'code', x.e->'coding'->0->>'display', x.e->>'text'
FROM <<raw>> r
CROSS JOIN LATERAL jsonb_array_elements(coalesce(r.resource->'specialty', '[]'::jsonb)) WITH ORDINALITY AS x(e, ord)
WHERE r.resource_type = 'PractitionerRole';

INSERT INTO <<t:practitioner_role_code>> (
    release_date, resource_id, ndjson_file_id, zst_file_id, seq, system, code, display, text)
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, x.ord,
       x.e->'coding'->0->>'system', x.e->'coding'->0->>'code', x.e->'coding'->0->>'display', x.e->>'text'
FROM <<raw>> r
CROSS JOIN LATERAL jsonb_array_elements(coalesce(r.resource->'code', '[]'::jsonb)) WITH ORDINALITY AS x(e, ord)
WHERE r.resource_type = 'PractitionerRole';
