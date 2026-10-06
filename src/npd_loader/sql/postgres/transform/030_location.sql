INSERT INTO <<t:location>> (
    release_date, resource_id, ndjson_file_id, zst_file_id, last_updated,
    status, name, description, mode, address_use, address_type, line1, line2, extra_lines,
    city, state, postal_code, country, latitude, longitude, managing_organization_id)
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, r.last_updated,
       r.resource->>'status', r.resource->>'name', r.resource->>'description', r.resource->>'mode',
       r.resource->'address'->>'use', r.resource->'address'->>'type',
       r.resource->'address'->'line'->>0, r.resource->'address'->'line'->>1,
       <<schema>>.join_text((r.resource->'address'->'line') - 0 - 0, ', '),
       r.resource->'address'->>'city', r.resource->'address'->>'state',
       r.resource->'address'->>'postalCode', r.resource->'address'->>'country',
       (r.resource->'position'->>'latitude')::double precision,
       (r.resource->'position'->>'longitude')::double precision,
       <<schema>>.ref_id(r.resource->'managingOrganization'->>'reference')
FROM <<raw>> r
WHERE r.resource_type = 'Location';

INSERT INTO <<t:location_telecom>> (
    release_date, resource_id, ndjson_file_id, zst_file_id, seq, system, use, value)
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, x.ord,
       x.e->>'system', x.e->>'use', x.e->>'value'
FROM <<raw>> r
CROSS JOIN LATERAL jsonb_array_elements(coalesce(r.resource->'telecom', '[]'::jsonb)) WITH ORDINALITY AS x(e, ord)
WHERE r.resource_type = 'Location';
