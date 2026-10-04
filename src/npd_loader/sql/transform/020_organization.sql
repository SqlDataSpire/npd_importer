INSERT INTO <<t:organization>> (
    release_date, resource_id, ndjson_file_id, zst_file_id, last_updated,
    npi, pseudo_ein, name, active, type_code, type_display, part_of_organization_id, verification_status)
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, r.last_updated,
       <<schema>>.identifier_value(r.resource, ARRAY['http://terminology.hl7.org/NamingSystem/npi',
                                                    'http://hl7.org/fhir/sid/us-npi']),
       <<schema>>.identifier_value(r.resource, ARRAY['https://npd.cms.gov/fhir/sid/us-pseudo-ein']),
       r.resource->>'name',
       (r.resource->>'active')::boolean,
       r.resource->'type'->0->'coding'->0->>'code',
       r.resource->'type'->0->'coding'->0->>'display',
       <<schema>>.ref_id(r.resource->'partOf'->>'reference'),
       <<schema>>.ext(r.resource, 'http://hl7.org/fhir/us/ndh/StructureDefinition/base-ext-verification-status')
           ->'valueCodeableConcept'->'coding'->0->>'code'
FROM <<raw>> r
WHERE r.resource_type = 'Organization';

INSERT INTO <<t:organization_address>> (
    release_date, resource_id, ndjson_file_id, zst_file_id, seq,
    use, type, line1, line2, extra_lines, city, state, postal_code, country)
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, x.ord,
       x.e->>'use', x.e->>'type', x.e->'line'->>0, x.e->'line'->>1,
       <<schema>>.join_text((x.e->'line') - 0 - 0, ', '),
       x.e->>'city', x.e->>'state', x.e->>'postalCode', x.e->>'country'
FROM <<raw>> r
CROSS JOIN LATERAL jsonb_array_elements(coalesce(r.resource->'address', '[]'::jsonb)) WITH ORDINALITY AS x(e, ord)
WHERE r.resource_type = 'Organization';

INSERT INTO <<t:organization_telecom>> (
    release_date, resource_id, ndjson_file_id, zst_file_id, seq, system, use, value)
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, x.ord,
       x.e->>'system', x.e->>'use', x.e->>'value'
FROM <<raw>> r
CROSS JOIN LATERAL jsonb_array_elements(coalesce(r.resource->'telecom', '[]'::jsonb)) WITH ORDINALITY AS x(e, ord)
WHERE r.resource_type = 'Organization';

INSERT INTO <<t:organization_endpoint>> (
    release_date, resource_id, ndjson_file_id, zst_file_id, seq, endpoint_id)
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, x.ord,
       <<schema>>.ref_id(x.e->>'reference')
FROM <<raw>> r
CROSS JOIN LATERAL jsonb_array_elements(coalesce(r.resource->'endpoint', '[]'::jsonb)) WITH ORDINALITY AS x(e, ord)
WHERE r.resource_type = 'Organization';
