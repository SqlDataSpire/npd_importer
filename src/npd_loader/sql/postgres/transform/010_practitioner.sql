INSERT INTO <<t:practitioner>> (
    release_date, resource_id, ndjson_file_id, zst_file_id, last_updated,
    npi, active, gender, name_family, name_given, name_prefix, name_suffix,
    identity_verified, medicare_enrolled, in_hhs_exclusion_list, aligned_with_data_network)
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, r.last_updated,
       <<schema>>.identifier_value(r.resource, ARRAY['http://terminology.hl7.org/NamingSystem/npi',
                                                    'http://hl7.org/fhir/sid/us-npi']),
       (r.resource->>'active')::boolean,
       r.resource->>'gender',
       n.e->>'family',
       <<schema>>.join_text(n.e->'given', ' '),
       <<schema>>.join_text(n.e->'prefix', ' '),
       <<schema>>.join_text(n.e->'suffix', ' '),
       (<<schema>>.ext(r.resource, 'http://hl7.org/fhir/us/ndh/StructureDefinition/base-ext-cms-identity-verified')->>'valueBoolean')::boolean,
       (<<schema>>.ext(r.resource, 'http://hl7.org/fhir/us/ndh/StructureDefinition/base-ext-cms_medicare_enrollment')->>'valueBoolean')::boolean,
       (<<schema>>.ext(r.resource, 'http://hl7.org/fhir/us/ndh/StructureDefinition/base-ext-hhs-in-exclusion-list')->>'valueBoolean')::boolean,
       (<<schema>>.ext(r.resource, 'http://hl7.org/fhir/us/ndh/StructureDefinition/base-ext-cms_aligned_with_data_network')->>'valueBoolean')::boolean
FROM <<raw>> r
LEFT JOIN LATERAL (
    -- the official name, else the first name
    SELECT x.e FROM jsonb_array_elements(coalesce(r.resource->'name', '[]'::jsonb)) WITH ORDINALITY AS x(e, ord)
    ORDER BY (x.e->>'use' = 'official') DESC NULLS LAST, x.ord
    LIMIT 1
) n ON true
WHERE r.resource_type = 'Practitioner';

INSERT INTO <<t:practitioner_name>> (
    release_date, resource_id, ndjson_file_id, zst_file_id, seq,
    use, family, given, prefix, suffix, period_start, period_end)
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, x.ord,
       x.e->>'use', x.e->>'family',
       <<schema>>.join_text(x.e->'given', ' '),
       <<schema>>.join_text(x.e->'prefix', ' '),
       <<schema>>.join_text(x.e->'suffix', ' '),
       <<schema>>.fhir_ts(x.e->'period'->>'start'),
       <<schema>>.fhir_ts(x.e->'period'->>'end')
FROM <<raw>> r
CROSS JOIN LATERAL jsonb_array_elements(coalesce(r.resource->'name', '[]'::jsonb)) WITH ORDINALITY AS x(e, ord)
WHERE r.resource_type = 'Practitioner';

INSERT INTO <<t:practitioner_address>> (
    release_date, resource_id, ndjson_file_id, zst_file_id, seq,
    use, type, line1, line2, extra_lines, city, state, postal_code, country)
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, x.ord,
       x.e->>'use', x.e->>'type', x.e->'line'->>0, x.e->'line'->>1,
       <<schema>>.join_text((x.e->'line') - 0 - 0, ', '),
       x.e->>'city', x.e->>'state', x.e->>'postalCode', x.e->>'country'
FROM <<raw>> r
CROSS JOIN LATERAL jsonb_array_elements(coalesce(r.resource->'address', '[]'::jsonb)) WITH ORDINALITY AS x(e, ord)
WHERE r.resource_type = 'Practitioner';

INSERT INTO <<t:practitioner_telecom>> (
    release_date, resource_id, ndjson_file_id, zst_file_id, seq, system, use, value)
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, x.ord,
       x.e->>'system', x.e->>'use', x.e->>'value'
FROM <<raw>> r
CROSS JOIN LATERAL jsonb_array_elements(coalesce(r.resource->'telecom', '[]'::jsonb)) WITH ORDINALITY AS x(e, ord)
WHERE r.resource_type = 'Practitioner';

INSERT INTO <<t:practitioner_qualification>> (
    release_date, resource_id, ndjson_file_id, zst_file_id, seq,
    code_system, code, code_display, code_text, identifier_value, identifier_type_code, issuer_organization_id)
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, x.ord,
       x.e->'code'->'coding'->0->>'system',
       x.e->'code'->'coding'->0->>'code',
       x.e->'code'->'coding'->0->>'display',
       x.e->'code'->>'text',
       x.e->'identifier'->0->>'value',
       x.e->'identifier'->0->'type'->'coding'->0->>'code',
       <<schema>>.ref_id(x.e->'issuer'->>'reference')
FROM <<raw>> r
CROSS JOIN LATERAL jsonb_array_elements(coalesce(r.resource->'qualification', '[]'::jsonb)) WITH ORDINALITY AS x(e, ord)
WHERE r.resource_type = 'Practitioner';
