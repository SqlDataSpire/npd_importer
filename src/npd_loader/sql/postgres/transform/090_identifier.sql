INSERT INTO <<t:identifier>> (
    release_date, resource_id, ndjson_file_id, zst_file_id, resource_type, seq,
    system, value, use, type_code, type_text, period_start, period_end)
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, r.resource_type, x.ord,
       x.e->>'system', x.e->>'value', x.e->>'use',
       x.e->'type'->'coding'->0->>'code',
       x.e->'type'->>'text',
       <<schema>>.fhir_ts(x.e->'period'->>'start'),
       <<schema>>.fhir_ts(x.e->'period'->>'end')
FROM <<raw>> r
CROSS JOIN LATERAL jsonb_array_elements(coalesce(r.resource->'identifier', '[]'::jsonb)) WITH ORDINALITY AS x(e, ord);
