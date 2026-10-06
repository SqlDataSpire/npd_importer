INSERT INTO <<t:identifier>> WITH (TABLOCK) (
    release_date, resource_id, ndjson_file_id, zst_file_id, resource_type, seq,
    [system], [value], [use], type_code, type_text, period_start, period_end)
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, r.resource_type, CAST(x.[key] AS int) + 1,
       JSON_VALUE(x.value, '$.system'), JSON_VALUE(x.value, '$.value'), JSON_VALUE(x.value, '$.use'),
       JSON_VALUE(x.value, '$.type.coding[0].code'),
       JSON_VALUE(x.value, '$.type.text'),
       <<schema>>.fhir_ts(JSON_VALUE(x.value, '$.period.start')),
       <<schema>>.fhir_ts(JSON_VALUE(x.value, '$.period.end'))
FROM <<raw>> r
CROSS APPLY OPENJSON(r.resource, '$.identifier') x
