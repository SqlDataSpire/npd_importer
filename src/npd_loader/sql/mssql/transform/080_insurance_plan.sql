INSERT INTO <<t:insurance_plan>> WITH (TABLOCK) (
    release_date, resource_id, ndjson_file_id, zst_file_id, last_updated,
    status, name, type_code, type_text, period_start, period_end,
    owned_by_organization_id, administered_by_organization_id)
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, r.last_updated,
       JSON_VALUE(r.resource, '$.status'), JSON_VALUE(r.resource, '$.name'),
       JSON_VALUE(r.resource, '$.type[0].coding[0].code'),
       JSON_VALUE(r.resource, '$.type[0].text'),
       <<schema>>.fhir_ts(JSON_VALUE(r.resource, '$.period.start')),
       <<schema>>.fhir_ts(JSON_VALUE(r.resource, '$.period.end')),
       <<schema>>.ref_id(JSON_VALUE(r.resource, '$.ownedBy.reference')),
       <<schema>>.ref_id(JSON_VALUE(r.resource, '$.administeredBy.reference'))
FROM <<raw>> r
WHERE r.resource_type = 'InsurancePlan'
GO
INSERT INTO <<t:insurance_plan_alias>> WITH (TABLOCK) (
    release_date, resource_id, ndjson_file_id, zst_file_id, seq, alias)
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, CAST(x.[key] AS int) + 1, x.value
FROM <<raw>> r
CROSS APPLY OPENJSON(r.resource, '$.alias') x
WHERE r.resource_type = 'InsurancePlan'
GO
INSERT INTO <<t:insurance_plan_network>> WITH (TABLOCK) (
    release_date, resource_id, ndjson_file_id, zst_file_id, seq, network_organization_id)
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, CAST(x.[key] AS int) + 1,
       <<schema>>.ref_id(JSON_VALUE(x.value, '$.reference'))
FROM <<raw>> r
CROSS APPLY OPENJSON(r.resource, '$.network') x
WHERE r.resource_type = 'InsurancePlan'
