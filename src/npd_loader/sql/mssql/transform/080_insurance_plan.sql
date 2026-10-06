-- chunked: InsurancePlan
-- Parse once; see the notes at the top of 010_practitioner.sql.
DROP TABLE IF EXISTS <<stage:insurance_plan>>
GO
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, r.last_updated, r.resource_type,
       j.status, j.name, j.type_code, j.type_text, j.period_start, j.period_end, j.owned_by, j.administered_by,
       j.alias, j.network, j.identifier
INTO <<stage:insurance_plan>>
FROM <<raw>> r
CROSS APPLY OPENJSON(CAST(r.resource AS nvarchar(max))) WITH (
    status nvarchar(4000) '$.status',
    name nvarchar(4000) '$.name',
    type_code nvarchar(4000) '$.type[0].coding[0].code',
    type_text nvarchar(4000) '$.type[0].text',
    period_start nvarchar(4000) '$.period.start',
    period_end nvarchar(4000) '$.period.end',
    owned_by nvarchar(4000) '$.ownedBy.reference',
    administered_by nvarchar(4000) '$.administeredBy.reference',
    alias nvarchar(max) AS JSON,
    network nvarchar(max) AS JSON,
    identifier nvarchar(max) AS JSON) j
WHERE r.resource_type = 'InsurancePlan' AND <<chunk>>
GO
INSERT INTO <<t:insurance_plan>> WITH (TABLOCK) (
    release_date, resource_id, ndjson_file_id, zst_file_id, last_updated,
    status, name, type_code, type_text, period_start, period_end,
    owned_by_organization_id, administered_by_organization_id)
SELECT s.release_date, s.resource_id, s.ndjson_file_id, s.zst_file_id, s.last_updated,
       s.status, s.name, s.type_code, s.type_text,
       <<schema>>.fhir_ts(s.period_start),
       <<schema>>.fhir_ts(s.period_end),
       <<schema>>.ref_id(s.owned_by),
       <<schema>>.ref_id(s.administered_by)
FROM <<stage:insurance_plan>> s
GO
INSERT INTO <<t:insurance_plan_alias>> WITH (TABLOCK) (
    release_date, resource_id, ndjson_file_id, zst_file_id, seq, alias)
SELECT s.release_date, s.resource_id, s.ndjson_file_id, s.zst_file_id, CAST(x.[key] AS int) + 1, x.value
FROM <<stage:insurance_plan>> s
CROSS APPLY OPENJSON(s.alias) x
GO
INSERT INTO <<t:insurance_plan_network>> WITH (TABLOCK) (
    release_date, resource_id, ndjson_file_id, zst_file_id, seq, network_organization_id)
SELECT s.release_date, s.resource_id, s.ndjson_file_id, s.zst_file_id, CAST(x.[key] AS int) + 1,
       <<schema>>.ref_id(JSON_VALUE(x.value, '$.reference'))
FROM <<stage:insurance_plan>> s
CROSS APPLY OPENJSON(s.network) x
GO
INSERT INTO <<t:identifier>> WITH (TABLOCK) (
    release_date, resource_id, ndjson_file_id, zst_file_id, resource_type, seq,
    [system], [value], [use], type_code, type_text, period_start, period_end)
SELECT s.release_date, s.resource_id, s.ndjson_file_id, s.zst_file_id, s.resource_type, CAST(x.[key] AS int) + 1,
       v.[system], v.[value], v.[use], v.type_code, v.type_text,
       <<schema>>.fhir_ts(v.period_start), <<schema>>.fhir_ts(v.period_end)
FROM <<stage:insurance_plan>> s
CROSS APPLY OPENJSON(s.identifier) x
OUTER APPLY OPENJSON(CASE WHEN x.type = 5 THEN x.value END) WITH (
    [system] nvarchar(4000), [value] nvarchar(4000), [use] nvarchar(4000),
    type_code nvarchar(4000) '$.type.coding[0].code', type_text nvarchar(4000) '$.type.text',
    period_start nvarchar(4000) '$.period.start', period_end nvarchar(4000) '$.period.end') v
OPTION (NO_PERFORMANCE_SPOOL)
GO
DROP TABLE <<stage:insurance_plan>>
