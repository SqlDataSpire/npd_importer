-- chunked: OrganizationAffiliation
-- Parse once; see the notes at the top of 010_practitioner.sql.
DROP TABLE IF EXISTS <<stage:organization_affiliation>>
GO
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, r.last_updated, r.resource_type,
       j.active, j.organization, j.participating_organization, j.role_code, j.role_display, j.role_text,
       j.period_start, j.period_end, j.network, j.identifier
INTO <<stage:organization_affiliation>>
FROM <<raw>> r
CROSS APPLY OPENJSON(CAST(r.resource AS nvarchar(max))) WITH (
    active nvarchar(4000) '$.active',
    organization nvarchar(4000) '$.organization.reference',
    participating_organization nvarchar(4000) '$.participatingOrganization.reference',
    role_code nvarchar(4000) '$.code[0].coding[0].code',
    role_display nvarchar(4000) '$.code[0].coding[0].display',
    role_text nvarchar(4000) '$.code[0].text',
    period_start nvarchar(4000) '$.period.start',
    period_end nvarchar(4000) '$.period.end',
    network nvarchar(max) AS JSON,
    identifier nvarchar(max) AS JSON) j
WHERE r.resource_type = 'OrganizationAffiliation' AND <<chunk>>
GO
INSERT INTO <<t:organization_affiliation>> WITH (TABLOCK) (
    release_date, resource_id, ndjson_file_id, zst_file_id, last_updated,
    active, organization_id, participating_organization_id, role_code, role_display, role_text,
    period_start, period_end)
SELECT s.release_date, s.resource_id, s.ndjson_file_id, s.zst_file_id, s.last_updated,
       CAST(s.active AS bit),
       <<schema>>.ref_id(s.organization),
       <<schema>>.ref_id(s.participating_organization),
       s.role_code, s.role_display, s.role_text,
       <<schema>>.fhir_ts(s.period_start),
       <<schema>>.fhir_ts(s.period_end)
FROM <<stage:organization_affiliation>> s
GO
INSERT INTO <<t:organization_affiliation_network>> WITH (TABLOCK) (
    release_date, resource_id, ndjson_file_id, zst_file_id, seq, network_organization_id)
SELECT s.release_date, s.resource_id, s.ndjson_file_id, s.zst_file_id, CAST(x.[key] AS int) + 1,
       <<schema>>.ref_id(JSON_VALUE(x.value, '$.reference'))
FROM <<stage:organization_affiliation>> s
CROSS APPLY OPENJSON(s.network) x
GO
INSERT INTO <<t:identifier>> WITH (TABLOCK) (
    release_date, resource_id, ndjson_file_id, zst_file_id, resource_type, seq,
    [system], [value], [use], type_code, type_text, period_start, period_end)
SELECT s.release_date, s.resource_id, s.ndjson_file_id, s.zst_file_id, s.resource_type, CAST(x.[key] AS int) + 1,
       v.[system], v.[value], v.[use], v.type_code, v.type_text,
       <<schema>>.fhir_ts(v.period_start), <<schema>>.fhir_ts(v.period_end)
FROM <<stage:organization_affiliation>> s
CROSS APPLY OPENJSON(s.identifier) x
OUTER APPLY OPENJSON(CASE WHEN x.type = 5 THEN x.value END) WITH (
    [system] nvarchar(4000), [value] nvarchar(4000), [use] nvarchar(4000),
    type_code nvarchar(4000) '$.type.coding[0].code', type_text nvarchar(4000) '$.type.text',
    period_start nvarchar(4000) '$.period.start', period_end nvarchar(4000) '$.period.end') v
OPTION (NO_PERFORMANCE_SPOOL)
GO
DROP TABLE <<stage:organization_affiliation>>
