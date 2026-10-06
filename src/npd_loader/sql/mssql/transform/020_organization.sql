-- chunked: Organization
-- Parse once; see the notes at the top of 010_practitioner.sql.
DROP TABLE IF EXISTS <<stage:organization>>
GO
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, r.last_updated, r.resource_type,
       j.name, j.active, j.type_code, j.type_display, j.part_of,
       j.extension, j.identifier, j.address, j.telecom, j.endpoint
INTO <<stage:organization>>
FROM <<raw>> r
CROSS APPLY OPENJSON(CAST(r.resource AS nvarchar(max))) WITH (
    name nvarchar(4000) '$.name',
    active nvarchar(4000) '$.active',
    type_code nvarchar(4000) '$.type[0].coding[0].code',
    type_display nvarchar(4000) '$.type[0].coding[0].display',
    part_of nvarchar(4000) '$.partOf.reference',
    extension nvarchar(max) AS JSON,
    identifier nvarchar(max) AS JSON,
    address nvarchar(max) AS JSON,
    telecom nvarchar(max) AS JSON,
    endpoint nvarchar(max) AS JSON) j
WHERE r.resource_type = 'Organization' AND <<chunk>>
GO
INSERT INTO <<t:organization>> WITH (TABLOCK) (
    release_date, resource_id, ndjson_file_id, zst_file_id, last_updated,
    npi, pseudo_ein, name, active, type_code, type_display, part_of_organization_id, verification_status)
SELECT s.release_date, s.resource_id, s.ndjson_file_id, s.zst_file_id, s.last_updated,
       CASE WHEN SUBSTRING(i.npi, 7, 1) = N'v' THEN STUFF(i.npi, 1, 7, N'') END,
       CASE WHEN SUBSTRING(i.ein, 7, 1) = N'v' THEN STUFF(i.ein, 1, 7, N'') END,
       s.name,
       CAST(s.active AS bit),
       s.type_code,
       s.type_display,
       <<schema>>.ref_id(s.part_of),
       e.verification_status
FROM <<stage:organization>> s
-- the values of the first identifier with an NPI system and of the first with the pseudo-EIN system
OUTER APPLY (SELECT MIN(CASE WHEN k.[system] IN (N'http://terminology.hl7.org/NamingSystem/npi',
                                                 N'http://hl7.org/fhir/sid/us-npi') THEN k.kv END) AS npi,
                    MIN(CASE WHEN k.[system] = N'https://npd.cms.gov/fhir/sid/us-pseudo-ein' THEN k.kv END) AS ein
             FROM OPENJSON(s.identifier) x
             CROSS APPLY (SELECT JSON_VALUE(x.value, '$.system') AS [system],
                                 RIGHT(N'00000' + x.[key], 6) + ISNULL(N'v' + JSON_VALUE(x.value, '$.value'), N'-')
                                 COLLATE Latin1_General_BIN2 AS kv) k) i
OUTER APPLY (
    SELECT MAX(CASE WHEN x.url = N'http://hl7.org/fhir/us/ndh/StructureDefinition/base-ext-verification-status'
                    THEN x.code END) AS verification_status
    FROM OPENJSON(s.extension)
         WITH (url nvarchar(4000), code nvarchar(4000) '$.valueCodeableConcept.coding[0].code') x) e
OPTION (NO_PERFORMANCE_SPOOL)
GO
INSERT INTO <<t:organization_address>> WITH (TABLOCK) (
    release_date, resource_id, ndjson_file_id, zst_file_id, seq,
    [use], [type], line1, line2, extra_lines, city, state, postal_code, country)
SELECT s.release_date, s.resource_id, s.ndjson_file_id, s.zst_file_id, CAST(x.[key] AS int) + 1,
       v.[use], v.[type], v.line1, v.line2, extra.txt, v.city, v.state, v.postal_code, v.country
FROM <<stage:organization>> s
CROSS APPLY OPENJSON(s.address) x
OUTER APPLY OPENJSON(CASE WHEN x.type = 5 THEN x.value END) WITH (
    [use] nvarchar(4000), [type] nvarchar(4000),
    line1 nvarchar(4000) '$.line[0]', line2 nvarchar(4000) '$.line[1]', line nvarchar(max) AS JSON,
    city nvarchar(4000), state nvarchar(4000), postal_code nvarchar(4000) '$.postalCode', country nvarchar(4000)) v
OUTER APPLY <<schema>>.join_text(v.line, N', ', 2) extra
OPTION (NO_PERFORMANCE_SPOOL)
GO
INSERT INTO <<t:organization_telecom>> WITH (TABLOCK) (
    release_date, resource_id, ndjson_file_id, zst_file_id, seq, [system], [use], [value])
SELECT s.release_date, s.resource_id, s.ndjson_file_id, s.zst_file_id, CAST(x.[key] AS int) + 1,
       JSON_VALUE(x.value, '$.system'), JSON_VALUE(x.value, '$.use'), JSON_VALUE(x.value, '$.value')
FROM <<stage:organization>> s
CROSS APPLY OPENJSON(s.telecom) x
GO
INSERT INTO <<t:organization_endpoint>> WITH (TABLOCK) (
    release_date, resource_id, ndjson_file_id, zst_file_id, seq, endpoint_id)
SELECT s.release_date, s.resource_id, s.ndjson_file_id, s.zst_file_id, CAST(x.[key] AS int) + 1,
       <<schema>>.ref_id(JSON_VALUE(x.value, '$.reference'))
FROM <<stage:organization>> s
CROSS APPLY OPENJSON(s.endpoint) x
GO
INSERT INTO <<t:identifier>> WITH (TABLOCK) (
    release_date, resource_id, ndjson_file_id, zst_file_id, resource_type, seq,
    [system], [value], [use], type_code, type_text, period_start, period_end)
SELECT s.release_date, s.resource_id, s.ndjson_file_id, s.zst_file_id, s.resource_type, CAST(x.[key] AS int) + 1,
       v.[system], v.[value], v.[use], v.type_code, v.type_text,
       <<schema>>.fhir_ts(v.period_start), <<schema>>.fhir_ts(v.period_end)
FROM <<stage:organization>> s
CROSS APPLY OPENJSON(s.identifier) x
OUTER APPLY OPENJSON(CASE WHEN x.type = 5 THEN x.value END) WITH (
    [system] nvarchar(4000), [value] nvarchar(4000), [use] nvarchar(4000),
    type_code nvarchar(4000) '$.type.coding[0].code', type_text nvarchar(4000) '$.type.text',
    period_start nvarchar(4000) '$.period.start', period_end nvarchar(4000) '$.period.end') v
OPTION (NO_PERFORMANCE_SPOOL)
GO
DROP TABLE <<stage:organization>>
