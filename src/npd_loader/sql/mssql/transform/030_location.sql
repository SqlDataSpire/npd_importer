-- chunked: Location
-- Parse once; see the notes at the top of 010_practitioner.sql.
DROP TABLE IF EXISTS <<stage:location>>
GO
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, r.last_updated, r.resource_type,
       j.status, j.name, j.description, j.mode, j.address_use, j.address_type, j.line1, j.line2, j.line,
       j.city, j.state, j.postal_code, j.country, j.latitude, j.longitude, j.managing_organization,
       j.identifier, j.telecom
INTO <<stage:location>>
FROM <<raw>> r
CROSS APPLY OPENJSON(CAST(r.resource AS nvarchar(max))) WITH (
    status nvarchar(4000) '$.status',
    name nvarchar(4000) '$.name',
    description nvarchar(max) '$.description',
    mode nvarchar(4000) '$.mode',
    address_use nvarchar(4000) '$.address.use',
    address_type nvarchar(4000) '$.address.type',
    line1 nvarchar(4000) '$.address.line[0]',
    line2 nvarchar(4000) '$.address.line[1]',
    line nvarchar(max) '$.address.line' AS JSON,
    city nvarchar(4000) '$.address.city',
    state nvarchar(4000) '$.address.state',
    postal_code nvarchar(4000) '$.address.postalCode',
    country nvarchar(4000) '$.address.country',
    latitude nvarchar(4000) '$.position.latitude',
    longitude nvarchar(4000) '$.position.longitude',
    managing_organization nvarchar(4000) '$.managingOrganization.reference',
    identifier nvarchar(max) AS JSON,
    telecom nvarchar(max) AS JSON) j
WHERE r.resource_type = 'Location' AND <<chunk>>
GO
INSERT INTO <<t:location>> WITH (TABLOCK) (
    release_date, resource_id, ndjson_file_id, zst_file_id, last_updated,
    status, name, description, mode, address_use, address_type, line1, line2, extra_lines,
    city, state, postal_code, country, latitude, longitude, managing_organization_id)
SELECT s.release_date, s.resource_id, s.ndjson_file_id, s.zst_file_id, s.last_updated,
       s.status, s.name,
       CASE WHEN LEN(s.description) <= 4000 THEN s.description END,   -- as JSON_VALUE: NULL past 4000 characters
       s.mode, s.address_use, s.address_type, s.line1, s.line2, extra.txt,
       s.city, s.state, s.postal_code, s.country,
       CAST(s.latitude AS float),
       CAST(s.longitude AS float),
       <<schema>>.ref_id(s.managing_organization)
FROM <<stage:location>> s
OUTER APPLY <<schema>>.join_text(s.line, N', ', 2) extra
OPTION (NO_PERFORMANCE_SPOOL)
GO
INSERT INTO <<t:location_telecom>> WITH (TABLOCK) (
    release_date, resource_id, ndjson_file_id, zst_file_id, seq, [system], [use], [value])
SELECT s.release_date, s.resource_id, s.ndjson_file_id, s.zst_file_id, CAST(x.[key] AS int) + 1,
       JSON_VALUE(x.value, '$.system'), JSON_VALUE(x.value, '$.use'), JSON_VALUE(x.value, '$.value')
FROM <<stage:location>> s
CROSS APPLY OPENJSON(s.telecom) x
GO
INSERT INTO <<t:identifier>> WITH (TABLOCK) (
    release_date, resource_id, ndjson_file_id, zst_file_id, resource_type, seq,
    [system], [value], [use], type_code, type_text, period_start, period_end)
SELECT s.release_date, s.resource_id, s.ndjson_file_id, s.zst_file_id, s.resource_type, CAST(x.[key] AS int) + 1,
       v.[system], v.[value], v.[use], v.type_code, v.type_text,
       <<schema>>.fhir_ts(v.period_start), <<schema>>.fhir_ts(v.period_end)
FROM <<stage:location>> s
CROSS APPLY OPENJSON(s.identifier) x
OUTER APPLY OPENJSON(CASE WHEN x.type = 5 THEN x.value END) WITH (
    [system] nvarchar(4000), [value] nvarchar(4000), [use] nvarchar(4000),
    type_code nvarchar(4000) '$.type.coding[0].code', type_text nvarchar(4000) '$.type.text',
    period_start nvarchar(4000) '$.period.start', period_end nvarchar(4000) '$.period.end') v
OPTION (NO_PERFORMANCE_SPOOL)
GO
DROP TABLE <<stage:location>>
