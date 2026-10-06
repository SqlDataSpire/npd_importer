-- chunked: HealthcareService
-- Parse once; see the notes at the top of 010_practitioner.sql.
DROP TABLE IF EXISTS <<stage:healthcare_service>>
GO
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, r.last_updated, r.resource_type,
       j.active, j.name, j.provided_by, j.extension, j.location, j.identifier
INTO <<stage:healthcare_service>>
FROM <<raw>> r
CROSS APPLY OPENJSON(CAST(r.resource AS nvarchar(max))) WITH (
    active nvarchar(4000) '$.active',
    name nvarchar(4000) '$.name',
    provided_by nvarchar(4000) '$.providedBy.reference',
    extension nvarchar(max) AS JSON,
    location nvarchar(max) AS JSON,
    identifier nvarchar(max) AS JSON) j
WHERE r.resource_type = 'HealthcareService' AND <<chunk>>
GO
INSERT INTO <<t:healthcare_service>> WITH (TABLOCK) (
    release_date, resource_id, ndjson_file_id, zst_file_id, last_updated,
    active, name, provided_by_organization_id, network_organization_id)
SELECT s.release_date, s.resource_id, s.ndjson_file_id, s.zst_file_id, s.last_updated,
       CAST(s.active AS bit),
       s.name,
       <<schema>>.ref_id(s.provided_by),
       <<schema>>.ref_id(e.network)
FROM <<stage:healthcare_service>> s
OUTER APPLY (
    SELECT MAX(CASE WHEN x.url = N'http://hl7.org/fhir/us/ndh/StructureDefinition/base-ext-network-reference'
                    THEN x.reference END) AS network
    FROM OPENJSON(s.extension) WITH (url nvarchar(4000), reference nvarchar(4000) '$.valueReference.reference') x) e
OPTION (NO_PERFORMANCE_SPOOL)
GO
INSERT INTO <<t:healthcare_service_location>> WITH (TABLOCK) (
    release_date, resource_id, ndjson_file_id, zst_file_id, seq, location_id)
SELECT s.release_date, s.resource_id, s.ndjson_file_id, s.zst_file_id, CAST(x.[key] AS int) + 1,
       <<schema>>.ref_id(JSON_VALUE(x.value, '$.reference'))
FROM <<stage:healthcare_service>> s
CROSS APPLY OPENJSON(s.location) x
GO
INSERT INTO <<t:identifier>> WITH (TABLOCK) (
    release_date, resource_id, ndjson_file_id, zst_file_id, resource_type, seq,
    [system], [value], [use], type_code, type_text, period_start, period_end)
SELECT s.release_date, s.resource_id, s.ndjson_file_id, s.zst_file_id, s.resource_type, CAST(x.[key] AS int) + 1,
       v.[system], v.[value], v.[use], v.type_code, v.type_text,
       <<schema>>.fhir_ts(v.period_start), <<schema>>.fhir_ts(v.period_end)
FROM <<stage:healthcare_service>> s
CROSS APPLY OPENJSON(s.identifier) x
OUTER APPLY OPENJSON(CASE WHEN x.type = 5 THEN x.value END) WITH (
    [system] nvarchar(4000), [value] nvarchar(4000), [use] nvarchar(4000),
    type_code nvarchar(4000) '$.type.coding[0].code', type_text nvarchar(4000) '$.type.text',
    period_start nvarchar(4000) '$.period.start', period_end nvarchar(4000) '$.period.end') v
OPTION (NO_PERFORMANCE_SPOOL)
GO
DROP TABLE <<stage:healthcare_service>>
