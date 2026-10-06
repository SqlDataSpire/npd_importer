-- chunked: PractitionerRole
-- Parse once; see the notes at the top of 010_practitioner.sql.
DROP TABLE IF EXISTS <<stage:practitioner_role>>
GO
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, r.last_updated, r.resource_type,
       j.active, j.practitioner, j.organization, j.period_start, j.period_end,
       j.extension, j.identifier, j.endpoint, j.location, j.specialty, j.code, j.telecom
INTO <<stage:practitioner_role>>
FROM <<raw>> r
CROSS APPLY OPENJSON(CAST(r.resource AS nvarchar(max))) WITH (
    active nvarchar(4000) '$.active',
    practitioner nvarchar(4000) '$.practitioner.reference',
    organization nvarchar(4000) '$.organization.reference',
    period_start nvarchar(4000) '$.period.start',
    period_end nvarchar(4000) '$.period.end',
    extension nvarchar(max) AS JSON,
    identifier nvarchar(max) AS JSON,
    endpoint nvarchar(max) AS JSON,
    location nvarchar(max) AS JSON,
    specialty nvarchar(max) AS JSON,
    code nvarchar(max) AS JSON,
    telecom nvarchar(max) AS JSON) j
WHERE r.resource_type = 'PractitionerRole' AND <<chunk>>
GO
INSERT INTO <<t:practitioner_role>> WITH (TABLOCK) (
    release_date, resource_id, ndjson_file_id, zst_file_id, last_updated,
    active, practitioner_id, organization_id, period_start, period_end,
    network_organization_id, accepting_patients)
SELECT s.release_date, s.resource_id, s.ndjson_file_id, s.zst_file_id, s.last_updated,
       CAST(s.active AS bit),
       <<schema>>.ref_id(s.practitioner),
       <<schema>>.ref_id(s.organization),
       <<schema>>.fhir_ts(s.period_start),
       <<schema>>.fhir_ts(s.period_end),
       <<schema>>.ref_id(e.network),
       a.accepting_patients
FROM <<stage:practitioner_role>> s
OUTER APPLY (
    SELECT MAX(CASE WHEN x.url = N'http://hl7.org/fhir/us/ndh/StructureDefinition/base-ext-network-reference'
                    THEN x.reference END) AS network,
           MAX(CASE WHEN x.url = N'http://hl7.org/fhir/us/ndh/StructureDefinition/base-ext-newpatients'
                    THEN x.extension END) AS newpatients
    FROM OPENJSON(s.extension) WITH (url nvarchar(4000), reference nvarchar(4000) '$.valueReference.reference',
                                     extension nvarchar(max) AS JSON) x) e
-- the acceptingPatients sub-extension of the newpatients extension
OUTER APPLY (
    SELECT MAX(CASE WHEN y.url = N'acceptingPatients' THEN y.code END) AS accepting_patients
    FROM OPENJSON(e.newpatients)
         WITH (url nvarchar(4000), code nvarchar(4000) '$.valueCodeableConcept.coding[0].code') y) a
OPTION (NO_PERFORMANCE_SPOOL)
GO
INSERT INTO <<t:practitioner_role_endpoint>> WITH (TABLOCK) (
    release_date, resource_id, ndjson_file_id, zst_file_id, seq, endpoint_id)
SELECT s.release_date, s.resource_id, s.ndjson_file_id, s.zst_file_id, CAST(x.[key] AS int) + 1,
       <<schema>>.ref_id(JSON_VALUE(x.value, '$.reference'))
FROM <<stage:practitioner_role>> s
CROSS APPLY OPENJSON(s.endpoint) x
GO
INSERT INTO <<t:practitioner_role_location>> WITH (TABLOCK) (
    release_date, resource_id, ndjson_file_id, zst_file_id, seq, location_id)
SELECT s.release_date, s.resource_id, s.ndjson_file_id, s.zst_file_id, CAST(x.[key] AS int) + 1,
       <<schema>>.ref_id(JSON_VALUE(x.value, '$.reference'))
FROM <<stage:practitioner_role>> s
CROSS APPLY OPENJSON(s.location) x
GO
INSERT INTO <<t:practitioner_role_specialty>> WITH (TABLOCK) (
    release_date, resource_id, ndjson_file_id, zst_file_id, seq, [system], code, display, [text])
SELECT s.release_date, s.resource_id, s.ndjson_file_id, s.zst_file_id, CAST(x.[key] AS int) + 1,
       v.[system], v.code, v.display, v.[text]
FROM <<stage:practitioner_role>> s
CROSS APPLY OPENJSON(s.specialty) x
OUTER APPLY OPENJSON(CASE WHEN x.type = 5 THEN x.value END) WITH (
    [system] nvarchar(4000) '$.coding[0].system', code nvarchar(4000) '$.coding[0].code',
    display nvarchar(4000) '$.coding[0].display', [text] nvarchar(4000) '$.text') v
OPTION (NO_PERFORMANCE_SPOOL)
GO
INSERT INTO <<t:practitioner_role_code>> WITH (TABLOCK) (
    release_date, resource_id, ndjson_file_id, zst_file_id, seq, [system], code, display, [text])
SELECT s.release_date, s.resource_id, s.ndjson_file_id, s.zst_file_id, CAST(x.[key] AS int) + 1,
       v.[system], v.code, v.display, v.[text]
FROM <<stage:practitioner_role>> s
CROSS APPLY OPENJSON(s.code) x
OUTER APPLY OPENJSON(CASE WHEN x.type = 5 THEN x.value END) WITH (
    [system] nvarchar(4000) '$.coding[0].system', code nvarchar(4000) '$.coding[0].code',
    display nvarchar(4000) '$.coding[0].display', [text] nvarchar(4000) '$.text') v
OPTION (NO_PERFORMANCE_SPOOL)
GO
INSERT INTO <<t:practitioner_role_telecom>> WITH (TABLOCK) (
    release_date, resource_id, ndjson_file_id, zst_file_id, seq, [system], [use], [value])
SELECT s.release_date, s.resource_id, s.ndjson_file_id, s.zst_file_id, CAST(x.[key] AS int) + 1,
       JSON_VALUE(x.value, '$.system'), JSON_VALUE(x.value, '$.use'), JSON_VALUE(x.value, '$.value')
FROM <<stage:practitioner_role>> s
CROSS APPLY OPENJSON(s.telecom) x
GO
INSERT INTO <<t:identifier>> WITH (TABLOCK) (
    release_date, resource_id, ndjson_file_id, zst_file_id, resource_type, seq,
    [system], [value], [use], type_code, type_text, period_start, period_end)
SELECT s.release_date, s.resource_id, s.ndjson_file_id, s.zst_file_id, s.resource_type, CAST(x.[key] AS int) + 1,
       v.[system], v.[value], v.[use], v.type_code, v.type_text,
       <<schema>>.fhir_ts(v.period_start), <<schema>>.fhir_ts(v.period_end)
FROM <<stage:practitioner_role>> s
CROSS APPLY OPENJSON(s.identifier) x
OUTER APPLY OPENJSON(CASE WHEN x.type = 5 THEN x.value END) WITH (
    [system] nvarchar(4000), [value] nvarchar(4000), [use] nvarchar(4000),
    type_code nvarchar(4000) '$.type.coding[0].code', type_text nvarchar(4000) '$.type.text',
    period_start nvarchar(4000) '$.period.start', period_end nvarchar(4000) '$.period.end') v
OPTION (NO_PERFORMANCE_SPOOL)
GO
DROP TABLE <<stage:practitioner_role>>
