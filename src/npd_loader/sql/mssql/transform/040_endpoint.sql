-- chunked: Endpoint
-- Parse once; see the notes at the top of 010_practitioner.sql.
DROP TABLE IF EXISTS <<stage:endpoint>>
GO
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, r.last_updated, r.resource_type,
       j.status, j.name, j.address, j.connection_type_system, j.connection_type_code,
       j.payload_type_system, j.payload_type_code, j.managing_organization, j.extension, j.identifier
INTO <<stage:endpoint>>
FROM <<raw>> r
CROSS APPLY OPENJSON(CAST(r.resource AS nvarchar(max))) WITH (
    status nvarchar(4000) '$.status',
    name nvarchar(4000) '$.name',
    address nvarchar(4000) '$.address',
    connection_type_system nvarchar(4000) '$.connectionType.system',
    connection_type_code nvarchar(4000) '$.connectionType.code',
    payload_type_system nvarchar(4000) '$.payloadType[0].coding[0].system',
    payload_type_code nvarchar(4000) '$.payloadType[0].coding[0].code',
    managing_organization nvarchar(4000) '$.managingOrganization.reference',
    extension nvarchar(max) AS JSON,
    identifier nvarchar(max) AS JSON) j
WHERE r.resource_type = 'Endpoint' AND <<chunk>>
GO
INSERT INTO <<t:endpoint>> WITH (TABLOCK) (
    release_date, resource_id, ndjson_file_id, zst_file_id, last_updated,
    status, name, address, connection_type_system, connection_type_code,
    payload_type_system, payload_type_code, managing_organization_id, verification_status)
SELECT s.release_date, s.resource_id, s.ndjson_file_id, s.zst_file_id, s.last_updated,
       s.status, s.name, s.address, s.connection_type_system, s.connection_type_code,
       s.payload_type_system, s.payload_type_code,
       <<schema>>.ref_id(s.managing_organization),
       e.verification_status
FROM <<stage:endpoint>> s
OUTER APPLY (
    SELECT MAX(CASE WHEN x.url = N'http://hl7.org/fhir/us/ndh/StructureDefinition/base-ext-verification-status'
                    THEN x.code END) AS verification_status
    FROM OPENJSON(s.extension)
         WITH (url nvarchar(4000), code nvarchar(4000) '$.valueCodeableConcept.coding[0].code') x) e
OPTION (NO_PERFORMANCE_SPOOL)
GO
INSERT INTO <<t:identifier>> WITH (TABLOCK) (
    release_date, resource_id, ndjson_file_id, zst_file_id, resource_type, seq,
    [system], [value], [use], type_code, type_text, period_start, period_end)
SELECT s.release_date, s.resource_id, s.ndjson_file_id, s.zst_file_id, s.resource_type, CAST(x.[key] AS int) + 1,
       v.[system], v.[value], v.[use], v.type_code, v.type_text,
       <<schema>>.fhir_ts(v.period_start), <<schema>>.fhir_ts(v.period_end)
FROM <<stage:endpoint>> s
CROSS APPLY OPENJSON(s.identifier) x
OUTER APPLY OPENJSON(CASE WHEN x.type = 5 THEN x.value END) WITH (
    [system] nvarchar(4000), [value] nvarchar(4000), [use] nvarchar(4000),
    type_code nvarchar(4000) '$.type.coding[0].code', type_text nvarchar(4000) '$.type.text',
    period_start nvarchar(4000) '$.period.start', period_end nvarchar(4000) '$.period.end') v
OPTION (NO_PERFORMANCE_SPOOL)
GO
DROP TABLE <<stage:endpoint>>
