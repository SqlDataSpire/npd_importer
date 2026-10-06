-- Identifiers of resource types without a script of their own (010-080 write their type's identifiers from their
-- stage, so the eight known types are not parsed again here).
INSERT INTO <<t:identifier>> WITH (TABLOCK) (
    release_date, resource_id, ndjson_file_id, zst_file_id, resource_type, seq,
    [system], [value], [use], type_code, type_text, period_start, period_end)
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, r.resource_type, CAST(x.[key] AS int) + 1,
       v.[system], v.[value], v.[use], v.type_code, v.type_text,
       <<schema>>.fhir_ts(v.period_start), <<schema>>.fhir_ts(v.period_end)
FROM <<raw>> r
CROSS APPLY OPENJSON(CAST(r.resource AS nvarchar(max)), '$.identifier') x
OUTER APPLY OPENJSON(CASE WHEN x.type = 5 THEN x.value END) WITH (
    [system] nvarchar(4000), [value] nvarchar(4000), [use] nvarchar(4000),
    type_code nvarchar(4000) '$.type.coding[0].code', type_text nvarchar(4000) '$.type.text',
    period_start nvarchar(4000) '$.period.start', period_end nvarchar(4000) '$.period.end') v
WHERE r.release_date = <<release>>
  AND r.resource_type NOT IN ('Practitioner', 'Organization', 'Location', 'Endpoint', 'PractitionerRole',
                              'OrganizationAffiliation', 'HealthcareService', 'InsurancePlan')
OPTION (NO_PERFORMANCE_SPOOL)
