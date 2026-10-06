INSERT INTO <<t:practitioner_role>> WITH (TABLOCK) (
    release_date, resource_id, ndjson_file_id, zst_file_id, last_updated,
    active, practitioner_id, organization_id, period_start, period_end,
    network_organization_id, accepting_patients)
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, r.last_updated,
       CAST(JSON_VALUE(r.resource, '$.active') AS bit),
       <<schema>>.ref_id(JSON_VALUE(r.resource, '$.practitioner.reference')),
       <<schema>>.ref_id(JSON_VALUE(r.resource, '$.organization.reference')),
       <<schema>>.fhir_ts(JSON_VALUE(r.resource, '$.period.start')),
       <<schema>>.fhir_ts(JSON_VALUE(r.resource, '$.period.end')),
       <<schema>>.ref_id(JSON_VALUE(net.ext, '$.valueReference.reference')),
       JSON_VALUE(acc.ext, '$.valueCodeableConcept.coding[0].code')
FROM <<raw>> r
OUTER APPLY <<schema>>.ext(r.resource, N'http://hl7.org/fhir/us/ndh/StructureDefinition/base-ext-network-reference') net
OUTER APPLY <<schema>>.ext(r.resource, N'http://hl7.org/fhir/us/ndh/StructureDefinition/base-ext-newpatients') np
OUTER APPLY <<schema>>.ext(np.ext, N'acceptingPatients') acc
WHERE r.resource_type = 'PractitionerRole'
GO
INSERT INTO <<t:practitioner_role_endpoint>> WITH (TABLOCK) (
    release_date, resource_id, ndjson_file_id, zst_file_id, seq, endpoint_id)
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, CAST(x.[key] AS int) + 1,
       <<schema>>.ref_id(JSON_VALUE(x.value, '$.reference'))
FROM <<raw>> r
CROSS APPLY OPENJSON(r.resource, '$.endpoint') x
WHERE r.resource_type = 'PractitionerRole'
GO
INSERT INTO <<t:practitioner_role_location>> WITH (TABLOCK) (
    release_date, resource_id, ndjson_file_id, zst_file_id, seq, location_id)
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, CAST(x.[key] AS int) + 1,
       <<schema>>.ref_id(JSON_VALUE(x.value, '$.reference'))
FROM <<raw>> r
CROSS APPLY OPENJSON(r.resource, '$.location') x
WHERE r.resource_type = 'PractitionerRole'
GO
INSERT INTO <<t:practitioner_role_specialty>> WITH (TABLOCK) (
    release_date, resource_id, ndjson_file_id, zst_file_id, seq, [system], code, display, [text])
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, CAST(x.[key] AS int) + 1,
       JSON_VALUE(x.value, '$.coding[0].system'), JSON_VALUE(x.value, '$.coding[0].code'),
       JSON_VALUE(x.value, '$.coding[0].display'), JSON_VALUE(x.value, '$.text')
FROM <<raw>> r
CROSS APPLY OPENJSON(r.resource, '$.specialty') x
WHERE r.resource_type = 'PractitionerRole'
GO
INSERT INTO <<t:practitioner_role_code>> WITH (TABLOCK) (
    release_date, resource_id, ndjson_file_id, zst_file_id, seq, [system], code, display, [text])
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, CAST(x.[key] AS int) + 1,
       JSON_VALUE(x.value, '$.coding[0].system'), JSON_VALUE(x.value, '$.coding[0].code'),
       JSON_VALUE(x.value, '$.coding[0].display'), JSON_VALUE(x.value, '$.text')
FROM <<raw>> r
CROSS APPLY OPENJSON(r.resource, '$.code') x
WHERE r.resource_type = 'PractitionerRole'
GO
INSERT INTO <<t:practitioner_role_telecom>> WITH (TABLOCK) (
    release_date, resource_id, ndjson_file_id, zst_file_id, seq, [system], [use], [value])
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, CAST(x.[key] AS int) + 1,
       JSON_VALUE(x.value, '$.system'), JSON_VALUE(x.value, '$.use'), JSON_VALUE(x.value, '$.value')
FROM <<raw>> r
CROSS APPLY OPENJSON(r.resource, '$.telecom') x
WHERE r.resource_type = 'PractitionerRole'
