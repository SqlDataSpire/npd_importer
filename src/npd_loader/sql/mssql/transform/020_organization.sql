INSERT INTO <<t:organization>> WITH (TABLOCK) (
    release_date, resource_id, ndjson_file_id, zst_file_id, last_updated,
    npi, pseudo_ein, name, active, type_code, type_display, part_of_organization_id, verification_status)
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, r.last_updated,
       npi.value, ein.value,
       JSON_VALUE(r.resource, '$.name'),
       CAST(JSON_VALUE(r.resource, '$.active') AS bit),
       JSON_VALUE(r.resource, '$.type[0].coding[0].code'),
       JSON_VALUE(r.resource, '$.type[0].coding[0].display'),
       <<schema>>.ref_id(JSON_VALUE(r.resource, '$.partOf.reference')),
       JSON_VALUE(vs.ext, '$.valueCodeableConcept.coding[0].code')
FROM <<raw>> r
OUTER APPLY <<schema>>.identifier_value(r.resource, N'["http://terminology.hl7.org/NamingSystem/npi","http://hl7.org/fhir/sid/us-npi"]') npi
OUTER APPLY <<schema>>.identifier_value(r.resource, N'["https://npd.cms.gov/fhir/sid/us-pseudo-ein"]') ein
OUTER APPLY <<schema>>.ext(r.resource, N'http://hl7.org/fhir/us/ndh/StructureDefinition/base-ext-verification-status') vs
WHERE r.resource_type = 'Organization'
GO
INSERT INTO <<t:organization_address>> WITH (TABLOCK) (
    release_date, resource_id, ndjson_file_id, zst_file_id, seq,
    [use], [type], line1, line2, extra_lines, city, state, postal_code, country)
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, CAST(x.[key] AS int) + 1,
       JSON_VALUE(x.value, '$.use'), JSON_VALUE(x.value, '$.type'),
       JSON_VALUE(x.value, '$.line[0]'), JSON_VALUE(x.value, '$.line[1]'), extra.txt,
       JSON_VALUE(x.value, '$.city'), JSON_VALUE(x.value, '$.state'),
       JSON_VALUE(x.value, '$.postalCode'), JSON_VALUE(x.value, '$.country')
FROM <<raw>> r
CROSS APPLY OPENJSON(r.resource, '$.address') x
OUTER APPLY <<schema>>.join_text(JSON_QUERY(x.value, '$.line'), N', ', 2) extra
WHERE r.resource_type = 'Organization'
GO
INSERT INTO <<t:organization_telecom>> WITH (TABLOCK) (
    release_date, resource_id, ndjson_file_id, zst_file_id, seq, [system], [use], [value])
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, CAST(x.[key] AS int) + 1,
       JSON_VALUE(x.value, '$.system'), JSON_VALUE(x.value, '$.use'), JSON_VALUE(x.value, '$.value')
FROM <<raw>> r
CROSS APPLY OPENJSON(r.resource, '$.telecom') x
WHERE r.resource_type = 'Organization'
GO
INSERT INTO <<t:organization_endpoint>> WITH (TABLOCK) (
    release_date, resource_id, ndjson_file_id, zst_file_id, seq, endpoint_id)
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, CAST(x.[key] AS int) + 1,
       <<schema>>.ref_id(JSON_VALUE(x.value, '$.reference'))
FROM <<raw>> r
CROSS APPLY OPENJSON(r.resource, '$.endpoint') x
WHERE r.resource_type = 'Organization'
