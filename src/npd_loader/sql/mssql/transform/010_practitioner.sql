INSERT INTO <<t:practitioner>> WITH (TABLOCK) (
    release_date, resource_id, ndjson_file_id, zst_file_id, last_updated,
    npi, active, gender, name_family, name_given, name_prefix, name_suffix,
    identity_verified, medicare_enrolled, in_hhs_exclusion_list, aligned_with_data_network)
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, r.last_updated,
       npi.value,
       CAST(JSON_VALUE(r.resource, '$.active') AS bit),
       JSON_VALUE(r.resource, '$.gender'),
       JSON_VALUE(n.e, '$.family'),
       g.txt, pre.txt, suf.txt,
       CAST(JSON_VALUE(e1.ext, '$.valueBoolean') AS bit),
       CAST(JSON_VALUE(e2.ext, '$.valueBoolean') AS bit),
       CAST(JSON_VALUE(e3.ext, '$.valueBoolean') AS bit),
       CAST(JSON_VALUE(e4.ext, '$.valueBoolean') AS bit)
FROM <<raw>> r
OUTER APPLY <<schema>>.identifier_value(r.resource, N'["http://terminology.hl7.org/NamingSystem/npi","http://hl7.org/fhir/sid/us-npi"]') npi
-- the official name, else a name with a use, else the first name (Postgres: (use = 'official') DESC NULLS LAST)
OUTER APPLY (SELECT TOP 1 x.value AS e FROM OPENJSON(r.resource, '$.name') x
             ORDER BY CASE WHEN JSON_VALUE(x.value, '$.use') = 'official' THEN 0
                           WHEN JSON_VALUE(x.value, '$.use') IS NOT NULL THEN 1 ELSE 2 END,
                      CAST(x.[key] AS int)) n
OUTER APPLY <<schema>>.join_text(JSON_QUERY(n.e, '$.given'), N' ', 0) g
OUTER APPLY <<schema>>.join_text(JSON_QUERY(n.e, '$.prefix'), N' ', 0) pre
OUTER APPLY <<schema>>.join_text(JSON_QUERY(n.e, '$.suffix'), N' ', 0) suf
OUTER APPLY <<schema>>.ext(r.resource, N'http://hl7.org/fhir/us/ndh/StructureDefinition/base-ext-cms-identity-verified') e1
OUTER APPLY <<schema>>.ext(r.resource, N'http://hl7.org/fhir/us/ndh/StructureDefinition/base-ext-cms_medicare_enrollment') e2
OUTER APPLY <<schema>>.ext(r.resource, N'http://hl7.org/fhir/us/ndh/StructureDefinition/base-ext-hhs-in-exclusion-list') e3
OUTER APPLY <<schema>>.ext(r.resource, N'http://hl7.org/fhir/us/ndh/StructureDefinition/base-ext-cms_aligned_with_data_network') e4
WHERE r.resource_type = 'Practitioner'
GO
INSERT INTO <<t:practitioner_name>> WITH (TABLOCK) (
    release_date, resource_id, ndjson_file_id, zst_file_id, seq,
    [use], family, given, prefix, suffix, period_start, period_end)
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, CAST(x.[key] AS int) + 1,
       JSON_VALUE(x.value, '$.use'), JSON_VALUE(x.value, '$.family'), g.txt, pre.txt, suf.txt,
       <<schema>>.fhir_ts(JSON_VALUE(x.value, '$.period.start')),
       <<schema>>.fhir_ts(JSON_VALUE(x.value, '$.period.end'))
FROM <<raw>> r
CROSS APPLY OPENJSON(r.resource, '$.name') x
OUTER APPLY <<schema>>.join_text(JSON_QUERY(x.value, '$.given'), N' ', 0) g
OUTER APPLY <<schema>>.join_text(JSON_QUERY(x.value, '$.prefix'), N' ', 0) pre
OUTER APPLY <<schema>>.join_text(JSON_QUERY(x.value, '$.suffix'), N' ', 0) suf
WHERE r.resource_type = 'Practitioner'
GO
INSERT INTO <<t:practitioner_address>> WITH (TABLOCK) (
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
WHERE r.resource_type = 'Practitioner'
GO
INSERT INTO <<t:practitioner_telecom>> WITH (TABLOCK) (
    release_date, resource_id, ndjson_file_id, zst_file_id, seq, [system], [use], [value])
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, CAST(x.[key] AS int) + 1,
       JSON_VALUE(x.value, '$.system'), JSON_VALUE(x.value, '$.use'), JSON_VALUE(x.value, '$.value')
FROM <<raw>> r
CROSS APPLY OPENJSON(r.resource, '$.telecom') x
WHERE r.resource_type = 'Practitioner'
GO
INSERT INTO <<t:practitioner_qualification>> WITH (TABLOCK) (
    release_date, resource_id, ndjson_file_id, zst_file_id, seq,
    code_system, code, code_display, code_text, identifier_value, identifier_type_code, issuer_organization_id)
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, CAST(x.[key] AS int) + 1,
       JSON_VALUE(x.value, '$.code.coding[0].system'),
       JSON_VALUE(x.value, '$.code.coding[0].code'),
       JSON_VALUE(x.value, '$.code.coding[0].display'),
       JSON_VALUE(x.value, '$.code.text'),
       JSON_VALUE(x.value, '$.identifier[0].value'),
       JSON_VALUE(x.value, '$.identifier[0].type.coding[0].code'),
       <<schema>>.ref_id(JSON_VALUE(x.value, '$.issuer.reference'))
FROM <<raw>> r
CROSS APPLY OPENJSON(r.resource, '$.qualification') x
WHERE r.resource_type = 'Practitioner'
