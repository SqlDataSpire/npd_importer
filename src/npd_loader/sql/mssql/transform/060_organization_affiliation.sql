INSERT INTO <<t:organization_affiliation>> WITH (TABLOCK) (
    release_date, resource_id, ndjson_file_id, zst_file_id, last_updated,
    active, organization_id, participating_organization_id, role_code, role_display, role_text,
    period_start, period_end)
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, r.last_updated,
       CAST(JSON_VALUE(r.resource, '$.active') AS bit),
       <<schema>>.ref_id(JSON_VALUE(r.resource, '$.organization.reference')),
       <<schema>>.ref_id(JSON_VALUE(r.resource, '$.participatingOrganization.reference')),
       JSON_VALUE(r.resource, '$.code[0].coding[0].code'),
       JSON_VALUE(r.resource, '$.code[0].coding[0].display'),
       JSON_VALUE(r.resource, '$.code[0].text'),
       <<schema>>.fhir_ts(JSON_VALUE(r.resource, '$.period.start')),
       <<schema>>.fhir_ts(JSON_VALUE(r.resource, '$.period.end'))
FROM <<raw>> r
WHERE r.resource_type = 'OrganizationAffiliation'
GO
INSERT INTO <<t:organization_affiliation_network>> WITH (TABLOCK) (
    release_date, resource_id, ndjson_file_id, zst_file_id, seq, network_organization_id)
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, CAST(x.[key] AS int) + 1,
       <<schema>>.ref_id(JSON_VALUE(x.value, '$.reference'))
FROM <<raw>> r
CROSS APPLY OPENJSON(r.resource, '$.network') x
WHERE r.resource_type = 'OrganizationAffiliation'
