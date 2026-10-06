INSERT INTO <<t:location>> WITH (TABLOCK) (
    release_date, resource_id, ndjson_file_id, zst_file_id, last_updated,
    status, name, description, mode, address_use, address_type, line1, line2, extra_lines,
    city, state, postal_code, country, latitude, longitude, managing_organization_id)
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, r.last_updated,
       JSON_VALUE(r.resource, '$.status'), JSON_VALUE(r.resource, '$.name'),
       JSON_VALUE(r.resource, '$.description'), JSON_VALUE(r.resource, '$.mode'),
       JSON_VALUE(r.resource, '$.address.use'), JSON_VALUE(r.resource, '$.address.type'),
       JSON_VALUE(r.resource, '$.address.line[0]'), JSON_VALUE(r.resource, '$.address.line[1]'), extra.txt,
       JSON_VALUE(r.resource, '$.address.city'), JSON_VALUE(r.resource, '$.address.state'),
       JSON_VALUE(r.resource, '$.address.postalCode'), JSON_VALUE(r.resource, '$.address.country'),
       CAST(JSON_VALUE(r.resource, '$.position.latitude') AS float),
       CAST(JSON_VALUE(r.resource, '$.position.longitude') AS float),
       <<schema>>.ref_id(JSON_VALUE(r.resource, '$.managingOrganization.reference'))
FROM <<raw>> r
OUTER APPLY <<schema>>.join_text(JSON_QUERY(r.resource, '$.address.line'), N', ', 2) extra
WHERE r.resource_type = 'Location'
GO
INSERT INTO <<t:location_telecom>> WITH (TABLOCK) (
    release_date, resource_id, ndjson_file_id, zst_file_id, seq, [system], [use], [value])
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, CAST(x.[key] AS int) + 1,
       JSON_VALUE(x.value, '$.system'), JSON_VALUE(x.value, '$.use'), JSON_VALUE(x.value, '$.value')
FROM <<raw>> r
CROSS APPLY OPENJSON(r.resource, '$.telecom') x
WHERE r.resource_type = 'Location'
