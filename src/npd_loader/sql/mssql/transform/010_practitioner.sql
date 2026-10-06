-- chunked: Practitioner
-- Parse once (the pattern of every transform script):
-- * One pass converts each raw document to nvarchar (one CAST per row) and parses it into a stage heap
--   (<<stage:...>>, dropped at the end) of scalars plus one small JSON fragment per repeating array. Every later
--   INSERT reads only the stage, never the raw document again.
-- * "-- chunked: <type>" (first line) makes the runner run the whole script once per chunk of resource_id range
--   (MssqlDialect.chunk_rows rows, <<chunk>> = the chunk's predicate), so the stage stays small enough to be read
--   back from memory; a full release's Practitioner stage would be ~35 GB.
-- * Child rows: OPENJSON(fragment) gives the 0-based [key] (seq = key + 1); an element with many fields is parsed once
--   with OPENJSON(element) WITH (...) (guarded to objects, type 5), one with few fields with JSON_VALUE.
-- * Extensions: one pass over the extension fragment with MAX(CASE WHEN url = ... END).
-- * "First element by array position" (the NPI, the official name): one MIN() over the fragment of
--   RIGHT(N'00000' + x.[key], 6) + value (collated Latin1_General_BIN2 like OPENJSON's key), decoded with STUFF();
--   values that may be NULL are stored as N'v' + value, or N'-' for NULL.
-- * Statements with per-row APPLYs carry OPTION (NO_PERFORMANCE_SPOOL): otherwise the optimizer sorts every row on
--   an nvarchar(max) fragment to feed a lazy spool, which made them 3-5x slower.
-- * Each type's identifiers go to the identifier table from its own stage (090_identifier.sql covers other types).
DROP TABLE IF EXISTS <<stage:practitioner>>
GO
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, r.last_updated, r.resource_type,
       j.active, j.gender, j.extension, j.identifier, j.name, j.address, j.telecom, j.qualification
INTO <<stage:practitioner>>
FROM <<raw>> r
CROSS APPLY OPENJSON(CAST(r.resource AS nvarchar(max))) WITH (
    active nvarchar(4000) '$.active',
    gender nvarchar(4000) '$.gender',
    extension nvarchar(max) AS JSON,
    identifier nvarchar(max) AS JSON,
    name nvarchar(max) AS JSON,
    address nvarchar(max) AS JSON,
    telecom nvarchar(max) AS JSON,
    qualification nvarchar(max) AS JSON) j
WHERE r.resource_type = 'Practitioner' AND <<chunk>>
GO
INSERT INTO <<t:practitioner>> WITH (TABLOCK) (
    release_date, resource_id, ndjson_file_id, zst_file_id, last_updated,
    npi, active, gender, name_family, name_given, name_prefix, name_suffix,
    identity_verified, medicare_enrolled, in_hhs_exclusion_list, aligned_with_data_network)
SELECT s.release_date, s.resource_id, s.ndjson_file_id, s.zst_file_id, s.last_updated,
       CASE WHEN SUBSTRING(i.npi, 7, 1) = N'v' THEN STUFF(i.npi, 1, 7, N'') END,
       CAST(s.active AS bit),
       s.gender,
       nm.family, g.txt, pre.txt, suf.txt,
       CAST(e.identity_verified AS bit),
       CAST(e.medicare_enrolled AS bit),
       CAST(e.in_hhs_exclusion_list AS bit),
       CAST(e.aligned_with_data_network AS bit)
FROM <<stage:practitioner>> s
-- the value of the first identifier with an NPI system
OUTER APPLY (SELECT MIN(CASE WHEN JSON_VALUE(x.value, '$.system') IN (N'http://terminology.hl7.org/NamingSystem/npi',
                                                                      N'http://hl7.org/fhir/sid/us-npi')
                             THEN RIGHT(N'00000' + x.[key], 6)
                                  + ISNULL(N'v' + JSON_VALUE(x.value, '$.value'), N'-') COLLATE Latin1_General_BIN2
                        END) AS npi
             FROM OPENJSON(s.identifier) x) i
-- the official name, else a name with a use, else the first name (Postgres: (use = 'official') DESC NULLS LAST)
OUTER APPLY (SELECT STUFF(MIN(CASE WHEN u.[use] = N'official' THEN N'0' WHEN u.[use] IS NOT NULL THEN N'1' ELSE N'2' END
                              + RIGHT(N'00000' + x.[key], 6) + x.value COLLATE Latin1_General_BIN2), 1, 7, N'') AS e
             FROM OPENJSON(s.name) x CROSS APPLY (SELECT JSON_VALUE(x.value, '$.use') AS [use]) u) n
OUTER APPLY OPENJSON(n.e) WITH (family nvarchar(4000), given nvarchar(max) AS JSON, prefix nvarchar(max) AS JSON,
                                suffix nvarchar(max) AS JSON) nm
OUTER APPLY <<schema>>.join_text(nm.given, N' ', 0) g
OUTER APPLY <<schema>>.join_text(nm.prefix, N' ', 0) pre
OUTER APPLY <<schema>>.join_text(nm.suffix, N' ', 0) suf
OUTER APPLY (
    SELECT MAX(CASE WHEN x.url = N'http://hl7.org/fhir/us/ndh/StructureDefinition/base-ext-cms-identity-verified'
                    THEN x.valueBoolean END) AS identity_verified,
           MAX(CASE WHEN x.url = N'http://hl7.org/fhir/us/ndh/StructureDefinition/base-ext-cms_medicare_enrollment'
                    THEN x.valueBoolean END) AS medicare_enrolled,
           MAX(CASE WHEN x.url = N'http://hl7.org/fhir/us/ndh/StructureDefinition/base-ext-hhs-in-exclusion-list'
                    THEN x.valueBoolean END) AS in_hhs_exclusion_list,
           MAX(CASE WHEN x.url
                         = N'http://hl7.org/fhir/us/ndh/StructureDefinition/base-ext-cms_aligned_with_data_network'
                    THEN x.valueBoolean END) AS aligned_with_data_network
    FROM OPENJSON(s.extension) WITH (url nvarchar(4000), valueBoolean nvarchar(4000)) x) e
OPTION (NO_PERFORMANCE_SPOOL)
GO
INSERT INTO <<t:practitioner_name>> WITH (TABLOCK) (
    release_date, resource_id, ndjson_file_id, zst_file_id, seq,
    [use], family, given, prefix, suffix, period_start, period_end)
SELECT s.release_date, s.resource_id, s.ndjson_file_id, s.zst_file_id, CAST(x.[key] AS int) + 1,
       v.[use], v.family, g.txt, pre.txt, suf.txt,
       <<schema>>.fhir_ts(v.period_start), <<schema>>.fhir_ts(v.period_end)
FROM <<stage:practitioner>> s
CROSS APPLY OPENJSON(s.name) x
OUTER APPLY OPENJSON(CASE WHEN x.type = 5 THEN x.value END) WITH (
    [use] nvarchar(4000), family nvarchar(4000),
    given nvarchar(max) AS JSON, prefix nvarchar(max) AS JSON, suffix nvarchar(max) AS JSON,
    period_start nvarchar(4000) '$.period.start', period_end nvarchar(4000) '$.period.end') v
OUTER APPLY <<schema>>.join_text(v.given, N' ', 0) g
OUTER APPLY <<schema>>.join_text(v.prefix, N' ', 0) pre
OUTER APPLY <<schema>>.join_text(v.suffix, N' ', 0) suf
OPTION (NO_PERFORMANCE_SPOOL)
GO
INSERT INTO <<t:practitioner_address>> WITH (TABLOCK) (
    release_date, resource_id, ndjson_file_id, zst_file_id, seq,
    [use], [type], line1, line2, extra_lines, city, state, postal_code, country)
SELECT s.release_date, s.resource_id, s.ndjson_file_id, s.zst_file_id, CAST(x.[key] AS int) + 1,
       v.[use], v.[type], v.line1, v.line2, extra.txt, v.city, v.state, v.postal_code, v.country
FROM <<stage:practitioner>> s
CROSS APPLY OPENJSON(s.address) x
OUTER APPLY OPENJSON(CASE WHEN x.type = 5 THEN x.value END) WITH (
    [use] nvarchar(4000), [type] nvarchar(4000),
    line1 nvarchar(4000) '$.line[0]', line2 nvarchar(4000) '$.line[1]', line nvarchar(max) AS JSON,
    city nvarchar(4000), state nvarchar(4000), postal_code nvarchar(4000) '$.postalCode', country nvarchar(4000)) v
OUTER APPLY <<schema>>.join_text(v.line, N', ', 2) extra
OPTION (NO_PERFORMANCE_SPOOL)
GO
INSERT INTO <<t:practitioner_telecom>> WITH (TABLOCK) (
    release_date, resource_id, ndjson_file_id, zst_file_id, seq, [system], [use], [value])
SELECT s.release_date, s.resource_id, s.ndjson_file_id, s.zst_file_id, CAST(x.[key] AS int) + 1,
       JSON_VALUE(x.value, '$.system'), JSON_VALUE(x.value, '$.use'), JSON_VALUE(x.value, '$.value')
FROM <<stage:practitioner>> s
CROSS APPLY OPENJSON(s.telecom) x
GO
INSERT INTO <<t:practitioner_qualification>> WITH (TABLOCK) (
    release_date, resource_id, ndjson_file_id, zst_file_id, seq,
    code_system, code, code_display, code_text, identifier_value, identifier_type_code, issuer_organization_id)
SELECT s.release_date, s.resource_id, s.ndjson_file_id, s.zst_file_id, CAST(x.[key] AS int) + 1,
       v.code_system, v.code, v.code_display, v.code_text, v.identifier_value, v.identifier_type_code,
       <<schema>>.ref_id(v.issuer)
FROM <<stage:practitioner>> s
CROSS APPLY OPENJSON(s.qualification) x
OUTER APPLY OPENJSON(CASE WHEN x.type = 5 THEN x.value END) WITH (
    code_system nvarchar(4000) '$.code.coding[0].system',
    code nvarchar(4000) '$.code.coding[0].code',
    code_display nvarchar(4000) '$.code.coding[0].display',
    code_text nvarchar(4000) '$.code.text',
    identifier_value nvarchar(4000) '$.identifier[0].value',
    identifier_type_code nvarchar(4000) '$.identifier[0].type.coding[0].code',
    issuer nvarchar(4000) '$.issuer.reference') v
OPTION (NO_PERFORMANCE_SPOOL)
GO
INSERT INTO <<t:identifier>> WITH (TABLOCK) (
    release_date, resource_id, ndjson_file_id, zst_file_id, resource_type, seq,
    [system], [value], [use], type_code, type_text, period_start, period_end)
SELECT s.release_date, s.resource_id, s.ndjson_file_id, s.zst_file_id, s.resource_type, CAST(x.[key] AS int) + 1,
       v.[system], v.[value], v.[use], v.type_code, v.type_text,
       <<schema>>.fhir_ts(v.period_start), <<schema>>.fhir_ts(v.period_end)
FROM <<stage:practitioner>> s
CROSS APPLY OPENJSON(s.identifier) x
OUTER APPLY OPENJSON(CASE WHEN x.type = 5 THEN x.value END) WITH (
    [system] nvarchar(4000), [value] nvarchar(4000), [use] nvarchar(4000),
    type_code nvarchar(4000) '$.type.coding[0].code', type_text nvarchar(4000) '$.type.text',
    period_start nvarchar(4000) '$.period.start', period_end nvarchar(4000) '$.period.end') v
OPTION (NO_PERFORMANCE_SPOOL)
GO
DROP TABLE <<stage:practitioner>>
