IF SCHEMA_ID(<<s:raw_schema>>) IS NULL
BEGIN
    DECLARE @sql nvarchar(400) = N'CREATE SCHEMA ' + QUOTENAME(<<s:raw_schema>>)
    EXEC (@sql)
END
GO
IF SCHEMA_ID(<<s:schema>>) IS NULL
BEGIN
    DECLARE @sql nvarchar(400) = N'CREATE SCHEMA ' + QUOTENAME(<<s:schema>>)
    EXEC (@sql)
END
GO
-- One partition function and scheme per schema; boundaries are added at publish (SPLIT) and removed by retention (MERGE).
IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = <<pfname:raw_schema>>)
    CREATE PARTITION FUNCTION <<pf:raw_schema>> (date) AS RANGE RIGHT FOR VALUES ()
GO
IF NOT EXISTS (SELECT 1 FROM sys.partition_schemes WHERE name = <<psname:raw_schema>>)
    CREATE PARTITION SCHEME <<ps:raw_schema>> AS PARTITION <<pf:raw_schema>> ALL TO ([PRIMARY])
GO
IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = <<pfname:schema>>)
    CREATE PARTITION FUNCTION <<pf:schema>> (date) AS RANGE RIGHT FOR VALUES ()
GO
IF NOT EXISTS (SELECT 1 FROM sys.partition_schemes WHERE name = <<psname:schema>>)
    CREATE PARTITION SCHEME <<ps:schema>> AS PARTITION <<pf:schema>> ALL TO ([PRIMARY])
GO
-- One row per published release; written in the same transaction that switches its partitions in.
IF OBJECT_ID(<<s:schema>> + N'.release', N'U') IS NULL
    CREATE TABLE <<schema>>.release (
        release_date  date         NOT NULL CONSTRAINT pk_release PRIMARY KEY,
        import_run_id int          NOT NULL,
        published_at  datetime2(3) NOT NULL DEFAULT SYSUTCDATETIME()
    )
GO
-- "Organization/Organization-123" -> "Organization-123". Scalar and inlinable (compatibility level 150).
CREATE OR ALTER FUNCTION <<schema>>.ref_id (@ref nvarchar(4000)) RETURNS varchar(128)
WITH SCHEMABINDING AS
BEGIN
    RETURN RIGHT(@ref, CHARINDEX(N'/', REVERSE(@ref) + N'/') - 1)
END
GO
-- FHIR dateTime, which may be partial ("2020", "2020-05"), as UTC datetime2(3).
CREATE OR ALTER FUNCTION <<schema>>.fhir_ts (@v nvarchar(100)) RETURNS datetime2(3)
WITH SCHEMABINDING AS
BEGIN
    RETURN CASE
        WHEN @v IS NULL THEN NULL
        WHEN @v LIKE N'[0-9][0-9][0-9][0-9]' THEN CAST(@v + N'-01-01' AS datetime2(3))
        WHEN @v LIKE N'[0-9][0-9][0-9][0-9]-[0-9][0-9]' THEN CAST(@v + N'-01' AS datetime2(3))
        WHEN @v LIKE N'[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]' THEN CAST(@v AS datetime2(3))
        ELSE CAST(SWITCHOFFSET(CAST(@v AS datetimeoffset(7)), '+00:00') AS datetime2(3))
    END
END
GO
-- First extension element with the given url (or no row).
CREATE OR ALTER FUNCTION <<schema>>.ext (@resource nvarchar(max), @url nvarchar(4000)) RETURNS TABLE AS RETURN
    SELECT TOP 1 e.value AS ext
    FROM OPENJSON(@resource, N'$.extension') e
    WHERE JSON_VALUE(e.value, N'$.url') = @url
    ORDER BY CAST(e.[key] AS int)
GO
-- Value of the first identifier whose system is in the JSON array @systems.
CREATE OR ALTER FUNCTION <<schema>>.identifier_value (@resource nvarchar(max), @systems nvarchar(4000))
RETURNS TABLE AS RETURN
    SELECT TOP 1 JSON_VALUE(i.value, N'$.value') AS value
    FROM OPENJSON(@resource, N'$.identifier') i
    WHERE JSON_VALUE(i.value, N'$.system') IN (SELECT s.value FROM OPENJSON(@systems) s)
    ORDER BY CAST(i.[key] AS int)
GO
-- Join a JSON array of strings from position @skip (0-based); NULL when empty.
CREATE OR ALTER FUNCTION <<schema>>.join_text (@arr nvarchar(max), @sep nvarchar(10), @skip int)
RETURNS TABLE AS RETURN
    -- NULLIF repeats its first argument internally, so the ordered aggregate lives in a derived table (error 8711).
    SELECT NULLIF(a.joined, N'') AS txt
    FROM (SELECT STRING_AGG(CAST(t.value AS nvarchar(max)), @sep) WITHIN GROUP (ORDER BY CAST(t.[key] AS int)) AS joined
          FROM OPENJSON(@arr) t
          WHERE CAST(t.[key] AS int) >= @skip) a
