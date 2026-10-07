IF SCHEMA_ID(<<s:schema>>) IS NULL
BEGIN
    DECLARE @sql nvarchar(400) = N'CREATE SCHEMA ' + QUOTENAME(<<s:schema>>)
    EXEC (@sql)
END
GO
IF SCHEMA_ID(<<s:stage_schema>>) IS NULL
BEGIN
    DECLARE @sql nvarchar(400) = N'CREATE SCHEMA ' + QUOTENAME(<<s:stage_schema>>)
    EXEC (@sql)
END
GO
-- One row per applied release, written in the delta transaction.
IF OBJECT_ID(<<s:schema>> + N'.release', N'U') IS NULL
    CREATE TABLE <<schema>>.release (
        release_date        date         NOT NULL CONSTRAINT pk_release PRIMARY KEY,
        import_run_id       int          NOT NULL,
        published_at        datetime2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
        new_resources       int          NULL,
        changed_resources   int          NULL,
        unchanged_resources int          NULL,
        not_seen_resources  int          NULL
    )
GO
-- Refuse to run over the pre-key schema (text resource ids): drop and re-create that schema instead.
IF COL_LENGTH(<<s:schema>> + N'.resource_state', N'resource_type') IS NOT NULL
    THROW 50001, 'resource_state predates surrogate keys: drop the data schema and run init-db again', 1
GO
IF OBJECT_ID(<<s:schema>> + N'.resource_type', N'U') IS NULL
    CREATE TABLE <<schema>>.resource_type (
        resource_type_id tinyint     NOT NULL CONSTRAINT pk_resource_type PRIMARY KEY,
        name             varchar(40) NOT NULL CONSTRAINT ux_resource_type_name UNIQUE
    )
GO
INSERT INTO <<schema>>.resource_type (resource_type_id, name)
SELECT v.id, v.name FROM (VALUES (1, 'Practitioner'), (2, 'Organization'), (3, 'Location'), (4, 'Endpoint'),
    (5, 'PractitionerRole'), (6, 'OrganizationAffiliation'), (7, 'HealthcareService'), (8, 'InsurancePlan')) v (id, name)
WHERE NOT EXISTS (SELECT 1 FROM <<schema>>.resource_type t WHERE t.resource_type_id = v.id)
GO
-- Key registry and current state of every resource. A key is assigned the first time an id is seen, as a resource or
-- as a reference target, and never changes or gets reused. An id that has only been referenced has NULL hash,
-- last_updated, release_date, run_id and last_seen_* (no data rows). Resources missing from a release are kept (aging
-- data); last_seen_release is the last release that contained them.
IF OBJECT_ID(<<s:schema>> + N'.resource_state', N'U') IS NULL
    CREATE TABLE <<schema>>.resource_state (
        resource_key      int          IDENTITY(1, 1) NOT NULL CONSTRAINT pk_resource_state PRIMARY KEY CLUSTERED,
        resource_type_id  tinyint      NOT NULL,
        resource_id       varchar(128) NOT NULL,   -- natural id without its 'Type-' prefix
        hash              binary(20)   NULL,       -- SHA-1 of SPEC_VERSION + the ndjson line
        last_updated      datetime2(3) NULL,
        release_date      date         NULL,       -- release whose content is current
        run_id            int          NULL,
        last_seen_release date         NULL,
        last_seen_run_id  int          NULL
    ) WITH (DATA_COMPRESSION = PAGE)
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(<<s:schema>> + N'.resource_state') AND name = N'ux_resource_state_id')
    CREATE UNIQUE INDEX ux_resource_state_id ON <<schema>>.resource_state (resource_type_id, resource_id) WITH (DATA_COMPRESSION = PAGE)
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(<<s:schema>> + N'.resource_state') AND name = N'resource_state_last_seen')
    CREATE INDEX resource_state_last_seen ON <<schema>>.resource_state (last_seen_release) WITH (DATA_COMPRESSION = PAGE)
GO
-- Fixed staging for resource hashes (the 26 table stages are created by init_db from the specs).
IF OBJECT_ID(<<s:stage_schema>> + N'.resource_hash', N'U') IS NULL
    CREATE TABLE <<stage_schema>>.resource_hash (
        resource_type  varchar(40)  NOT NULL,
        resource_id    varchar(128) NOT NULL,
        hash           char(40)     NOT NULL,
        last_updated   datetime2(3) NULL,
        release_date   date         NOT NULL,
        ndjson_file_id int          NOT NULL,
        line_number    bigint       NOT NULL
    )
GO
-- Phase 1 helper functions are no longer used.
DROP FUNCTION IF EXISTS <<schema>>.ref_id
GO
DROP FUNCTION IF EXISTS <<schema>>.fhir_ts
GO
DROP FUNCTION IF EXISTS <<schema>>.join_text
GO
DROP FUNCTION IF EXISTS <<schema>>.ext
GO
DROP FUNCTION IF EXISTS <<schema>>.identifier_value
