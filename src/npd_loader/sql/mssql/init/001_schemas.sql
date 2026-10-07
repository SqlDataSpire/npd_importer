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
-- The current version of every resource: its content hash decides new/changed/unchanged. Resources missing from a
-- release are kept (aging data); last_seen_release is the last release that contained them.
IF OBJECT_ID(<<s:schema>> + N'.resource_state', N'U') IS NULL
    CREATE TABLE <<schema>>.resource_state (
        resource_type     varchar(40)  NOT NULL,
        resource_id       varchar(128) NOT NULL,
        hash              char(40)     NOT NULL,
        last_updated      datetime2(3) NULL,
        release_date      date         NOT NULL,   -- release whose content is current
        run_id            int          NOT NULL,
        last_seen_release date         NOT NULL,
        last_seen_run_id  int          NOT NULL,
        CONSTRAINT pk_resource_state PRIMARY KEY CLUSTERED (resource_type, resource_id)
    ) WITH (DATA_COMPRESSION = PAGE)
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
