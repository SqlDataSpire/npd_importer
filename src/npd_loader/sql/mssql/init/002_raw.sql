-- Raw resources. Each release is one standalone table switched into this parent at publish.
IF OBJECT_ID(<<s:raw_schema>> + N'.resource', N'U') IS NULL
    CREATE TABLE <<raw_schema>>.resource (
        release_date   date          NOT NULL,
        resource_type  varchar(40)   NOT NULL,
        resource_id    varchar(128)  NOT NULL,
        last_updated   datetime2(3)  NULL,
        ndjson_file_id int           NOT NULL,
        zst_file_id    int           NOT NULL,
        line_number    bigint        NOT NULL,
        resource       varchar(max)  COLLATE Latin1_General_100_CI_AS_SC_UTF8 NOT NULL
    ) ON <<ps:raw_schema>> (release_date) WITH (DATA_COMPRESSION = PAGE)
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(<<s:raw_schema>> + N'.resource') AND name = N'resource_key')
    CREATE UNIQUE INDEX resource_key ON <<raw_schema>>.resource (release_date, resource_type, resource_id)
    WITH (DATA_COMPRESSION = PAGE)
