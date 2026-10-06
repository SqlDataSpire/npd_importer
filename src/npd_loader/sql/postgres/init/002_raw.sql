-- Each release partition is itself partitioned BY LIST (resource_type); see raw_load.py.
CREATE TABLE IF NOT EXISTS <<raw_schema>>.resource (
    release_date    date        NOT NULL,
    resource_type   text        NOT NULL,
    resource_id     text        NOT NULL,
    last_updated    timestamptz,
    ndjson_file_id  integer     NOT NULL,
    zst_file_id     integer     NOT NULL,
    line_number     bigint      NOT NULL,
    resource        jsonb       NOT NULL
) PARTITION BY LIST (release_date);

CREATE UNIQUE INDEX IF NOT EXISTS resource_key ON <<raw_schema>>.resource (release_date, resource_type, resource_id);
