-- Post-deployment schema changes. init-db runs this file last, on every run.
--
-- Convention: once deployed, NEVER edit the CREATE TABLE statements in 00x files to change an existing table (the
-- OBJECT_ID guard skips tables that already exist). Append guarded statements here, separated by GO, e.g.:
--
--   IF COL_LENGTH(<<s:schema>> + N'.practitioner', N'new_column') IS NULL
--       ALTER TABLE <<schema>>.practitioner ADD new_column nvarchar(1000) NULL
--   GO
--
-- Columns added to a partitioned parent apply to every partition. Standalone tables are created from the parent,
-- so they get new columns too. init-db recreates the v_* views afterwards.
--
IF COL_LENGTH(<<s:schema>> + N'.release', N'new_resources') IS NULL
    ALTER TABLE <<schema>>.release ADD new_resources int NULL, changed_resources int NULL,
        unchanged_resources int NULL, not_seen_resources int NULL
