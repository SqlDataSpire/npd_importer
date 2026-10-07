-- Post-deployment schema changes. init-db runs this file last, on every run.
--
-- Convention: once deployed, NEVER edit the CREATE TABLE statements in 00x files to change an existing table (the
-- OBJECT_ID guard skips tables that already exist). Append guarded statements here, separated by GO, e.g.:
--
--   IF COL_LENGTH(<<s:schema>> + N'.practitioner', N'new_column') IS NULL
--       ALTER TABLE <<schema>>.practitioner ADD new_column nvarchar(1000) NULL
--   GO
--
-- init-db recreates the v_* views afterwards. A column added to a permanent table reaches staging automatically on
-- init-db (a staging table whose columns differ from the spec is dropped and recreated). Existing rows only get the new
-- column's values after a SPEC_VERSION bump (flatten/specs.py), which makes the next import rewrite every resource.
-- 2026-10-07: surrogate keys were introduced by editing 001/003 in place (nothing deployed yet); from now on follow the
-- convention above.
--
IF COL_LENGTH(<<s:schema>> + N'.release', N'new_resources') IS NULL
    ALTER TABLE <<schema>>.release ADD new_resources int NULL, changed_resources int NULL,
        unchanged_resources int NULL, not_seen_resources int NULL
