CREATE SCHEMA IF NOT EXISTS <<raw_schema>>;
CREATE SCHEMA IF NOT EXISTS <<schema>>;

-- One row per published release; written in the same transaction that attaches its partitions.
CREATE TABLE IF NOT EXISTS <<schema>>.release (
    release_date   date        PRIMARY KEY,
    import_run_id  integer     NOT NULL,
    published_at   timestamptz NOT NULL DEFAULT now()
);

-- "Organization/Organization-123" -> "Organization-123"
CREATE OR REPLACE FUNCTION <<schema>>.ref_id(ref text) RETURNS text
LANGUAGE sql IMMUTABLE AS $$ SELECT substring(ref from '([^/]+)$') $$;

-- First extension element with the given url, or NULL.
CREATE OR REPLACE FUNCTION <<schema>>.ext(resource jsonb, url text) RETURNS jsonb
LANGUAGE sql IMMUTABLE AS $$
    SELECT e FROM jsonb_array_elements(coalesce(resource->'extension', '[]'::jsonb)) AS e
    WHERE e->>'url' = url LIMIT 1
$$;

-- Join a JSON array of strings; NULL when empty.
CREATE OR REPLACE FUNCTION <<schema>>.join_text(arr jsonb, sep text) RETURNS text
LANGUAGE sql IMMUTABLE AS $$
    SELECT nullif(string_agg(t.v, sep ORDER BY t.ord), '')
    FROM jsonb_array_elements_text(coalesce(arr, '[]'::jsonb)) WITH ORDINALITY AS t(v, ord)
$$;

-- Value of the first identifier whose system is in `systems`.
CREATE OR REPLACE FUNCTION <<schema>>.identifier_value(resource jsonb, systems text[]) RETURNS text
LANGUAGE sql IMMUTABLE AS $$
    SELECT i.e->>'value'
    FROM jsonb_array_elements(coalesce(resource->'identifier', '[]'::jsonb)) WITH ORDINALITY AS i(e, ord)
    WHERE i.e->>'system' = ANY (systems)
    ORDER BY i.ord LIMIT 1
$$;

-- FHIR dateTime, which may be partial ("2020", "2020-05"), as timestamptz.
CREATE OR REPLACE FUNCTION <<schema>>.fhir_ts(v text) RETURNS timestamptz
LANGUAGE sql STABLE AS $$
    SELECT CASE
        WHEN v IS NULL THEN NULL
        WHEN v ~ '^\d{4}$' THEN (v || '-01-01')::timestamptz
        WHEN v ~ '^\d{4}-\d{2}$' THEN (v || '-01')::timestamptz
        ELSE v::timestamptz
    END
$$;
