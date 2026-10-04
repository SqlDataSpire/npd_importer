-- Assumed DDL for css_catalog_local (R2): the real server at 192.10.0.6 was unreachable from this
-- machine, so this is the brief's assumed schema, written verbatim. Replace with a real pg_dump
-- (per task-4-brief.md Step 1) before deploying; Task 19 repeats this check.
CREATE TABLE public.master_warehouse_run (
    run_id            serial PRIMARY KEY,
    parent_run_id     integer,
    project           varchar(100),
    run_type          varchar(200),
    run_class         varchar(100),
    run_description   varchar(500),
    date_started      timestamp DEFAULT now(),
    date_completed    timestamp,
    completion_status varchar(50),
    result            text,
    xml_config        xml,
    xml_output        xml
);

CREATE TABLE public.data_file (
    id                  serial PRIMARY KEY,
    run_id              integer REFERENCES public.master_warehouse_run (run_id),
    file_set            varchar(100),
    file_type           varchar(50),
    source_uri          text,
    source_version_name varchar(200),
    source_version_num  varchar(100),
    file_name           text,
    file_rel_path       text,
    run_type_root_dir   text,
    parent_file         integer REFERENCES public.data_file (id),
    file_size           bigint,
    file_hash           varchar(128),
    date_modified       timestamp,
    date_created        timestamp,
    date_loaded         timestamp,
    exceptions          text
);
