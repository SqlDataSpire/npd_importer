-- Every table: release_date, resource_id, ndjson_file_id, zst_file_id first; child tables add seq (1-based
-- position in the repeating element). Column lists come from profiling release 2026-09-29.

CREATE TABLE IF NOT EXISTS <<schema>>.practitioner (
    release_date date NOT NULL, resource_id text NOT NULL, ndjson_file_id integer NOT NULL, zst_file_id integer NOT NULL,
    last_updated timestamptz,
    npi text,
    active boolean,
    gender text,
    name_family text,
    name_given text,
    name_prefix text,
    name_suffix text,
    identity_verified boolean,
    medicare_enrolled boolean,
    in_hhs_exclusion_list boolean,
    aligned_with_data_network boolean
) PARTITION BY LIST (release_date);
CREATE UNIQUE INDEX IF NOT EXISTS practitioner_key ON <<schema>>.practitioner (release_date, resource_id);
CREATE INDEX IF NOT EXISTS practitioner_npi ON <<schema>>.practitioner (release_date, npi);

CREATE TABLE IF NOT EXISTS <<schema>>.practitioner_name (
    release_date date NOT NULL, resource_id text NOT NULL, ndjson_file_id integer NOT NULL, zst_file_id integer NOT NULL,
    seq integer NOT NULL,
    use text, family text, given text, prefix text, suffix text,
    period_start timestamptz, period_end timestamptz
) PARTITION BY LIST (release_date);
CREATE UNIQUE INDEX IF NOT EXISTS practitioner_name_key ON <<schema>>.practitioner_name (release_date, resource_id, seq);

CREATE TABLE IF NOT EXISTS <<schema>>.practitioner_address (
    release_date date NOT NULL, resource_id text NOT NULL, ndjson_file_id integer NOT NULL, zst_file_id integer NOT NULL,
    seq integer NOT NULL,
    use text, type text, line1 text, line2 text, extra_lines text,
    city text, state text, postal_code text, country text
) PARTITION BY LIST (release_date);
CREATE UNIQUE INDEX IF NOT EXISTS practitioner_address_key ON <<schema>>.practitioner_address (release_date, resource_id, seq);

CREATE TABLE IF NOT EXISTS <<schema>>.practitioner_telecom (
    release_date date NOT NULL, resource_id text NOT NULL, ndjson_file_id integer NOT NULL, zst_file_id integer NOT NULL,
    seq integer NOT NULL,
    system text, use text, value text
) PARTITION BY LIST (release_date);
CREATE UNIQUE INDEX IF NOT EXISTS practitioner_telecom_key ON <<schema>>.practitioner_telecom (release_date, resource_id, seq);

CREATE TABLE IF NOT EXISTS <<schema>>.practitioner_qualification (
    release_date date NOT NULL, resource_id text NOT NULL, ndjson_file_id integer NOT NULL, zst_file_id integer NOT NULL,
    seq integer NOT NULL,
    code_system text, code text, code_display text, code_text text,
    identifier_value text, identifier_type_code text, issuer_organization_id text
) PARTITION BY LIST (release_date);
CREATE UNIQUE INDEX IF NOT EXISTS practitioner_qualification_key ON <<schema>>.practitioner_qualification (release_date, resource_id, seq);
CREATE INDEX IF NOT EXISTS practitioner_qualification_code ON <<schema>>.practitioner_qualification (release_date, code);

CREATE TABLE IF NOT EXISTS <<schema>>.organization (
    release_date date NOT NULL, resource_id text NOT NULL, ndjson_file_id integer NOT NULL, zst_file_id integer NOT NULL,
    last_updated timestamptz,
    npi text,
    pseudo_ein text,
    name text,
    active boolean,
    type_code text,
    type_display text,
    part_of_organization_id text,
    verification_status text
) PARTITION BY LIST (release_date);
CREATE UNIQUE INDEX IF NOT EXISTS organization_key ON <<schema>>.organization (release_date, resource_id);
CREATE INDEX IF NOT EXISTS organization_npi ON <<schema>>.organization (release_date, npi);

CREATE TABLE IF NOT EXISTS <<schema>>.organization_address (
    release_date date NOT NULL, resource_id text NOT NULL, ndjson_file_id integer NOT NULL, zst_file_id integer NOT NULL,
    seq integer NOT NULL,
    use text, type text, line1 text, line2 text, extra_lines text,
    city text, state text, postal_code text, country text
) PARTITION BY LIST (release_date);
CREATE UNIQUE INDEX IF NOT EXISTS organization_address_key ON <<schema>>.organization_address (release_date, resource_id, seq);

CREATE TABLE IF NOT EXISTS <<schema>>.organization_telecom (
    release_date date NOT NULL, resource_id text NOT NULL, ndjson_file_id integer NOT NULL, zst_file_id integer NOT NULL,
    seq integer NOT NULL,
    system text, use text, value text
) PARTITION BY LIST (release_date);
CREATE UNIQUE INDEX IF NOT EXISTS organization_telecom_key ON <<schema>>.organization_telecom (release_date, resource_id, seq);

CREATE TABLE IF NOT EXISTS <<schema>>.organization_endpoint (
    release_date date NOT NULL, resource_id text NOT NULL, ndjson_file_id integer NOT NULL, zst_file_id integer NOT NULL,
    seq integer NOT NULL,
    endpoint_id text
) PARTITION BY LIST (release_date);
CREATE UNIQUE INDEX IF NOT EXISTS organization_endpoint_key ON <<schema>>.organization_endpoint (release_date, resource_id, seq);

CREATE TABLE IF NOT EXISTS <<schema>>.location (
    release_date date NOT NULL, resource_id text NOT NULL, ndjson_file_id integer NOT NULL, zst_file_id integer NOT NULL,
    last_updated timestamptz,
    status text, name text, description text, mode text,
    address_use text, address_type text, line1 text, line2 text, extra_lines text,
    city text, state text, postal_code text, country text,
    latitude double precision, longitude double precision,
    managing_organization_id text
) PARTITION BY LIST (release_date);
CREATE UNIQUE INDEX IF NOT EXISTS location_key ON <<schema>>.location (release_date, resource_id);
CREATE INDEX IF NOT EXISTS location_managing_organization ON <<schema>>.location (release_date, managing_organization_id);

CREATE TABLE IF NOT EXISTS <<schema>>.location_telecom (
    release_date date NOT NULL, resource_id text NOT NULL, ndjson_file_id integer NOT NULL, zst_file_id integer NOT NULL,
    seq integer NOT NULL,
    system text, use text, value text
) PARTITION BY LIST (release_date);
CREATE UNIQUE INDEX IF NOT EXISTS location_telecom_key ON <<schema>>.location_telecom (release_date, resource_id, seq);

CREATE TABLE IF NOT EXISTS <<schema>>.endpoint (
    release_date date NOT NULL, resource_id text NOT NULL, ndjson_file_id integer NOT NULL, zst_file_id integer NOT NULL,
    last_updated timestamptz,
    status text, name text, address text,
    connection_type_system text, connection_type_code text,
    payload_type_system text, payload_type_code text,
    managing_organization_id text,
    verification_status text
) PARTITION BY LIST (release_date);
CREATE UNIQUE INDEX IF NOT EXISTS endpoint_key ON <<schema>>.endpoint (release_date, resource_id);

CREATE TABLE IF NOT EXISTS <<schema>>.practitioner_role (
    release_date date NOT NULL, resource_id text NOT NULL, ndjson_file_id integer NOT NULL, zst_file_id integer NOT NULL,
    last_updated timestamptz,
    active boolean,
    practitioner_id text,
    organization_id text,
    period_start timestamptz, period_end timestamptz,
    network_organization_id text,
    accepting_patients text
) PARTITION BY LIST (release_date);
CREATE UNIQUE INDEX IF NOT EXISTS practitioner_role_key ON <<schema>>.practitioner_role (release_date, resource_id);
CREATE INDEX IF NOT EXISTS practitioner_role_practitioner ON <<schema>>.practitioner_role (release_date, practitioner_id);
CREATE INDEX IF NOT EXISTS practitioner_role_organization ON <<schema>>.practitioner_role (release_date, organization_id);

CREATE TABLE IF NOT EXISTS <<schema>>.practitioner_role_telecom (
    release_date date NOT NULL, resource_id text NOT NULL, ndjson_file_id integer NOT NULL, zst_file_id integer NOT NULL,
    seq integer NOT NULL,
    system text, use text, value text
) PARTITION BY LIST (release_date);
CREATE UNIQUE INDEX IF NOT EXISTS practitioner_role_telecom_key ON <<schema>>.practitioner_role_telecom (release_date, resource_id, seq);

CREATE TABLE IF NOT EXISTS <<schema>>.practitioner_role_endpoint (
    release_date date NOT NULL, resource_id text NOT NULL, ndjson_file_id integer NOT NULL, zst_file_id integer NOT NULL,
    seq integer NOT NULL,
    endpoint_id text
) PARTITION BY LIST (release_date);
CREATE UNIQUE INDEX IF NOT EXISTS practitioner_role_endpoint_key ON <<schema>>.practitioner_role_endpoint (release_date, resource_id, seq);

CREATE TABLE IF NOT EXISTS <<schema>>.practitioner_role_location (
    release_date date NOT NULL, resource_id text NOT NULL, ndjson_file_id integer NOT NULL, zst_file_id integer NOT NULL,
    seq integer NOT NULL,
    location_id text
) PARTITION BY LIST (release_date);
CREATE UNIQUE INDEX IF NOT EXISTS practitioner_role_location_key ON <<schema>>.practitioner_role_location (release_date, resource_id, seq);

CREATE TABLE IF NOT EXISTS <<schema>>.practitioner_role_specialty (
    release_date date NOT NULL, resource_id text NOT NULL, ndjson_file_id integer NOT NULL, zst_file_id integer NOT NULL,
    seq integer NOT NULL,
    system text, code text, display text, text text
) PARTITION BY LIST (release_date);
CREATE UNIQUE INDEX IF NOT EXISTS practitioner_role_specialty_key ON <<schema>>.practitioner_role_specialty (release_date, resource_id, seq);

CREATE TABLE IF NOT EXISTS <<schema>>.practitioner_role_code (
    release_date date NOT NULL, resource_id text NOT NULL, ndjson_file_id integer NOT NULL, zst_file_id integer NOT NULL,
    seq integer NOT NULL,
    system text, code text, display text, text text
) PARTITION BY LIST (release_date);
CREATE UNIQUE INDEX IF NOT EXISTS practitioner_role_code_key ON <<schema>>.practitioner_role_code (release_date, resource_id, seq);

CREATE TABLE IF NOT EXISTS <<schema>>.organization_affiliation (
    release_date date NOT NULL, resource_id text NOT NULL, ndjson_file_id integer NOT NULL, zst_file_id integer NOT NULL,
    last_updated timestamptz,
    active boolean,
    organization_id text,
    participating_organization_id text,
    role_code text, role_display text, role_text text,
    period_start timestamptz, period_end timestamptz
) PARTITION BY LIST (release_date);
CREATE UNIQUE INDEX IF NOT EXISTS organization_affiliation_key ON <<schema>>.organization_affiliation (release_date, resource_id);

CREATE TABLE IF NOT EXISTS <<schema>>.organization_affiliation_network (
    release_date date NOT NULL, resource_id text NOT NULL, ndjson_file_id integer NOT NULL, zst_file_id integer NOT NULL,
    seq integer NOT NULL,
    network_organization_id text
) PARTITION BY LIST (release_date);
CREATE UNIQUE INDEX IF NOT EXISTS organization_affiliation_network_key ON <<schema>>.organization_affiliation_network (release_date, resource_id, seq);

CREATE TABLE IF NOT EXISTS <<schema>>.healthcare_service (
    release_date date NOT NULL, resource_id text NOT NULL, ndjson_file_id integer NOT NULL, zst_file_id integer NOT NULL,
    last_updated timestamptz,
    active boolean,
    name text,
    provided_by_organization_id text,
    network_organization_id text
) PARTITION BY LIST (release_date);
CREATE UNIQUE INDEX IF NOT EXISTS healthcare_service_key ON <<schema>>.healthcare_service (release_date, resource_id);

CREATE TABLE IF NOT EXISTS <<schema>>.healthcare_service_location (
    release_date date NOT NULL, resource_id text NOT NULL, ndjson_file_id integer NOT NULL, zst_file_id integer NOT NULL,
    seq integer NOT NULL,
    location_id text
) PARTITION BY LIST (release_date);
CREATE UNIQUE INDEX IF NOT EXISTS healthcare_service_location_key ON <<schema>>.healthcare_service_location (release_date, resource_id, seq);

CREATE TABLE IF NOT EXISTS <<schema>>.insurance_plan (
    release_date date NOT NULL, resource_id text NOT NULL, ndjson_file_id integer NOT NULL, zst_file_id integer NOT NULL,
    last_updated timestamptz,
    status text, name text,
    type_code text, type_text text,
    period_start timestamptz, period_end timestamptz,
    owned_by_organization_id text,
    administered_by_organization_id text
) PARTITION BY LIST (release_date);
CREATE UNIQUE INDEX IF NOT EXISTS insurance_plan_key ON <<schema>>.insurance_plan (release_date, resource_id);

CREATE TABLE IF NOT EXISTS <<schema>>.insurance_plan_alias (
    release_date date NOT NULL, resource_id text NOT NULL, ndjson_file_id integer NOT NULL, zst_file_id integer NOT NULL,
    seq integer NOT NULL,
    alias text
) PARTITION BY LIST (release_date);
CREATE UNIQUE INDEX IF NOT EXISTS insurance_plan_alias_key ON <<schema>>.insurance_plan_alias (release_date, resource_id, seq);

CREATE TABLE IF NOT EXISTS <<schema>>.insurance_plan_network (
    release_date date NOT NULL, resource_id text NOT NULL, ndjson_file_id integer NOT NULL, zst_file_id integer NOT NULL,
    seq integer NOT NULL,
    network_organization_id text
) PARTITION BY LIST (release_date);
CREATE UNIQUE INDEX IF NOT EXISTS insurance_plan_network_key ON <<schema>>.insurance_plan_network (release_date, resource_id, seq);

-- Every identifier from every resource type (join key for NPI etc.).
CREATE TABLE IF NOT EXISTS <<schema>>.identifier (
    release_date date NOT NULL, resource_id text NOT NULL, ndjson_file_id integer NOT NULL, zst_file_id integer NOT NULL,
    resource_type text NOT NULL,
    seq integer NOT NULL,
    system text, value text, use text, type_code text, type_text text,
    period_start timestamptz, period_end timestamptz
) PARTITION BY LIST (release_date);
CREATE UNIQUE INDEX IF NOT EXISTS identifier_key ON <<schema>>.identifier (release_date, resource_type, resource_id, seq);
CREATE INDEX IF NOT EXISTS identifier_value ON <<schema>>.identifier (release_date, system, value);
