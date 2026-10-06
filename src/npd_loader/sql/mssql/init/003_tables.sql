-- Every table: release_date, resource_id, ndjson_file_id, zst_file_id first; child tables add seq (1-based
-- position in the repeating element). Same columns as sql/postgres/init/003_tables.sql.

IF OBJECT_ID(<<s:schema>> + N'.practitioner', N'U') IS NULL
    CREATE TABLE <<schema>>.practitioner (
        release_date date NOT NULL, resource_id varchar(128) NOT NULL, ndjson_file_id int NOT NULL, zst_file_id int NOT NULL,
        last_updated datetime2(3),
        npi varchar(128),
        active bit,
        gender varchar(256),
        name_family nvarchar(1000),
        name_given nvarchar(1000),
        name_prefix nvarchar(1000),
        name_suffix nvarchar(1000),
        identity_verified bit,
        medicare_enrolled bit,
        in_hhs_exclusion_list bit,
        aligned_with_data_network bit
    ) ON <<ps:schema>> (release_date) WITH (DATA_COMPRESSION = PAGE)
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(<<s:schema>> + N'.practitioner') AND name = N'practitioner_key')
    CREATE UNIQUE INDEX practitioner_key ON <<schema>>.practitioner (release_date, resource_id) WITH (DATA_COMPRESSION = PAGE)
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(<<s:schema>> + N'.practitioner') AND name = N'practitioner_npi')
    CREATE INDEX practitioner_npi ON <<schema>>.practitioner (release_date, npi) WITH (DATA_COMPRESSION = PAGE)
GO

IF OBJECT_ID(<<s:schema>> + N'.practitioner_name', N'U') IS NULL
    CREATE TABLE <<schema>>.practitioner_name (
        release_date date NOT NULL, resource_id varchar(128) NOT NULL, ndjson_file_id int NOT NULL, zst_file_id int NOT NULL,
        seq int NOT NULL,
        [use] varchar(256),
        family nvarchar(1000),
        given nvarchar(1000),
        prefix nvarchar(1000),
        suffix nvarchar(1000),
        period_start datetime2(3),
        period_end datetime2(3)
    ) ON <<ps:schema>> (release_date) WITH (DATA_COMPRESSION = PAGE)
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(<<s:schema>> + N'.practitioner_name') AND name = N'practitioner_name_key')
    CREATE UNIQUE INDEX practitioner_name_key ON <<schema>>.practitioner_name (release_date, resource_id, seq) WITH (DATA_COMPRESSION = PAGE)
GO

IF OBJECT_ID(<<s:schema>> + N'.practitioner_address', N'U') IS NULL
    CREATE TABLE <<schema>>.practitioner_address (
        release_date date NOT NULL, resource_id varchar(128) NOT NULL, ndjson_file_id int NOT NULL, zst_file_id int NOT NULL,
        seq int NOT NULL,
        [use] varchar(256),
        [type] varchar(256),
        line1 nvarchar(1000),
        line2 nvarchar(1000),
        extra_lines nvarchar(2000),
        city nvarchar(1000),
        state varchar(256),
        postal_code varchar(256),
        country varchar(256)
    ) ON <<ps:schema>> (release_date) WITH (DATA_COMPRESSION = PAGE)
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(<<s:schema>> + N'.practitioner_address') AND name = N'practitioner_address_key')
    CREATE UNIQUE INDEX practitioner_address_key ON <<schema>>.practitioner_address (release_date, resource_id, seq) WITH (DATA_COMPRESSION = PAGE)
GO

IF OBJECT_ID(<<s:schema>> + N'.practitioner_telecom', N'U') IS NULL
    CREATE TABLE <<schema>>.practitioner_telecom (
        release_date date NOT NULL, resource_id varchar(128) NOT NULL, ndjson_file_id int NOT NULL, zst_file_id int NOT NULL,
        seq int NOT NULL,
        [system] varchar(512),
        [use] varchar(256),
        [value] nvarchar(1000)
    ) ON <<ps:schema>> (release_date) WITH (DATA_COMPRESSION = PAGE)
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(<<s:schema>> + N'.practitioner_telecom') AND name = N'practitioner_telecom_key')
    CREATE UNIQUE INDEX practitioner_telecom_key ON <<schema>>.practitioner_telecom (release_date, resource_id, seq) WITH (DATA_COMPRESSION = PAGE)
GO

IF OBJECT_ID(<<s:schema>> + N'.practitioner_qualification', N'U') IS NULL
    CREATE TABLE <<schema>>.practitioner_qualification (
        release_date date NOT NULL, resource_id varchar(128) NOT NULL, ndjson_file_id int NOT NULL, zst_file_id int NOT NULL,
        seq int NOT NULL,
        code_system varchar(512),
        code varchar(256),
        code_display nvarchar(1000),
        code_text nvarchar(1000),
        identifier_value nvarchar(400),
        identifier_type_code varchar(256),
        issuer_organization_id varchar(128)
    ) ON <<ps:schema>> (release_date) WITH (DATA_COMPRESSION = PAGE)
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(<<s:schema>> + N'.practitioner_qualification') AND name = N'practitioner_qualification_key')
    CREATE UNIQUE INDEX practitioner_qualification_key ON <<schema>>.practitioner_qualification (release_date, resource_id, seq) WITH (DATA_COMPRESSION = PAGE)
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(<<s:schema>> + N'.practitioner_qualification') AND name = N'practitioner_qualification_code')
    CREATE INDEX practitioner_qualification_code ON <<schema>>.practitioner_qualification (release_date, code) WITH (DATA_COMPRESSION = PAGE)
GO

IF OBJECT_ID(<<s:schema>> + N'.organization', N'U') IS NULL
    CREATE TABLE <<schema>>.organization (
        release_date date NOT NULL, resource_id varchar(128) NOT NULL, ndjson_file_id int NOT NULL, zst_file_id int NOT NULL,
        last_updated datetime2(3),
        npi varchar(128),
        pseudo_ein varchar(128),
        name nvarchar(1000),
        active bit,
        type_code varchar(256),
        type_display nvarchar(1000),
        part_of_organization_id varchar(128),
        verification_status varchar(256)
    ) ON <<ps:schema>> (release_date) WITH (DATA_COMPRESSION = PAGE)
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(<<s:schema>> + N'.organization') AND name = N'organization_key')
    CREATE UNIQUE INDEX organization_key ON <<schema>>.organization (release_date, resource_id) WITH (DATA_COMPRESSION = PAGE)
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(<<s:schema>> + N'.organization') AND name = N'organization_npi')
    CREATE INDEX organization_npi ON <<schema>>.organization (release_date, npi) WITH (DATA_COMPRESSION = PAGE)
GO

IF OBJECT_ID(<<s:schema>> + N'.organization_address', N'U') IS NULL
    CREATE TABLE <<schema>>.organization_address (
        release_date date NOT NULL, resource_id varchar(128) NOT NULL, ndjson_file_id int NOT NULL, zst_file_id int NOT NULL,
        seq int NOT NULL,
        [use] varchar(256),
        [type] varchar(256),
        line1 nvarchar(1000),
        line2 nvarchar(1000),
        extra_lines nvarchar(2000),
        city nvarchar(1000),
        state varchar(256),
        postal_code varchar(256),
        country varchar(256)
    ) ON <<ps:schema>> (release_date) WITH (DATA_COMPRESSION = PAGE)
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(<<s:schema>> + N'.organization_address') AND name = N'organization_address_key')
    CREATE UNIQUE INDEX organization_address_key ON <<schema>>.organization_address (release_date, resource_id, seq) WITH (DATA_COMPRESSION = PAGE)
GO

IF OBJECT_ID(<<s:schema>> + N'.organization_telecom', N'U') IS NULL
    CREATE TABLE <<schema>>.organization_telecom (
        release_date date NOT NULL, resource_id varchar(128) NOT NULL, ndjson_file_id int NOT NULL, zst_file_id int NOT NULL,
        seq int NOT NULL,
        [system] varchar(512),
        [use] varchar(256),
        [value] nvarchar(1000)
    ) ON <<ps:schema>> (release_date) WITH (DATA_COMPRESSION = PAGE)
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(<<s:schema>> + N'.organization_telecom') AND name = N'organization_telecom_key')
    CREATE UNIQUE INDEX organization_telecom_key ON <<schema>>.organization_telecom (release_date, resource_id, seq) WITH (DATA_COMPRESSION = PAGE)
GO

IF OBJECT_ID(<<s:schema>> + N'.organization_endpoint', N'U') IS NULL
    CREATE TABLE <<schema>>.organization_endpoint (
        release_date date NOT NULL, resource_id varchar(128) NOT NULL, ndjson_file_id int NOT NULL, zst_file_id int NOT NULL,
        seq int NOT NULL,
        endpoint_id varchar(128)
    ) ON <<ps:schema>> (release_date) WITH (DATA_COMPRESSION = PAGE)
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(<<s:schema>> + N'.organization_endpoint') AND name = N'organization_endpoint_key')
    CREATE UNIQUE INDEX organization_endpoint_key ON <<schema>>.organization_endpoint (release_date, resource_id, seq) WITH (DATA_COMPRESSION = PAGE)
GO

IF OBJECT_ID(<<s:schema>> + N'.location', N'U') IS NULL
    CREATE TABLE <<schema>>.location (
        release_date date NOT NULL, resource_id varchar(128) NOT NULL, ndjson_file_id int NOT NULL, zst_file_id int NOT NULL,
        last_updated datetime2(3),
        status varchar(256),
        name nvarchar(1000),
        description nvarchar(4000),
        mode varchar(256),
        address_use varchar(256),
        address_type varchar(256),
        line1 nvarchar(1000),
        line2 nvarchar(1000),
        extra_lines nvarchar(2000),
        city nvarchar(1000),
        state varchar(256),
        postal_code varchar(256),
        country varchar(256),
        latitude float,
        longitude float,
        managing_organization_id varchar(128)
    ) ON <<ps:schema>> (release_date) WITH (DATA_COMPRESSION = PAGE)
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(<<s:schema>> + N'.location') AND name = N'location_key')
    CREATE UNIQUE INDEX location_key ON <<schema>>.location (release_date, resource_id) WITH (DATA_COMPRESSION = PAGE)
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(<<s:schema>> + N'.location') AND name = N'location_managing_organization')
    CREATE INDEX location_managing_organization ON <<schema>>.location (release_date, managing_organization_id) WITH (DATA_COMPRESSION = PAGE)
GO

IF OBJECT_ID(<<s:schema>> + N'.location_telecom', N'U') IS NULL
    CREATE TABLE <<schema>>.location_telecom (
        release_date date NOT NULL, resource_id varchar(128) NOT NULL, ndjson_file_id int NOT NULL, zst_file_id int NOT NULL,
        seq int NOT NULL,
        [system] varchar(512),
        [use] varchar(256),
        [value] nvarchar(1000)
    ) ON <<ps:schema>> (release_date) WITH (DATA_COMPRESSION = PAGE)
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(<<s:schema>> + N'.location_telecom') AND name = N'location_telecom_key')
    CREATE UNIQUE INDEX location_telecom_key ON <<schema>>.location_telecom (release_date, resource_id, seq) WITH (DATA_COMPRESSION = PAGE)
GO

IF OBJECT_ID(<<s:schema>> + N'.endpoint', N'U') IS NULL
    CREATE TABLE <<schema>>.endpoint (
        release_date date NOT NULL, resource_id varchar(128) NOT NULL, ndjson_file_id int NOT NULL, zst_file_id int NOT NULL,
        last_updated datetime2(3),
        status varchar(256),
        name nvarchar(1000),
        address nvarchar(2000),
        connection_type_system varchar(512),
        connection_type_code varchar(256),
        payload_type_system varchar(512),
        payload_type_code varchar(256),
        managing_organization_id varchar(128),
        verification_status varchar(256)
    ) ON <<ps:schema>> (release_date) WITH (DATA_COMPRESSION = PAGE)
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(<<s:schema>> + N'.endpoint') AND name = N'endpoint_key')
    CREATE UNIQUE INDEX endpoint_key ON <<schema>>.endpoint (release_date, resource_id) WITH (DATA_COMPRESSION = PAGE)
GO

IF OBJECT_ID(<<s:schema>> + N'.practitioner_role', N'U') IS NULL
    CREATE TABLE <<schema>>.practitioner_role (
        release_date date NOT NULL, resource_id varchar(128) NOT NULL, ndjson_file_id int NOT NULL, zst_file_id int NOT NULL,
        last_updated datetime2(3),
        active bit,
        practitioner_id varchar(128),
        organization_id varchar(128),
        period_start datetime2(3),
        period_end datetime2(3),
        network_organization_id varchar(128),
        accepting_patients varchar(256)
    ) ON <<ps:schema>> (release_date) WITH (DATA_COMPRESSION = PAGE)
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(<<s:schema>> + N'.practitioner_role') AND name = N'practitioner_role_key')
    CREATE UNIQUE INDEX practitioner_role_key ON <<schema>>.practitioner_role (release_date, resource_id) WITH (DATA_COMPRESSION = PAGE)
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(<<s:schema>> + N'.practitioner_role') AND name = N'practitioner_role_practitioner')
    CREATE INDEX practitioner_role_practitioner ON <<schema>>.practitioner_role (release_date, practitioner_id) WITH (DATA_COMPRESSION = PAGE)
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(<<s:schema>> + N'.practitioner_role') AND name = N'practitioner_role_organization')
    CREATE INDEX practitioner_role_organization ON <<schema>>.practitioner_role (release_date, organization_id) WITH (DATA_COMPRESSION = PAGE)
GO

IF OBJECT_ID(<<s:schema>> + N'.practitioner_role_telecom', N'U') IS NULL
    CREATE TABLE <<schema>>.practitioner_role_telecom (
        release_date date NOT NULL, resource_id varchar(128) NOT NULL, ndjson_file_id int NOT NULL, zst_file_id int NOT NULL,
        seq int NOT NULL,
        [system] varchar(512),
        [use] varchar(256),
        [value] nvarchar(1000)
    ) ON <<ps:schema>> (release_date) WITH (DATA_COMPRESSION = PAGE)
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(<<s:schema>> + N'.practitioner_role_telecom') AND name = N'practitioner_role_telecom_key')
    CREATE UNIQUE INDEX practitioner_role_telecom_key ON <<schema>>.practitioner_role_telecom (release_date, resource_id, seq) WITH (DATA_COMPRESSION = PAGE)
GO

IF OBJECT_ID(<<s:schema>> + N'.practitioner_role_endpoint', N'U') IS NULL
    CREATE TABLE <<schema>>.practitioner_role_endpoint (
        release_date date NOT NULL, resource_id varchar(128) NOT NULL, ndjson_file_id int NOT NULL, zst_file_id int NOT NULL,
        seq int NOT NULL,
        endpoint_id varchar(128)
    ) ON <<ps:schema>> (release_date) WITH (DATA_COMPRESSION = PAGE)
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(<<s:schema>> + N'.practitioner_role_endpoint') AND name = N'practitioner_role_endpoint_key')
    CREATE UNIQUE INDEX practitioner_role_endpoint_key ON <<schema>>.practitioner_role_endpoint (release_date, resource_id, seq) WITH (DATA_COMPRESSION = PAGE)
GO

IF OBJECT_ID(<<s:schema>> + N'.practitioner_role_location', N'U') IS NULL
    CREATE TABLE <<schema>>.practitioner_role_location (
        release_date date NOT NULL, resource_id varchar(128) NOT NULL, ndjson_file_id int NOT NULL, zst_file_id int NOT NULL,
        seq int NOT NULL,
        location_id varchar(128)
    ) ON <<ps:schema>> (release_date) WITH (DATA_COMPRESSION = PAGE)
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(<<s:schema>> + N'.practitioner_role_location') AND name = N'practitioner_role_location_key')
    CREATE UNIQUE INDEX practitioner_role_location_key ON <<schema>>.practitioner_role_location (release_date, resource_id, seq) WITH (DATA_COMPRESSION = PAGE)
GO

IF OBJECT_ID(<<s:schema>> + N'.practitioner_role_specialty', N'U') IS NULL
    CREATE TABLE <<schema>>.practitioner_role_specialty (
        release_date date NOT NULL, resource_id varchar(128) NOT NULL, ndjson_file_id int NOT NULL, zst_file_id int NOT NULL,
        seq int NOT NULL,
        [system] varchar(512),
        code varchar(256),
        display nvarchar(1000),
        [text] nvarchar(1000)
    ) ON <<ps:schema>> (release_date) WITH (DATA_COMPRESSION = PAGE)
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(<<s:schema>> + N'.practitioner_role_specialty') AND name = N'practitioner_role_specialty_key')
    CREATE UNIQUE INDEX practitioner_role_specialty_key ON <<schema>>.practitioner_role_specialty (release_date, resource_id, seq) WITH (DATA_COMPRESSION = PAGE)
GO

IF OBJECT_ID(<<s:schema>> + N'.practitioner_role_code', N'U') IS NULL
    CREATE TABLE <<schema>>.practitioner_role_code (
        release_date date NOT NULL, resource_id varchar(128) NOT NULL, ndjson_file_id int NOT NULL, zst_file_id int NOT NULL,
        seq int NOT NULL,
        [system] varchar(512),
        code varchar(256),
        display nvarchar(1000),
        [text] nvarchar(1000)
    ) ON <<ps:schema>> (release_date) WITH (DATA_COMPRESSION = PAGE)
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(<<s:schema>> + N'.practitioner_role_code') AND name = N'practitioner_role_code_key')
    CREATE UNIQUE INDEX practitioner_role_code_key ON <<schema>>.practitioner_role_code (release_date, resource_id, seq) WITH (DATA_COMPRESSION = PAGE)
GO

IF OBJECT_ID(<<s:schema>> + N'.organization_affiliation', N'U') IS NULL
    CREATE TABLE <<schema>>.organization_affiliation (
        release_date date NOT NULL, resource_id varchar(128) NOT NULL, ndjson_file_id int NOT NULL, zst_file_id int NOT NULL,
        last_updated datetime2(3),
        active bit,
        organization_id varchar(128),
        participating_organization_id varchar(128),
        role_code varchar(256),
        role_display nvarchar(1000),
        role_text nvarchar(1000),
        period_start datetime2(3),
        period_end datetime2(3)
    ) ON <<ps:schema>> (release_date) WITH (DATA_COMPRESSION = PAGE)
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(<<s:schema>> + N'.organization_affiliation') AND name = N'organization_affiliation_key')
    CREATE UNIQUE INDEX organization_affiliation_key ON <<schema>>.organization_affiliation (release_date, resource_id) WITH (DATA_COMPRESSION = PAGE)
GO

IF OBJECT_ID(<<s:schema>> + N'.organization_affiliation_network', N'U') IS NULL
    CREATE TABLE <<schema>>.organization_affiliation_network (
        release_date date NOT NULL, resource_id varchar(128) NOT NULL, ndjson_file_id int NOT NULL, zst_file_id int NOT NULL,
        seq int NOT NULL,
        network_organization_id varchar(128)
    ) ON <<ps:schema>> (release_date) WITH (DATA_COMPRESSION = PAGE)
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(<<s:schema>> + N'.organization_affiliation_network') AND name = N'organization_affiliation_network_key')
    CREATE UNIQUE INDEX organization_affiliation_network_key ON <<schema>>.organization_affiliation_network (release_date, resource_id, seq) WITH (DATA_COMPRESSION = PAGE)
GO

IF OBJECT_ID(<<s:schema>> + N'.healthcare_service', N'U') IS NULL
    CREATE TABLE <<schema>>.healthcare_service (
        release_date date NOT NULL, resource_id varchar(128) NOT NULL, ndjson_file_id int NOT NULL, zst_file_id int NOT NULL,
        last_updated datetime2(3),
        active bit,
        name nvarchar(1000),
        provided_by_organization_id varchar(128),
        network_organization_id varchar(128)
    ) ON <<ps:schema>> (release_date) WITH (DATA_COMPRESSION = PAGE)
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(<<s:schema>> + N'.healthcare_service') AND name = N'healthcare_service_key')
    CREATE UNIQUE INDEX healthcare_service_key ON <<schema>>.healthcare_service (release_date, resource_id) WITH (DATA_COMPRESSION = PAGE)
GO

IF OBJECT_ID(<<s:schema>> + N'.healthcare_service_location', N'U') IS NULL
    CREATE TABLE <<schema>>.healthcare_service_location (
        release_date date NOT NULL, resource_id varchar(128) NOT NULL, ndjson_file_id int NOT NULL, zst_file_id int NOT NULL,
        seq int NOT NULL,
        location_id varchar(128)
    ) ON <<ps:schema>> (release_date) WITH (DATA_COMPRESSION = PAGE)
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(<<s:schema>> + N'.healthcare_service_location') AND name = N'healthcare_service_location_key')
    CREATE UNIQUE INDEX healthcare_service_location_key ON <<schema>>.healthcare_service_location (release_date, resource_id, seq) WITH (DATA_COMPRESSION = PAGE)
GO

IF OBJECT_ID(<<s:schema>> + N'.insurance_plan', N'U') IS NULL
    CREATE TABLE <<schema>>.insurance_plan (
        release_date date NOT NULL, resource_id varchar(128) NOT NULL, ndjson_file_id int NOT NULL, zst_file_id int NOT NULL,
        last_updated datetime2(3),
        status varchar(256),
        name nvarchar(1000),
        type_code varchar(256),
        type_text nvarchar(1000),
        period_start datetime2(3),
        period_end datetime2(3),
        owned_by_organization_id varchar(128),
        administered_by_organization_id varchar(128)
    ) ON <<ps:schema>> (release_date) WITH (DATA_COMPRESSION = PAGE)
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(<<s:schema>> + N'.insurance_plan') AND name = N'insurance_plan_key')
    CREATE UNIQUE INDEX insurance_plan_key ON <<schema>>.insurance_plan (release_date, resource_id) WITH (DATA_COMPRESSION = PAGE)
GO

IF OBJECT_ID(<<s:schema>> + N'.insurance_plan_alias', N'U') IS NULL
    CREATE TABLE <<schema>>.insurance_plan_alias (
        release_date date NOT NULL, resource_id varchar(128) NOT NULL, ndjson_file_id int NOT NULL, zst_file_id int NOT NULL,
        seq int NOT NULL,
        alias nvarchar(1000)
    ) ON <<ps:schema>> (release_date) WITH (DATA_COMPRESSION = PAGE)
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(<<s:schema>> + N'.insurance_plan_alias') AND name = N'insurance_plan_alias_key')
    CREATE UNIQUE INDEX insurance_plan_alias_key ON <<schema>>.insurance_plan_alias (release_date, resource_id, seq) WITH (DATA_COMPRESSION = PAGE)
GO

IF OBJECT_ID(<<s:schema>> + N'.insurance_plan_network', N'U') IS NULL
    CREATE TABLE <<schema>>.insurance_plan_network (
        release_date date NOT NULL, resource_id varchar(128) NOT NULL, ndjson_file_id int NOT NULL, zst_file_id int NOT NULL,
        seq int NOT NULL,
        network_organization_id varchar(128)
    ) ON <<ps:schema>> (release_date) WITH (DATA_COMPRESSION = PAGE)
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(<<s:schema>> + N'.insurance_plan_network') AND name = N'insurance_plan_network_key')
    CREATE UNIQUE INDEX insurance_plan_network_key ON <<schema>>.insurance_plan_network (release_date, resource_id, seq) WITH (DATA_COMPRESSION = PAGE)
GO

-- Every identifier from every resource type (join key for NPI etc.).
IF OBJECT_ID(<<s:schema>> + N'.identifier', N'U') IS NULL
    CREATE TABLE <<schema>>.identifier (
        release_date date NOT NULL, resource_id varchar(128) NOT NULL, ndjson_file_id int NOT NULL, zst_file_id int NOT NULL,
        resource_type varchar(128) NOT NULL,
        seq int NOT NULL,
        [system] varchar(512),
        [value] nvarchar(400),
        [use] varchar(256),
        type_code varchar(256),
        type_text nvarchar(1000),
        period_start datetime2(3),
        period_end datetime2(3)
    ) ON <<ps:schema>> (release_date) WITH (DATA_COMPRESSION = PAGE)
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(<<s:schema>> + N'.identifier') AND name = N'identifier_key')
    CREATE UNIQUE INDEX identifier_key ON <<schema>>.identifier (release_date, resource_type, resource_id, seq) WITH (DATA_COMPRESSION = PAGE)
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(<<s:schema>> + N'.identifier') AND name = N'identifier_value')
    CREATE INDEX identifier_value ON <<schema>>.identifier (release_date, [system], [value]) WITH (DATA_COMPRESSION = PAGE)
GO
