-- Phase 2: one current dataset, no partitioning. Every table: release_date (release the row came from), resource_key, ndjson_file_id, zst_file_id first; child tables add seq (1-based
-- position in the repeating element).

IF OBJECT_ID(<<s:schema>> + N'.practitioner', N'U') IS NULL
    CREATE TABLE <<schema>>.practitioner (
        release_date date NOT NULL, resource_key int NOT NULL, ndjson_file_id int NOT NULL, zst_file_id int NOT NULL,
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
    ) WITH (DATA_COMPRESSION = PAGE)
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(<<s:schema>> + N'.practitioner') AND name = N'pk_practitioner')
    ALTER TABLE <<schema>>.practitioner ADD CONSTRAINT pk_practitioner PRIMARY KEY CLUSTERED (resource_key) WITH (DATA_COMPRESSION = PAGE)
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(<<s:schema>> + N'.practitioner') AND name = N'practitioner_npi')
    CREATE INDEX practitioner_npi ON <<schema>>.practitioner (npi) WITH (DATA_COMPRESSION = PAGE)
GO

IF OBJECT_ID(<<s:schema>> + N'.practitioner_name', N'U') IS NULL
    CREATE TABLE <<schema>>.practitioner_name (
        release_date date NOT NULL, resource_key int NOT NULL, ndjson_file_id int NOT NULL, zst_file_id int NOT NULL,
        seq int NOT NULL,
        [use] varchar(256),
        family nvarchar(1000),
        given nvarchar(1000),
        prefix nvarchar(1000),
        suffix nvarchar(1000),
        period_start datetime2(3),
        period_end datetime2(3)
    ) WITH (DATA_COMPRESSION = PAGE)
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(<<s:schema>> + N'.practitioner_name') AND name = N'pk_practitioner_name')
    ALTER TABLE <<schema>>.practitioner_name ADD CONSTRAINT pk_practitioner_name PRIMARY KEY CLUSTERED (resource_key, seq) WITH (DATA_COMPRESSION = PAGE)
GO

IF OBJECT_ID(<<s:schema>> + N'.practitioner_address', N'U') IS NULL
    CREATE TABLE <<schema>>.practitioner_address (
        release_date date NOT NULL, resource_key int NOT NULL, ndjson_file_id int NOT NULL, zst_file_id int NOT NULL,
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
    ) WITH (DATA_COMPRESSION = PAGE)
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(<<s:schema>> + N'.practitioner_address') AND name = N'pk_practitioner_address')
    ALTER TABLE <<schema>>.practitioner_address ADD CONSTRAINT pk_practitioner_address PRIMARY KEY CLUSTERED (resource_key, seq) WITH (DATA_COMPRESSION = PAGE)
GO

IF OBJECT_ID(<<s:schema>> + N'.practitioner_telecom', N'U') IS NULL
    CREATE TABLE <<schema>>.practitioner_telecom (
        release_date date NOT NULL, resource_key int NOT NULL, ndjson_file_id int NOT NULL, zst_file_id int NOT NULL,
        seq int NOT NULL,
        [system] varchar(512),
        [use] varchar(256),
        [value] nvarchar(1000)
    ) WITH (DATA_COMPRESSION = PAGE)
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(<<s:schema>> + N'.practitioner_telecom') AND name = N'pk_practitioner_telecom')
    ALTER TABLE <<schema>>.practitioner_telecom ADD CONSTRAINT pk_practitioner_telecom PRIMARY KEY CLUSTERED (resource_key, seq) WITH (DATA_COMPRESSION = PAGE)
GO

IF OBJECT_ID(<<s:schema>> + N'.practitioner_qualification', N'U') IS NULL
    CREATE TABLE <<schema>>.practitioner_qualification (
        release_date date NOT NULL, resource_key int NOT NULL, ndjson_file_id int NOT NULL, zst_file_id int NOT NULL,
        seq int NOT NULL,
        code_system varchar(512),
        code varchar(256),
        code_display nvarchar(1000),
        code_text nvarchar(1000),
        identifier_value nvarchar(400),
        identifier_type_code varchar(256),
        issuer_organization_key int
    ) WITH (DATA_COMPRESSION = PAGE)
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(<<s:schema>> + N'.practitioner_qualification') AND name = N'pk_practitioner_qualification')
    ALTER TABLE <<schema>>.practitioner_qualification ADD CONSTRAINT pk_practitioner_qualification PRIMARY KEY CLUSTERED (resource_key, seq) WITH (DATA_COMPRESSION = PAGE)
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(<<s:schema>> + N'.practitioner_qualification') AND name = N'practitioner_qualification_code')
    CREATE INDEX practitioner_qualification_code ON <<schema>>.practitioner_qualification (code) WITH (DATA_COMPRESSION = PAGE)
GO

IF OBJECT_ID(<<s:schema>> + N'.organization', N'U') IS NULL
    CREATE TABLE <<schema>>.organization (
        release_date date NOT NULL, resource_key int NOT NULL, ndjson_file_id int NOT NULL, zst_file_id int NOT NULL,
        last_updated datetime2(3),
        npi varchar(128),
        pseudo_ein varchar(128),
        name nvarchar(1000),
        active bit,
        type_code varchar(256),
        type_display nvarchar(1000),
        part_of_organization_key int,
        verification_status varchar(256)
    ) WITH (DATA_COMPRESSION = PAGE)
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(<<s:schema>> + N'.organization') AND name = N'pk_organization')
    ALTER TABLE <<schema>>.organization ADD CONSTRAINT pk_organization PRIMARY KEY CLUSTERED (resource_key) WITH (DATA_COMPRESSION = PAGE)
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(<<s:schema>> + N'.organization') AND name = N'organization_npi')
    CREATE INDEX organization_npi ON <<schema>>.organization (npi) WITH (DATA_COMPRESSION = PAGE)
GO

IF OBJECT_ID(<<s:schema>> + N'.organization_address', N'U') IS NULL
    CREATE TABLE <<schema>>.organization_address (
        release_date date NOT NULL, resource_key int NOT NULL, ndjson_file_id int NOT NULL, zst_file_id int NOT NULL,
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
    ) WITH (DATA_COMPRESSION = PAGE)
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(<<s:schema>> + N'.organization_address') AND name = N'pk_organization_address')
    ALTER TABLE <<schema>>.organization_address ADD CONSTRAINT pk_organization_address PRIMARY KEY CLUSTERED (resource_key, seq) WITH (DATA_COMPRESSION = PAGE)
GO

IF OBJECT_ID(<<s:schema>> + N'.organization_telecom', N'U') IS NULL
    CREATE TABLE <<schema>>.organization_telecom (
        release_date date NOT NULL, resource_key int NOT NULL, ndjson_file_id int NOT NULL, zst_file_id int NOT NULL,
        seq int NOT NULL,
        [system] varchar(512),
        [use] varchar(256),
        [value] nvarchar(1000)
    ) WITH (DATA_COMPRESSION = PAGE)
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(<<s:schema>> + N'.organization_telecom') AND name = N'pk_organization_telecom')
    ALTER TABLE <<schema>>.organization_telecom ADD CONSTRAINT pk_organization_telecom PRIMARY KEY CLUSTERED (resource_key, seq) WITH (DATA_COMPRESSION = PAGE)
GO

IF OBJECT_ID(<<s:schema>> + N'.organization_endpoint', N'U') IS NULL
    CREATE TABLE <<schema>>.organization_endpoint (
        release_date date NOT NULL, resource_key int NOT NULL, ndjson_file_id int NOT NULL, zst_file_id int NOT NULL,
        seq int NOT NULL,
        endpoint_key int
    ) WITH (DATA_COMPRESSION = PAGE)
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(<<s:schema>> + N'.organization_endpoint') AND name = N'pk_organization_endpoint')
    ALTER TABLE <<schema>>.organization_endpoint ADD CONSTRAINT pk_organization_endpoint PRIMARY KEY CLUSTERED (resource_key, seq) WITH (DATA_COMPRESSION = PAGE)
GO

IF OBJECT_ID(<<s:schema>> + N'.location', N'U') IS NULL
    CREATE TABLE <<schema>>.location (
        release_date date NOT NULL, resource_key int NOT NULL, ndjson_file_id int NOT NULL, zst_file_id int NOT NULL,
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
        managing_organization_key int
    ) WITH (DATA_COMPRESSION = PAGE)
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(<<s:schema>> + N'.location') AND name = N'pk_location')
    ALTER TABLE <<schema>>.location ADD CONSTRAINT pk_location PRIMARY KEY CLUSTERED (resource_key) WITH (DATA_COMPRESSION = PAGE)
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(<<s:schema>> + N'.location') AND name = N'location_managing_organization')
    CREATE INDEX location_managing_organization ON <<schema>>.location (managing_organization_key) WITH (DATA_COMPRESSION = PAGE)
GO

IF OBJECT_ID(<<s:schema>> + N'.location_telecom', N'U') IS NULL
    CREATE TABLE <<schema>>.location_telecom (
        release_date date NOT NULL, resource_key int NOT NULL, ndjson_file_id int NOT NULL, zst_file_id int NOT NULL,
        seq int NOT NULL,
        [system] varchar(512),
        [use] varchar(256),
        [value] nvarchar(1000)
    ) WITH (DATA_COMPRESSION = PAGE)
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(<<s:schema>> + N'.location_telecom') AND name = N'pk_location_telecom')
    ALTER TABLE <<schema>>.location_telecom ADD CONSTRAINT pk_location_telecom PRIMARY KEY CLUSTERED (resource_key, seq) WITH (DATA_COMPRESSION = PAGE)
GO

IF OBJECT_ID(<<s:schema>> + N'.endpoint', N'U') IS NULL
    CREATE TABLE <<schema>>.endpoint (
        release_date date NOT NULL, resource_key int NOT NULL, ndjson_file_id int NOT NULL, zst_file_id int NOT NULL,
        last_updated datetime2(3),
        status varchar(256),
        name nvarchar(1000),
        address nvarchar(2000),
        connection_type_system varchar(512),
        connection_type_code varchar(256),
        payload_type_system varchar(512),
        payload_type_code varchar(256),
        managing_organization_key int,
        verification_status varchar(256)
    ) WITH (DATA_COMPRESSION = PAGE)
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(<<s:schema>> + N'.endpoint') AND name = N'pk_endpoint')
    ALTER TABLE <<schema>>.endpoint ADD CONSTRAINT pk_endpoint PRIMARY KEY CLUSTERED (resource_key) WITH (DATA_COMPRESSION = PAGE)
GO

IF OBJECT_ID(<<s:schema>> + N'.practitioner_role', N'U') IS NULL
    CREATE TABLE <<schema>>.practitioner_role (
        release_date date NOT NULL, resource_key int NOT NULL, ndjson_file_id int NOT NULL, zst_file_id int NOT NULL,
        last_updated datetime2(3),
        active bit,
        practitioner_key int,
        organization_key int,
        period_start datetime2(3),
        period_end datetime2(3),
        network_organization_key int,
        accepting_patients varchar(256)
    ) WITH (DATA_COMPRESSION = PAGE)
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(<<s:schema>> + N'.practitioner_role') AND name = N'pk_practitioner_role')
    ALTER TABLE <<schema>>.practitioner_role ADD CONSTRAINT pk_practitioner_role PRIMARY KEY CLUSTERED (resource_key) WITH (DATA_COMPRESSION = PAGE)
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(<<s:schema>> + N'.practitioner_role') AND name = N'practitioner_role_practitioner')
    CREATE INDEX practitioner_role_practitioner ON <<schema>>.practitioner_role (practitioner_key) WITH (DATA_COMPRESSION = PAGE)
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(<<s:schema>> + N'.practitioner_role') AND name = N'practitioner_role_organization')
    CREATE INDEX practitioner_role_organization ON <<schema>>.practitioner_role (organization_key) WITH (DATA_COMPRESSION = PAGE)
GO

IF OBJECT_ID(<<s:schema>> + N'.practitioner_role_telecom', N'U') IS NULL
    CREATE TABLE <<schema>>.practitioner_role_telecom (
        release_date date NOT NULL, resource_key int NOT NULL, ndjson_file_id int NOT NULL, zst_file_id int NOT NULL,
        seq int NOT NULL,
        [system] varchar(512),
        [use] varchar(256),
        [value] nvarchar(1000)
    ) WITH (DATA_COMPRESSION = PAGE)
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(<<s:schema>> + N'.practitioner_role_telecom') AND name = N'pk_practitioner_role_telecom')
    ALTER TABLE <<schema>>.practitioner_role_telecom ADD CONSTRAINT pk_practitioner_role_telecom PRIMARY KEY CLUSTERED (resource_key, seq) WITH (DATA_COMPRESSION = PAGE)
GO

IF OBJECT_ID(<<s:schema>> + N'.practitioner_role_endpoint', N'U') IS NULL
    CREATE TABLE <<schema>>.practitioner_role_endpoint (
        release_date date NOT NULL, resource_key int NOT NULL, ndjson_file_id int NOT NULL, zst_file_id int NOT NULL,
        seq int NOT NULL,
        endpoint_key int
    ) WITH (DATA_COMPRESSION = PAGE)
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(<<s:schema>> + N'.practitioner_role_endpoint') AND name = N'pk_practitioner_role_endpoint')
    ALTER TABLE <<schema>>.practitioner_role_endpoint ADD CONSTRAINT pk_practitioner_role_endpoint PRIMARY KEY CLUSTERED (resource_key, seq) WITH (DATA_COMPRESSION = PAGE)
GO

IF OBJECT_ID(<<s:schema>> + N'.practitioner_role_location', N'U') IS NULL
    CREATE TABLE <<schema>>.practitioner_role_location (
        release_date date NOT NULL, resource_key int NOT NULL, ndjson_file_id int NOT NULL, zst_file_id int NOT NULL,
        seq int NOT NULL,
        location_key int
    ) WITH (DATA_COMPRESSION = PAGE)
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(<<s:schema>> + N'.practitioner_role_location') AND name = N'pk_practitioner_role_location')
    ALTER TABLE <<schema>>.practitioner_role_location ADD CONSTRAINT pk_practitioner_role_location PRIMARY KEY CLUSTERED (resource_key, seq) WITH (DATA_COMPRESSION = PAGE)
GO

IF OBJECT_ID(<<s:schema>> + N'.practitioner_role_specialty', N'U') IS NULL
    CREATE TABLE <<schema>>.practitioner_role_specialty (
        release_date date NOT NULL, resource_key int NOT NULL, ndjson_file_id int NOT NULL, zst_file_id int NOT NULL,
        seq int NOT NULL,
        [system] varchar(512),
        code varchar(256),
        display nvarchar(1000),
        [text] nvarchar(1000)
    ) WITH (DATA_COMPRESSION = PAGE)
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(<<s:schema>> + N'.practitioner_role_specialty') AND name = N'pk_practitioner_role_specialty')
    ALTER TABLE <<schema>>.practitioner_role_specialty ADD CONSTRAINT pk_practitioner_role_specialty PRIMARY KEY CLUSTERED (resource_key, seq) WITH (DATA_COMPRESSION = PAGE)
GO

IF OBJECT_ID(<<s:schema>> + N'.practitioner_role_code', N'U') IS NULL
    CREATE TABLE <<schema>>.practitioner_role_code (
        release_date date NOT NULL, resource_key int NOT NULL, ndjson_file_id int NOT NULL, zst_file_id int NOT NULL,
        seq int NOT NULL,
        [system] varchar(512),
        code varchar(256),
        display nvarchar(1000),
        [text] nvarchar(1000)
    ) WITH (DATA_COMPRESSION = PAGE)
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(<<s:schema>> + N'.practitioner_role_code') AND name = N'pk_practitioner_role_code')
    ALTER TABLE <<schema>>.practitioner_role_code ADD CONSTRAINT pk_practitioner_role_code PRIMARY KEY CLUSTERED (resource_key, seq) WITH (DATA_COMPRESSION = PAGE)
GO

IF OBJECT_ID(<<s:schema>> + N'.organization_affiliation', N'U') IS NULL
    CREATE TABLE <<schema>>.organization_affiliation (
        release_date date NOT NULL, resource_key int NOT NULL, ndjson_file_id int NOT NULL, zst_file_id int NOT NULL,
        last_updated datetime2(3),
        active bit,
        organization_key int,
        participating_organization_key int,
        role_code varchar(256),
        role_display nvarchar(1000),
        role_text nvarchar(1000),
        period_start datetime2(3),
        period_end datetime2(3)
    ) WITH (DATA_COMPRESSION = PAGE)
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(<<s:schema>> + N'.organization_affiliation') AND name = N'pk_organization_affiliation')
    ALTER TABLE <<schema>>.organization_affiliation ADD CONSTRAINT pk_organization_affiliation PRIMARY KEY CLUSTERED (resource_key) WITH (DATA_COMPRESSION = PAGE)
GO

IF OBJECT_ID(<<s:schema>> + N'.organization_affiliation_network', N'U') IS NULL
    CREATE TABLE <<schema>>.organization_affiliation_network (
        release_date date NOT NULL, resource_key int NOT NULL, ndjson_file_id int NOT NULL, zst_file_id int NOT NULL,
        seq int NOT NULL,
        network_organization_key int
    ) WITH (DATA_COMPRESSION = PAGE)
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(<<s:schema>> + N'.organization_affiliation_network') AND name = N'pk_organization_affiliation_network')
    ALTER TABLE <<schema>>.organization_affiliation_network ADD CONSTRAINT pk_organization_affiliation_network PRIMARY KEY CLUSTERED (resource_key, seq) WITH (DATA_COMPRESSION = PAGE)
GO

IF OBJECT_ID(<<s:schema>> + N'.healthcare_service', N'U') IS NULL
    CREATE TABLE <<schema>>.healthcare_service (
        release_date date NOT NULL, resource_key int NOT NULL, ndjson_file_id int NOT NULL, zst_file_id int NOT NULL,
        last_updated datetime2(3),
        active bit,
        name nvarchar(1000),
        provided_by_organization_key int,
        network_organization_key int
    ) WITH (DATA_COMPRESSION = PAGE)
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(<<s:schema>> + N'.healthcare_service') AND name = N'pk_healthcare_service')
    ALTER TABLE <<schema>>.healthcare_service ADD CONSTRAINT pk_healthcare_service PRIMARY KEY CLUSTERED (resource_key) WITH (DATA_COMPRESSION = PAGE)
GO

IF OBJECT_ID(<<s:schema>> + N'.healthcare_service_location', N'U') IS NULL
    CREATE TABLE <<schema>>.healthcare_service_location (
        release_date date NOT NULL, resource_key int NOT NULL, ndjson_file_id int NOT NULL, zst_file_id int NOT NULL,
        seq int NOT NULL,
        location_key int
    ) WITH (DATA_COMPRESSION = PAGE)
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(<<s:schema>> + N'.healthcare_service_location') AND name = N'pk_healthcare_service_location')
    ALTER TABLE <<schema>>.healthcare_service_location ADD CONSTRAINT pk_healthcare_service_location PRIMARY KEY CLUSTERED (resource_key, seq) WITH (DATA_COMPRESSION = PAGE)
GO

IF OBJECT_ID(<<s:schema>> + N'.insurance_plan', N'U') IS NULL
    CREATE TABLE <<schema>>.insurance_plan (
        release_date date NOT NULL, resource_key int NOT NULL, ndjson_file_id int NOT NULL, zst_file_id int NOT NULL,
        last_updated datetime2(3),
        status varchar(256),
        name nvarchar(1000),
        type_code varchar(256),
        type_text nvarchar(1000),
        period_start datetime2(3),
        period_end datetime2(3),
        owned_by_organization_key int,
        administered_by_organization_key int
    ) WITH (DATA_COMPRESSION = PAGE)
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(<<s:schema>> + N'.insurance_plan') AND name = N'pk_insurance_plan')
    ALTER TABLE <<schema>>.insurance_plan ADD CONSTRAINT pk_insurance_plan PRIMARY KEY CLUSTERED (resource_key) WITH (DATA_COMPRESSION = PAGE)
GO

IF OBJECT_ID(<<s:schema>> + N'.insurance_plan_alias', N'U') IS NULL
    CREATE TABLE <<schema>>.insurance_plan_alias (
        release_date date NOT NULL, resource_key int NOT NULL, ndjson_file_id int NOT NULL, zst_file_id int NOT NULL,
        seq int NOT NULL,
        alias nvarchar(1000)
    ) WITH (DATA_COMPRESSION = PAGE)
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(<<s:schema>> + N'.insurance_plan_alias') AND name = N'pk_insurance_plan_alias')
    ALTER TABLE <<schema>>.insurance_plan_alias ADD CONSTRAINT pk_insurance_plan_alias PRIMARY KEY CLUSTERED (resource_key, seq) WITH (DATA_COMPRESSION = PAGE)
GO

IF OBJECT_ID(<<s:schema>> + N'.insurance_plan_network', N'U') IS NULL
    CREATE TABLE <<schema>>.insurance_plan_network (
        release_date date NOT NULL, resource_key int NOT NULL, ndjson_file_id int NOT NULL, zst_file_id int NOT NULL,
        seq int NOT NULL,
        network_organization_key int
    ) WITH (DATA_COMPRESSION = PAGE)
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(<<s:schema>> + N'.insurance_plan_network') AND name = N'pk_insurance_plan_network')
    ALTER TABLE <<schema>>.insurance_plan_network ADD CONSTRAINT pk_insurance_plan_network PRIMARY KEY CLUSTERED (resource_key, seq) WITH (DATA_COMPRESSION = PAGE)
GO

-- Every identifier from every resource type (join key for NPI etc.).
IF OBJECT_ID(<<s:schema>> + N'.identifier', N'U') IS NULL
    CREATE TABLE <<schema>>.identifier (
        release_date date NOT NULL, resource_key int NOT NULL, ndjson_file_id int NOT NULL, zst_file_id int NOT NULL,
        seq int NOT NULL,
        [system] varchar(512),
        [value] nvarchar(400),
        [use] varchar(256),
        type_code varchar(256),
        type_text nvarchar(1000),
        period_start datetime2(3),
        period_end datetime2(3)
    ) WITH (DATA_COMPRESSION = PAGE)
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(<<s:schema>> + N'.identifier') AND name = N'pk_identifier')
    ALTER TABLE <<schema>>.identifier ADD CONSTRAINT pk_identifier PRIMARY KEY CLUSTERED (resource_key, seq) WITH (DATA_COMPRESSION = PAGE)
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(<<s:schema>> + N'.identifier') AND name = N'identifier_value')
    CREATE INDEX identifier_value ON <<schema>>.identifier ([system], [value]) WITH (DATA_COMPRESSION = PAGE)
GO
