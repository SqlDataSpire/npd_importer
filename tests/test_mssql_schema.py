from datetime import date

PARENTS = {"endpoint", "healthcare_service", "healthcare_service_location", "identifier", "insurance_plan",
           "insurance_plan_alias", "insurance_plan_network", "location", "location_telecom", "organization",
           "organization_address", "organization_affiliation", "organization_affiliation_network",
           "organization_endpoint", "organization_telecom", "practitioner", "practitioner_address",
           "practitioner_name", "practitioner_qualification", "practitioner_role", "practitioner_role_code",
           "practitioner_role_endpoint", "practitioner_role_location", "practitioner_role_specialty",
           "practitioner_role_telecom", "practitioner_telecom"}


def scalar(d, sql, *args):
    with d.engine.connect() as conn:
        return conn.exec_driver_sql(sql, args).scalar()


def test_init_db_creates_partitioned_parents_and_views(mssql_dialect):
    d = mssql_dialect
    with d.engine.connect() as conn:
        assert set(d.parent_tables(conn, d.cfg.schema)) == PARENTS
        assert d.parent_tables(conn, d.cfg.raw_schema) == ["resource"]
    for schema in (d.cfg.raw_schema, d.cfg.schema):
        assert scalar(d, "SELECT count(*) FROM sys.partition_functions WHERE name = ?", f"pf_{schema}_release") == 1
    assert scalar(d, "SELECT count(*) FROM sys.views WHERE schema_id = SCHEMA_ID(?)", d.cfg.schema) == len(PARENTS)
    assert scalar(d, f"SELECT count(*) FROM {d.q(d.cfg.schema, 'v_practitioner')}") == 0
    assert scalar(d, "SELECT collation_name FROM sys.columns WHERE object_id = OBJECT_ID(?) AND name = 'resource'",
                  f"{d.cfg.raw_schema}.resource") == "Latin1_General_100_CI_AS_SC_UTF8"
    assert scalar(d, "SELECT DISTINCT data_compression_desc FROM sys.partitions WHERE object_id = OBJECT_ID(?)",
                  f"{d.cfg.schema}.practitioner") == "PAGE"
    assert d.published_releases() == [] and d.partitioned_releases() == set()
    assert d.is_published(date(2026, 9, 29)) is False


def test_init_db_is_idempotent(mssql_dialect):
    mssql_dialect.init_db()
    mssql_dialect.init_db()


def test_helper_functions(mssql_dialect):
    d = mssql_dialect
    s = d.q(d.cfg.schema)
    res = '{"extension": [{"url": "a", "valueBoolean": true}], "identifier": [{"system": "x", "value": "1"}, {"system": "npi", "value": "2"}], "name": ["A", "B", "C"]}'
    assert scalar(d, f"SELECT {s}.ref_id('Organization/Organization-1')") == "Organization-1"
    assert scalar(d, f"SELECT {s}.ref_id(NULL)") is None
    assert scalar(d, f"SELECT JSON_VALUE(e.ext, '$.valueBoolean') FROM {s}.ext(?, 'a') e", res) == "true"
    assert scalar(d, f"SELECT v.value FROM {s}.identifier_value(?, '[\"npi\",\"y\"]') v", res) == "2"
    assert scalar(d, f"SELECT j.txt FROM {s}.join_text(JSON_QUERY(?, '$.name'), ' ', 0) j", res) == "A B C"
    assert scalar(d, f"SELECT j.txt FROM {s}.join_text(JSON_QUERY(?, '$.name'), ', ', 2) j", res) == "C"
    assert scalar(d, f"SELECT j.txt FROM {s}.join_text(NULL, ' ', 0) j") is None
    assert str(scalar(d, f"SELECT {s}.fhir_ts('2020')")) == "2020-01-01 00:00:00"
    assert str(scalar(d, f"SELECT {s}.fhir_ts('2020-05')")) == "2020-05-01 00:00:00"
    assert str(scalar(d, f"SELECT {s}.fhir_ts('2026-09-29T04:34:00.724328Z')")) == "2026-09-29 04:34:00.724000"
    assert str(scalar(d, f"SELECT {s}.fhir_ts('2026-09-29T01:00:00-05:00')")) == "2026-09-29 06:00:00"
