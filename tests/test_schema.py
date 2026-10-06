from datetime import date

import pytest
from sqlalchemy import create_engine

from npd_loader.config import NpdDbConfig
from npd_loader.dialect.postgres import MAX_IDENTIFIER, PostgresDialect, csv_batch, lock_key
from npd_loader.sqltext import render, sql_scripts, standalone_name
from helpers import pg_dialect
from pg_helpers import connect

list_parent_tables = PostgresDialect.parent_tables
list_release_partitions = PostgresDialect.release_partitions


def offline_dialect() -> PostgresDialect:
    """A dialect whose engine never connects (create_engine is lazy): for the pure-Python helpers."""
    return PostgresDialect(create_engine("postgresql+psycopg2://u:p@localhost:5432/npd"),
                           NpdDbConfig(connection="data", raw_schema="npd_raw", schema="npd"))

NPD_TABLES = [
    "endpoint", "healthcare_service", "healthcare_service_location", "identifier", "insurance_plan",
    "insurance_plan_alias", "insurance_plan_network", "location", "location_telecom", "organization",
    "organization_address", "organization_affiliation", "organization_affiliation_network",
    "organization_endpoint", "organization_telecom",
    "practitioner", "practitioner_address", "practitioner_name", "practitioner_qualification",
    "practitioner_role", "practitioner_role_code", "practitioner_role_endpoint", "practitioner_role_location",
    "practitioner_role_specialty", "practitioner_role_telecom", "practitioner_telecom",
]


def test_init_db_is_idempotent_and_creates_parents_and_views(npd_db):
    pg_dialect(npd_db).init_db()
    with connect(npd_db) as conn:
        assert list_parent_tables(conn.raw, "npd") == NPD_TABLES
        assert list_parent_tables(conn.raw, "npd_raw") == ["resource"]
        views = {r[0] for r in conn.execute(
            "SELECT schemaname || '.' || viewname FROM pg_views WHERE schemaname IN ('npd', 'npd_raw')")}
        assert views == {f"npd.v_{t}" for t in NPD_TABLES} | {"npd_raw.v_resource"}


def test_migrations_script_runs_last():
    names = [name for name, _ in sql_scripts("postgres", "init")]
    assert names[-1] == "900_migrations.sql" and names == sorted(names)


def test_init_db_applies_add_column_migrations_idempotently(npd_db):
    migration = "ALTER TABLE <<schema>>.practitioner ADD COLUMN IF NOT EXISTS test_added text;"
    with connect(npd_db) as conn:          # an existing published release
        conn.execute("CREATE TABLE npd.practitioner__20260929__r1 (LIKE npd.practitioner)")
        conn.execute("ALTER TABLE npd.practitioner ATTACH PARTITION npd.practitioner__20260929__r1 "
                     "FOR VALUES IN ('2026-09-29')")
    for _ in range(2):
        pg_dialect(npd_db).init_db(extra_scripts=[migration])
    with connect(npd_db) as conn:
        def has_column(table):
            return conn.execute("SELECT count(*) FROM information_schema.columns WHERE table_schema = 'npd' "
                                "AND table_name = %s AND column_name = 'test_added'", (table,)).fetchone()[0]
        assert has_column("practitioner") == 1
        assert has_column("practitioner__20260929__r1") == 1
        assert has_column("v_practitioner") == 1      # views are recreated with the new column


def test_helper_functions(npd_db):
    with connect(npd_db) as conn:
        def one(q, *args):
            return conn.execute(q, args).fetchone()[0]
        assert one("SELECT npd.ref_id('Organization/Organization-123')") == "Organization-123"
        assert one("SELECT npd.ref_id(NULL)") is None
        res = '{"extension": [{"url": "u1", "valueBoolean": true}], ' \
              '"identifier": [{"system": "a", "value": "1"}, {"system": "b", "value": "2"}]}'
        assert one("SELECT npd.ext(%s::jsonb, 'u1')->>'valueBoolean'", res) == "true"
        assert one("SELECT npd.ext(%s::jsonb, 'nope')", res) is None
        assert one("SELECT npd.identifier_value(%s::jsonb, ARRAY['b', 'a'])", res) == "1"
        assert one("SELECT npd.join_text('[\"A\", \"B\"]'::jsonb, ' ')") == "A B"
        assert one("SELECT npd.join_text('[]'::jsonb, ' ')") is None
        conn.execute("SET TIME ZONE 'UTC'")
        assert one("SELECT npd.fhir_ts('2020')::text") == "2020-01-01 00:00:00+00"
        assert one("SELECT npd.fhir_ts('2020-05')::text") == "2020-05-01 00:00:00+00"
        assert one("SELECT npd.fhir_ts('2007-08-31T00:00:00Z')::text") == "2007-08-31 00:00:00+00"


def test_release_partitions_are_discovered(npd_db):
    with connect(npd_db) as conn:
        conn.execute("CREATE TABLE npd.endpoint__20260929__r1 (LIKE npd.endpoint INCLUDING DEFAULTS)")
        conn.execute("ALTER TABLE npd.endpoint ATTACH PARTITION npd.endpoint__20260929__r1 "
                     "FOR VALUES IN ('2026-09-29')")
        assert list_release_partitions(conn.raw, "npd", "endpoint") == {date(2026, 9, 29): "endpoint__20260929__r1"}
        assert list_release_partitions(conn.raw, "npd", "location") == {}
        conn.execute("INSERT INTO npd.release (release_date, import_run_id) VALUES ('2026-09-29', 1)")
    d = pg_dialect(npd_db)
    assert d.published_releases() == [date(2026, 9, 29)]
    assert d.partitioned_releases() == {date(2026, 9, 29)}
    assert d.is_published(date(2026, 9, 29)) is False      # npd_raw.resource has no partition for it


def test_render_tokens_quote_like_psycopg():
    d = offline_dialect()
    text = "SELECT * FROM <<t:practitioner>> WHERE release_date = <<release>>"
    out = render(text, {"t:practitioner": d.q("npd", "p"), "release": d.lit(date(2026, 9, 29))})
    assert out == "SELECT * FROM \"npd\".\"p\" WHERE release_date = '2026-09-29'::date"
    assert d.q('we"ird') == '"we""ird"'
    assert d.lit("Practitioner") == "'Practitioner'" and d.lit("O'Brien") == "'O''Brien'"
    with pytest.raises(KeyError, match="<<raw>>"):
        render("SELECT <<raw>>", {})


def test_csv_batch_nulls_and_quoting():
    batch = csv_batch([("2026-09-29", "Practitioner", "P-1", None, 500, 501, 1, '{"a": "x,\\"y\\"", "b": ""}'),
                       ("2026-09-29", "Practitioner", "P-2", "", 500, 501, 2, "{}")])
    assert batch.read() == (
        '"2026-09-29","Practitioner","P-1",,"500","501","1","{""a"": ""x,\\""y\\"""", ""b"": """"}"\n'
        '"2026-09-29","Practitioner","P-2","","500","501","2","{}"\n')


def test_lock_keys_are_stable_and_distinct():
    assert lock_key("import") == lock_key("import") != lock_key("download")
    assert -2**63 <= lock_key("import") < 2**63


def test_standalone_names_fit_postgres_limit():
    longest = max(NPD_TABLES + ["resource"], key=len)
    assert standalone_name(longest, date(2026, 9, 29), 999_999_999, MAX_IDENTIFIER).endswith("__20260929__r999999999")
    assert len(standalone_name("resource", date(2026, 9, 29), 999_999_999, MAX_IDENTIFIER)
               + "__organizationaffiliation") <= 63
    with pytest.raises(ValueError):
        standalone_name("x" * 60, date(2026, 9, 29), 1, MAX_IDENTIFIER)


def test_advisory_lock_is_exclusive(npd_db):
    d = pg_dialect(npd_db)
    with d.run_lock("import") as first:
        assert first is True
        with d.run_lock("import") as second:
            assert second is False
        with d.run_lock("download") as other:
            assert other is True
    with d.run_lock("import") as again:
        assert again is True
