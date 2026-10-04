from datetime import date

import psycopg
import pytest
from psycopg import sql

from npd_loader.db import (advisory_lock, list_parent_tables, list_release_partitions, published_releases,
                           render_sql, standalone_name)
from npd_loader.schema import init_db

NPD_TABLES = [
    "endpoint", "healthcare_service", "healthcare_service_location", "identifier", "insurance_plan",
    "insurance_plan_alias", "insurance_plan_network", "location", "location_telecom", "organization",
    "organization_address", "organization_affiliation", "organization_endpoint", "organization_telecom",
    "practitioner", "practitioner_address", "practitioner_name", "practitioner_qualification",
    "practitioner_role", "practitioner_role_code", "practitioner_role_endpoint", "practitioner_role_location",
    "practitioner_role_specialty", "practitioner_telecom",
]


def test_init_db_is_idempotent_and_creates_parents_and_views(npd_db):
    init_db(npd_db, "npd_raw", "npd")
    with psycopg.connect(npd_db) as conn:
        assert list_parent_tables(conn, "npd") == NPD_TABLES
        assert list_parent_tables(conn, "npd_raw") == ["resource"]
        views = {r[0] for r in conn.execute(
            "SELECT schemaname || '.' || viewname FROM pg_views WHERE schemaname IN ('npd', 'npd_raw')")}
        assert views == {f"npd.v_{t}" for t in NPD_TABLES} | {"npd_raw.v_resource"}


def test_helper_functions(npd_db):
    with psycopg.connect(npd_db) as conn:
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
    with psycopg.connect(npd_db) as conn:
        conn.execute("CREATE TABLE npd.endpoint__20260929__r1 (LIKE npd.endpoint INCLUDING DEFAULTS)")
        conn.execute("ALTER TABLE npd.endpoint ATTACH PARTITION npd.endpoint__20260929__r1 "
                     "FOR VALUES IN ('2026-09-29')")
        assert list_release_partitions(conn, "npd", "endpoint") == {date(2026, 9, 29): "endpoint__20260929__r1"}
        assert list_release_partitions(conn, "npd", "location") == {}
        conn.execute("INSERT INTO npd.release (release_date, import_run_id) VALUES ('2026-09-29', 1)")
    assert published_releases(npd_db, "npd") == [date(2026, 9, 29)]


def test_render_sql(npd_db):
    with psycopg.connect(npd_db) as conn:
        text = "SELECT * FROM <<t:practitioner>> WHERE release_date = <<release>>"
        out = render_sql(text, {"t:practitioner": sql.Identifier("npd", "p"),
                                "release": sql.Literal(date(2026, 9, 29))}, conn)
        assert out == "SELECT * FROM \"npd\".\"p\" WHERE release_date = '2026-09-29'::date"
        with pytest.raises(KeyError, match="<<raw>>"):
            render_sql("SELECT <<raw>>", {}, conn)


def test_standalone_names_fit_postgres_limit():
    longest = max(NPD_TABLES + ["resource"], key=len)
    assert standalone_name(longest, date(2026, 9, 29), 999_999_999).endswith("__20260929__r999999999")
    assert len(standalone_name("resource", date(2026, 9, 29), 999_999_999) + "__organizationaffiliation") <= 63
    with pytest.raises(ValueError):
        standalone_name("x" * 60, date(2026, 9, 29), 1)


def test_advisory_lock_is_exclusive(npd_db):
    with advisory_lock(npd_db, "import") as first:
        assert first is True
        with advisory_lock(npd_db, "import") as second:
            assert second is False
        with advisory_lock(npd_db, "download") as other:
            assert other is True
    with advisory_lock(npd_db, "import") as again:
        assert again is True
