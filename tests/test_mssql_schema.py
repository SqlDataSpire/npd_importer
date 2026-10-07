import pytest

from npd_loader.flatten.engine import columns, key_columns, ref_columns
from npd_loader.flatten.specs import ALL_TABLES, SPECS
from npd_loader.flatten.stagefiles import HASH_COLUMNS


def scalar(d, sql, *args):
    with d.engine.connect() as conn:
        return conn.exec_driver_sql(sql, args).scalar()


def test_tables_have_spec_columns_and_primary_keys(mssql_dialect):
    d = mssql_dialect
    with d.engine.connect() as conn:
        for t in ALL_TABLES:
            cols = [r[0] for r in conn.exec_driver_sql(
                "SELECT name FROM sys.columns WHERE object_id = OBJECT_ID(?) ORDER BY column_id",
                (f"{d.cfg.schema}.{t.name}",))]
            assert cols == key_columns(t), t.name
            assert conn.exec_driver_sql("SELECT count(*) FROM sys.key_constraints WHERE parent_object_id = OBJECT_ID(?) "
                                        "AND type = 'PK'", (f"{d.cfg.schema}.{t.name}",)).scalar() == 1, t.name
            stage_cols = [r[0] for r in conn.exec_driver_sql(
                "SELECT name FROM sys.columns WHERE object_id = OBJECT_ID(?) ORDER BY column_id",
                (f"{d.cfg.stage_schema}.{t.name}",))]
            assert stage_cols == columns(t), f"stage {t.name}"
    assert scalar(d, "SELECT count(*) FROM sys.tables WHERE schema_id = SCHEMA_ID(?)", d.cfg.stage_schema) == 27
    assert scalar(d, "SELECT count(*) FROM sys.partition_functions WHERE name LIKE ?", f"pf_{d.cfg.schema}%") == 0
    assert scalar(d, "SELECT count(*) FROM sys.views WHERE schema_id = SCHEMA_ID(?)", d.cfg.schema) == 26
    assert scalar(d, "SELECT count(*) FROM sys.objects WHERE schema_id = SCHEMA_ID(?) AND type IN ('FN','IF')",
                  d.cfg.schema) == 0
    for col in ("resource_key", "resource_type_id", "resource_id", "hash", "release_date", "run_id",
                "last_seen_release", "last_seen_run_id"):
        assert scalar(d, "SELECT COL_LENGTH(?, ?)", f"{d.cfg.schema}.resource_state", col) is not None, col
    assert scalar(d, "SELECT COL_LENGTH(?, 'resource_type')", f"{d.cfg.schema}.resource_state") is None
    assert scalar(d, "SELECT COL_LENGTH(?, 'not_seen_resources')", f"{d.cfg.schema}.release") is not None


def test_resource_types_are_seeded_and_keys_are_ints(mssql_dialect):
    d = mssql_dialect
    with d.engine.connect() as conn:
        types = dict(conn.exec_driver_sql(f"SELECT name, resource_type_id FROM {d.q(d.cfg.schema, 'resource_type')}").fetchall())
        assert set(types) == set(SPECS) and sorted(types.values()) == list(range(1, 9))
        assert conn.exec_driver_sql(
            "SELECT TYPE_NAME(system_type_id) + ':' + CAST(is_identity AS varchar(1)) FROM sys.columns "
            "WHERE object_id = OBJECT_ID(?) AND name = 'resource_key'", (f"{d.cfg.schema}.resource_state",)).scalar() == "int:1"
        assert conn.exec_driver_sql(
            "SELECT TYPE_NAME(system_type_id) FROM sys.columns WHERE object_id = OBJECT_ID(?) AND name = 'hash'",
            (f"{d.cfg.schema}.resource_state",)).scalar() == "binary"
        assert conn.exec_driver_sql(
            "SELECT count(*) FROM sys.indexes WHERE object_id = OBJECT_ID(?) AND is_unique = 1 AND is_primary_key = 0",
            (f"{d.cfg.schema}.resource_state",)).scalar() == 1
        for t in ALL_TABLES:                         # every *_key column is int; staging id columns are varchar
            kinds = dict(conn.exec_driver_sql(
                "SELECT name, TYPE_NAME(system_type_id) FROM sys.columns WHERE object_id = OBJECT_ID(?)",
                (f"{d.cfg.schema}.{t.name}",)).fetchall())
            assert all(v == "int" for k, v in kinds.items() if k.endswith("_key")), t.name
            stage = dict(conn.exec_driver_sql(
                "SELECT name, TYPE_NAME(system_type_id) FROM sys.columns WHERE object_id = OBJECT_ID(?)",
                (f"{d.cfg.stage_schema}.{t.name}",)).fetchall())
            assert stage["resource_id"] == "varchar" and all(stage[c] == "varchar" for c in ref_columns(t)), t.name


def test_init_db_refuses_a_pre_key_resource_state(mssql_dialect):
    d = mssql_dialect
    st = d.q(d.cfg.schema, "resource_state")
    with d.engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        conn.exec_driver_sql(f"DROP TABLE {st}")
        conn.exec_driver_sql(f"CREATE TABLE {st} (resource_type varchar(40) NOT NULL, resource_id varchar(128) NOT NULL)")
    with pytest.raises(Exception, match="predates surrogate keys"):
        d.init_db()


def test_init_db_is_idempotent(mssql_dialect):
    mssql_dialect.init_db()
    mssql_dialect.init_db()


def test_init_db_recreates_a_staging_table_whose_columns_changed(mssql_dialect):
    d = mssql_dialect
    stage = d.cfg.stage_schema
    with d.engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        conn.exec_driver_sql(f"ALTER TABLE {d.q(stage, 'practitioner')} DROP COLUMN npi")
        conn.exec_driver_sql(f"ALTER TABLE {d.q(stage, 'resource_hash')} DROP COLUMN line_number")
    d.init_db()
    practitioner = next(t for t in ALL_TABLES if t.name == "practitioner")
    with d.engine.connect() as conn:
        assert d._columns_of(conn, stage, "practitioner") == columns(practitioner)
        assert d._columns_of(conn, stage, "resource_hash") == list(HASH_COLUMNS)
