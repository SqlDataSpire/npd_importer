from mssql_helpers import drop_schemas


def test_schemas_are_created_and_dropped(mssql_engine, mssql_schemas):
    raw, data = mssql_schemas("raw", "")
    assert raw == data + "_raw"
    with mssql_engine.connect() as conn:
        found = {r[0] for r in conn.exec_driver_sql(
            "SELECT name FROM sys.schemas WHERE name IN (?, ?)", (raw, data))}
    assert found == {raw, data}


def test_drop_schemas_removes_objects(mssql_engine, mssql_schemas):
    (s,) = mssql_schemas("x")
    with mssql_engine.begin() as conn:
        conn.exec_driver_sql(f"CREATE PARTITION FUNCTION [pf_{s}_release] (date) AS RANGE RIGHT FOR VALUES ()")
        conn.exec_driver_sql(f"CREATE PARTITION SCHEME [ps_{s}_release] AS PARTITION [pf_{s}_release] ALL TO ([PRIMARY])")
        conn.exec_driver_sql(f"CREATE TABLE [{s}].[t] (release_date date NOT NULL) ON [ps_{s}_release] (release_date)")
        conn.exec_driver_sql(f"CREATE VIEW [{s}].[v_t] AS SELECT * FROM [{s}].[t]")
    drop_schemas(mssql_engine, [s])
    with mssql_engine.connect() as conn:
        assert conn.exec_driver_sql("SELECT count(*) FROM sys.schemas WHERE name = ?", (s,)).scalar() == 0
        assert conn.exec_driver_sql("SELECT count(*) FROM sys.partition_functions WHERE name = ?",
                                    (f"pf_{s}_release",)).scalar() == 0
