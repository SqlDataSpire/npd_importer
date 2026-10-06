"""SQL Server test helpers. Tests get a connection from NPD_TEST_MSSQL_DB (a database.env-style JSON document)."""
from __future__ import annotations

import contextlib
import io

from sqlalchemy.engine import Engine

MSSQL_ENV = "NPD_TEST_MSSQL_DB"


def sql_connection_object(name: str, doc: dict):
    with contextlib.redirect_stdout(io.StringIO()):
        import DataEngine
    return DataEngine.SqlConnectionObject(name=name, server=doc["server"], database=doc["database"],
                                          UN=doc.get("UN", ""), PW=doc.get("PW", ""),
                                          trusted=doc.get("trusted", "no"))


def drop_schemas(engine: Engine, schemas: list[str]) -> None:
    """Drop every view, table and function in `schemas`, their pf_/ps_ partition objects, then the schemas."""
    order = "CASE o.type WHEN 'V' THEN 0 WHEN 'U' THEN 1 ELSE 2 END"
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        for s in schemas:
            objects = conn.exec_driver_sql(
                f"SELECT o.name, RTRIM(o.type) FROM sys.objects o WHERE o.schema_id = SCHEMA_ID(?) "
                f"AND o.type IN ('V', 'U', 'IF', 'FN', 'TF') ORDER BY {order}", (s,)).fetchall()
            for name, kind in objects:
                what = {"V": "VIEW", "U": "TABLE"}.get(kind, "FUNCTION")
                conn.exec_driver_sql(f"DROP {what} [{s}].[{name}]")
            if conn.exec_driver_sql("SELECT count(*) FROM sys.partition_schemes WHERE name = ?",
                                    (f"ps_{s}_release",)).scalar():
                conn.exec_driver_sql(f"DROP PARTITION SCHEME [ps_{s}_release]")
            if conn.exec_driver_sql("SELECT count(*) FROM sys.partition_functions WHERE name = ?",
                                    (f"pf_{s}_release",)).scalar():
                conn.exec_driver_sql(f"DROP PARTITION FUNCTION [pf_{s}_release]")
            if conn.exec_driver_sql("SELECT count(*) FROM sys.schemas WHERE name = ?", (s,)).scalar():
                conn.exec_driver_sql(f"DROP SCHEMA [{s}]")
