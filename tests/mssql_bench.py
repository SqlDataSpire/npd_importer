"""Benchmark the SQL Server transform scripts on a sample of a real raw release (not collected by pytest).

Copies up to --per-type rows of each resource type from a read-only source raw table (default: the dev import's
npd_dev.npd_raw.resource__20260929__r7009) into scratch schemas of the NPD_TEST_MSSQL_DB database, runs init_db and
run_transforms there, prints rows/s per script with an extrapolation to the full release, and drops the scratch schemas.

    set NPD_TEST_MSSQL_DB={"type":"mssql","server":"cssnpi","database":"npd_test","trusted":"yes"}
    .venv\\Scripts\\python tests\\mssql_bench.py --per-type 100000
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
import uuid
from datetime import date, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from mssql_helpers import MSSQL_ENV, drop_schemas, sql_connection_object  # noqa: E402
from npd_loader.config import NpdDbConfig  # noqa: E402
from npd_loader.dialect.mssql import MssqlDialect  # noqa: E402
from npd_loader.dialect import mssql  # noqa: E402
from npd_loader.raw_load import RAW_PARENT  # noqa: E402
from npd_loader.sqltext import sql_scripts  # noqa: E402
from sqlalchemy import event  # noqa: E402

SOURCE = "npd_dev.npd_raw.resource__20260929__r7009"
RELEASE = date(2026, 9, 29)
RUN_ID = 9
# Full-release rows per resource type (2026-09-29), for the extrapolation.
FULL = {"Practitioner": 7_481_906, "PractitionerRole": 11_059_682, "Location": 2_558_069,
        "Organization": 2_058_139, "Endpoint": 1_140_616, "OrganizationAffiliation": 493_222,
        "HealthcareService": 54_445, "InsurancePlan": 6_143}
# The resource types each script reads (None: every type).
SCRIPT_TYPES = {"010": ["Practitioner"], "020": ["Organization"], "030": ["Location"], "040": ["Endpoint"],
                "050": ["PractitionerRole"], "060": ["OrganizationAffiliation"], "070": ["HealthcareService"],
                "080": ["InsurancePlan"], "090": None}


def session_cpu(conn) -> int:
    """CPU milliseconds used so far by this connection's session (all workers of finished statements)."""
    cur = conn.connection.dbapi_connection.cursor()
    try:
        return cur.execute("SELECT cpu_time FROM sys.dm_exec_sessions WHERE session_id = @@SPID").fetchone()[0]
    finally:
        cur.close()


class ScriptTimer(logging.Handler):
    """Timestamps the dialect's 'running transform <script>' log lines."""

    def __init__(self) -> None:
        super().__init__()
        self.marks: list[tuple[str, float]] = []

    def emit(self, record: logging.LogRecord) -> None:
        if record.getMessage().startswith("running transform "):
            self.marks.append((record.args[0], time.perf_counter()))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--per-type", type=int, default=100_000)
    parser.add_argument("--source", default=SOURCE)
    parser.add_argument("--keep", action="store_true", help="keep the scratch schemas (drop them by hand)")
    parser.add_argument("--types", help="comma-separated resource types to sample (default: all)")
    parser.add_argument("--scripts", help="comma-separated script prefixes to run, e.g. 010,090 (default: all)")
    parser.add_argument("--statements", action="store_true", help="also print the time of every statement")
    parser.add_argument("--chunk-rows", type=int, help=f"rows per transform chunk (default {mssql.CHUNK_ROWS:,})")
    args = parser.parse_args()
    types = args.types.split(",") if args.types else list(FULL)
    if args.scripts:
        wanted = tuple(args.scripts.split(","))
        mssql.sql_scripts = lambda flavor, kind: [s for s in sql_scripts(flavor, kind)
                                                  if kind != "transform" or s[0].startswith(wanted)]

    engine = sql_connection_object("bench", json.loads(os.environ[MSSQL_ENV])).engine
    base = f"bench{uuid.uuid4().hex[:8]}"
    raw_schema, schema = f"{base}_raw", base
    dialect = MssqlDialect(engine, NpdDbConfig(connection="data", raw_schema=raw_schema, schema=schema))
    if args.chunk_rows:
        dialect.chunk_rows = args.chunk_rows
    timer = ScriptTimer()
    log = logging.getLogger("npd_loader.dialect.mssql")
    log.setLevel(logging.INFO)
    log.addHandler(timer)
    print(f"{datetime.now():%Y-%m-%d %H:%M:%S} scratch schemas {schema}, {raw_schema}; source {args.source}")
    try:
        dialect.init_db()
        sample: dict[str, int] = {}
        t0 = time.perf_counter()
        with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            raw_table = dialect._create_standalone(conn, raw_schema, RAW_PARENT, RELEASE, RUN_ID)
            for rtype in types:
                conn.exec_driver_sql(
                    f"INSERT INTO {dialect.q(raw_schema, raw_table)} WITH (TABLOCK) "
                    f"SELECT TOP ({args.per_type}) * FROM {args.source} "
                    f"WHERE release_date = ? AND resource_type = ? ORDER BY resource_id", (RELEASE, rtype))
                sample[rtype] = conn.exec_driver_sql(
                    f"SELECT COUNT_BIG(*) FROM {dialect.q(raw_schema, raw_table)} WHERE resource_type = ?",
                    (rtype,)).scalar()
            dialect._clone_indexes(conn, raw_schema, RAW_PARENT, raw_table)
            others = conn.exec_driver_sql(
                "SELECT COUNT(*) FROM sys.dm_exec_requests WHERE session_id <> @@SPID AND session_id > 50 "
                "AND status IN ('running', 'runnable', 'suspended')").scalar()
        print(f"sampled {sum(sample.values()):,} rows in {time.perf_counter() - t0:.0f} s: {sample}")
        print(f"other active user requests on the server at start: {others}")

        if args.statements:
            @event.listens_for(engine, "before_cursor_execute")
            def before(conn, cursor, statement, parameters, context, executemany):
                conn.info["bench_t"] = time.perf_counter()
                conn.info["bench_cpu"] = session_cpu(conn)

            @event.listens_for(engine, "after_cursor_execute")
            def after(conn, cursor, statement, parameters, context, executemany):
                code = [line for line in statement.splitlines() if line.strip() and not line.lstrip().startswith("--")]
                if code and code[0].lstrip().startswith(("INSERT", "SELECT r.", "DROP TABLE")):
                    cpu = (session_cpu(conn) - conn.info["bench_cpu"]) / 1000
                    print(f"    {time.perf_counter() - conn.info['bench_t']:8.1f} s {cpu:8.1f} cpu-s  "
                          f"{code[0].strip()[:60]}")

        start = time.perf_counter()
        original_clone = dialect._clone_indexes
        index_start: list[float] = []

        def clone(*a, **kw):
            if not index_start:
                index_start.append(time.perf_counter())
            return original_clone(*a, **kw)

        dialect._clone_indexes = clone
        result = dialect.run_transforms(raw_table, RELEASE, RUN_ID)
        end = time.perf_counter()
        marks = timer.marks + [("(indexes + counts)", index_start[0])]

        print(f"\n{datetime.now():%Y-%m-%d %H:%M:%S} transforms finished in {end - start:.1f} s")
        print(f"{'script':<36}{'seconds':>9}{'rows in':>10}{'rows/s':>10}{'full min':>10}")
        total_full = 0.0
        for (script, t), (_, t_next) in zip(marks, marks[1:] + [("", end)]):
            seconds = t_next - t
            if script.startswith("("):
                rows_in, full_rows = sum(sample.values()), sum(FULL[t] for t in sample)
                index_min = seconds * full_rows / rows_in / 60
                print(f"{script:<36}{seconds:>9.1f}{rows_in:>10,}{'':>10}{index_min:>10.1f}")
                continue
            script_types = [t for t in (SCRIPT_TYPES.get(script[:3]) or FULL) if t in sample]
            rows_in = sum(sample[t] for t in script_types)
            full_rows = sum(FULL[t] for t in script_types)
            rate = rows_in / seconds if seconds > 0 else float("inf")
            full_min = full_rows / rate / 60 if rate else 0.0
            total_full += full_min
            print(f"{script:<36}{seconds:>9.1f}{rows_in:>10,}{rate:>10,.0f}{full_min:>10.1f}")
        print(f"{'all scripts, extrapolated':<36}{'':>9}{'':>10}{'':>10}{total_full:>10.1f}")
        print(f"{'  + indexes and counts (linear)':<36}{'':>9}{'':>10}{'':>10}{total_full + index_min:>10.1f}")
        print("\nrow counts:", json.dumps(result.counts, sort_keys=True))
    finally:
        if args.keep:
            print(f"kept scratch schemas {schema}, {raw_schema}")
        else:
            drop_schemas(engine, [schema, raw_schema])
            print(f"dropped scratch schemas {schema}, {raw_schema}")
        engine.dispose()


if __name__ == "__main__":
    main()
