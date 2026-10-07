"""SQL Server flavor: every npd-database operation, on a SqlConnectionObject's engine (pyodbc)."""
from __future__ import annotations

import logging
import re
import shutil
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import date
from typing import Callable, Iterator, TypeVar

from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import DBAPIError

from npd_loader.config import NpdDbConfig
from npd_loader.dialect import DeltaResult, LockUnavailable, StageResult
from npd_loader.dialect.bcp import bcp_in, bcp_target
from npd_loader.flatten.engine import columns
from npd_loader.flatten.specs import ALL_TABLES, TABLE_TYPES
from npd_loader.flatten.stagefiles import HASH_TABLE, FlattenError, flatten_files
from npd_loader.sqltext import render, split_batches, sql_scripts

log = logging.getLogger(__name__)
ERROR_NUMBER_RE = re.compile(r"\((\d+)\)")
LOCK_ATTEMPTS = 3
LOCK_BACKOFF_SECONDS = 2.0
T = TypeVar("T")


def error_number(exc: BaseException) -> int | None:
    """SQL Server native error number from a pyodbc error message, e.g. '... exceeded. (1222) (SQLExecDirectW)'."""
    match = ERROR_NUMBER_RE.search(str(getattr(exc, "orig", exc)))
    return int(match.group(1)) if match else None


class MssqlDialect:
    name = "mssql"

    def __init__(self, engine: Engine, cfg: NpdDbConfig, sleep: Callable[[float], None] = time.sleep):
        self.engine = engine
        self.cfg = cfg
        self._sleep = sleep

    # -- naming -------------------------------------------------------------------------------------------
    @staticmethod
    def q(*parts: str) -> str:
        return ".".join("[" + p.replace("]", "]]") + "]" for p in parts)


    @staticmethod
    def lit(value: str) -> str:
        return "N'" + value.replace("'", "''") + "'"

    def _tokens(self) -> dict[str, str]:
        return {"schema": self.q(self.cfg.schema), "s:schema": self.lit(self.cfg.schema),
                "stage_schema": self.q(self.cfg.stage_schema), "s:stage_schema": self.lit(self.cfg.stage_schema)}

    def _autocommit(self) -> Connection:
        return self.engine.connect().execution_options(isolation_level="AUTOCOMMIT")

    # -- catalog views ------------------------------------------------------------------------------------


    # -- Dialect --------------------------------------------------------------------------------------------
    def init_db(self) -> None:
        tokens = self._tokens()
        schema, stage = self.cfg.schema, self.cfg.stage_schema
        with self._autocommit() as conn:
            for name, text in sql_scripts("mssql", "init"):
                for batch in split_batches(render(text, tokens)):
                    conn.exec_driver_sql(batch)
            for t in ALL_TABLES:
                # fixed staging heap, same columns (in spec order) as the permanent table; created once, reused
                if conn.exec_driver_sql("SELECT OBJECT_ID(?, 'U')", (f"{stage}.{t.name}",)).scalar() is None:
                    cols = ", ".join(self.q(c) for c in columns(t))
                    conn.exec_driver_sql(f"SELECT TOP 0 {cols} INTO {self.q(stage, t.name)} FROM {self.q(schema, t.name)}")
                conn.exec_driver_sql(f"CREATE OR ALTER VIEW {self.q(schema, 'v_' + t.name)} AS "
                                     f"SELECT * FROM {self.q(schema, t.name)}")

    def published_releases(self) -> list[date]:
        with self.engine.connect() as conn:
            rows = conn.exec_driver_sql(
                f"SELECT release_date FROM {self.q(self.cfg.schema, 'release')} ORDER BY release_date").fetchall()
        return [r[0] for r in rows]


    # -- raw load -----------------------------------------------------------------------------------------


    def _stage_tables(self) -> list[str]:
        return [t.name for t in ALL_TABLES] + [HASH_TABLE]

    def truncate_stage(self) -> None:
        with self._autocommit() as conn:
            for table in self._stage_tables():
                conn.exec_driver_sql(f"TRUNCATE TABLE {self.q(self.cfg.stage_schema, table)}")

    def stage_release(self, storage, release: date, run_id: int, inputs: list) -> StageResult:
        """Empty the fixed staging tables, flatten the release's files in parallel and bcp them in. Holding the import
        lock, this is the only writer of staging; leftovers of a killed run are removed by the truncate."""
        s = self.cfg.stage_schema
        self.truncate_stage()
        out_dir = storage.local_path(f"stage/run_{run_id}")
        try:
            flat = flatten_files(inputs, storage, release, out_dir, self.cfg.flatten_workers)
            server, database = bcp_target(self.engine)
            with ThreadPoolExecutor(self.cfg.bcp_workers) as pool:
                futures = [pool.submit(bcp_in, server, database, s, f.table, f.path, f.rows) for f in flat.files]
                for fut in futures:
                    fut.result()
        finally:
            shutil.rmtree(out_dir, ignore_errors=True)
        with self.engine.connect() as conn:
            for table, expected in flat.rows.items():
                got = conn.exec_driver_sql(f"SELECT COUNT_BIG(*) FROM {self.q(s, table)}").scalar()
                if got != expected:
                    raise FlattenError(f"staging table {table}: flattened {expected} rows but loaded {got}")
            dups = conn.exec_driver_sql(
                f"SELECT TOP 20 resource_type, resource_id, MIN(ndjson_file_id), STRING_AGG(CAST(line_number AS "
                f"varchar(20)), ',') WITHIN GROUP (ORDER BY line_number) FROM {self.q(s, HASH_TABLE)} "
                f"GROUP BY resource_type, resource_id HAVING COUNT(*) > 1 ORDER BY 1, 2").fetchall()
        if dups:
            detail = "; ".join(f"{t} {i} at lines {lines}" for t, i, _, lines in dups)
            raise FlattenError(f"duplicate resource ids: {detail}", dups[0][2])
        return StageResult(flat.rows, flat.resources)

    KINDS = {"N": "new", "C": "changed", "U": "unchanged"}

    def apply_delta(self, release: date, run_id: int) -> DeltaResult:
        """Upsert the staged release: replace the rows of changed resources, insert new ones, mark every resource of
        the release as seen. Resources missing from the release are kept (aging data)."""
        s, schema = self.cfg.stage_schema, self.cfg.schema
        hashes, state = self.q(s, HASH_TABLE), self.q(schema, "resource_state")
        delta = self.q(s, "delta")
        with self._autocommit() as conn:
            conn.exec_driver_sql(f"DROP TABLE IF EXISTS {delta}")
            conn.exec_driver_sql(
                f"SELECT h.resource_type, h.resource_id, h.hash, h.last_updated, "
                f"CAST(CASE WHEN st.resource_id IS NULL THEN 'N' WHEN st.hash <> h.hash THEN 'C' ELSE 'U' END AS char(1)) AS kind "
                f"INTO {delta} FROM {hashes} h LEFT JOIN {state} st "
                f"ON st.resource_type = h.resource_type AND st.resource_id = h.resource_id")
            conn.exec_driver_sql(f"CREATE UNIQUE CLUSTERED INDEX ux_delta ON {delta} (resource_type, resource_id)")
            kinds: dict[str, dict[str, int]] = {}
            for rtype, kind, n in conn.exec_driver_sql(
                    f"SELECT resource_type, kind, COUNT_BIG(*) FROM {delta} GROUP BY resource_type, kind"):
                kinds.setdefault(rtype, {"new": 0, "changed": 0, "unchanged": 0, "not_seen": 0})[self.KINDS[kind]] = n
            for rtype, n in conn.exec_driver_sql(
                    f"SELECT st.resource_type, COUNT_BIG(*) FROM {state} st WHERE NOT EXISTS (SELECT 1 FROM {delta} d "
                    f"WHERE d.resource_type = st.resource_type AND d.resource_id = st.resource_id) "
                    f"GROUP BY st.resource_type"):
                kinds.setdefault(rtype, {"new": 0, "changed": 0, "unchanged": 0, "not_seen": 0})["not_seen"] = n

        def work(conn) -> tuple[dict[str, int], dict[str, int]]:
            inserted, replaced = {}, {}
            for t in ALL_TABLES:
                target, staged = self.q(schema, t.name), self.q(s, t.name)
                rtype = TABLE_TYPES[t.name]
                match = "d.resource_type = x.resource_type" if rtype is None else f"d.resource_type = '{rtype}'"
                replaced[t.name] = conn.exec_driver_sql(
                    f"DELETE x FROM {target} x JOIN {delta} d ON d.resource_id = x.resource_id AND {match} "
                    f"AND d.kind = 'C'").rowcount
                cols = ", ".join(self.q(c) for c in columns(t))
                inserted[t.name] = conn.exec_driver_sql(
                    f"INSERT INTO {target} WITH (TABLOCK) ({cols}) SELECT {cols} FROM {staged} x WHERE EXISTS "
                    f"(SELECT 1 FROM {delta} d WHERE d.resource_id = x.resource_id AND {match} "
                    f"AND d.kind IN ('N', 'C'))").rowcount
            conn.exec_driver_sql(
                f"MERGE {state} AS st USING {delta} AS d "
                f"ON st.resource_type = d.resource_type AND st.resource_id = d.resource_id "
                f"WHEN MATCHED THEN UPDATE SET "          # MERGE allows one UPDATE branch: CASE keeps unchanged rows
                f"hash = CASE WHEN d.kind = 'C' THEN d.hash ELSE st.hash END, "
                f"last_updated = CASE WHEN d.kind = 'C' THEN d.last_updated ELSE st.last_updated END, "
                f"release_date = CASE WHEN d.kind = 'C' THEN CAST(? AS date) ELSE st.release_date END, "
                f"run_id = CASE WHEN d.kind = 'C' THEN ? ELSE st.run_id END, "
                f"last_seen_release = ?, last_seen_run_id = ? "
                f"WHEN NOT MATCHED THEN INSERT (resource_type, resource_id, hash, last_updated, release_date, run_id, "
                f"last_seen_release, last_seen_run_id) VALUES (d.resource_type, d.resource_id, d.hash, d.last_updated, "
                f"?, ?, ?, ?);",
                (release, run_id, release, run_id, release, run_id, release, run_id))
            totals = {k: sum(v[k] for v in kinds.values()) for k in ("new", "changed", "unchanged", "not_seen")}
            conn.exec_driver_sql(
                f"MERGE {self.q(schema, 'release')} AS t USING (SELECT CAST(? AS date) AS release_date) AS s "
                f"ON t.release_date = s.release_date "
                f"WHEN MATCHED THEN UPDATE SET import_run_id = ?, published_at = SYSUTCDATETIME(), new_resources = ?, "
                f"changed_resources = ?, unchanged_resources = ?, not_seen_resources = ? "
                f"WHEN NOT MATCHED THEN INSERT (release_date, import_run_id, new_resources, changed_resources, "
                f"unchanged_resources, not_seen_resources) VALUES (s.release_date, ?, ?, ?, ?, ?);",
                (release, run_id, totals["new"], totals["changed"], totals["unchanged"], totals["not_seen"],
                 run_id, totals["new"], totals["changed"], totals["unchanged"], totals["not_seen"]))
            return inserted, replaced

        inserted, replaced = self._locked_transaction(work, f"apply release {release}")
        log.info("applied release %s: %s", release, {t: k for t, k in sorted(kinds.items())})
        with self._autocommit() as conn:            # best effort: the delta is already committed
            for t in ALL_TABLES:
                if inserted.get(t.name) or replaced.get(t.name):
                    try:
                        conn.exec_driver_sql(f"UPDATE STATISTICS {self.q(schema, t.name)}")
                    except Exception as exc:
                        log.warning("UPDATE STATISTICS %s.%s failed: %s", schema, t.name, exc)
        return DeltaResult(kinds, inserted, replaced)


    def _locked_transaction(self, work: Callable[[Connection], "T"], what: str) -> "T":
        """Run `work` in one transaction with SET LOCK_TIMEOUT; retry the whole transaction LOCK_ATTEMPTS times on
        error 1222 (lock request timeout), then raise LockUnavailable."""
        timeout_ms = max(1, round(self.cfg.lock_timeout_seconds * 1000))
        for attempt in range(1, LOCK_ATTEMPTS + 1):
            try:
                with self.engine.connect() as conn:
                    try:
                        with conn.begin():
                            conn.exec_driver_sql(f"SET XACT_ABORT ON; SET LOCK_TIMEOUT {timeout_ms}")
                            return work(conn)
                    finally:
                        conn.exec_driver_sql("SET LOCK_TIMEOUT -1")
                        conn.rollback()
            except DBAPIError as exc:
                if error_number(exc) != 1222:
                    raise
                if attempt == LOCK_ATTEMPTS:
                    raise LockUnavailable(f"{what}: lock request timed out after {self.cfg.lock_timeout_seconds}s "
                                          f"({LOCK_ATTEMPTS} attempts)") from exc
                log.warning("%s: lock request timed out (attempt %d of %d); retrying", what, attempt, LOCK_ATTEMPTS)
                self._sleep(LOCK_BACKOFF_SECONDS * attempt)
        raise AssertionError("unreachable")


    @contextmanager
    def run_lock(self, stage: str) -> Iterator[bool]:
        """Session-owned application lock on its own connection, held for the whole stage."""
        conn = self._autocommit()
        resource = f"npd_loader:{self.cfg.schema}:{stage}"
        try:
            rc = conn.exec_driver_sql(
                "SET NOCOUNT ON; DECLARE @rc int; EXEC @rc = sp_getapplock @Resource = ?, @LockMode = 'Exclusive', "
                "@LockOwner = 'Session', @LockTimeout = 0; SELECT @rc", (resource,)).scalar()
        except Exception:
            conn.close()
            raise
        acquired = rc is not None and rc >= 0
        if not acquired:
            conn.close()
            yield False
            return
        try:
            yield True
        finally:
            try:
                conn.exec_driver_sql("EXEC sp_releaseapplock @Resource = ?, @LockOwner = 'Session'", (resource,))
            finally:
                conn.close()

