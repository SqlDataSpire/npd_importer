"""SQL Server flavor: every npd-database operation, on a SqlConnectionObject's engine (pyodbc)."""
from __future__ import annotations

import logging
import re
import shutil
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Callable, Iterator, TypeVar

import pyodbc
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import DBAPIError

from npd_loader.config import NpdDbConfig
from npd_loader.dialect import DeltaResult, LockUnavailable, StageResult
from npd_loader.dialect.bcp import bcp_in, bcp_target
from npd_loader.flatten.engine import columns
from npd_loader.flatten.specs import ALL_TABLES, TABLE_TYPES
from npd_loader.flatten.stagefiles import HASH_TABLE, FlattenError, flatten_files
from npd_loader.raw_load import RAW_PARENT, NdjsonInput, RawLoadError, RawLoadResult, iter_lines, validate_line
from npd_loader.sqltext import render, split_batches, sql_scripts, standalone_name
from npd_loader.storage import Storage

log = logging.getLogger(__name__)
MAX_IDENTIFIER = 128
BATCH_ROWS = 5000
RAW_COLUMNS = ("release_date", "resource_type", "resource_id", "last_updated", "ndjson_file_id", "zst_file_id",
               "line_number", "resource")
ERROR_NUMBER_RE = re.compile(r"\((\d+)\)")
LOCK_ATTEMPTS = 3
LOCK_BACKOFF_SECONDS = 2.0
STANDALONE_RE = r"__r{run}(__[a-z]+)?$"
# A transform script with this line runs once per chunk of CHUNK_ROWS resources of that type, <<chunk>> rendered
# to the chunk's predicate on the raw table (alias r).
CHUNK_RE = re.compile(r"-- chunked: ([A-Za-z]+)[ \t]*(\r?\n|$)")   # first line of the script only
CHUNK_ROWS = 200_000
CHUNK_LOG_EVERY = 10
T = TypeVar("T")


def error_number(exc: BaseException) -> int | None:
    """SQL Server native error number from a pyodbc error message, e.g. '... exceeded. (1222) (SQLExecDirectW)'."""
    match = ERROR_NUMBER_RE.search(str(getattr(exc, "orig", exc)))
    return int(match.group(1)) if match else None


def to_utc_naive(text: str | None) -> datetime | None:
    """FHIR instant (meta.lastUpdated) as a naive UTC datetime for a datetime2 column."""
    if text is None:
        return None
    value = datetime.fromisoformat(text)
    if value.tzinfo is not None:
        value = value.astimezone(timezone.utc).replace(tzinfo=None)
    return value



class PublishConflict(Exception):
    """Phase 1 leftover, removed with the Phase 1 methods in Task 10."""


@dataclass
class TransformResult:
    """Phase 1 leftover, removed with the Phase 1 methods in Task 10."""
    tables: dict[str, str]
    counts: dict[str, int]


class MssqlDialect:
    name = "mssql"
    chunk_rows = CHUNK_ROWS

    def __init__(self, engine: Engine, cfg: NpdDbConfig, sleep: Callable[[float], None] = time.sleep):
        self.engine = engine
        self.cfg = cfg
        self._sleep = sleep

    # -- naming -------------------------------------------------------------------------------------------
    @staticmethod
    def q(*parts: str) -> str:
        return ".".join("[" + p.replace("]", "]]") + "]" for p in parts)

    @staticmethod
    def pf(schema: str) -> str:
        return f"pf_{schema}_release"

    @staticmethod
    def ps(schema: str) -> str:
        return f"ps_{schema}_release"

    @staticmethod
    def lit(value: str) -> str:
        return "N'" + value.replace("'", "''") + "'"

    def _tokens(self) -> dict[str, str]:
        return {"schema": self.q(self.cfg.schema), "s:schema": self.lit(self.cfg.schema),
                "stage_schema": self.q(self.cfg.stage_schema), "s:stage_schema": self.lit(self.cfg.stage_schema)}

    def _autocommit(self) -> Connection:
        return self.engine.connect().execution_options(isolation_level="AUTOCOMMIT")

    # -- catalog views ------------------------------------------------------------------------------------
    @staticmethod
    def parent_tables(conn: Connection, schema: str) -> list[str]:
        """Partitioned (published) tables of `schema`; standalone tables are never partitioned."""
        rows = conn.exec_driver_sql(
            "SELECT t.name FROM sys.tables t "
            "JOIN sys.indexes i ON i.object_id = t.object_id AND i.index_id IN (0, 1) "
            "JOIN sys.partition_schemes s ON s.data_space_id = i.data_space_id "
            "WHERE t.schema_id = SCHEMA_ID(?) ORDER BY t.name", (schema,)).fetchall()
        return [r[0] for r in rows]

    def _boundaries(self, conn: Connection, schema: str) -> set[date]:
        rows = conn.exec_driver_sql(
            "SELECT CAST(v.value AS date) FROM sys.partition_range_values v "
            "JOIN sys.partition_functions f ON f.function_id = v.function_id WHERE f.name = ?",
            (self.pf(schema),)).fetchall()
        return {r[0] for r in rows}

    # -- Dialect --------------------------------------------------------------------------------------------
    def init_db(self) -> None:
        from npd_loader.flatten.engine import columns
        from npd_loader.flatten.specs import ALL_TABLES
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

    def partitioned_releases(self) -> set[date]:
        """A release is published exactly when its boundary exists: publish SPLITs it in, retention MERGEs it out."""
        with self.engine.connect() as conn:
            return self._boundaries(conn, self.cfg.raw_schema) | self._boundaries(conn, self.cfg.schema)

    def is_published(self, release: date) -> bool:
        with self.engine.connect() as conn:
            return release in self._boundaries(conn, self.cfg.raw_schema)

    @contextmanager
    def session(self) -> Iterator[None]:
        yield

    # -- raw load -----------------------------------------------------------------------------------------
    def _create_standalone(self, conn: Connection, schema: str, parent: str, release: date, run_id: int) -> str:
        """Empty heap shaped like `parent`, PAGE-compressed, CHECKed to one release so SWITCH is metadata only."""
        name = standalone_name(parent, release, run_id, MAX_IDENTIFIER - 3)   # room for the ck_ constraint name
        target = self.q(schema, name)
        conn.exec_driver_sql(f"SELECT TOP 0 * INTO {target} FROM {self.q(schema, parent)}")
        conn.exec_driver_sql(f"ALTER TABLE {target} REBUILD WITH (DATA_COMPRESSION = PAGE)")
        conn.exec_driver_sql(f"ALTER TABLE {target} WITH CHECK ADD CONSTRAINT {self.q('ck_' + name)} "
                             f"CHECK (release_date = '{release.isoformat()}')")
        return name

    def _clone_indexes(self, conn: Connection, schema: str, parent: str, name: str) -> None:
        """Build the parent's indexes on standalone table `name` (same names, columns, uniqueness, compression)."""
        rows = conn.exec_driver_sql(
            "SELECT i.name, i.is_unique, c.name FROM sys.indexes i "
            "JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id "
            "JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id "
            "WHERE i.object_id = OBJECT_ID(?) AND i.index_id > 0 AND ic.is_included_column = 0 "
            "ORDER BY i.index_id, ic.key_ordinal", (f"{schema}.{parent}",)).fetchall()
        indexes: dict[str, tuple[bool, list[str]]] = {}
        for index, unique, column in rows:
            indexes.setdefault(index, (bool(unique), []))[1].append(column)
        for index, (unique, columns) in indexes.items():
            conn.exec_driver_sql(
                f"CREATE {'UNIQUE ' if unique else ''}INDEX {self.q(index)} ON {self.q(schema, name)} "
                f"({', '.join(self.q(c) for c in columns)}) WITH (DATA_COMPRESSION = PAGE)")

    def _load_file(self, raw_conn, table: str, release: date, inp: NdjsonInput, storage: Storage) -> int:
        """Insert one .ndjson in batches of BATCH_ROWS, committing each batch as soon as it lands."""
        cur = raw_conn.cursor()
        cur.fast_executemany = True
        insert = (f"INSERT INTO {self.q(self.cfg.raw_schema, table)} ({', '.join(RAW_COLUMNS)}) "
                  f"VALUES ({', '.join('?' * len(RAW_COLUMNS))})")
        lines, batch = 0, []
        try:
            with storage.open_read(inp.rel_path) as f:
                for number, text in iter_lines(f):
                    resource_id, last_updated = validate_line(text, number, inp.resource_type)
                    batch.append((release, inp.resource_type, resource_id, to_utc_naive(last_updated),
                                  inp.file_id, inp.zst_file_id, number, text))
                    if len(batch) == BATCH_ROWS:
                        cur.executemany(insert, batch)
                        raw_conn.commit()
                        lines += len(batch)
                        batch = []
            if batch:
                cur.executemany(insert, batch)
                raw_conn.commit()
                lines += len(batch)
        except RawLoadError as exc:
            raw_conn.rollback()
            raise RawLoadError(f"{inp.name}: {exc}", inp.file_id) from exc
        except pyodbc.Error as exc:
            raw_conn.rollback()
            raise RawLoadError(f"{inp.name}: insert failed: {exc}", inp.file_id) from exc
        except (OSError, ValueError) as exc:
            raw_conn.rollback()
            raise RawLoadError(f"{inp.name}: cannot read {inp.rel_path}: {exc}", inp.file_id) from exc
        log.info("loaded %d %s resources from %s", lines, inp.resource_type, inp.rel_path)
        return lines

    def load_raw(self, storage: Storage, release: date, run_id: int, inputs: list[NdjsonInput]) -> RawLoadResult:
        raw_schema = self.cfg.raw_schema
        with self._autocommit() as conn:
            table = self._create_standalone(conn, raw_schema, RAW_PARENT, release, run_id)
        raw_conn = self.engine.raw_connection()
        try:
            rows: dict[str, int] = {}
            for inp in inputs:
                rows[inp.resource_type] = rows.get(inp.resource_type, 0) + self._load_file(
                    raw_conn, table, release, inp, storage)
        finally:
            raw_conn.close()
        try:
            with self._autocommit() as conn:
                self._clone_indexes(conn, raw_schema, RAW_PARENT, table)
        except DBAPIError as exc:
            if error_number(exc) != 1505:
                raise
            with self.engine.connect() as conn:
                dups = conn.exec_driver_sql(
                    f"SELECT TOP 20 resource_type, resource_id, STRING_AGG(CAST(line_number AS varchar(20)), ',') "
                    f"WITHIN GROUP (ORDER BY line_number) FROM {self.q(raw_schema, table)} "
                    f"GROUP BY resource_type, resource_id HAVING COUNT(*) > 1 ORDER BY 1, 2").fetchall()
            detail = "; ".join(f"{t} {i} at lines {lines}" for t, i, lines in dups)
            file_ids = {inp.resource_type: inp.file_id for inp in inputs}
            raise RawLoadError(f"duplicate resource ids: {detail}", file_ids.get(dups[0][0]) if dups else None) from exc
        self._check_row_counts(raw_schema, table, inputs, rows)
        return RawLoadResult(table, rows)

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

    def _check_row_counts(self, schema: str, table: str, inputs: list[NdjsonInput], read: dict[str, int]) -> None:
        """One scan (of the narrow resource_key index) comparing rows per resource type with the lines read."""
        with self.engine.connect() as conn:
            loaded = {t: n for t, n in conn.exec_driver_sql(
                f"SELECT resource_type, COUNT_BIG(*) FROM {self.q(schema, table)} GROUP BY resource_type")}
        for resource_type, lines in read.items():
            if loaded.get(resource_type, 0) != lines:
                files = [inp for inp in inputs if inp.resource_type == resource_type]
                raise RawLoadError(f"{', '.join(i.name for i in files)}: read {lines} {resource_type} lines but "
                                   f"loaded {loaded.get(resource_type, 0)} rows", files[0].file_id)

    def _chunks(self, raw: str, release: date, resource_type: str) -> list[str]:
        """<<chunk>> predicates splitting the `resource_type` rows of `raw` into resource_id ranges of chunk_rows rows
        (one seek each on resource_key), so a script's stage heap stays small enough to be read back from memory."""
        with self.engine.connect() as conn:
            starts = [r[0] for r in conn.exec_driver_sql(
                f"SELECT resource_id FROM (SELECT resource_id, ROW_NUMBER() OVER (ORDER BY resource_id) - 1 AS n "
                f"FROM {raw} WHERE release_date = ? AND resource_type = CAST(? AS varchar(40))) x "
                f"WHERE n % ? = 0 ORDER BY resource_id",
                (release, resource_type, self.chunk_rows))]
        day = f"r.release_date = '{release.isoformat()}'"
        if not starts:
            return [day]
        chunks = []
        for i, lo in enumerate(starts):
            chunk = f"{day} AND r.resource_id >= {self.vlit(lo)}"
            if i + 1 < len(starts):
                chunk += f" AND r.resource_id < {self.vlit(starts[i + 1])}"
            chunks.append(chunk)
        return chunks

    @staticmethod
    def vlit(value: str) -> str:
        """varchar literal (resource_id is varchar: an N'' literal would convert the column and lose the seek)."""
        if not value.isascii():
            raise ValueError(f"non-ASCII resource_id {value!r}")
        return "'" + value.replace("'", "''") + "'"

    def run_transforms(self, raw_table: str, release: date, run_id: int) -> TransformResult:
        schema = self.cfg.schema
        with self._autocommit() as conn:
            tables = {p: self._create_standalone(conn, schema, p, release, run_id)
                      for p in self.parent_tables(conn, schema)}
        # <<stage:<parent>>>: a scratch heap a script parses its resource type into once and drops at its end; the
        # __r<run> suffix lets drop_standalone_tables remove one a failed run left behind.
        stages = {f"stage:{p}": self.q(schema, standalone_name(f"stage_{p}", release, run_id, MAX_IDENTIFIER))
                  for p in tables}
        raw = self.q(self.cfg.raw_schema, raw_table)
        tokens = {"raw": raw, "release": f"'{release.isoformat()}'",
                  "schema": self.q(schema), **{f"t:{p}": self.q(schema, n) for p, n in tables.items()}, **stages}
        for script, text in sql_scripts("mssql", "transform"):
            log.info("running transform %s", script)
            directive = CHUNK_RE.match(text)
            if directive and "<<chunk>>" not in text:
                raise ValueError(f"{script} has a '-- chunked:' directive but no <<chunk>> token")
            chunks = self._chunks(raw, release, directive.group(1)) if directive else [None]
            for number, chunk in enumerate(chunks, 1):
                if chunk is not None:
                    log.debug("transform %s chunk %d of %d: %s", script, number, len(chunks), chunk)
                    if number % CHUNK_LOG_EVERY == 0 or number == len(chunks):
                        log.info("%s: chunk %d/%d", script, number, len(chunks))
                batches = split_batches(render(text, tokens if chunk is None else {**tokens, "chunk": chunk}))
                for batch in batches:
                    with self.engine.begin() as conn:      # each statement commits as soon as it lands
                        conn.exec_driver_sql(batch)
        counts: dict[str, int] = {}
        with self._autocommit() as conn:
            for parent, name in tables.items():
                self._clone_indexes(conn, schema, parent, name)
                counts[parent] = conn.exec_driver_sql(f"SELECT COUNT_BIG(*) FROM {self.q(schema, name)}").scalar()
        return TransformResult(tables, counts)

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

    def _partition_rows(self, conn: Connection, schema: str, table: str, release: date) -> int:
        return conn.exec_driver_sql(
            f"SELECT COALESCE(SUM(p.rows), 0) FROM sys.partitions p WHERE p.object_id = OBJECT_ID(?) "
            f"AND p.index_id IN (0, 1) AND p.partition_number = $PARTITION.{self.q(self.pf(schema))}(?)",
            (f"{schema}.{table}", release)).scalar()

    def _partition_number(self, conn: Connection, schema: str, release: date) -> int:
        return conn.exec_driver_sql(f"SELECT $PARTITION.{self.q(self.pf(schema))}(?)", (release,)).scalar()

    def _switch_out_and_drop(self, conn: Connection, schema: str, parent: str, release: date, run_id: int) -> None:
        name = standalone_name(f"{parent}__out", release, run_id, MAX_IDENTIFIER)
        target = self.q(schema, name)
        conn.exec_driver_sql(f"SELECT TOP 0 * INTO {target} FROM {self.q(schema, parent)}")
        conn.exec_driver_sql(f"ALTER TABLE {target} REBUILD WITH (DATA_COMPRESSION = PAGE)")
        self._clone_indexes(conn, schema, parent, name)
        number = self._partition_number(conn, schema, release)
        conn.exec_driver_sql(f"ALTER TABLE {self.q(schema, parent)} SWITCH PARTITION {number} TO {target}")
        conn.exec_driver_sql(f"DROP TABLE {target}")

    def publish(self, raw_table: str, tables: dict[str, str], release: date, run_id: int, force: bool) -> None:
        raw_schema, schema = self.cfg.raw_schema, self.cfg.schema
        targets = [(raw_schema, RAW_PARENT, raw_table)] + [(schema, p, n) for p, n in sorted(tables.items())]
        day = f"'{release.isoformat()}'"

        def work(conn: Connection) -> None:
            for s in (raw_schema, schema):
                if release not in self._boundaries(conn, s):
                    conn.exec_driver_sql(f"ALTER PARTITION SCHEME {self.q(self.ps(s))} NEXT USED [PRIMARY]")
                    conn.exec_driver_sql(f"ALTER PARTITION FUNCTION {self.q(self.pf(s))}() SPLIT RANGE ({day})")
            for s, parent, new in targets:
                if self._partition_rows(conn, s, parent, release):
                    if not force:
                        raise PublishConflict(f"release {release} is already published in {s}.{parent}; "
                                              f"rerun with --force to replace it")
                    self._switch_out_and_drop(conn, s, parent, release, run_id)
                number = self._partition_number(conn, s, release)
                conn.exec_driver_sql(f"ALTER TABLE {self.q(s, new)} SWITCH TO {self.q(s, parent)} PARTITION {number}")
                conn.exec_driver_sql(f"DROP TABLE {self.q(s, new)}")
            conn.exec_driver_sql(
                f"MERGE {self.q(schema, 'release')} AS t USING (SELECT CAST(? AS date) AS release_date, ? AS run_id) AS s "
                f"ON t.release_date = s.release_date "
                f"WHEN MATCHED THEN UPDATE SET import_run_id = s.run_id, published_at = SYSUTCDATETIME() "
                f"WHEN NOT MATCHED THEN INSERT (release_date, import_run_id) VALUES (s.release_date, s.run_id);",
                (release, run_id))

        self._locked_transaction(work, f"publish release {release}")
        log.info("published release %s (%d tables)", release, len(targets))
        with self._autocommit() as conn:        # best effort: the release is already published
            for s, parent, _ in targets:
                try:
                    conn.exec_driver_sql(f"UPDATE STATISTICS {self.q(s, parent)}")
                except Exception as exc:
                    log.warning("UPDATE STATISTICS %s.%s failed (release %s is published): %s", s, parent, release, exc)

    def drop_release(self, release: date) -> list[str]:
        raw_schema, schema = self.cfg.raw_schema, self.cfg.schema

        def work(conn: Connection) -> list[str]:
            dropped: list[str] = []
            for s in (raw_schema, schema):
                if release not in self._boundaries(conn, s):
                    continue
                for parent in self.parent_tables(conn, s):
                    if self._partition_rows(conn, s, parent, release):
                        self._switch_out_and_drop(conn, s, parent, release, 0)
                        dropped.append(f"{s}.{parent}")
                conn.exec_driver_sql(f"ALTER PARTITION FUNCTION {self.q(self.pf(s))}() "
                                     f"MERGE RANGE ('{release.isoformat()}')")
            conn.exec_driver_sql(f"DELETE FROM {self.q(schema, 'release')} WHERE release_date = ?", (release,))
            return dropped

        return self._locked_transaction(work, f"drop release {release}")

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

    def drop_standalone_tables(self, run_id: int | None = None) -> list[str]:
        """Drop unpublished tables of import `run_id` (any run when None). Only safe with the import lock held."""
        pattern = re.compile(STANDALONE_RE.format(run=run_id if run_id is not None else r"\d+"))
        dropped: list[str] = []
        with self._autocommit() as conn:
            for schema in (self.cfg.raw_schema, self.cfg.schema):
                published = set(self.parent_tables(conn, schema))
                names = [r[0] for r in conn.exec_driver_sql(
                    "SELECT name FROM sys.tables WHERE schema_id = SCHEMA_ID(?) ORDER BY name", (schema,))]
                for name in names:
                    if name not in published and pattern.search(name):
                        conn.exec_driver_sql(f"DROP TABLE {self.q(schema, name)}")
                        dropped.append(f"{schema}.{name}")
        return dropped
