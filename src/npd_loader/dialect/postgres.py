"""Postgres flavor: every npd-database operation, on a PgConnectionObject's engine (SQLAlchemy + psycopg2).

Raw layout built by load_raw (not visible to readers until publish attaches it):
  {raw}.resource__YYYYMMDD__rRUN                 PARTITION BY LIST (resource_type)
  {raw}.resource__YYYYMMDD__rRUN__practitioner   one leaf per file, CHECKed on release_date and resource_type

Every method opens and closes its own raw psycopg2 connection (engine.raw_connection()); a connection that is not
autocommit commits when its block ends without an error and rolls back otherwise.
"""
from __future__ import annotations

import csv
import hashlib
import io
import logging
import re
import time
from contextlib import contextmanager
from datetime import date
from typing import Callable, Iterable, Iterator, TypeVar

import psycopg2
import psycopg2.errors
from sqlalchemy.engine import Engine

from npd_loader.config import NpdDbConfig
from npd_loader.dialect import LockUnavailable, PublishConflict, TransformResult
from npd_loader.raw_load import RAW_PARENT, NdjsonInput, RawLoadError, RawLoadResult, iter_lines, validate_line
from npd_loader.sqltext import render, sql_scripts, standalone_name
from npd_loader.storage import Storage

log = logging.getLogger(__name__)
MAX_IDENTIFIER = 63
BATCH_ROWS = 5000
RAW_COLUMNS = ("release_date", "resource_type", "resource_id", "last_updated", "ndjson_file_id", "zst_file_id",
               "line_number", "resource")
BOUND_RE = re.compile(r"FOR VALUES IN \('(\d{4}-\d{2}-\d{2})'\)")
LOCK_ATTEMPTS = 3
LOCK_BACKOFF_SECONDS = 2.0
T = TypeVar("T")


def lock_key(name: str) -> int:
    return int.from_bytes(hashlib.sha256(f"npd_loader:{name}".encode()).digest()[:8], "big", signed=True)


def csv_batch(rows: Iterable[tuple]) -> io.StringIO:
    """COPY ... (FORMAT csv) input for `rows`: None becomes an unquoted empty field (NULL), every other value is
    quoted, so an empty string stays an empty string."""
    stream = io.StringIO()
    csv.writer(stream, quoting=csv.QUOTE_NOTNULL, lineterminator="\n").writerows(rows)
    stream.seek(0)
    return stream


def _rows(conn, query: str, args: tuple | list | None = None) -> list[tuple]:
    with conn.cursor() as cur:
        cur.execute(query, args or None)
        return cur.fetchall()


def _execute(conn, query: str, args: tuple | list | None = None) -> None:
    with conn.cursor() as cur:
        cur.execute(query, args or None)


class PostgresDialect:
    name = "postgres"

    def __init__(self, engine: Engine, cfg: NpdDbConfig, sleep: Callable[[float], None] = time.sleep):
        self.engine = engine
        self.cfg = cfg
        self._sleep = sleep

    # -- naming -------------------------------------------------------------------------------------------
    def q(self, *parts: str) -> str:
        """Always-quoted, dot-joined identifier (what psycopg's sql.Identifier(*parts) rendered)."""
        quote = self.engine.dialect.identifier_preparer.quote_identifier
        return ".".join(quote(p) for p in parts)

    @staticmethod
    def lit(value: str | date) -> str:
        """SQL literal, as psycopg's sql.Literal rendered it: '2026-09-29'::date for a date, 'text' for a str."""
        if isinstance(value, date):
            return f"'{value.isoformat()}'::date"
        return "'" + value.replace("'", "''") + "'"

    # -- connections --------------------------------------------------------------------------------------
    @contextmanager
    def _connect(self, autocommit: bool = False) -> Iterator:
        """A raw psycopg2 connection from the engine's pool. Not autocommit: commit at the end of the block, or
        roll back on an error (what the psycopg 3 connection context manager did). Autocommit is switched off
        again before the connection goes back to the pool."""
        raw = self.engine.raw_connection()
        dbapi = raw.dbapi_connection
        try:
            if autocommit:
                dbapi.rollback()               # psycopg2 refuses to switch inside a transaction
                dbapi.autocommit = True
            try:
                yield raw
            except BaseException:
                if not autocommit and not dbapi.closed:
                    try:
                        raw.rollback()
                    except Exception:
                        log.debug("rollback failed", exc_info=True)
                raise
            if not autocommit:
                raw.commit()
        finally:
            try:
                if autocommit and not dbapi.closed:
                    dbapi.autocommit = False
            except Exception:
                raw.invalidate()
            raw.close()

    # -- catalog views ------------------------------------------------------------------------------------
    @staticmethod
    def parent_tables(conn, schema: str) -> list[str]:
        rows = _rows(conn,
                     "SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
                     "WHERE n.nspname = %s AND c.relkind = 'p' AND NOT c.relispartition ORDER BY c.relname", (schema,))
        return [r[0] for r in rows]

    @staticmethod
    def release_partitions(conn, schema: str, parent: str) -> dict[date, str]:
        rows = _rows(conn,
                     "SELECT c.relname, pg_get_expr(c.relpartbound, c.oid) FROM pg_inherits i "
                     "JOIN pg_class c ON c.oid = i.inhrelid JOIN pg_class p ON p.oid = i.inhparent "
                     "JOIN pg_namespace n ON n.oid = p.relnamespace WHERE n.nspname = %s AND p.relname = %s",
                     (schema, parent))
        found = {}
        for name, bound in rows:
            m = BOUND_RE.search(bound or "")
            if m:
                found[date.fromisoformat(m.group(1))] = name
        return found

    @staticmethod
    def clone_parent_indexes(conn, schema: str, parent: str, target: str) -> None:
        """Build the parent's indexes on a standalone table (`target` is already quoted) so ATTACH PARTITION reuses
        them instead of building them while holding the parent lock."""
        rows = _rows(conn,
                     "SELECT pg_get_indexdef(i.indexrelid), i.indisunique FROM pg_index i "
                     "JOIN pg_class c ON c.oid = i.indrelid JOIN pg_namespace n ON n.oid = c.relnamespace "
                     "WHERE n.nspname = %s AND c.relname = %s ORDER BY i.indexrelid", (schema, parent))
        for indexdef, unique in rows:
            tail = indexdef[indexdef.index(" USING "):]
            _execute(conn, f"CREATE {'UNIQUE ' if unique else ''}INDEX ON {target}{tail}")

    # -- Dialect --------------------------------------------------------------------------------------------
    def init_db(self, extra_scripts: Iterable[str] = ()) -> None:
        """Create schemas, parent tables, helper functions and latest-release views; safe to re-run. Runs the init
        scripts (900_migrations.sql last), then `extra_scripts` (tests use this to exercise a migration without
        shipping one), then recreates the views so they pick up any new column. One transaction."""
        raw_schema, schema = self.cfg.raw_schema, self.cfg.schema
        tokens = {"raw_schema": self.q(raw_schema), "schema": self.q(schema)}
        with self._connect() as conn:
            for text in [text for _, text in sql_scripts("postgres", "init")] + list(extra_scripts):
                _execute(conn, render(text, tokens))
            for s in (raw_schema, schema):
                for table in self.parent_tables(conn, s):
                    _execute(conn,
                             f"CREATE OR REPLACE VIEW {self.q(s, 'v_' + table)} AS SELECT * FROM {self.q(s, table)} "
                             f"WHERE release_date = (SELECT max(release_date) FROM {self.q(schema, 'release')})")

    @contextmanager
    def run_lock(self, stage: str) -> Iterator[bool]:
        """Session-level advisory lock held on its own autocommit connection for the whole stage."""
        key = lock_key(stage)
        with self._connect(autocommit=True) as conn:
            acquired = _rows(conn, "SELECT pg_try_advisory_lock(%s)", (key,))[0][0]
            try:
                yield acquired
            finally:
                if acquired:
                    _execute(conn, "SELECT pg_advisory_unlock(%s)", (key,))

    def published_releases(self) -> list[date]:
        with self._connect() as conn:
            rows = _rows(conn, f"SELECT release_date FROM {self.q(self.cfg.schema, 'release')} ORDER BY release_date")
        return [r[0] for r in rows]

    def partitioned_releases(self) -> set[date]:
        found: set[date] = set()
        with self._connect() as conn:
            for schema in (self.cfg.raw_schema, self.cfg.schema):
                for parent in self.parent_tables(conn, schema):
                    found |= set(self.release_partitions(conn, schema, parent))
        return found

    def is_published(self, release: date) -> bool:
        with self._connect() as conn:
            return release in self.release_partitions(conn, self.cfg.raw_schema, RAW_PARENT)

    @contextmanager
    def session(self) -> Iterator[None]:
        """No-op: load_raw, run_transforms and publish each open their own connection."""
        yield

    # -- raw load -----------------------------------------------------------------------------------------
    def _create_release_table(self, conn, release: date, run_id: int) -> str:
        raw_schema = self.cfg.raw_schema
        name = standalone_name(RAW_PARENT, release, run_id, MAX_IDENTIFIER)
        _execute(conn, f"CREATE TABLE {self.q(raw_schema, name)} (LIKE {self.q(raw_schema, RAW_PARENT)} "
                       f"INCLUDING DEFAULTS) PARTITION BY LIST (resource_type)")
        conn.commit()
        return name

    def _load_file(self, conn, storage: Storage, table: str, release: date, inp: NdjsonInput) -> int:
        """COPY one .ndjson into its own leaf in batches of BATCH_ROWS, committing each batch as soon as it lands;
        then check the row count and attach the leaf to the release table."""
        raw_schema = self.cfg.raw_schema
        leaf = f"{table}__{inp.resource_type.lower()}"
        if len(leaf) > MAX_IDENTIFIER:
            raise RawLoadError(f"table name {leaf!r} is too long", inp.file_id)
        leaf_id = self.q(raw_schema, leaf)
        _execute(conn, f"CREATE TABLE {leaf_id} (LIKE {self.q(raw_schema, RAW_PARENT)} INCLUDING DEFAULTS)")
        _execute(conn, f"ALTER TABLE {leaf_id} ADD CHECK (release_date = {self.lit(release)} "
                       f"AND resource_type = {self.lit(inp.resource_type)})")
        copy_sql = f"COPY {leaf_id} ({', '.join(self.q(c) for c in RAW_COLUMNS)}) FROM STDIN (FORMAT csv)"
        day = release.isoformat()
        lines, batch = 0, []

        def flush() -> None:
            nonlocal lines, batch
            with conn.cursor() as cur:
                cur.copy_expert(copy_sql, csv_batch(batch))
            conn.commit()
            lines += len(batch)
            batch = []

        try:
            with storage.open_read(inp.rel_path) as f:
                for number, text in iter_lines(f):
                    resource_id, last_updated = validate_line(text, number, inp.resource_type)
                    batch.append((day, inp.resource_type, resource_id, last_updated, inp.file_id,
                                  inp.zst_file_id, number, text))
                    if len(batch) == BATCH_ROWS:
                        flush()
            if batch:
                flush()
        except RawLoadError as exc:
            conn.rollback()
            raise RawLoadError(f"{inp.name}: {exc}", inp.file_id) from exc
        except psycopg2.Error as exc:
            conn.rollback()
            raise RawLoadError(f"{inp.name}: COPY failed: {exc}", inp.file_id) from exc
        except OSError as exc:
            conn.rollback()
            raise RawLoadError(f"{inp.name}: cannot read {inp.rel_path}: {exc}", inp.file_id) from exc
        count = _rows(conn, f"SELECT count(*) FROM {leaf_id}")[0][0]
        if count != lines:
            conn.rollback()
            raise RawLoadError(f"{inp.name}: read {lines} lines but loaded {count} rows", inp.file_id)
        _execute(conn, f"ALTER TABLE {self.q(raw_schema, table)} ATTACH PARTITION {leaf_id} "
                       f"FOR VALUES IN ({self.lit(inp.resource_type)})")
        conn.commit()
        log.info("loaded %d %s resources from %s", lines, inp.resource_type, inp.rel_path)
        return lines

    def _index_and_check_duplicates(self, conn, table: str, file_ids: dict[str, int]) -> None:
        """`file_ids` maps resource_type to its .ndjson data_file id, so the error names the file to blame."""
        raw_schema = self.cfg.raw_schema
        target = self.q(raw_schema, table)
        try:
            self.clone_parent_indexes(conn, raw_schema, RAW_PARENT, target)
            conn.commit()
        except psycopg2.errors.UniqueViolation:
            conn.rollback()
            dups = _rows(conn,
                         f"SELECT resource_type, resource_id, array_agg(line_number ORDER BY line_number) FROM {target} "
                         f"GROUP BY 1, 2 HAVING count(*) > 1 ORDER BY 1, 2 LIMIT 20")
            detail = "; ".join(f"{t} {i} at lines {lines}" for t, i, lines in dups)
            raise RawLoadError(f"duplicate resource ids: {detail}", file_ids.get(dups[0][0]) if dups else None)

    def load_raw(self, storage: Storage, release: date, run_id: int, inputs: list[NdjsonInput]) -> RawLoadResult:
        with self._connect() as conn:
            table = self._create_release_table(conn, release, run_id)
            rows = {inp.resource_type: self._load_file(conn, storage, table, release, inp) for inp in inputs}
            self._index_and_check_duplicates(conn, table, {inp.resource_type: inp.file_id for inp in inputs})
        return RawLoadResult(table, rows)

    # -- transforms ---------------------------------------------------------------------------------------
    def run_transforms(self, raw_table: str, release: date, run_id: int) -> TransformResult:
        """Run sql/postgres/transform/*.sql (in name order) into standalone tables for one release."""
        raw_schema, schema = self.cfg.raw_schema, self.cfg.schema
        with self._connect() as conn:
            _execute(conn, "SET TIME ZONE 'UTC'")
            tables: dict[str, str] = {}
            for parent in self.parent_tables(conn, schema):
                name = standalone_name(parent, release, run_id, MAX_IDENTIFIER)
                target = self.q(schema, name)
                _execute(conn, f"CREATE TABLE {target} (LIKE {self.q(schema, parent)} INCLUDING DEFAULTS)")
                _execute(conn, f"ALTER TABLE {target} ADD CHECK (release_date = {self.lit(release)})")
                tables[parent] = name
            conn.commit()

            tokens = {
                "raw": self.q(raw_schema, raw_table),
                "release": self.lit(release),
                "schema": self.q(schema),
                **{f"t:{parent}": self.q(schema, name) for parent, name in tables.items()},
            }
            for script_name, text in sql_scripts("postgres", "transform"):
                log.info("running transform %s", script_name)
                _execute(conn, render(text, tokens))
                conn.commit()

            for parent, name in tables.items():
                self.clone_parent_indexes(conn, schema, parent, self.q(schema, name))
            conn.commit()
            counts = {parent: _rows(conn, f"SELECT count(*) FROM {self.q(schema, name)}")[0][0]
                      for parent, name in tables.items()}
        return TransformResult(tables, counts)

    # -- publish and retention ----------------------------------------------------------------------------
    def _locked_transaction(self, conn, work: Callable[[], "T"], what: str) -> "T":
        """Run `work` in one transaction with a transaction-local lock_timeout, so DDL that needs ACCESS EXCLUSIVE on
        a parent (DETACH) gives up instead of queueing every reader behind a long query. Retries the whole
        transaction LOCK_ATTEMPTS times on lock_not_available (55P03), then raises LockUnavailable."""
        conn.commit()
        timeout = f"{max(1, round(self.cfg.lock_timeout_seconds * 1000))}ms"
        for attempt in range(1, LOCK_ATTEMPTS + 1):
            try:
                _execute(conn, "SELECT set_config('lock_timeout', %s, true)", (timeout,))
                result = work()
                conn.commit()
                return result
            except psycopg2.errors.LockNotAvailable as exc:
                conn.rollback()
                if attempt == LOCK_ATTEMPTS:
                    raise LockUnavailable(str(exc).strip()) from exc
                log.warning("%s: %s (attempt %d of %d); retrying", what, str(exc).strip(), attempt, LOCK_ATTEMPTS)
                self._sleep(LOCK_BACKOFF_SECONDS * attempt)
            except BaseException:
                conn.rollback()
                raise
        raise AssertionError("unreachable")

    def _detach_and_drop(self, conn, schema: str, parent: str, name: str) -> None:
        _execute(conn, f"ALTER TABLE {self.q(schema, parent)} DETACH PARTITION {self.q(schema, name)}")
        _execute(conn, f"DROP TABLE {self.q(schema, name)}")

    def publish(self, raw_table: str, tables: dict[str, str], release: date, run_id: int, force: bool) -> None:
        """Make a release visible in one transaction: ATTACH every standalone table (DETACH and drop the old
        partition first with --force) and upsert npd.release."""
        raw_schema, schema = self.cfg.raw_schema, self.cfg.schema
        targets = [(raw_schema, RAW_PARENT, raw_table)] + [(schema, p, n) for p, n in sorted(tables.items())]
        with self._connect() as conn:
            def work() -> None:
                for s, parent, new in targets:
                    existing = self.release_partitions(conn, s, parent).get(release)
                    if existing is not None:
                        if not force:
                            raise PublishConflict(f"release {release} is already published in {s}.{parent}; "
                                                  f"rerun with --force to replace it")
                        self._detach_and_drop(conn, s, parent, existing)
                    _execute(conn, f"ALTER TABLE {self.q(s, parent)} ATTACH PARTITION {self.q(s, new)} "
                                   f"FOR VALUES IN ({self.lit(release)})")
                _execute(conn,
                         f"INSERT INTO {self.q(schema, 'release')} (release_date, import_run_id, published_at) "
                         f"VALUES (%s, %s, now()) ON CONFLICT (release_date) DO UPDATE SET "
                         f"import_run_id = EXCLUDED.import_run_id, published_at = now()", (release, run_id))

            self._locked_transaction(conn, work, f"publish release {release}")
            log.info("published release %s (%d tables)", release, len(targets))
            for s, _, new in targets:
                _execute(conn, f"ANALYZE {self.q(s, new)}")
            conn.commit()

    def drop_release(self, release: date) -> list[str]:
        """Detach and drop every partition of `release`, in one transaction. Checks what actually exists first.
        Raises LockUnavailable if a parent stays locked through every attempt (nothing is dropped)."""
        raw_schema, schema = self.cfg.raw_schema, self.cfg.schema
        with self._connect() as conn:
            def work() -> list[str]:
                dropped: list[str] = []
                for s in (raw_schema, schema):
                    for parent in self.parent_tables(conn, s):
                        name = self.release_partitions(conn, s, parent).get(release)
                        if name is not None:
                            self._detach_and_drop(conn, s, parent, name)
                            dropped.append(f"{s}.{name}")
                _execute(conn, f"DELETE FROM {self.q(schema, 'release')} WHERE release_date = %s", (release,))
                return dropped

            return self._locked_transaction(conn, work, f"drop release {release}")

    def drop_standalone_tables(self, run_id: int | None = None) -> list[str]:
        """Drop the unpublished (non-partition) tables of import run `run_id`, or of any run when run_id is None.
        Only safe with the import lock held. Returns the dropped tables."""
        run = str(run_id) if run_id is not None else r"\d+"
        schemas = [self.cfg.raw_schema, self.cfg.schema]
        dropped = []
        with self._connect(autocommit=True) as conn:
            rows = _rows(conn,
                         "SELECT n.nspname, c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
                         "WHERE n.nspname = ANY(%s) AND c.relkind IN ('r', 'p') AND NOT c.relispartition "
                         "AND c.relname ~ %s ORDER BY 1, 2", (schemas, rf"__r{run}(__[a-z]+)?$"))
            for schema, name in rows:
                _execute(conn, f"DROP TABLE IF EXISTS {self.q(schema, name)}")
                dropped.append(f"{schema}.{name}")
        return dropped
