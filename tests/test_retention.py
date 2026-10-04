import time
from contextlib import contextmanager
from dataclasses import replace
from datetime import date, timedelta

import psycopg
import pytest
from psycopg import sql

from npd_loader.db import list_parent_tables, list_release_partitions
from npd_loader.retention import apply_retention
from helpers import make_ctx

BASE = date(2026, 8, 4)
RELEASES = [BASE + timedelta(weeks=k) for k in range(7)]


def publish_empty(conn, release: date, run_id: int) -> None:
    for schema in ("npd_raw", "npd"):
        for parent in list_parent_tables(conn, schema):
            name = f"{parent}__{release:%Y%m%d}__r{run_id}"
            conn.execute(sql.SQL("CREATE TABLE {} (LIKE {} INCLUDING DEFAULTS)").format(
                sql.Identifier(schema, name), sql.Identifier(schema, parent)))
            conn.execute(sql.SQL("ALTER TABLE {} ATTACH PARTITION {} FOR VALUES IN ({})").format(
                sql.Identifier(schema, parent), sql.Identifier(schema, name), sql.Literal(release)))
    conn.execute("INSERT INTO npd.release (release_date, import_run_id) VALUES (%s, %s)", (release, run_id))
    conn.commit()


def seed(ctx, release: date) -> tuple[str, str]:
    run = ctx.catalog.add_successful_run("IMPORT", release)
    zst_rel = f"run_1_{release}/file_1_06-Practitioner.ndjson.zst"
    nd_rel = f"run_2_{release}/file_2_06-Practitioner.ndjson"
    zst = ctx.catalog.add_data_file(run, file_type="ndjson.zst", source_version_num=release.isoformat(),
                                    file_rel_path=zst_rel)
    ctx.catalog.add_data_file(run, file_type="ndjson", source_version_num=release.isoformat(),
                              file_rel_path=nd_rel, parent_file=zst, file_hash="x")
    for rel in (zst_rel, nd_rel):
        with ctx.storage.open_write(rel) as f:
            f.write(b"data")
    return zst_rel, nd_rel


@pytest.fixture
def ctx(tmp_path, cms, npd_db):
    c = make_ctx(tmp_path, cms, npd_conninfo=npd_db)
    c.paths = {}
    with psycopg.connect(npd_db) as conn:
        for i, release in enumerate(RELEASES):
            publish_empty(conn, release, 100 + i)
            c.paths[release] = seed(c, release)
    return c


def published(ctx):
    with psycopg.connect(ctx.npd_conninfo) as conn:
        raw = sorted(list_release_partitions(conn, "npd_raw", "resource"))
        npd = sorted(list_release_partitions(conn, "npd", "practitioner"))
        rel = [r[0] for r in conn.execute("SELECT release_date FROM npd.release ORDER BY 1")]
    assert raw == npd == rel
    return raw


def test_keeps_newest_five(ctx):
    files_before = len(ctx.catalog.files)
    assert apply_retention(ctx, RELEASES[-1]) == []
    assert published(ctx) == RELEASES[2:]
    for release in RELEASES:
        zst_rel, nd_rel = ctx.paths[release]
        assert ctx.storage.exists(zst_rel)                         # .zst never deleted
        assert ctx.storage.exists(nd_rel) == (release in RELEASES[2:])
    assert len(ctx.catalog.files) == files_before                  # catalog rows never deleted


def test_just_imported_old_release_is_kept(ctx):
    assert apply_retention(ctx, RELEASES[0]) == []
    assert published(ctx) == [RELEASES[0]] + RELEASES[2:]
    assert ctx.storage.exists(ctx.paths[RELEASES[0]][1])
    assert not ctx.storage.exists(ctx.paths[RELEASES[1]][1])


def test_already_dropped_release_is_not_an_error(ctx):
    apply_retention(ctx, RELEASES[-1])
    with ctx.storage.open_write(ctx.paths[RELEASES[0]][1]) as f:   # e.g. someone re-extracted it
        f.write(b"again")
    assert apply_retention(ctx, RELEASES[-1]) == []
    assert not ctx.storage.exists(ctx.paths[RELEASES[0]][1])


def test_errors_become_warnings(ctx, monkeypatch):
    def broken(rel):
        raise OSError("disk unavailable")
    monkeypatch.setattr(ctx.storage, "delete", broken)
    warnings = apply_retention(ctx, RELEASES[-1])
    assert len(warnings) == 2 and all("disk unavailable" in w for w in warnings)
    assert published(ctx) == RELEASES[2:]


@contextmanager
def lock_held(conninfo: str, table: str):
    """Another session (e.g. a long analyst query) holding ACCESS SHARE on `table` until the block ends."""
    with psycopg.connect(conninfo) as other:
        other.execute(f"LOCK TABLE {table} IN ACCESS SHARE MODE")
        yield
        other.rollback()


def set_lock_timeout(ctx, seconds: float) -> None:
    ctx.config = replace(ctx.config, npd_db=replace(ctx.config.npd_db, lock_timeout_seconds=seconds))


def test_locked_parent_gives_up_with_a_warning(ctx):
    set_lock_timeout(ctx, 1)
    sleeps = []
    ctx.sleep = sleeps.append
    with lock_held(ctx.npd_conninfo, "npd.practitioner"):
        start = time.monotonic()
        warnings = apply_retention(ctx, RELEASES[-1])
        elapsed = time.monotonic() - start
    assert elapsed < 6                                   # 3 attempts x 1s, then the other release is skipped
    assert len(sleeps) == 2                              # retried twice, with backoff, for the first release only
    assert warnings and all("lock" in w for w in warnings)
    assert published(ctx) == RELEASES                    # nothing detached, npd_raw included (rolled back)
    assert all(ctx.storage.exists(ctx.paths[r][1]) for r in RELEASES)
    assert apply_retention(ctx, RELEASES[-1]) == []      # retried on the next run
    assert published(ctx) == RELEASES[2:]
