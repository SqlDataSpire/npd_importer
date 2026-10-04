import time
from dataclasses import replace
from datetime import date

import psycopg
import pytest

from npd_loader.catalog import FAILED, SUCCESS
from npd_loader.db import list_release_partitions
from npd_loader.download import run_download
from npd_loader.import_stage import run_import
from npd_loader.stages import Outcome, StageFailed
from helpers import make_ctx
from release_builder import build_release

R = date(2026, 9, 29)


def downloaded_ctx(tmp_path, cms, npd_db, release):
    cms.publish(release)
    ctx = make_ctx(tmp_path, cms, npd_conninfo=npd_db)
    run_download(ctx)
    return ctx


@pytest.fixture
def ctx(tmp_path, cms, npd_db):
    return downloaded_ctx(tmp_path, cms, npd_db, build_release("2026-09-29"))


def one(conninfo, query, *args):
    with psycopg.connect(conninfo) as conn:
        return conn.execute(query, args).fetchone()[0]


def test_import_extracts_loads_and_publishes(ctx):
    assert run_import(ctx) is Outcome.SUCCESS
    runs = ctx.catalog.runs
    assert [r["run_class"] for r in runs.values()] == ["DOWNLOAD", "EXTRACT", "IMPORT"]
    download_id, extract_id, import_id = sorted(runs)
    imp = runs[import_id]
    assert imp["status"] == SUCCESS
    assert imp["description"] == "NPD FHIR Import 2026-09-29"
    assert f"<download_run_id>{download_id}</download_run_id>" in imp["config_xml"]
    assert f"<extract_run_ids>{extract_id}</extract_run_ids>" in imp["config_xml"]
    assert '<table table="npd.practitioner" rows="2" />' in imp["output_xml"]
    assert one(ctx.npd_conninfo, "SELECT count(*) FROM npd_raw.resource WHERE release_date = %s", R) == 12
    assert one(ctx.npd_conninfo, "SELECT import_run_id FROM npd.release WHERE release_date = %s", R) == import_id
    assert one(ctx.npd_conninfo, "SELECT count(*) FROM npd.v_practitioner") == 2
    ndjson = {r.id: r for r in ctx.catalog.get_data_files(R, "ndjson")}
    with psycopg.connect(ctx.npd_conninfo) as conn:
        pairs = conn.execute("SELECT DISTINCT ndjson_file_id, zst_file_id FROM npd_raw.resource").fetchall()
    assert sorted(pairs) == sorted((r.id, r.parent_file) for r in ndjson.values())
    assert all(r.date_loaded is not None for r in ndjson.values())


def test_second_import_is_skipped(ctx):
    run_import(ctx)
    assert run_import(ctx) is Outcome.SKIPPED


def test_force_replaces_the_release(ctx):
    run_import(ctx)
    assert run_import(ctx, force=True) is Outcome.SUCCESS
    second = max(ctx.catalog.runs)
    with psycopg.connect(ctx.npd_conninfo) as conn:
        assert list_release_partitions(conn, "npd", "practitioner") == {R: f"practitioner__20260929__r{second}"}
        assert list_release_partitions(conn, "npd_raw", "resource") == {R: f"resource__20260929__r{second}"}
    assert one(ctx.npd_conninfo, "SELECT import_run_id FROM npd.release") == second
    assert one(ctx.npd_conninfo, "SELECT count(*) FROM npd_raw.resource") == 12
    assert one(ctx.npd_conninfo, "SELECT count(*) FROM npd.v_practitioner") == 2


def test_force_gives_up_on_a_locked_parent(ctx):
    run_import(ctx)
    first = max(ctx.catalog.runs)
    ctx.config = replace(ctx.config, npd_db=replace(ctx.config.npd_db, lock_timeout_seconds=1))
    sleeps = []
    ctx.sleep = sleeps.append
    with psycopg.connect(ctx.npd_conninfo) as other:
        other.execute("LOCK TABLE npd.practitioner IN ACCESS SHARE MODE")  # a long analyst query
        start = time.monotonic()
        with pytest.raises(StageFailed, match="lock timeout"):
            run_import(ctx, force=True)
        assert time.monotonic() - start < 15
        other.rollback()
    assert len(sleeps) == 2
    assert ctx.catalog.runs[max(ctx.catalog.runs)]["status"] == FAILED
    with psycopg.connect(ctx.npd_conninfo) as conn:
        assert list_release_partitions(conn, "npd", "practitioner") == {R: f"practitioner__20260929__r{first}"}
        assert list_release_partitions(conn, "npd_raw", "resource") == {R: f"resource__20260929__r{first}"}
    assert one(ctx.npd_conninfo, "SELECT count(*) FROM pg_class WHERE relname ~ '__r[0-9]+' "
                                 "AND NOT relispartition") == 0
    assert one(ctx.npd_conninfo, "SELECT import_run_id FROM npd.release") == first


def test_bad_data_fails_cleanly(tmp_path, cms, npd_db):
    bad = b'{"resourceType": "Practitioner", "id": "P1"}\n{oops\n'
    ctx = downloaded_ctx(tmp_path, cms, npd_db,
                         build_release("2026-09-29", raw_ndjson={"06-Practitioner.ndjson": bad}))
    with pytest.raises(StageFailed, match="line 2: invalid JSON"):
        run_import(ctx)
    imp = ctx.catalog.runs[max(ctx.catalog.runs)]
    assert imp["run_class"] == "IMPORT" and imp["status"] == FAILED and "line 2" in imp["result"]
    prac = next(r for r in ctx.catalog.get_data_files(R, "ndjson") if r.file_name.endswith("_06-Practitioner.ndjson"))
    assert "line 2" in prac.exceptions and prac.date_loaded is None
    assert one(npd_db, "SELECT count(*) FROM pg_class WHERE relname ~ '__r[0-9]+'") == 0
    assert one(npd_db, "SELECT count(*) FROM npd.release") == 0


def test_published_without_catalog_success_requires_force(ctx):
    run_import(ctx)
    ctx.catalog.runs[max(ctx.catalog.runs)]["status"] = FAILED  # crash after the publish commit
    with pytest.raises(StageFailed, match="--force"):
        run_import(ctx)
    assert one(ctx.npd_conninfo, "SELECT count(*) FROM npd.v_practitioner") == 2  # still live
    assert run_import(ctx, force=True) is Outcome.SUCCESS


def test_no_download_fails(tmp_path, cms, npd_db):
    ctx = make_ctx(tmp_path, cms, npd_conninfo=npd_db)
    with pytest.raises(StageFailed, match="no successful download"):
        run_import(ctx)
