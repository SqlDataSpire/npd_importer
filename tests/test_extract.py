from datetime import date

import pytest

from npd_loader.catalog import FAILED, SUCCESS
from npd_loader.download import run_download
from npd_loader.extract import run_extract
from npd_loader.stages import Outcome, StageFailed
from helpers import make_ctx
from release_builder import build_release

R = date(2026, 9, 29)


@pytest.fixture
def downloaded(tmp_path, cms):
    rel = build_release("2026-09-29")
    cms.publish(rel)
    ctx = make_ctx(tmp_path, cms)
    run_download(ctx)
    return ctx, rel


def ndjson_rows(ctx):
    return ctx.catalog.get_data_files(R, "ndjson")


def test_first_extract_creates_rows_and_files(downloaded):
    ctx, rel = downloaded
    assert run_extract(ctx) is Outcome.SUCCESS
    zsts = {z.id: z for z in ctx.catalog.get_data_files(R, "ndjson.zst")}
    rows = ndjson_rows(ctx)
    assert len(rows) == 8
    extract_run = max(ctx.catalog.runs)
    assert ctx.catalog.runs[extract_run]["run_class"] == "EXTRACT"
    assert ctx.catalog.runs[extract_run]["status"] == SUCCESS
    for row in rows:
        zst = zsts[row.parent_file]
        original = row.file_name.removeprefix(f"file_{row.id}_")
        assert zst.file_name.endswith(original + ".zst")
        assert row.run_id == extract_run
        assert row.source_uri == zst.source_uri
        assert row.file_rel_path.startswith(f"run_{extract_run}_")
        with ctx.storage.open_read(row.file_rel_path) as f:
            assert f.read() == rel.ndjson[original]
        assert row.file_size == len(rel.ndjson[original])


def test_second_extract_is_skipped(downloaded):
    ctx, _ = downloaded
    run_extract(ctx)
    runs = len(ctx.catalog.runs)
    assert run_extract(ctx) is Outcome.SKIPPED
    assert len(ctx.catalog.runs) == runs


def test_missing_file_is_restored_with_same_id(downloaded):
    ctx, rel = downloaded
    run_extract(ctx)
    before = ndjson_rows(ctx)
    victim = before[3]
    ctx.storage.delete(victim.file_rel_path)
    assert run_extract(ctx) is Outcome.SUCCESS
    after = ndjson_rows(ctx)
    assert [r.id for r in after] == [r.id for r in before]
    assert ctx.storage.exists(victim.file_rel_path)
    assert 'action="restored"' in ctx.catalog.runs[max(ctx.catalog.runs)]["output_xml"]


def test_restore_with_hash_mismatch_fails_and_keeps_rows(downloaded):
    ctx, _ = downloaded
    run_extract(ctx)
    victim = ndjson_rows(ctx)[0]
    ctx.storage.delete(victim.file_rel_path)
    ctx.catalog.update_data_file(victim.id, file_hash="0" * 64)
    with pytest.raises(StageFailed, match="does not match"):
        run_extract(ctx)
    assert ctx.catalog.runs[max(ctx.catalog.runs)]["status"] == FAILED
    assert len(ndjson_rows(ctx)) == 8
    assert not ctx.storage.exists(victim.file_rel_path)
    assert "does not match" in ctx.catalog.get_data_files(R, "ndjson")[0].exceptions


def test_incomplete_row_from_failed_run_is_ignored(downloaded):
    ctx, _ = downloaded
    zst = ctx.catalog.get_data_files(R, "ndjson.zst")[0]
    dead_run = ctx.catalog.start_run("EXTRACT", "died", "<WAREHOUSE_RUN_CONFIG/>")
    dead = ctx.catalog.add_data_file(dead_run, file_type="ndjson", source_version_num="2026-09-29",
                                     parent_file=zst.id, file_name="file_1_x.ndjson",
                                     file_rel_path="run_1_2026-01-01-000000/file_1_x.ndjson")
    assert run_extract(ctx) is Outcome.SUCCESS
    children = [r for r in ndjson_rows(ctx) if r.parent_file == zst.id]
    assert len(children) == 2
    completed = [r for r in children if r.file_hash]
    assert len(completed) == 1 and completed[0].id != dead


def test_original_bytes_mismatch_fails(tmp_path, cms):
    cms.publish(build_release("2026-09-29",
                              manifest_overrides={"02-Location.ndjson": {"original_bytes": 10}}))
    ctx = make_ctx(tmp_path, cms)
    run_download(ctx)
    with pytest.raises(StageFailed, match="original_bytes"):
        run_extract(ctx)


def test_no_download_fails(tmp_path, cms):
    ctx = make_ctx(tmp_path, cms)
    with pytest.raises(StageFailed, match="no successful download"):
        run_extract(ctx)


def test_force_reextracts_with_same_ids(downloaded):
    ctx, _ = downloaded
    run_extract(ctx)
    ids = [r.id for r in ndjson_rows(ctx)]
    assert run_extract(ctx, force=True) is Outcome.SUCCESS
    assert [r.id for r in ndjson_rows(ctx)] == ids
