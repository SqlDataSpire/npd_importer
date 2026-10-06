import hashlib
import re
from contextlib import contextmanager
from datetime import date

import pytest

from npd_loader.catalog import FAILED, SUCCESS
from npd_loader.download import run_download
from npd_loader.stages import Outcome, StageFailed
from helpers import make_ctx
from release_builder import build_release

R = date(2026, 9, 29)
PRAC = "06-Practitioner.ndjson.zst"


@pytest.fixture
def release(cms):
    rel = build_release("2026-09-29")
    cms.publish(rel)
    return rel


def zst_rows(ctx):
    return ctx.catalog.get_data_files(R, "ndjson.zst")


def test_downloads_release_and_records_catalog_rows(tmp_path, cms, release):
    ctx = make_ctx(tmp_path, cms)
    assert run_download(ctx) is Outcome.SUCCESS
    [run] = ctx.catalog.runs.values()
    assert run["status"] == SUCCESS and run["run_class"] == "DOWNLOAD"
    assert run["description"] == "NPD FHIR Download 2026-09-29"
    assert "<release_date>2026-09-29</release_date>" in run["config_xml"]
    assert "<file_set>NPD_FHIR</file_set>" in run["config_xml"]
    [manifest] = ctx.catalog.get_data_files(R, "manifest")
    assert manifest.parent_file is None
    assert manifest.source_uri == cms.manifest_url
    assert re.fullmatch(rf"run_{manifest.run_id}_\d{{4}}-\d\d-\d\d-\d{{6}}/file_{manifest.id}_manifest\.json",
                        manifest.file_rel_path)
    rows = zst_rows(ctx)
    assert len(rows) == 8
    for row in rows:
        original = row.file_name.removeprefix(f"file_{row.id}_")
        data = release.files[original]
        assert row.parent_file == manifest.id
        assert row.source_uri == cms.public_url(original)  # public URL, never the signed /s3/ one
        assert "/s3/" not in row.source_uri
        assert row.file_rel_path == f"{manifest.file_rel_path.split('/')[0]}/{row.file_name}"
        assert (row.file_size, row.file_hash) == (len(data), hashlib.sha256(data).hexdigest())
        assert row.date_modified is not None and row.date_created is not None
        with ctx.storage.open_read(row.file_rel_path) as f:
            assert f.read() == data
    assert not [p for p in ctx.storage.list("") if p.endswith(".part")]


def test_second_run_is_skipped_unless_forced(tmp_path, cms, release):
    ctx = make_ctx(tmp_path, cms)
    run_download(ctx)
    assert run_download(ctx) is Outcome.SKIPPED
    assert len(ctx.catalog.runs) == 1
    assert run_download(ctx, force=True) is Outcome.SUCCESS
    assert len(ctx.catalog.runs) == 2
    assert len(zst_rows(ctx)) == 16


def test_resumes_after_dropped_connection(tmp_path, cms, release):
    cms.drops[PRAC] = [100]
    ctx = make_ctx(tmp_path, cms)
    assert run_download(ctx) is Outcome.SUCCESS
    assert ("GET", f"/s3/{PRAC}", "bytes=100-") in cms.requests
    row = next(r for r in zst_rows(ctx) if r.file_name.endswith(PRAC))
    assert row.file_hash == hashlib.sha256(release.files[PRAC]).hexdigest()


def test_resume_when_server_ignores_range(tmp_path, cms, release):
    cms.drops[PRAC] = [100]
    cms.ignore_range.add(PRAC)
    ctx = make_ctx(tmp_path, cms)
    assert run_download(ctx) is Outcome.SUCCESS
    row = next(r for r in zst_rows(ctx) if r.file_name.endswith(PRAC))
    with ctx.storage.open_read(row.file_rel_path) as f:
        assert f.read() == release.files[PRAC]


def test_expired_signed_url_gets_a_fresh_one(tmp_path, cms, release):
    cms.expire_next.add(PRAC)
    ctx = make_ctx(tmp_path, cms)
    assert run_download(ctx) is Outcome.SUCCESS
    assert sum(1 for r in cms.requests if r[1] == f"/downloads/{PRAC}") == 2


def test_gives_up_after_max_attempts(tmp_path, cms, release):
    cms.drops[PRAC] = [10] * 10
    ctx = make_ctx(tmp_path, cms)
    with pytest.raises(StageFailed, match="giving up after 3 attempts"):
        run_download(ctx)
    [run] = ctx.catalog.runs.values()
    assert run["status"] == FAILED and "giving up" in run["result"]
    row = next(r for r in zst_rows(ctx) if r.file_name.endswith(PRAC))
    assert "giving up" in row.exceptions and row.file_hash is None


def test_size_mismatch_fails(tmp_path, cms):
    cms.publish(build_release("2026-09-29", manifest_overrides={"06-Practitioner.ndjson": {"compressed_bytes": 5}}))
    ctx = make_ctx(tmp_path, cms)
    with pytest.raises(StageFailed, match="more bytes than the manifest"):
        run_download(ctx)


def test_manifest_changing_mid_run_fails(tmp_path, cms):
    cms.publish(build_release("2026-09-29"), later_manifest=build_release("2026-10-06").manifest)
    ctx = make_ctx(tmp_path, cms)
    with pytest.raises(StageFailed, match="generated_at changed"):
        run_download(ctx)
    [run] = ctx.catalog.runs.values()
    assert run["status"] == FAILED


def test_lock_held_elsewhere_exits_quietly(tmp_path, cms, release):
    @contextmanager
    def busy(stage):
        yield False
    ctx = make_ctx(tmp_path, cms)
    ctx.lock = busy
    assert run_download(ctx) is Outcome.LOCKED
    assert ctx.catalog.runs == {}


def test_open_download_run_of_a_killed_process_is_closed(tmp_path, cms, release):
    ctx = make_ctx(tmp_path, cms)
    dead = ctx.catalog.start_run("DOWNLOAD", "killed", "<WAREHOUSE_RUN_CONFIG/>")
    assert run_download(ctx) is Outcome.SUCCESS
    assert ctx.catalog.runs[dead.id]["status"] == FAILED
    assert ctx.catalog.runs[dead.id]["result"].startswith("interrupted")
