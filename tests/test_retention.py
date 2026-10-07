from datetime import date, timedelta

from npd_loader.retention import apply_retention
from fakes import FakeCatalog
from helpers import make_ctx

BASE = date(2026, 8, 4)


def seed(ctx, release):
    run = ctx.catalog.add_successful_run("EXTRACT", release)
    rel = f"run_{release}/file_06-Practitioner.ndjson"
    ctx.catalog.add_data_file(run, file_type="ndjson", source_version_num=release.isoformat(), file_rel_path=rel,
                              file_hash="x")
    with ctx.storage.open_write(rel) as f:
        f.write(b"data")
    return rel


def test_keeps_ndjson_of_newest_releases_only(tmp_path, cms):
    ctx = make_ctx(tmp_path, cms, catalog=FakeCatalog(), keep_releases=2)
    rels = {BASE + timedelta(weeks=k): seed(ctx, BASE + timedelta(weeks=k)) for k in range(4)}
    newest = max(rels)
    assert apply_retention(ctx, newest) == []
    kept = {r for r, p in rels.items() if ctx.storage.exists(p)}
    assert kept == set(sorted(rels)[-2:])
