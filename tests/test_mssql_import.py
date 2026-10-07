from datetime import date

import pytest

from npd_loader.download import run_download
from npd_loader.import_stage import run_import
from npd_loader.stages import Outcome, StageFailed
from fakes import FakeCatalog
from helpers import make_ctx
from release_builder import build_release


@pytest.fixture
def ctx(tmp_path, cms, mssql_dialect):
    return make_ctx(tmp_path, cms, catalog=FakeCatalog(), dialect=mssql_dialect)


def test_import_flattens_and_applies(ctx, cms):
    cms.publish(build_release("2026-09-29"))
    run_download(ctx)
    assert run_import(ctx) is Outcome.SUCCESS
    imp = [r for r in ctx.catalog.runs.values() if r["run_class"] == "IMPORT"][0]
    assert '<delta resource_type="Practitioner" new="2"' in imp["output_xml"]
    assert ctx.dialect.published_releases() == [date(2026, 9, 29)]
    assert run_import(ctx) is Outcome.SKIPPED


def test_older_release_is_refused_without_force(ctx, cms):
    cms.publish(build_release("2026-10-06"))
    run_download(ctx)
    run_import(ctx)
    cms.publish(build_release("2026-09-29"))
    run_download(ctx)
    with pytest.raises(StageFailed, match="older than the current release 2026-10-06"):
        run_import(ctx, release=date(2026, 9, 29))
    assert run_import(ctx, release=date(2026, 9, 29), force=True) is Outcome.SUCCESS


def test_killed_run_leftovers_are_never_applied_and_run_closed(ctx, cms):
    cms.publish(build_release("2026-09-29"))
    run_download(ctx)
    d = ctx.dialect
    with d._autocommit() as c:                                     # a killed run left a stale staged practitioner
        c.exec_driver_sql(f"INSERT INTO {d.q(d.cfg.stage_schema, 'practitioner')} (release_date, resource_id, "
                          f"ndjson_file_id, zst_file_id) VALUES ('2026-01-01', 'STALE', 1, 2)")
    open_run = ctx.catalog.start_run("IMPORT", "killed", "<WAREHOUSE_RUN_CONFIG />")
    assert run_import(ctx) is Outcome.SUCCESS
    with d.engine.connect() as c:
        assert c.exec_driver_sql(f"SELECT count(*) FROM {d.q(d.cfg.schema, 'resource_state')} "
                                 f"WHERE resource_id = 'STALE'").scalar() == 0      # never gets a key or a row
    assert ctx.catalog.runs[open_run.id]["status"] == "Failed"


def test_flatten_error_is_recorded_on_the_file(ctx, cms):
    bad = build_release("2026-09-29", raw_ndjson={"01-Organization.ndjson": b'{"resourceType": "Organization"}\n'})
    cms.publish(bad)
    run_download(ctx)
    with pytest.raises(StageFailed, match="missing id"):
        run_import(ctx)
    org = [f for f in ctx.catalog.files.values() if (f.get("file_name") or "").endswith("01-Organization.ndjson")]
    assert org and "missing id" in (org[0].get("exceptions") or "")
    assert ctx.dialect.published_releases() == []
