import threading
from datetime import date

import pytest

from npd_loader.dialect import LockUnavailable, PublishConflict
from npd_loader.raw_load import RawLoadError
from npd_loader.storage import LocalStorage
from mssql_fixture_load import R, fetch, load_fixture_raw
import fixture_data


def import_release(d, tmp_path, release=R, run_id=7, force=False):
    raw = load_fixture_raw(d, LocalStorage(tmp_path / f"data{run_id}"), release=release, run_id=run_id)
    res = d.run_transforms(raw.table, release, run_id)
    d.publish(raw.table, res.tables, release, run_id, force)
    return res


def count(d, schema, table, release=None):
    where = f" WHERE release_date = '{release}'" if release else ""
    return fetch(d, f"SELECT count(*) FROM {d.q(schema, table)}{where}")[0][0]


def test_publish_switches_in_and_records(mssql_dialect, tmp_path):
    d = mssql_dialect
    import_release(d, tmp_path)
    assert count(d, d.cfg.raw_schema, "resource", R) == 12
    assert count(d, d.cfg.schema, "practitioner", R) == 2
    assert count(d, d.cfg.schema, "v_practitioner") == 2
    assert d.published_releases() == [R] and d.partitioned_releases() == {R} and d.is_published(R)
    assert fetch(d, f"SELECT import_run_id FROM {d.q(d.cfg.schema, 'release')}")[0][0] == 7
    assert d.drop_standalone_tables() == []          # standalone tables were consumed by SWITCH


def test_republish_needs_force_and_force_replaces(mssql_dialect, tmp_path):
    d = mssql_dialect
    import_release(d, tmp_path, run_id=7)
    with pytest.raises(PublishConflict, match="already published"):
        import_release(d, tmp_path, run_id=8)
    assert d.drop_standalone_tables(8)               # the failed attempt's tables are cleanable
    import_release(d, tmp_path, run_id=9, force=True)
    assert count(d, d.cfg.raw_schema, "resource", R) == 12
    assert fetch(d, f"SELECT import_run_id FROM {d.q(d.cfg.schema, 'release')}")[0][0] == 9


def test_two_releases_and_drop(mssql_dialect, tmp_path):
    d = mssql_dialect
    r1, r2 = date(2026, 9, 22), R
    import_release(d, tmp_path, release=r2, run_id=7)
    import_release(d, tmp_path, release=r1, run_id=8)          # older release published after a newer one
    assert d.partitioned_releases() == {r1, r2}
    assert count(d, d.cfg.schema, "v_practitioner") == 2 and count(d, d.cfg.schema, "practitioner") == 4
    dropped = d.drop_release(r1)
    assert f"{d.cfg.schema}.practitioner" in dropped
    assert d.partitioned_releases() == {r2} and d.published_releases() == [r2]
    assert count(d, d.cfg.schema, "practitioner") == 2


def test_drop_release_gives_up_on_lock(mssql_dialect, mssql_engine, tmp_path):
    d = mssql_dialect
    import_release(d, tmp_path)
    blocker = mssql_engine.connect()
    tx = blocker.begin()
    blocker.exec_driver_sql(f"SELECT TOP 1 * FROM {d.q(d.cfg.schema, 'practitioner')} WITH (TABLOCKX, HOLDLOCK)")
    try:
        with pytest.raises(LockUnavailable):
            d.drop_release(R)
    finally:
        tx.rollback()
        blocker.close()
    assert d.partitioned_releases() == {R}           # nothing dropped


def test_run_lock_is_exclusive(mssql_dialect):
    d = mssql_dialect
    with d.run_lock("import") as first:
        assert first is True
        result = {}
        t = threading.Thread(target=lambda: result.setdefault("second", d.run_lock("import").__enter__()))
        t.start(); t.join()
        assert result["second"] is False
        with d.run_lock("download") as other:
            assert other is True
    with d.run_lock("import") as again:
        assert again is True


def test_failed_raw_load_leaves_droppable_table(mssql_dialect, tmp_path):
    d = mssql_dialect
    good = (__import__("json").dumps(fixture_data.ORG1) + "\n").encode()
    with pytest.raises(RawLoadError):
        load_fixture_raw(d, LocalStorage(tmp_path / "data"), ndjson={"01-Organization.ndjson": good + b"{nope\n"})
    assert count(d, d.cfg.raw_schema, "resource") == 0
    assert d.drop_standalone_tables(7) == [f"{d.cfg.raw_schema}.resource__20260929__r7"]


def test_statistics_failure_after_commit_is_only_a_warning(mssql_dialect, mssql_engine, tmp_path, caplog):
    from sqlalchemy import event

    def refuse(conn, cursor, statement, parameters, context, executemany):
        if statement.startswith("UPDATE STATISTICS"):
            raise RuntimeError("statistics refused")

    d = mssql_dialect
    event.listen(mssql_engine, "before_cursor_execute", refuse)
    try:
        with caplog.at_level("WARNING"):
            import_release(d, tmp_path)            # must not raise
    finally:
        event.remove(mssql_engine, "before_cursor_execute", refuse)
    assert d.published_releases() == [R] and d.is_published(R)
    assert count(d, d.cfg.schema, "practitioner", R) == 2
    assert "UPDATE STATISTICS" in caplog.text and "statistics refused" in caplog.text
