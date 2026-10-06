from datetime import date

from pg_helpers import write_inputs  # pure file writing; no database
from release_builder import build_release

R = date(2026, 9, 29)


def load_fixture_raw(dialect, storage, ndjson=None, release=R, run_id=7):
    ndjson = ndjson if ndjson is not None else build_release(release.isoformat()).ndjson
    return dialect.load_raw(storage, release, run_id, write_inputs(storage, ndjson))


def fetch(dialect, sql, *args):
    with dialect.engine.connect() as conn:
        return conn.exec_driver_sql(sql, args).fetchall()
