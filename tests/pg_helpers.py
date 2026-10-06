from datetime import date

from npd_loader.manifest import resource_type_for
from npd_loader.raw_load import NdjsonInput, load_raw
from npd_loader.transform import run_transforms
from release_builder import build_release

R = date(2026, 9, 29)


def write_inputs(storage, ndjson: dict[str, bytes], first_id: int = 500) -> list[NdjsonInput]:
    inputs = []
    for i, (name, data) in enumerate(sorted(ndjson.items())):
        file_id = first_id + 2 * i
        rel = f"run_1_2026-09-29-000000/file_{file_id}_{name}"
        with storage.open_write(rel) as f:
            f.write(data)
        inputs.append(NdjsonInput(file_id=file_id, zst_file_id=file_id + 1, rel_path=rel,
                                  resource_type=resource_type_for(name), name=name))
    return inputs


def load_fixture_raw(conn, storage, ndjson=None, release=R, run_id=7):
    ndjson = ndjson if ndjson is not None else build_release(release.isoformat()).ndjson
    return load_raw(conn, storage, "npd_raw", release, run_id, write_inputs(storage, ndjson))


def transformed(conn, storage, run_id=7):
    raw = load_fixture_raw(conn, storage, run_id=run_id)
    return run_transforms(conn, "npd_raw", raw.table, "npd", R, run_id)


def rows(conn, result, table: str, columns: str) -> list[tuple]:
    from psycopg import sql as _sql
    has_seq = conn.execute("SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'npd' "
                           "AND table_name = %s AND column_name = 'seq')", (table,)).fetchone()[0]
    order = "resource_id, seq" if has_seq else "resource_id"
    query = _sql.SQL("SELECT {} FROM {} ORDER BY {}").format(
        _sql.SQL(columns), _sql.Identifier("npd", result.tables[table]), _sql.SQL(order))
    return conn.execute(query).fetchall()
