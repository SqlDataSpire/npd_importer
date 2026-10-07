from datetime import date

import orjson
import pytest

from npd_loader.flatten.stagefiles import FlattenError
from npd_loader.dialect.bcp import BcpError
from npd_loader.raw_load import NdjsonInput
from npd_loader.storage import LocalStorage
from release_builder import build_release
import fixture_data

R = date(2026, 9, 29)


def inputs_for(storage, ndjson):
    out = []
    for i, (name, data) in enumerate(sorted(ndjson.items())):
        with storage.open_write(name) as f:
            f.write(data)
        out.append(NdjsonInput(500 + 2 * i, 501 + 2 * i, name, name.split("-", 1)[1].split(".")[0], name))
    return out


def count(d, table):
    with d.engine.connect() as c:
        return c.exec_driver_sql(f"SELECT COUNT_BIG(*) FROM {d.q(d.cfg.stage_schema, table)}").scalar()


def test_stage_release_loads_every_table(mssql_dialect, tmp_path):
    d = mssql_dialect
    storage = LocalStorage(tmp_path / "data")
    res = d.stage_release(storage, R, 7, inputs_for(storage, build_release("2026-09-29").ndjson))
    assert sum(res.resources.values()) == 12
    assert count(d, "resource_hash") == 12
    assert count(d, "practitioner") == 2 and count(d, "identifier") == res.rows["identifier"]
    assert not list((tmp_path / "data" / "stage").rglob("*.dat"))          # stage files deleted after load


def test_each_import_starts_from_empty_staging(mssql_dialect, tmp_path):
    d = mssql_dialect
    storage = LocalStorage(tmp_path / "data")
    inputs = inputs_for(storage, build_release("2026-09-29").ndjson)
    d.stage_release(storage, R, 7, inputs)
    d.stage_release(storage, R, 8, inputs)                                   # e.g. after a killed run
    assert count(d, "resource_hash") == 12 and count(d, "practitioner") == 2


def test_duplicate_ids_name_the_file(mssql_dialect, tmp_path):
    d = mssql_dialect
    storage = LocalStorage(tmp_path / "data")
    line = orjson.dumps(fixture_data.ORG1) + b"\n"
    with pytest.raises(FlattenError, match="duplicate resource ids: Organization Organization-1336200294") as e:
        d.stage_release(storage, R, 7, inputs_for(storage, {"01-Organization.ndjson": line * 2}))
    assert e.value.file_id == 500


def test_too_long_value_fails_with_the_bcp_error(mssql_dialect, tmp_path):
    d = mssql_dialect
    storage = LocalStorage(tmp_path / "data")
    rec = {**fixture_data.ORG1, "address": [{"city": "x" * 1500}]}         # organization_address.city nvarchar(1000)
    with pytest.raises(BcpError, match="organization_address"):
        d.stage_release(storage, R, 7, inputs_for(storage, {"01-Organization.ndjson": orjson.dumps(rec) + b"\n"}))


def test_stage_release_removes_leftover_stage_files(mssql_dialect, tmp_path):
    d = mssql_dialect
    storage = LocalStorage(tmp_path / "data")
    leftover = tmp_path / "data" / "stage" / "run_1" / "practitioner.x.dat"
    leftover.parent.mkdir(parents=True)
    leftover.write_text("junk")
    d.stage_release(storage, R, 7, inputs_for(storage, build_release("2026-09-29").ndjson))
    assert not leftover.exists() and not (tmp_path / "data" / "stage").exists()
    assert count(d, "resource_hash") == 12 and count(d, "practitioner") == 2
