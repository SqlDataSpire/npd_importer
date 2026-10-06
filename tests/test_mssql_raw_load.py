from datetime import datetime

import pytest

from npd_loader.dialect.mssql import BATCH_ROWS, to_utc_naive
from npd_loader.raw_load import RawLoadError
from npd_loader.storage import LocalStorage
from mssql_fixture_load import R, fetch, load_fixture_raw
import fixture_data


def test_to_utc_naive():
    assert to_utc_naive("2026-09-29T04:34:00.724328Z") == datetime(2026, 9, 29, 4, 34, 0, 724328)
    assert to_utc_naive("2026-09-29T01:00:00-05:00") == datetime(2026, 9, 29, 6, 0)
    assert to_utc_naive(None) is None


def test_load_raw_counts_lineage_and_index(mssql_dialect, tmp_path):
    d = mssql_dialect
    res = load_fixture_raw(d, LocalStorage(tmp_path / "data"))
    assert res.table == "resource__20260929__r7"
    assert sum(res.rows.values()) == 12 and res.rows["Practitioner"] == 2
    t = d.q(d.cfg.raw_schema, res.table)
    assert fetch(d, f"SELECT count(*), count(DISTINCT ndjson_file_id), MIN(release_date) FROM {t}")[0] == (12, 8, R)
    org = fetch(d, f"SELECT last_updated, ISJSON(resource) FROM {t} WHERE resource_id = ?",
                fixture_data.ORG1["id"])[0]
    assert org == (datetime(2026, 9, 29, 4, 29, 5, 411000), 1)
    assert fetch(d, "SELECT count(*) FROM sys.indexes WHERE object_id = OBJECT_ID(?) AND name = 'resource_key'",
                 f"{d.cfg.raw_schema}.{res.table}")[0][0] == 1
    assert fetch(d, f"SELECT count(*) FROM {d.q(d.cfg.raw_schema, 'resource')}")[0][0] == 0   # not published


def test_batches_commit_and_many_rows_load(mssql_dialect, tmp_path):
    d = mssql_dialect
    base = fixture_data.ORG1
    n = BATCH_ROWS * 2 + 7
    lines = b"".join((__import__("json").dumps({**base, "id": f"Organization-{i}"}) + "\n").encode()
                     for i in range(n))
    res = load_fixture_raw(d, LocalStorage(tmp_path / "data"), ndjson={"01-Organization.ndjson": lines})
    assert res.rows == {"Organization": n}


def test_duplicate_ids_name_the_file(mssql_dialect, tmp_path):
    line = (__import__("json").dumps(fixture_data.ORG1) + "\n").encode()
    with pytest.raises(RawLoadError, match="duplicate resource ids: Organization Organization-1336200294") as e:
        load_fixture_raw(mssql_dialect, LocalStorage(tmp_path / "data"), ndjson={"01-Organization.ndjson": line * 2})
    assert e.value.file_id == 500


def test_bad_line_fails_and_nothing_is_published(mssql_dialect, tmp_path):
    d = mssql_dialect
    good = (__import__("json").dumps(fixture_data.ORG1) + "\n").encode()
    with pytest.raises(RawLoadError, match="line 2: invalid JSON"):
        load_fixture_raw(d, LocalStorage(tmp_path / "data"), ndjson={"01-Organization.ndjson": good + b"{nope\n"})
    assert fetch(d, f"SELECT count(*) FROM {d.q(d.cfg.raw_schema, 'resource')}")[0][0] == 0


def test_too_long_value_fails_loudly(mssql_dialect, tmp_path):
    rec = {**fixture_data.ORG1, "id": "x" * 200}            # resource_id is varchar(128)
    with pytest.raises(RawLoadError, match="Organization"):
        load_fixture_raw(mssql_dialect, LocalStorage(tmp_path / "data"),
                         ndjson={"01-Organization.ndjson": (__import__("json").dumps(rec) + "\n").encode()})
