import orjson
import pytest

from npd_loader.flatten.stagefiles import FIELD, ROW, FlattenError, flatten_file, flatten_files
from npd_loader.raw_load import NdjsonInput
from npd_loader.storage import LocalStorage
from release_builder import build_release
import fixture_data


def write(tmp_path, name, data: bytes, fid=500):
    p = tmp_path / name
    p.write_bytes(data)
    return NdjsonInput(file_id=fid, zst_file_id=fid + 1, rel_path=name, resource_type=name.split("-", 1)[1].split(".")[0],
                       name=name), str(p)


def read_rows(path):
    text = open(path, encoding="utf-8").read()
    return [r.split(FIELD) for r in text.split(ROW) if r]


def test_flatten_file_writes_rows_and_hashes(tmp_path):
    inp, src = write(tmp_path, "06-Practitioner.ndjson", build_release("2026-09-29").ndjson["06-Practitioner.ndjson"])
    res = flatten_file(inp, src, "2026-09-29", str(tmp_path / "out"), chunk_rows=2)
    assert res.resources == {"Practitioner": 2}
    assert res.rows["practitioner"] == 2 and res.rows["resource_hash"] == 2 and res.rows["practitioner_name"] == 3
    chunks = [f for f in res.files if f.table == "practitioner_name"]
    assert [f.rows for f in chunks] == [2, 1]                       # rotates every chunk_rows rows
    h = read_rows([f for f in res.files if f.table == "resource_hash"][0].path)[0]
    assert h[0] == "Practitioner" and h[1] == "1003000100" and len(h[2]) == 40 and h[4] == "2026-09-29" and h[6] == "1"
    p = read_rows([f for f in res.files if f.table == "practitioner"][0].path)[0]
    assert p[1] == "1003000100" and p[6] == "1" and p[10] == ""   # active=1, prefix NULL -> empty


def test_bad_input_names_file_and_line(tmp_path):
    good = orjson.dumps(fixture_data.ORG1) + b"\n"
    for data, msg in [(good + b"{nope\n", "line 2: invalid JSON"),
                      (good + orjson.dumps({**fixture_data.ORG1, "resourceType": "Location"}) + b"\n", "line 2: resourceType"),
                      (orjson.dumps({**fixture_data.ORG1, "name": "A\x1fB"}) + b"\n", "line 1: .*0x1F"),
                      (orjson.dumps({**fixture_data.ORG1, "active": "yes"}) + b"\n", "line 1: .*not a boolean")]:
        inp, src = write(tmp_path, "01-Organization.ndjson", data)
        with pytest.raises(FlattenError, match=msg) as e:
            flatten_file(inp, src, "2026-09-29", str(tmp_path / "out"))
        assert e.value.file_id == 500


def test_flatten_files_in_parallel(tmp_path):
    storage = LocalStorage(tmp_path / "data")
    inputs = []
    for i, (name, data) in enumerate(sorted(build_release("2026-09-29").ndjson.items())):
        with storage.open_write(name) as f:
            f.write(data)
        inputs.append(NdjsonInput(500 + 2 * i, 501 + 2 * i, name, name.split("-", 1)[1].split(".")[0], name))
    from datetime import date
    res = flatten_files(inputs, storage, date(2026, 9, 29), str(tmp_path / "out"), workers=2)
    assert sum(res.resources.values()) == 12 and res.rows["resource_hash"] == 12
    assert res.rows["identifier"] > 0 and res.rows["practitioner_role"] > 0


def test_encode_matches_json_value_semantics():
    from npd_loader.flatten.convert import ConvertError
    from npd_loader.flatten.stagefiles import encode
    assert encode(None) == "" and encode(True) == "true" and encode(False) == "false"
    assert encode({"a": 1}) == "" and encode([1]) == ""
    assert encode("x") == "x" and encode(5) == "5" and encode(1.5) == "1.5"
    with pytest.raises(ConvertError):
        encode("ab")


def test_hash_depends_on_spec_version(tmp_path, monkeypatch):
    from npd_loader.flatten import stagefiles
    inp, src = write(tmp_path, "01-Organization.ndjson", orjson.dumps(fixture_data.ORG1) + b"\n")
    hashes = []
    for version in (1, 2):
        monkeypatch.setattr(stagefiles, "SPEC_VERSION", version)
        res = flatten_file(inp, src, "2026-09-29", str(tmp_path / f"out{version}"))
        hashes.append(read_rows([f for f in res.files if f.table == "resource_hash"][0].path)[0][2])
    assert hashes[0] != hashes[1]


def test_flatten_files_without_inputs_is_empty(tmp_path):
    from datetime import date
    res = flatten_files([], LocalStorage(tmp_path), date(2026, 9, 29), str(tmp_path / "out"), 4)
    assert res.files == [] and res.rows == {} and res.resources == {}
