import json

import zstandard

from npd_loader.profile import leaf_paths, load_mapped_paths, profile_file, profile_records, unmapped
from fixture_data import PRAC1


def test_leaf_paths_arrays_and_extensions():
    paths = list(leaf_paths({"resourceType": "Practitioner", "name": [{"given": ["A", "B"]}],
                             "extension": [{"url": "http://x/flag", "valueBoolean": True}]}))
    assert paths == [".resourceType", ".name[].given[]", ".name[].given[]", ".extension[http://x/flag].valueBoolean"]


def test_profile_counts_per_type():
    prof = profile_records([PRAC1, PRAC1, {"resourceType": "Location", "id": "L"}])
    assert prof.resources == {"Practitioner": 2, "Location": 1}
    assert prof.paths["Practitioner"][".telecom[].value"] == 6
    assert prof.paths["Location"][".id"] == 1


def test_profile_reads_zst(tmp_path):
    path = tmp_path / "06-Practitioner.ndjson.zst"
    path.write_bytes(zstandard.ZstdCompressor().compress((json.dumps(PRAC1) + "\n").encode() * 3))
    assert profile_file(path).resources == {"Practitioner": 3}
    assert profile_file(path, limit=1).resources == {"Practitioner": 1}


def test_unmapped_honours_wildcards():
    prof = profile_records([{"resourceType": "Location", "id": "L", "status": "active", "mode": "x"}])
    mapped = {("*", ".id"), ("*", ".resourceType"), ("Location", ".status")}
    assert unmapped(prof, mapped) == [("Location", ".mode", 1)]


def test_mapped_paths_file_loads():
    mapped = load_mapped_paths()
    assert ("*", ".id") in mapped and ("*", ".meta.lastUpdated") in mapped
