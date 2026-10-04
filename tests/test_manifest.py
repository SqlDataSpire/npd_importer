import json
from datetime import date

import httpx
import pytest

from npd_loader.manifest import (ManifestError, fetch_manifest, file_url, parse_manifest,
                                 resource_type_for)
from release_builder import build_release

REAL = {"compression_algorithm": "zstd",
        "files": {"01-Organization.ndjson": {"compressed_bytes": 298129327, "compression_ratio_pct": 92.5,
                                             "original_bytes": 3973425929},
                  "06-Practitioner.ndjson": {"compressed_bytes": 844943714, "compression_ratio_pct": 94.95,
                                             "original_bytes": 16739630848}},
        "generated_at": "2026-09-29", "totals": {}}


def raw(d) -> bytes:
    return json.dumps(d).encode()


def test_parse_real_format():
    m = parse_manifest(raw(REAL))
    assert m.release_date == date(2026, 9, 29)
    assert [f.name for f in m.files] == ["01-Organization.ndjson", "06-Practitioner.ndjson"]
    p = m.file("06-Practitioner.ndjson")
    assert (p.compressed_bytes, p.original_bytes, p.zst_name, p.resource_type) == (
        844943714, 16739630848, "06-Practitioner.ndjson.zst", "Practitioner")
    assert m.raw == raw(REAL)


def test_generated_at_may_be_a_timestamp():
    assert parse_manifest(raw({**REAL, "generated_at": "2026-09-29T03:00:00Z"})).release_date == date(2026, 9, 29)


@pytest.mark.parametrize("patch,message", [
    ({"compression_algorithm": "gzip"}, "compression_algorithm"),
    ({"generated_at": "soon"}, "generated_at"),
    ({"files": {}}, "no files"),
    ({"files": {"Organization.json": {"compressed_bytes": 1, "original_bytes": 1}}}, "file name"),
    ({"files": {"01-Organization.ndjson": {"compressed_bytes": 1}}}, "original_bytes"),
    ({"files": {"01-Organization.ndjson": {"compressed_bytes": -1, "original_bytes": 1}}}, "compressed_bytes"),
])
def test_rejects_bad_manifests(patch, message):
    with pytest.raises(ManifestError, match=message):
        parse_manifest(raw({**REAL, **patch}))


def test_rejects_non_json():
    with pytest.raises(ManifestError, match="JSON"):
        parse_manifest(b"<html>")


def test_resource_type_for():
    assert resource_type_for("07-PractitionerRole.ndjson") == "PractitionerRole"
    assert resource_type_for("07-PractitionerRole.ndjson.zst") == "PractitionerRole"
    assert resource_type_for("file_12345_08-OrganizationAffiliation.ndjson") == "OrganizationAffiliation"
    with pytest.raises(ManifestError):
        resource_type_for("practitioner.csv")


def test_file_url():
    assert file_url("https://directory.cms.gov/downloads/manifest.json", "01-Organization.ndjson.zst") == \
        "https://directory.cms.gov/downloads/01-Organization.ndjson.zst"


def test_fetch_follows_redirect(cms):
    cms.publish(build_release("2026-09-29"))
    with httpx.Client() as client:
        m = fetch_manifest(client, cms.manifest_url)
    assert m.release_date == date(2026, 9, 29)
    assert len(m.files) == 8
