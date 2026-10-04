import httpx
import pytest
import zstandard

from release_builder import build_release


def test_redirect_range_and_drop(cms):
    rel = build_release("2026-09-29")
    cms.publish(rel)
    name = "06-Practitioner.ndjson.zst"
    with httpx.Client() as client:
        first = client.get(cms.public_url(name), follow_redirects=False)
        assert first.status_code == 302 and "/s3/" in first.headers["location"]
        full = client.get(cms.public_url(name), follow_redirects=True)
        assert full.content == rel.files[name]
        assert zstandard.ZstdDecompressor().decompress(full.content) == rel.ndjson["06-Practitioner.ndjson"]
        part = client.get(cms.public_url(name), headers={"Range": "bytes=10-"}, follow_redirects=True)
        assert part.status_code == 206 and part.content == rel.files[name][10:]
        cms.drops[name] = [5]
        with pytest.raises(httpx.RemoteProtocolError):
            client.get(cms.public_url(name), follow_redirects=True)
        cms.expire_next.add(name)
        assert client.get(cms.public_url(name), follow_redirects=True).status_code == 403


def test_manifest_changes_after_first_fetch(cms):
    cms.publish(build_release("2026-09-29"), later_manifest=build_release("2026-10-06").manifest)
    with httpx.Client(follow_redirects=True) as client:
        assert b"2026-09-29" in client.get(cms.manifest_url).content
        assert b"2026-10-06" in client.get(cms.manifest_url).content
        assert b"2026-10-06" in client.get(cms.manifest_url).content
