"""The release manifest: release date (generated_at) and the expected files with their sizes."""
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import date, datetime

import httpx

FILE_NAME_RE = re.compile(r"^(?:file_\d+_)?\d+-([A-Za-z]+)\.ndjson(?:\.zst)?$")


class ManifestError(Exception):
    pass


def resource_type_for(file_name: str) -> str:
    m = FILE_NAME_RE.match(file_name)
    if not m:
        raise ManifestError(f"unexpected file name {file_name!r} (expected NN-ResourceType.ndjson)")
    return m.group(1)


@dataclass(frozen=True)
class ManifestFile:
    name: str
    compressed_bytes: int
    original_bytes: int

    @property
    def zst_name(self) -> str:
        return self.name + ".zst"

    @property
    def resource_type(self) -> str:
        return resource_type_for(self.name)


@dataclass(frozen=True)
class Manifest:
    release_date: date
    files: tuple[ManifestFile, ...]
    raw: bytes

    def file(self, name: str) -> ManifestFile:
        for f in self.files:
            if f.name == name:
                return f
        raise ManifestError(f"{name} is not in the manifest for {self.release_date}")


def _release_date(value: object) -> date:
    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError:
            try:
                return datetime.fromisoformat(value).date()
            except ValueError:
                pass
    raise ManifestError(f"generated_at {value!r} is not a date")


def _size(entry: dict, name: str, key: str) -> int:
    value = entry.get(key)
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ManifestError(f"{name}: {key} must be a positive integer, got {value!r}")
    return value


def parse_manifest(raw: bytes) -> Manifest:
    try:
        data = json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise ManifestError(f"manifest is not valid JSON: {exc}") from exc
    if not isinstance(data, dict):
        raise ManifestError("manifest is not a JSON object")
    if data.get("compression_algorithm") != "zstd":
        raise ManifestError(f"unsupported compression_algorithm {data.get('compression_algorithm')!r}")
    release = _release_date(data.get("generated_at"))
    entries = data.get("files")
    if not isinstance(entries, dict) or not entries:
        raise ManifestError("manifest lists no files")
    files = []
    for name in sorted(entries):
        if not FILE_NAME_RE.match(name) or name.endswith(".zst"):
            raise ManifestError(f"unexpected file name {name!r} in manifest")
        entry = entries[name]
        if not isinstance(entry, dict):
            raise ManifestError(f"{name}: entry is not an object")
        files.append(ManifestFile(name, _size(entry, name, "compressed_bytes"), _size(entry, name, "original_bytes")))
    return Manifest(release, tuple(files), raw)


def fetch_manifest(client: httpx.Client, url: str) -> Manifest:
    response = client.get(url, follow_redirects=True)
    response.raise_for_status()
    return parse_manifest(response.content)


def file_url(manifest_url: str, zst_name: str) -> str:
    return manifest_url.rsplit("/", 1)[0] + "/" + zst_name
