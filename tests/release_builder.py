"""Build a release (manifest + .zst bytes) in memory, in the same format directory.cms.gov uses."""
from __future__ import annotations

import json
from dataclasses import dataclass

import zstandard

import fixture_data


@dataclass
class BuiltRelease:
    release_date: str
    manifest: bytes
    files: dict[str, bytes]     # "06-Practitioner.ndjson.zst" -> compressed bytes
    ndjson: dict[str, bytes]    # "06-Practitioner.ndjson" -> uncompressed bytes


def build_release(release_date: str, records: dict[str, list[dict]] | None = None,
                  raw_ndjson: dict[str, bytes] | None = None,
                  manifest_overrides: dict[str, dict] | None = None) -> BuiltRelease:
    """`records` replaces the default fixture records; `raw_ndjson` adds or replaces files byte-for-byte
    (for bad-data variants); `manifest_overrides` patches a file's manifest entry, e.g. a wrong size."""
    raw = {name: fixture_data.ndjson_bytes(recs) for name, recs in (records or fixture_data.RECORDS).items()}
    raw.update(raw_ndjson or {})
    files: dict[str, bytes] = {}
    entries: dict[str, dict] = {}
    for name, data in sorted(raw.items()):
        compressed = zstandard.ZstdCompressor().compress(data)
        files[name + ".zst"] = compressed
        entries[name] = {"compressed_bytes": len(compressed), "compression_ratio_pct": 0.0,
                         "original_bytes": len(data)}
    for name, patch in (manifest_overrides or {}).items():
        entries[name].update(patch)
    manifest = {"compression_algorithm": "zstd", "files": entries, "generated_at": release_date,
                "totals": {"compressed_bytes": sum(e["compressed_bytes"] for e in entries.values()),
                           "original_bytes": sum(e["original_bytes"] for e in entries.values())}}
    return BuiltRelease(release_date, json.dumps(manifest, indent=2).encode(), files, raw)
