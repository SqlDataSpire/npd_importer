"""Profile the JSON paths in an NDJSON(.zst) file, and compare them with what the transforms map."""
from __future__ import annotations

import io
import json
from collections import Counter
from dataclasses import dataclass, field
from importlib import resources
from pathlib import Path
from typing import BinaryIO, Iterable, Iterator

import zstandard


def leaf_paths(obj: object, prefix: str = "") -> Iterator[str]:
    if isinstance(obj, dict):
        for key, value in obj.items():
            if key == "extension" and isinstance(value, list):
                for ext in value:
                    if isinstance(ext, dict) and isinstance(ext.get("url"), str):
                        rest = {k: v for k, v in ext.items() if k != "url"}
                        yield from leaf_paths(rest, f"{prefix}.extension[{ext['url']}]")
                    else:
                        yield from leaf_paths(ext, f"{prefix}.extension[]")
            else:
                yield from leaf_paths(value, f"{prefix}.{key}")
    elif isinstance(obj, list):
        for value in obj:
            yield from leaf_paths(value, prefix + "[]")
    else:
        yield prefix


@dataclass
class Profile:
    resources: Counter = field(default_factory=Counter)
    paths: dict[str, Counter] = field(default_factory=dict)

    def add(self, record: dict) -> None:
        rtype = str(record.get("resourceType"))
        self.resources[rtype] += 1
        self.paths.setdefault(rtype, Counter()).update(leaf_paths(record))


def profile_records(records: Iterable[dict]) -> Profile:
    prof = Profile()
    for record in records:
        prof.add(record)
    return prof


def _open(path: Path) -> BinaryIO:
    raw = open(path, "rb")
    if path.suffix == ".zst":
        return io.BufferedReader(zstandard.ZstdDecompressor().stream_reader(raw, read_across_frames=True,
                                                                            closefd=True))
    return raw


def profile_file(path: str | Path, limit: int | None = None) -> Profile:
    prof = Profile()
    with _open(Path(path)) as f:
        for n, line in enumerate(f):
            if limit is not None and n >= limit:
                break
            if line.strip():
                prof.add(json.loads(line))
    return prof


def load_mapped_paths() -> set[tuple[str, str]]:
    text = (resources.files("npd_loader") / "sql" / "mapped_paths.txt").read_text(encoding="utf-8")
    mapped = set()
    for line in text.splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            rtype, path = line.split(maxsplit=1)
            mapped.add((rtype, path))
    return mapped


def unmapped(prof: Profile, mapped: set[tuple[str, str]]) -> list[tuple[str, str, int]]:
    return sorted((rtype, path, count) for rtype, counter in prof.paths.items() for path, count in counter.items()
                  if (rtype, path) not in mapped and ("*", path) not in mapped)


def format_report(prof: Profile, rows: list[tuple[str, str, int]] | None = None) -> str:
    lines = []
    for rtype in sorted(prof.resources):
        lines.append(f"== {rtype}: {prof.resources[rtype]} resources")
        selected = sorted(prof.paths[rtype].items()) if rows is None else \
            [(path, count) for t, path, count in rows if t == rtype]
        lines.extend(f"{count:>12} {path}" for path, count in selected)
    return "\n".join(lines)
