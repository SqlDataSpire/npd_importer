"""IMPORT, raw part: the engine-neutral pieces of the raw load (line reading and strict validation).

Each dialect streams every .ndjson into its standalone raw table (dialect/postgres.py: one COPY leaf per file;
dialect/mssql.py: batched inserts).
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import BinaryIO, Iterator

RAW_PARENT = "resource"
# A \u0000 escape in JSON text: "u0000" after an odd number of backslashes. After an even number it is just
# escaped backslashes followed by the text "u0000", which jsonb stores fine.
NUL_ESCAPE_RE = re.compile(r"(?<!\\)(?:\\\\)*\\u0000")


class RawLoadError(Exception):
    def __init__(self, message: str, file_id: int | None = None):
        super().__init__(message)
        self.file_id = file_id


@dataclass(frozen=True)
class NdjsonInput:
    file_id: int
    zst_file_id: int
    rel_path: str
    resource_type: str
    name: str


@dataclass
class RawLoadResult:
    table: str
    rows: dict[str, int]


def iter_lines(f: BinaryIO) -> Iterator[tuple[int, str]]:
    """Yield (line number, text). Accepts LF or CRLF and a missing final newline. Blank lines are only
    allowed at the very end of the file."""
    blank_at: int | None = None
    for number, raw in enumerate(f, start=1):
        line = raw.rstrip(b"\r\n")
        if not line.strip():
            blank_at = blank_at or number
            continue
        if blank_at is not None:
            raise RawLoadError(f"line {blank_at}: blank line")
        try:
            yield number, line.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise RawLoadError(f"line {number}: not UTF-8: {exc}") from exc


def validate_line(text: str, number: int, expected_type: str) -> tuple[str, str | None]:
    if NUL_ESCAPE_RE.search(text):
        raise RawLoadError(f"line {number}: contains \\u0000, which Postgres jsonb cannot store")
    try:
        obj = json.loads(text)
    except json.JSONDecodeError as exc:
        raise RawLoadError(f"line {number}: invalid JSON: {exc}") from exc
    if not isinstance(obj, dict):
        raise RawLoadError(f"line {number}: not a JSON object")
    if obj.get("resourceType") != expected_type:
        raise RawLoadError(f"line {number}: resourceType {obj.get('resourceType')!r}, expected {expected_type!r}")
    resource_id = obj.get("id")
    if not isinstance(resource_id, str) or not resource_id:
        raise RawLoadError(f"line {number}: missing id")
    meta = obj.get("meta")
    last_updated = meta.get("lastUpdated") if isinstance(meta, dict) else None
    return resource_id, last_updated if isinstance(last_updated, str) else None
