"""Engine-neutral pieces of the import input: the .ndjson file descriptor and line reading."""
from __future__ import annotations

from dataclasses import dataclass
from typing import BinaryIO, Iterator


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
