"""Stream one .ndjson: parse each line once (orjson), validate, hash, flatten with its type's specs, and write
chunked stage files (bcp -c, field 0x1F, row 0x1E) per table plus one resource_hash row per resource."""
from __future__ import annotations

import hashlib
import multiprocessing as mp
import os
from dataclasses import dataclass, field
from datetime import date

import orjson

from npd_loader.flatten.convert import ConvertError, instant
from npd_loader.flatten.engine import flatten_resource
from npd_loader.flatten.specs import SPEC_VERSION, tables_for
from npd_loader.raw_load import NdjsonInput, RawLoadError, iter_lines

FIELD, ROW = "\x1f", "\x1e"
HASH_TABLE = "resource_hash"
HASH_COLUMNS = ("resource_type", "resource_id", "hash", "last_updated", "release_date", "ndjson_file_id", "line_number")


class FlattenError(Exception):
    def __init__(self, message: str, file_id: int | None = None):
        super().__init__(message)
        self.file_id = file_id


@dataclass
class StagedFile:
    table: str
    path: str
    rows: int


@dataclass
class FlattenResult:
    files: list[StagedFile] = field(default_factory=list)
    rows: dict[str, int] = field(default_factory=dict)
    resources: dict[str, int] = field(default_factory=dict)

    def merge(self, other: "FlattenResult") -> None:
        self.files += other.files
        for k, v in other.rows.items():
            self.rows[k] = self.rows.get(k, 0) + v
        for k, v in other.resources.items():
            self.resources[k] = self.resources.get(k, 0) + v


def encode(v) -> str:
    if v is None:
        return ""
    if isinstance(v, bool):                 # before int: bool is an int subclass
        return "true" if v else "false"
    if isinstance(v, (dict, list)):         # JSON_VALUE returns NULL for non-scalars
        return ""
    s = v if isinstance(v, str) else str(v)
    if FIELD in s or ROW in s:
        raise ConvertError("value contains a 0x1F/0x1E control character")
    return s


class _Writer:
    """One table's stage files: rotates to a new file every chunk_rows rows."""

    def __init__(self, table: str, out_dir: str, prefix: str, chunk_rows: int):
        self.table, self.out_dir, self.prefix, self.chunk_rows = table, out_dir, prefix, chunk_rows
        self.files: list[StagedFile] = []
        self._fh = None
        self._rows = 0

    def write(self, values: tuple) -> None:
        if self._fh is None:
            path = os.path.join(self.out_dir, f"{self.table}.{self.prefix}.{len(self.files):04d}.dat")
            self._fh = open(path, "w", encoding="utf-8", newline="", buffering=1 << 20)
            self.files.append(StagedFile(self.table, path, 0))
            self._rows = 0
        self._fh.write(FIELD.join(encode(v) for v in values))
        self._fh.write(ROW)
        self._rows += 1
        self.files[-1].rows = self._rows
        if self._rows >= self.chunk_rows:
            self.close()

    def close(self) -> None:
        if self._fh is not None:
            self._fh.close()
            self._fh = None


def flatten_file(inp: NdjsonInput, src_path: str, release: str, out_dir: str,
                 chunk_rows: int = 500_000) -> FlattenResult:
    os.makedirs(out_dir, exist_ok=True)
    tables = tables_for(inp.resource_type)
    prefix = f"f{inp.file_id}"
    writers = {t.name: _Writer(t.name, out_dir, prefix, chunk_rows) for t in tables}
    writers[HASH_TABLE] = _Writer(HASH_TABLE, out_dir, prefix, chunk_rows)
    resources = 0
    number = 0
    try:
        with open(src_path, "rb") as f:
            for number, text in iter_lines(f):
                try:
                    res = orjson.loads(text)
                except orjson.JSONDecodeError as exc:
                    raise FlattenError(f"{inp.name}: line {number}: invalid JSON: {exc}", inp.file_id) from exc
                if not isinstance(res, dict):
                    raise FlattenError(f"{inp.name}: line {number}: not a JSON object", inp.file_id)
                if res.get("resourceType") != inp.resource_type:
                    raise FlattenError(f"{inp.name}: line {number}: resourceType {res.get('resourceType')!r}, "
                                       f"expected {inp.resource_type!r}", inp.file_id)
                rid = res.get("id")
                if not isinstance(rid, str) or not rid:
                    raise FlattenError(f"{inp.name}: line {number}: missing id", inp.file_id)
                try:
                    for table, values in flatten_resource(res, tables, (release, rid, inp.file_id, inp.zst_file_id)):
                        writers[table].write(values)
                    writers[HASH_TABLE].write((inp.resource_type, rid, hashlib.sha1(f"{SPEC_VERSION}\n".encode()
                                                                  + text.encode("utf-8")).hexdigest(),
                                               instant((res.get("meta") or {}).get("lastUpdated")), release,
                                               inp.file_id, number))
                except ConvertError as exc:
                    raise FlattenError(f"{inp.name}: line {number}: {exc}", inp.file_id) from exc
                resources += 1
    except RawLoadError as exc:                        # iter_lines: blank line in the middle, not UTF-8
        raise FlattenError(f"{inp.name}: {exc}", inp.file_id) from exc
    except OSError as exc:
        raise FlattenError(f"{inp.name}: cannot read {src_path}: {exc}", inp.file_id) from exc
    finally:
        for w in writers.values():
            w.close()
    result = FlattenResult(resources={inp.resource_type: resources})
    for name, w in writers.items():
        result.files += [f for f in w.files if f.rows]
        result.rows[name] = sum(f.rows for f in w.files)
    return result


def _job(args):
    return flatten_file(*args)


def flatten_files(inputs: list[NdjsonInput], storage, release: date, out_dir: str, workers: int,
                  chunk_rows: int = 500_000) -> FlattenResult:
    jobs = [(inp, storage.local_path(inp.rel_path), release.isoformat(), out_dir, chunk_rows) for inp in inputs]
    total = FlattenResult()
    if not jobs:
        return total
    if workers <= 1 or len(jobs) == 1:
        for job in jobs:
            total.merge(_job(job))
        return total
    with mp.get_context("spawn").Pool(min(workers, len(jobs))) as pool:
        for result in pool.imap_unordered(_job, jobs):
            total.merge(result)
    return total
