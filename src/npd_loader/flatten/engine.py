"""Declarative table specs and the generic flattener: one parsed FHIR resource -> rows for every table of its type."""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Callable, Iterator

from npd_loader.flatten.convert import ref_to

Getter = Callable[[Any], Any]
_STEP = re.compile(r"([^.\[\]]+)|\[(\d+)\]")
LINEAGE = ("release_date", "resource_id", "ndjson_file_id", "zst_file_id")
KEY_LINEAGE = ("release_date", "resource_key", "ndjson_file_id", "zst_file_id")


def path(expr: str) -> Getter:
    steps = [(name, int(idx)) if idx else (name, None) for name, idx in _STEP.findall(expr)]

    def get(obj: Any) -> Any:
        for name, idx in steps:
            if name:
                obj = obj.get(name) if isinstance(obj, dict) else None
            else:
                obj = obj[idx] if isinstance(obj, list) and len(obj) > idx else None
            if obj is None:
                return None
        return obj
    return get


@dataclass(frozen=True)
class Col:
    source: str                      # "r" (resource) or "e" (repeating element)
    get: Getter
    conv: Callable[[Any], Any] | None = None
    target: str | None = None        # reference column: the resource type it points to

    def value(self, res: Any, el: Any) -> Any:
        v = self.get(res if self.source == "r" else el)
        return self.conv(v) if self.conv else v


def _getter(get: str | Getter) -> Getter:
    return path(get) if isinstance(get, str) else get


def R(get: str | Getter, conv: Callable[[Any], Any] | None = None, *, target: str | None = None) -> Col:
    return Col("r", _getter(get), ref_to(target) if target else conv, target)


def E(get: str | Getter, conv: Callable[[Any], Any] | None = None, *, target: str | None = None) -> Col:
    return Col("e", _getter(get), ref_to(target) if target else conv, target)


def ext(url: str, inner: Getter | None = None) -> Getter:
    """First extension element with `url` on the object (or on `inner(object)` for nested extensions)."""
    def get(obj: Any) -> Any:
        if inner is not None:
            obj = inner(obj)
        for e in (obj.get("extension") or ()) if isinstance(obj, dict) else ():
            if isinstance(e, dict) and e.get("url") == url:
                return e
        return None
    return get


def identifier(systems: tuple[str, ...]) -> Getter:
    def get(res: Any) -> Any:
        for i in (res.get("identifier") or ()) if isinstance(res, dict) else ():
            if isinstance(i, dict) and i.get("system") in systems:
                return i.get("value")
        return None
    return get


def official_name(res: Any) -> dict | None:
    names = [n for n in (res.get("name") or ()) if isinstance(n, dict)] if isinstance(res, dict) else []
    if not names:
        return None
    rank = lambda p: (0 if p[1].get("use") == "official" else 1 if p[1].get("use") is not None else 2, p[0])
    return min(enumerate(names), key=rank)[1]


@dataclass(frozen=True)
class Table:
    name: str
    cols: dict[str, Col] = field(hash=False)
    each: str | None = None
    with_type: bool = False

    def __post_init__(self):
        for name, col in self.cols.items():
            if col.target and not name.endswith("_id"):
                raise ValueError(f"{self.name}.{name}: a reference column's name must end in _id")


def columns(t: Table) -> list[str]:
    return list(LINEAGE) + (["resource_type"] if t.with_type else []) + (["seq"] if t.each else []) + list(t.cols)


def ref_columns(t: Table) -> dict[str, str]:
    """Staging reference columns (text ids) -> the resource type they point to."""
    return {name: c.target for name, c in t.cols.items() if c.target}


def key_columns(t: Table) -> list[str]:
    """Permanent table columns: resource_key for resource_id, <name>_key for each reference column <name>_id, no
    resource_type (the key implies it)."""
    return (list(KEY_LINEAGE) + (["seq"] if t.each else [])
            + [name[:-3] + "_key" if c.target else name for name, c in t.cols.items()])


def flatten_resource(res: dict, tables: list[Table], lineage: tuple) -> Iterator[tuple[str, tuple]]:
    rtype = res.get("resourceType")
    for t in tables:
        head = lineage + ((rtype,) if t.with_type else ())
        if t.each is None:
            yield t.name, head + tuple(c.value(res, None) for c in t.cols.values())
        else:
            items = res.get(t.each)
            for i, el in enumerate(items if isinstance(items, list) else (), start=1):
                yield t.name, head + (i,) + tuple(c.value(res, el) for c in t.cols.values())
