"""Value converters for flattened columns; text forms are what bcp -c loads into the SQL Server column types."""
from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from typing import Any

_PARTIAL = re.compile(r"^\d{4}(-\d{2}(-\d{2})?)?$")


class ConvertError(ValueError):
    pass


def ts(v: Any) -> str | None:
    """FHIR date/dateTime/instant -> 'YYYY-MM-DD HH:MM:SS.fff' in UTC (datetime2(3)); partial dates -> midnight."""
    if v is None or v == "":
        return None
    if not isinstance(v, str):
        raise ConvertError(f"not a date/time: {v!r}")
    if _PARTIAL.match(v):
        parts = (v + "-01-01")[:10] if len(v) == 4 else (v + "-01")[:10] if len(v) == 7 else v
        return f"{parts} 00:00:00.000"
    try:
        d = datetime.fromisoformat(v)
    except ValueError as exc:
        raise ConvertError(f"not a date/time: {v!r}") from exc
    if d.tzinfo is not None:
        d = d.astimezone(timezone.utc).replace(tzinfo=None)
    ms, rem = divmod(d.microsecond, 1000)
    d = d.replace(microsecond=0) + timedelta(milliseconds=ms + (1 if rem >= 500 else 0))
    return d.isoformat(sep=" ", timespec="milliseconds")


def instant(v):
    """meta.lastUpdated: Phase 1 loaded it into the raw datetime2(3) column from a Python datetime (the driver
    truncates microseconds), unlike fhir_ts which rounds; the golden values keep that truncation."""
    return ts(re.sub(r"(\.\d{3})\d+", lambda m: m.group(1), v)) if isinstance(v, str) else ts(v)


def boolean(v: Any) -> str | None:
    if v is None:
        return None
    if isinstance(v, bool):
        return "1" if v else "0"
    raise ConvertError(f"not a boolean: {v!r}")


def number(v: Any) -> str | None:
    if v is None:
        return None
    if isinstance(v, bool) or not isinstance(v, (int, float)):
        raise ConvertError(f"not a number: {v!r}")
    return repr(float(v))


def join(v: Any, sep: str = " ", skip: int = 0) -> str | None:
    if not isinstance(v, list):
        return None
    text = sep.join(str(x) for x in v[skip:] if x is not None)
    return text or None


def strip_id(rtype: str, rid: str) -> str:
    """'Practitioner-1003000100' -> '1003000100'. Ids without their own type's prefix (or with nothing after it) are
    kept whole."""
    prefix = rtype + "-"
    return rid[len(prefix):] if rid.startswith(prefix) and len(rid) > len(prefix) else rid


def ref_to(target: str):
    """Converter for a reference to a `target` resource: 'Organization/Organization-1' -> '1'. A reference naming
    another resource type is an error: the column holds keys of `target` only."""
    def conv(v: Any) -> str | None:
        if not isinstance(v, str) or v.startswith("#"):        # contained reference: no target resource
            return None
        v = re.sub(r"/_history/[^/]*$", "", v)                 # versioned reference: drop the version
        rtype, _, rid = v.rpartition("/")
        if rtype and rtype.rsplit("/", 1)[-1] != target:
            raise ConvertError(f"{v} is not a {target} reference")
        return strip_id(target, rid) if rid else None
    return conv
