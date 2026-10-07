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


def ref(v: Any) -> str | None:
    if not isinstance(v, str):
        return None
    return v.rsplit("/", 1)[-1] or None
