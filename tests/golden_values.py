"""Golden table values for the fixture release, produced once by the Phase 1 T-SQL path (tests/make_golden.py)."""
from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path

GOLDEN = Path(__file__).resolve().parent / "golden" / "fixture_tables.json"


def normalize(v):
    if v is None:
        return None
    if isinstance(v, bool):
        return "1" if v else "0"
    if isinstance(v, datetime):
        return v.isoformat(sep=" ", timespec="milliseconds")
    if isinstance(v, date):
        return v.isoformat()
    if isinstance(v, float):
        return repr(v)
    return str(v)


def load_golden() -> dict[str, list[list[str | None]]]:
    return json.loads(GOLDEN.read_text(encoding="utf-8"))
