"""Engine-neutral SQL text helpers: token rendering, GO batches, standalone table names, packaged scripts."""
from __future__ import annotations

import re
from datetime import date
from importlib import resources
from typing import Mapping

TOKEN_RE = re.compile(r"<<([a-z_]+(?::[a-z_]+)?)>>")
GO_RE = re.compile(r"^[ \t]*GO[ \t]*\r?$", re.IGNORECASE | re.MULTILINE)


def render(text: str, tokens: Mapping[str, str]) -> str:
    def substitute(match: re.Match) -> str:
        key = match.group(1)
        if key not in tokens:
            raise KeyError(f"unknown SQL token <<{key}>>")
        return tokens[key]
    return TOKEN_RE.sub(substitute, text)


def split_batches(text: str) -> list[str]:
    return [b.strip() for b in GO_RE.split(text) if b.strip()]


def standalone_name(table: str, release: date, run_id: int, max_len: int) -> str:
    name = f"{table}__{release:%Y%m%d}__r{run_id}"
    if len(name) > max_len:
        raise ValueError(f"table name {name!r} is longer than {max_len} characters")
    return name


def sql_scripts(flavor: str, kind: str) -> list[tuple[str, str]]:
    folder = resources.files("npd_loader") / "sql" / flavor / kind
    return [(p.name, p.read_text(encoding="utf-8")) for p in sorted(folder.iterdir(), key=lambda p: p.name)
            if p.name.endswith(".sql")]
