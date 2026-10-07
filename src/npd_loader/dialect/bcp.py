"""bcp.exe bulk loads of stage files (Windows auth). Success is judged by the output and row counts, never the exit
code alone: bcp exits 0 even when every row fails."""
from __future__ import annotations

import re
import subprocess
import urllib.parse

from sqlalchemy.engine import Engine

_COPIED = re.compile(r"(\d+) rows copied")


class BcpError(Exception):
    pass


def bcp_target(engine: Engine) -> tuple[str, str]:
    odbc = urllib.parse.unquote_plus(engine.url.query.get("odbc_connect", ""))
    parts = {k.strip().upper(): v.strip() for k, _, v in (p.partition("=") for p in odbc.split(";")) if k}
    return parts["SERVER"], parts["DATABASE"]


def bcp_in(server: str, database: str, schema: str, table: str, path: str, expected_rows: int) -> int:
    cmd = ["bcp", f"[{database}].[{schema}].[{table}]", "in", path, "-S", server, "-T", "-c", "-C", "65001",
           "-t", "0x1f", "-r", "0x1e", "-k", "-m", "1", "-b", "500000", "-h", "TABLOCK"]
    proc = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace")
    out = proc.stdout + proc.stderr
    m = _COPIED.search(out)
    copied = int(m.group(1)) if m else 0
    if proc.returncode != 0 or "Error" in out or copied != expected_rows:
        raise BcpError(f"bcp into {schema}.{table} from {path}: copied {copied} of {expected_rows} rows: {out[-600:].strip()}")
    return copied
