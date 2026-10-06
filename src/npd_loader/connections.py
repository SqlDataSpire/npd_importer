"""DataEngine connection objects, built from the env file named in [databases] env_file."""
from __future__ import annotations

import contextlib
import io
import json
from pathlib import Path

from dotenv import dotenv_values

from npd_loader.config import Config, ConfigError

TYPES = {"mssql", "postgres"}


def _dataengine():
    """Import DataEngine without its import-time stdout output (it prints, and auto-loads ./database.env)."""
    with contextlib.redirect_stdout(io.StringIO()):
        import DataEngine
    return DataEngine


def read_env_file(path: str) -> dict[str, dict]:
    if not Path(path).is_file():
        raise ConfigError(f"database env file not found: {path}")
    raw = dotenv_values(path).get("databases")
    if not raw:
        raise ConfigError(f"{path} has no 'databases' entry")
    try:
        docs = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ConfigError(f"{path}: 'databases' is not valid JSON: {exc}") from exc
    if not isinstance(docs, dict):
        raise ConfigError(f"{path}: 'databases' must be a JSON object")
    return docs


def build_connection(name: str, doc: dict) -> object:
    kind = doc.get("type")
    if kind not in TYPES:
        raise ConfigError(f"connection {name!r} has type {kind!r}; expected one of {sorted(TYPES)}")
    for key in ("server", "database"):
        if not doc.get(key):
            raise ConfigError(f"connection {name!r} is missing {key!r}")
    de = _dataengine()
    common = {"name": name, "server": doc["server"], "database": doc["database"],
              "UN": doc.get("UN", ""), "PW": doc.get("PW", "")}
    if kind == "mssql":
        return de.SqlConnectionObject(**common, trusted=doc.get("trusted", "no"))
    return de.PgConnectionObject(**common)


def open_connection(config: Config, target: str) -> object:
    name = getattr(config, target).connection
    docs = read_env_file(config.databases.env_file)
    if name not in docs:
        raise ConfigError(f"[{target}] connection {name!r} is not in {config.databases.env_file}")
    return build_connection(name, docs[name])


def pg_conninfo(obj: object) -> str:
    """psycopg URI for a PgConnectionObject (used by the Postgres dialect until it moves to SQLAlchemy)."""
    return obj.engine.url.set(drivername="postgresql").render_as_string(hide_password=False)
