"""Builders for configs and stage contexts used across tests."""
from __future__ import annotations

from pathlib import Path

import httpx
from psycopg.conninfo import conninfo_to_dict

from fakes import FakeCatalog
from npd_loader.config import parse_config
from npd_loader.stages import Context, no_lock
from npd_loader.storage import LocalStorage


def _db(info: str | None, extra: dict) -> dict:
    if info is None:
        base = {"host": "localhost", "port": 5432, "dbname": "unused", "user": "u", "password": "p"}
    else:
        d = conninfo_to_dict(info)
        base = {"host": d["host"], "port": int(d["port"]), "dbname": d["dbname"], "user": d["user"],
                "password": d["password"]}
    return {**base, **extra}


def config_data(storage_root: Path, manifest_url: str, npd_conninfo: str | None = None,
                catalog_conninfo: str | None = None, keep_releases: int = 5) -> dict:
    return {
        "source": {"manifest_url": manifest_url},
        "storage": {"backend": "local", "root": str(storage_root)},
        "credentials": {"backend": "config"},
        "npd_db": _db(npd_conninfo, {"raw_schema": "npd_raw", "schema": "npd"}),
        "catalog": _db(catalog_conninfo, {"backend": "css_catalog_pg", "project": "NPD",
                                          "run_type": "National Provider Directory", "file_set": "NPD_FHIR"}),
        "download": {"max_attempts": 3, "backoff_seconds": 0, "timeout_seconds": 10},
        "retention": {"keep_releases": keep_releases},
    }


def make_ctx(tmp_path: Path, cms, catalog=None, npd_conninfo: str | None = None, **config_kw) -> Context:
    config = parse_config(config_data(tmp_path / "data", cms.manifest_url, npd_conninfo=npd_conninfo, **config_kw))
    return Context(config=config, catalog=catalog if catalog is not None else FakeCatalog(),
                   storage=LocalStorage(tmp_path / "data"), http=httpx.Client(), lock=no_lock,
                   npd_conninfo=npd_conninfo, sleep=lambda seconds: None)


def to_toml(data: dict) -> str:
    """Minimal TOML writer for one level of tables holding str/int/float/bool values."""
    def value(v):
        if isinstance(v, bool):
            return "true" if v else "false"
        if isinstance(v, (int, float)):
            return str(v)
        return '"' + str(v).replace("\\", "\\\\").replace('"', '\\"') + '"'
    lines = []
    for section, items in data.items():
        lines.append(f"[{section}]")
        lines.extend(f"{k} = {value(v)}" for k, v in items.items())
        lines.append("")
    return "\n".join(lines)
