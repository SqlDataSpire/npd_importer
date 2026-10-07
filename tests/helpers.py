"""Builders for configs and stage contexts used across tests."""
from __future__ import annotations

import json
from pathlib import Path

import httpx
from fakes import FakeCatalog
from npd_loader.config import parse_config
from npd_loader.stages import Context, no_lock
from npd_loader.storage import LocalStorage


def write_env_file(path, docs: dict) -> str:
    path.write_text(f"databases = '{json.dumps(docs)}'\n", encoding="utf-8")
    return str(path)


def config_data(storage_root: Path, manifest_url: str, env_file: str | None = None,
                schemas: tuple[str, str] = ("npd_stage", "npd"), catalog_tables: tuple[str, str] | None = None,
                keep_releases: int = 5) -> dict:
    catalog = {"connection": "catalog", "project": "NPD", "run_type": "National Provider Directory",
               "file_set": "NPD_FHIR"}
    if catalog_tables:
        catalog["run_table"], catalog["file_table"] = catalog_tables
    return {
        "source": {"manifest_url": manifest_url},
        "storage": {"backend": "local", "root": str(storage_root)},
        "databases": {"env_file": env_file or "unused.env"},
        "npd_db": {"connection": "data", "stage_schema": schemas[0], "schema": schemas[1]},
        "catalog": catalog,
        "download": {"max_attempts": 3, "backoff_seconds": 0, "timeout_seconds": 10},
        "retention": {"keep_releases": keep_releases},
    }


def make_ctx(tmp_path: Path, cms, catalog=None, dialect=None, **config_kw) -> Context:
    config = parse_config(config_data(tmp_path / "data", cms.manifest_url, **config_kw))
    return Context(config=config, catalog=catalog if catalog is not None else FakeCatalog(),
                   storage=LocalStorage(tmp_path / "data"), http=httpx.Client(), lock=no_lock,
                   dialect=dialect, sleep=lambda seconds: None)


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
