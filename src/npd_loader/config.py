"""Load and validate the TOML config. Every name, path, and catalog label lives here."""
from __future__ import annotations

import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any


class ConfigError(Exception):
    pass


@dataclass(frozen=True)
class SourceConfig:
    manifest_url: str


@dataclass(frozen=True)
class StorageConfig:
    backend: str
    root: str


@dataclass(frozen=True)
class DatabasesConfig:
    env_file: str


@dataclass(frozen=True)
class NpdDbConfig:
    connection: str
    schema: str = "npd"
    stage_schema: str = "npd_stage"
    lock_timeout_seconds: float = 30.0   # how long the delta apply waits for a table lock (3 attempts)
    flatten_workers: int = 4             # parallel .ndjson files being flattened
    bcp_workers: int = 8                 # parallel bcp loads


@dataclass(frozen=True)
class CatalogConfig:
    connection: str
    project: str
    run_type: str
    file_set: str
    run_table: str = "public.master_warehouse_run"
    file_table: str = "public.data_file"
    run_class_download: str = "DOWNLOAD"
    run_class_extract: str = "EXTRACT"
    run_class_import: str = "IMPORT"
    description_template: str = "NPD FHIR {stage} {release}"
    source_version_name: str = "NPD release"
    status_success: str = "Success"
    status_failed: str = "Failed"
    file_type_manifest: str = "manifest"
    file_type_zst: str = "ndjson.zst"
    file_type_ndjson: str = "ndjson"


@dataclass(frozen=True)
class DownloadConfig:
    max_attempts: int = 8
    backoff_seconds: float = 5.0
    timeout_seconds: float = 60.0
    chunk_bytes: int = 1 << 20


@dataclass(frozen=True)
class RetentionConfig:
    keep_releases: int = 5


@dataclass(frozen=True)
class Config:
    source: SourceConfig
    storage: StorageConfig
    databases: DatabasesConfig
    npd_db: NpdDbConfig
    catalog: CatalogConfig
    download: DownloadConfig
    retention: RetentionConfig


STORAGE_BACKENDS = {"local"}
LEGACY_KEYS = {"host", "port", "dbname", "user", "password", "backend"}


def load_config(path: str | Path) -> Config:
    try:
        with open(path, "rb") as f:
            data = tomllib.load(f)
    except FileNotFoundError as exc:
        raise ConfigError(f"config file not found: {path}") from exc
    except tomllib.TOMLDecodeError as exc:
        raise ConfigError(f"invalid TOML in {path}: {exc}") from exc
    return parse_config(data)


def _section(data: dict[str, Any], name: str, required: bool = True) -> dict[str, Any]:
    section = data.get(name)
    if section is None:
        if required:
            raise ConfigError(f"missing config section [{name}]")
        return {}
    if not isinstance(section, dict):
        raise ConfigError(f"[{name}] must be a table")
    return section


def _req(section: dict[str, Any], name: str, key: str, kind: type = str) -> Any:
    if key not in section:
        raise ConfigError(f"missing config key [{name}] {key}")
    value = section[key]
    if kind is float and isinstance(value, int):
        value = float(value)
    if not isinstance(value, kind) or (kind is int and isinstance(value, bool)):
        raise ConfigError(f"[{name}] {key} must be {kind.__name__}")
    return value


def _backend(section: dict[str, Any], name: str, allowed: set[str]) -> str:
    backend = _req(section, name, "backend")
    if backend not in allowed:
        raise ConfigError(f"[{name}] backend {backend!r} is not available (choose from {sorted(allowed)})")
    return backend


def _optional(section: dict[str, Any], name: str, cls: type) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for field_name, field in cls.__dataclass_fields__.items():
        if field_name in section:
            kind = {"int": int, "float": float, "str": str}.get(str(field.type), str)
            out[field_name] = _req(section, name, field_name, kind)
    return out


def _no_legacy(section: dict[str, Any], name: str) -> None:
    found = sorted(LEGACY_KEYS & set(section))
    if found:
        raise ConfigError(f"[{name}] {', '.join(found)}: connection settings moved to the database.env file "
                          f"named by [databases] env_file; set [{name}] connection instead")


def parse_config(data: dict[str, Any]) -> Config:
    src = _section(data, "source")
    sto = _section(data, "storage")
    dbs = _section(data, "databases")
    npd = _section(data, "npd_db")
    cat = _section(data, "catalog")
    dl = _section(data, "download", required=False)
    ret = _section(data, "retention", required=False)
    _no_legacy(npd, "npd_db")
    if "raw_schema" in npd:
        raise ConfigError("[npd_db] raw_schema was replaced by stage_schema (Phase 2 has no raw table)")
    _no_legacy(cat, "catalog")

    catalog_known = {"connection", "project", "run_type", "file_set"}
    catalog_extra = _optional({k: v for k, v in cat.items() if k not in catalog_known}, "catalog", CatalogConfig)

    config = Config(
        source=SourceConfig(manifest_url=_req(src, "source", "manifest_url")),
        storage=StorageConfig(backend=_backend(sto, "storage", STORAGE_BACKENDS), root=_req(sto, "storage", "root")),
        databases=DatabasesConfig(env_file=_req(dbs, "databases", "env_file")),
        npd_db=NpdDbConfig(connection=_req(npd, "npd_db", "connection"),
                           **_optional({k: v for k, v in npd.items() if k != "connection"}, "npd_db", NpdDbConfig)),
        catalog=CatalogConfig(
            connection=_req(cat, "catalog", "connection"),
            project=_req(cat, "catalog", "project"),
            run_type=_req(cat, "catalog", "run_type"),
            file_set=_req(cat, "catalog", "file_set"),
            **catalog_extra,
        ),
        download=DownloadConfig(**_optional(dl, "download", DownloadConfig)),
        retention=RetentionConfig(**_optional(ret, "retention", RetentionConfig)),
    )
    if config.npd_db.schema == config.npd_db.stage_schema:
        raise ConfigError("[npd_db] stage_schema and schema must differ")
    if config.npd_db.flatten_workers < 1:
        raise ConfigError("[npd_db] flatten_workers must be at least 1")
    if config.npd_db.bcp_workers < 1:
        raise ConfigError("[npd_db] bcp_workers must be at least 1")
    if config.npd_db.lock_timeout_seconds <= 0:
        raise ConfigError("[npd_db] lock_timeout_seconds must be greater than 0")
    if config.retention.keep_releases < 1:
        raise ConfigError("[retention] keep_releases must be at least 1")
    if config.download.max_attempts < 1:
        raise ConfigError("[download] max_attempts must be at least 1")
    return config
