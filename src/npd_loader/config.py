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
class CredentialsConfig:
    backend: str


@dataclass(frozen=True)
class NpdDbConfig:
    host: str
    port: int
    dbname: str
    user: str | None
    password: str | None
    raw_schema: str
    schema: str


@dataclass(frozen=True)
class CatalogConfig:
    backend: str
    host: str
    port: int
    dbname: str
    user: str | None
    password: str | None
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
    credentials: CredentialsConfig
    npd_db: NpdDbConfig
    catalog: CatalogConfig
    download: DownloadConfig
    retention: RetentionConfig


STORAGE_BACKENDS = {"local"}
CREDENTIAL_BACKENDS = {"config"}
CATALOG_BACKENDS = {"css_catalog_pg"}


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


def parse_config(data: dict[str, Any]) -> Config:
    src = _section(data, "source")
    sto = _section(data, "storage")
    cred = _section(data, "credentials")
    npd = _section(data, "npd_db")
    cat = _section(data, "catalog")
    dl = _section(data, "download", required=False)
    ret = _section(data, "retention", required=False)

    catalog_known = {"backend", "host", "port", "dbname", "user", "password", "project", "run_type", "file_set"}
    catalog_extra = _optional({k: v for k, v in cat.items() if k not in catalog_known}, "catalog", CatalogConfig)

    config = Config(
        source=SourceConfig(manifest_url=_req(src, "source", "manifest_url")),
        storage=StorageConfig(backend=_backend(sto, "storage", STORAGE_BACKENDS), root=_req(sto, "storage", "root")),
        credentials=CredentialsConfig(backend=_backend(cred, "credentials", CREDENTIAL_BACKENDS)),
        npd_db=NpdDbConfig(
            host=_req(npd, "npd_db", "host"),
            port=_req(npd, "npd_db", "port", int),
            dbname=_req(npd, "npd_db", "dbname"),
            user=npd.get("user"),
            password=npd.get("password"),
            raw_schema=npd.get("raw_schema", "npd_raw"),
            schema=npd.get("schema", "npd"),
        ),
        catalog=CatalogConfig(
            backend=_backend(cat, "catalog", CATALOG_BACKENDS),
            host=_req(cat, "catalog", "host"),
            port=_req(cat, "catalog", "port", int),
            dbname=_req(cat, "catalog", "dbname"),
            user=cat.get("user"),
            password=cat.get("password"),
            project=_req(cat, "catalog", "project"),
            run_type=_req(cat, "catalog", "run_type"),
            file_set=_req(cat, "catalog", "file_set"),
            **catalog_extra,
        ),
        download=DownloadConfig(**_optional(dl, "download", DownloadConfig)),
        retention=RetentionConfig(**_optional(ret, "retention", RetentionConfig)),
    )
    if config.retention.keep_releases < 1:
        raise ConfigError("[retention] keep_releases must be at least 1")
    if config.download.max_attempts < 1:
        raise ConfigError("[download] max_attempts must be at least 1")
    return config
