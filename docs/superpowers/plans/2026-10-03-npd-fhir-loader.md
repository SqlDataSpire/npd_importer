# NPD FHIR Loader Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `npd_loader`, a Python CLI that downloads each CMS National Provider Directory FHIR bulk release, decompresses it, and loads it into Postgres. Every resource lands as raw JSONB and is also flattened into analyst tables, and every row can be traced back to its catalog `data_file` rows.

**Architecture:** There are three idempotent stages (DOWNLOAD, EXTRACT, IMPORT). Each stage is its own catalog run in `css_catalog_local`. Credentials, the catalog, and file storage sit behind swappable interfaces. IMPORT streams NDJSON into standalone tables with `COPY`, then runs numbered SQL transforms into standalone tables. It attaches every partition for the release in one transaction, so readers see a release completely or not at all. Retention keeps the newest 5 imported releases.

**Tech Stack:** Python 3.12, `psycopg` 3 (binary), `zstandard`, `httpx`, stdlib `tomllib`/`argparse`/`xml.etree`. Tests use `pytest`, `testcontainers` (Docker Postgres), and a stdlib `http.server` fake of `directory.cms.gov`.

**Spec:** `docs/superpowers/specs/2026-10-03-npd-fhir-loader-design.md`

## Global Constraints

- Python `>=3.12` (server runs 3.12.3). Runtime dependencies are only `psycopg[binary]>=3.1`, `zstandard>=0.22`, and `httpx>=0.27`. Test dependencies are `pytest>=8` and `testcontainers[postgres]>=4`.
- Postgres 13 or later. The test image defaults to `postgres:16` and can be overridden with env `NPD_TEST_PG_IMAGE` to match the server version (spec open item).
- Never hard-code database names, schema names, paths, or catalog labels. They come from config (`config.py` defaults where the spec gives a value).
- Catalog labels (spec §5): `project = "NPD"`, `run_type = "National Provider Directory"`, `run_class` ∈ `DOWNLOAD`/`EXTRACT`/`IMPORT`, descriptions `"NPD FHIR {Download|Extract|Import} {release}"`, `file_set = "NPD_FHIR"`, `source_version_name = "NPD release"`, `file_type` ∈ `manifest`/`ndjson.zst`/`ndjson`, statuses `Success`/`Failed`.
- `source_uri` is always the public URL (`https://directory.cms.gov/downloads/...`), never the signed S3 URL.
- Run folder: `run_{run id}_{start time as %Y-%m-%d-%H%M%S}`. File name: `file_{data_file.id}_{original name}`. In-progress files end in `.part` and are renamed only after verification.
- `data_file.exceptions` and `master_warehouse_run.result` are truncated to 8000 characters.
- The loader never deletes `.zst` files, manifest files, or catalog rows. Only retention deletes `.ndjson` files and partitions.
- Catalog writes use their own autocommit connection, separate from data loads.
- Exit code 0 means success, nothing to do, or another run holds the lock. Any failure exits non-zero. Logs go to stderr.
- Every `npd_raw`/`npd` row carries `release_date`, `resource_id`, `ndjson_file_id`, and `zst_file_id`. Stored references are bare ids (`"Organization/Organization-123"` becomes `"Organization-123"`).

## Review Focus

1. **Re-running extract after an EXTRACT run died mid-file.** That run left an `.ndjson` `data_file` row with no hash and a stray `.part`. The next extract must ignore the incomplete row and create a new one; it must not try to "restore" against a row that has no hash. Test: `test_incomplete_row_from_failed_run_is_ignored` in Task 8.
2. **Forced re-import of a release older than the newest 5.** Retention runs right after the import and must not drop the release that was just imported. Test: `test_just_imported_old_release_is_kept` in Task 16.
3. **Server ignores `Range` on resume and returns `200` with the full body.** The downloader must discard the bytes it already wrote, not append a second copy. Test: `test_resume_when_server_ignores_range` in Task 7.
4. **Manifest lists a resource type the loader has never seen** (e.g. `09-Medication.ndjson`). Download, extract, and the raw load must still succeed. Transforms ignore it, and `profile --unmapped` reports it. Test: `test_unknown_resource_type_loads_raw` in Task 11.
5. **Crash after the publish transaction committed but before the catalog run was marked `Success`.** Data is live but the catalog says nothing succeeded. The next plain `import` must fail with a message that names `--force`, and `import --force` must replace the release cleanly. Test: `test_published_without_catalog_success_requires_force` in Task 15.

## Decisions this plan makes beyond the spec

These are small additions the spec leaves open. Each is used consistently below.

- **`npd.release` table** (release_date PK, import_run_id, published_at) is upserted inside the publish transaction. Latest-release views filter on `max(release_date)` from it.
- **Standalone table names** are `{table}__{yyyymmdd}__r{import run id}`, and raw leaves are `resource__{yyyymmdd}__r{id}__{resourcetype}`. Retention and `--force` find partitions through `pg_inherits` plus the partition bound, not through names.
- **`Catalog.get_data_files` takes an optional `run_id`**, and `Catalog.successful_releases(run_class)` is added. `Run` carries `release_date`, parsed from `<release_date>` in `xml_config`.
- **`[download]` config section** (`max_attempts`, `backoff_seconds`, `timeout_seconds`, `chunk_bytes`) has defaults. The CLI adds a `profile` command.
- **`npd-loader run` = download → import.** Import runs the extract stage itself when an `.ndjson` is missing (spec §8.3 step 2). This avoids re-hashing 34 GB on every daily run just to learn that nothing changed. `npd-loader extract` is still available on its own.
- **The import stage checks `.ndjson` files by existence and size, not SHA-256.** Every line is validated during the load anyway.

## File Structure

```
pyproject.toml                       package metadata, deps, console script, pytest config
config.example.toml                  committed example config (spec §11 + [download])
README.md                            install, init-db, cron/systemd (Task 19)
docs/profile/                        full-release profile report (Task 19)
src/npd_loader/
  __init__.py
  config.py          load/validate TOML → frozen dataclasses
  credentials.py     get_db_credentials(), conninfo()
  storage.py         Storage protocol, LocalStorage
  catalog.py         Run, DataFile, Catalog protocol, shared helpers, CssCatalogPg
  runxml.py          <WAREHOUSE_RUN_CONFIG> / output XML builders, release parser
  manifest.py        Manifest parsing/fetching, resource_type_for(), file_url()
  stages.py          Context, Outcome, StageFailed, run-folder/file-name helpers, fail_run, no_lock
  download.py        DOWNLOAD stage
  extract.py         EXTRACT stage
  db.py              render_sql, standalone names, partition discovery, index cloning, advisory lock
  schema.py          init_db(): runs sql/init/*.sql, creates latest views
  raw_load.py        IMPORT raw part (COPY into standalone raw tables)
  transform.py       IMPORT table part (runs sql/transform/*.sql)
  publish.py         one-transaction attach / force replace
  import_stage.py    IMPORT stage orchestration
  retention.py       drop old releases' partitions and .ndjson files
  profile.py         JSON path profiler + mapped-path coverage
  cli.py             argparse entry point
  sql/init/001_schemas.sql 002_raw.sql 003_tables.sql
  sql/transform/010_practitioner.sql 020_organization.sql 030_location.sql 040_endpoint.sql
                050_practitioner_role.sql 060_organization_affiliation.sql
                070_healthcare_service.sql 080_insurance_plan.sql 090_identifier.sql
  sql/mapped_paths.txt  JSON paths consumed by transforms or deliberately left raw-only
tests/
  conftest.py        Postgres container, per-test databases, cms fixture
  helpers.py         config_data(), make_ctx(), to_toml()
  fakes.py           FakeCatalog
  fixture_data.py    sample FHIR records (real records from release 2026-09-29 + synthetic)
  release_builder.py build_release() → manifest + .zst bytes
  fake_cms.py        FakeCms HTTP server
  storage_contract.py, catalog_contract.py   shared suites
  sql/css_catalog_schema.sql   copy of the catalog tables' DDL
  test_*.py
```

---

### Task 1: Project scaffold, config, credentials

**Files:**
- Create: `pyproject.toml`, `config.example.toml`, `src/npd_loader/__init__.py`, `src/npd_loader/config.py`, `src/npd_loader/credentials.py`
- Test: `tests/test_config.py`

**Interfaces:**
- Produces: `load_config(path) -> Config`, `parse_config(data: dict) -> Config`, `ConfigError`. `Config` has attributes `source`, `storage`, `credentials`, `npd_db`, `catalog`, `download`, `retention` (field names exactly as in the code below). `get_db_credentials(config, target) -> DbCredentials` and `conninfo(config, target) -> str`, where `target` is `"npd_db"` or `"catalog"`.

- [ ] **Step 1: Create packaging files**

`pyproject.toml`:

```toml
[build-system]
requires = ["setuptools>=68"]
build-backend = "setuptools.build_meta"

[project]
name = "npd-loader"
version = "0.1.0"
description = "Load the CMS National Provider Directory FHIR bulk release into Postgres"
requires-python = ">=3.12"
dependencies = [
    "psycopg[binary]>=3.1",
    "zstandard>=0.22",
    "httpx>=0.27",
]

[project.optional-dependencies]
test = ["pytest>=8", "testcontainers[postgres]>=4"]

[project.scripts]
npd-loader = "npd_loader.cli:main"

[tool.setuptools.packages.find]
where = ["src"]

[tool.setuptools.package-data]
npd_loader = ["sql/init/*.sql", "sql/transform/*.sql", "sql/mapped_paths.txt"]

[tool.pytest.ini_options]
testpaths = ["tests"]
addopts = "-rs"
```

`src/npd_loader/__init__.py`:

```python
"""Load the CMS National Provider Directory FHIR bulk release into Postgres."""
```

`config.example.toml`:

```toml
# Copy to /etc/npd-loader/config.toml (mode 600). Never commit the real file.

[source]
manifest_url = "https://directory.cms.gov/downloads/manifest.json"

[storage]
backend = "local"
root = "/data/npd"            # npd_data_folder

[credentials]
backend = "config"

[npd_db]
host = "192.10.0.6"
port = 5432
dbname = "npd"
user = "..."
password = "..."
raw_schema = "npd_raw"
schema = "npd"

[catalog]
backend = "css_catalog_pg"
host = "192.10.0.6"
port = 5432
dbname = "css_catalog_local"
user = "..."
password = "..."
project = "NPD"
run_type = "National Provider Directory"
file_set = "NPD_FHIR"

[download]
max_attempts = 8
backoff_seconds = 5
timeout_seconds = 60

[retention]
keep_releases = 5
```

Then create the virtual environment and install:

```bash
python -m venv .venv
# Windows: .venv\Scripts\activate    Linux: source .venv/bin/activate
python -m pip install -e ".[test]"
```

- [ ] **Step 2: Write the failing tests**

`tests/test_config.py`:

```python
from pathlib import Path

import pytest

from npd_loader.config import ConfigError, load_config, parse_config
from npd_loader.credentials import DbCredentials, conninfo, get_db_credentials

EXAMPLE = Path(__file__).resolve().parents[1] / "config.example.toml"


def minimal() -> dict:
    db = {"host": "h", "port": 5432, "dbname": "d", "user": "u", "password": "p"}
    return {
        "source": {"manifest_url": "https://example.test/downloads/manifest.json"},
        "storage": {"backend": "local", "root": "/data/npd"},
        "credentials": {"backend": "config"},
        "npd_db": dict(db),
        "catalog": {**db, "backend": "css_catalog_pg", "project": "NPD",
                    "run_type": "National Provider Directory", "file_set": "NPD_FHIR"},
    }


def test_example_config_loads():
    cfg = load_config(EXAMPLE)
    assert cfg.source.manifest_url == "https://directory.cms.gov/downloads/manifest.json"
    assert cfg.storage.root == "/data/npd"
    assert cfg.npd_db.raw_schema == "npd_raw"
    assert cfg.npd_db.schema == "npd"
    assert cfg.catalog.file_set == "NPD_FHIR"
    assert cfg.retention.keep_releases == 5
    assert cfg.download.max_attempts == 8


def test_defaults_for_optional_sections_and_labels():
    cfg = parse_config(minimal())
    assert cfg.npd_db.raw_schema == "npd_raw"
    assert cfg.catalog.run_class_download == "DOWNLOAD"
    assert cfg.catalog.run_class_extract == "EXTRACT"
    assert cfg.catalog.run_class_import == "IMPORT"
    assert cfg.catalog.description_template == "NPD FHIR {stage} {release}"
    assert cfg.catalog.source_version_name == "NPD release"
    assert cfg.catalog.file_type_zst == "ndjson.zst"
    assert cfg.catalog.file_type_ndjson == "ndjson"
    assert cfg.catalog.file_type_manifest == "manifest"
    assert cfg.catalog.status_success == "Success"
    assert cfg.catalog.status_failed == "Failed"
    assert cfg.catalog.run_table == "public.master_warehouse_run"
    assert cfg.catalog.file_table == "public.data_file"
    assert cfg.download.backoff_seconds == 5.0
    assert cfg.download.chunk_bytes == 1 << 20
    assert cfg.retention.keep_releases == 5


def test_missing_section_is_named():
    data = minimal()
    del data["source"]
    with pytest.raises(ConfigError, match=r"\[source\]"):
        parse_config(data)


def test_missing_key_is_named():
    data = minimal()
    del data["catalog"]["file_set"]
    with pytest.raises(ConfigError, match=r"\[catalog\] file_set"):
        parse_config(data)


@pytest.mark.parametrize("section,value", [("storage", "s3"), ("credentials", "1password"),
                                           ("catalog", "azure_api")])
def test_unbuilt_backends_are_rejected(section, value):
    data = minimal()
    data[section]["backend"] = value
    with pytest.raises(ConfigError, match="backend"):
        parse_config(data)


def test_keep_releases_must_be_positive():
    data = minimal()
    data["retention"] = {"keep_releases": 0}
    with pytest.raises(ConfigError, match="keep_releases"):
        parse_config(data)


def test_credentials_from_config():
    cfg = parse_config(minimal())
    assert get_db_credentials(cfg, "npd_db") == DbCredentials("u", "p")
    info = conninfo(cfg, "catalog")
    assert "host=h" in info and "dbname=d" in info and "user=u" in info


def test_credentials_missing_password():
    data = minimal()
    del data["npd_db"]["password"]
    cfg = parse_config(data)
    with pytest.raises(ConfigError, match="password"):
        get_db_credentials(cfg, "npd_db")
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `python -m pytest tests/test_config.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'npd_loader.config'`

- [ ] **Step 4: Implement `config.py` and `credentials.py`**

`src/npd_loader/config.py`:

```python
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
```

Note: `field.type` is a string because of `from __future__ import annotations`, so `_optional` maps `"int"`/`"float"`/`"str"` to the real types.

`src/npd_loader/credentials.py`:

```python
"""Database credentials. Backend 'config' reads them from the config file; 1Password comes later."""
from __future__ import annotations

from dataclasses import dataclass

from psycopg.conninfo import make_conninfo

from npd_loader.config import Config, ConfigError


@dataclass(frozen=True)
class DbCredentials:
    user: str
    password: str


def get_db_credentials(config: Config, target: str) -> DbCredentials:
    """Return credentials for `target`, which is "npd_db" or "catalog"."""
    section = getattr(config, target)
    if config.credentials.backend == "config":
        if not section.user or section.password is None:
            raise ConfigError(f"[{target}] user and password are required with credentials backend 'config'")
        return DbCredentials(section.user, section.password)
    raise ConfigError(f"credentials backend {config.credentials.backend!r} is not available")


def conninfo(config: Config, target: str) -> str:
    section = getattr(config, target)
    creds = get_db_credentials(config, target)
    return make_conninfo(host=section.host, port=section.port, dbname=section.dbname,
                         user=creds.user, password=creds.password)
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `python -m pytest tests/test_config.py -v`
Expected: 10 passed

- [ ] **Step 6: Commit**

```bash
git add pyproject.toml config.example.toml src/npd_loader/__init__.py src/npd_loader/config.py src/npd_loader/credentials.py tests/test_config.py
git commit -m "feat: project scaffold, config loading, config-file credentials"
```

---

### Task 2: Storage interface and LocalStorage

**Files:**
- Create: `src/npd_loader/storage.py`, `tests/storage_contract.py`
- Test: `tests/test_storage_local.py`

**Interfaces:**
- Produces: `Storage` (Protocol) with `open_write(rel) -> BinaryIO` (truncates, creates parent folders), `open_read(rel) -> BinaryIO`, `exists(rel) -> bool`, `size(rel) -> int`, `delete(rel) -> None` (no error if missing), `rename(src, dst) -> None` (replaces dst), `list(prefix) -> list[str]` (sorted relative POSIX paths), `uri(rel="") -> str`. Also `LocalStorage(root)`. Relative paths always use `/`.

- [ ] **Step 1: Write the shared contract suite and the LocalStorage test**

`tests/storage_contract.py`:

```python
"""Tests every Storage implementation must pass. Subclass and provide a `storage` fixture."""
import pytest


class StorageContract:
    def test_write_then_read(self, storage):
        with storage.open_write("run_1/a.bin") as f:
            f.write(b"hello")
        with storage.open_read("run_1/a.bin") as f:
            assert f.read() == b"hello"

    def test_open_write_truncates(self, storage):
        with storage.open_write("a.bin") as f:
            f.write(b"long content")
        with storage.open_write("a.bin") as f:
            f.write(b"x")
        assert storage.size("a.bin") == 1

    def test_exists_and_size(self, storage):
        assert not storage.exists("run_1/missing.bin")
        with storage.open_write("run_1/b.bin") as f:
            f.write(b"12345")
        assert storage.exists("run_1/b.bin")
        assert storage.size("run_1/b.bin") == 5

    def test_rename_creates_folders_and_replaces(self, storage):
        with storage.open_write("x.part") as f:
            f.write(b"new")
        with storage.open_write("d/x") as f:
            f.write(b"old")
        storage.rename("x.part", "d/x")
        assert not storage.exists("x.part")
        with storage.open_read("d/x") as f:
            assert f.read() == b"new"

    def test_delete_is_idempotent(self, storage):
        with storage.open_write("gone.bin") as f:
            f.write(b"1")
        storage.delete("gone.bin")
        storage.delete("gone.bin")
        assert not storage.exists("gone.bin")

    def test_list_prefix_sorted(self, storage):
        for rel in ["run_2/b", "run_1/z", "run_1/a", "other/c"]:
            with storage.open_write(rel) as f:
                f.write(b"1")
        assert storage.list("run_1/") == ["run_1/a", "run_1/z"]
        assert storage.list("") == ["other/c", "run_1/a", "run_1/z", "run_2/b"]

    @pytest.mark.parametrize("bad", ["../escape", "/abs/path", "a/../../b", ""])
    def test_rejects_unsafe_paths(self, storage, bad):
        with pytest.raises(ValueError):
            storage.exists(bad)

    def test_uri(self, storage):
        assert storage.uri().startswith(("file://", "s3://", "https://"))
        assert storage.uri("run_1/a.bin").endswith("/run_1/a.bin")
```

`tests/test_storage_local.py`:

```python
import pytest

from npd_loader.storage import LocalStorage
from storage_contract import StorageContract


class TestLocalStorage(StorageContract):
    @pytest.fixture
    def storage(self, tmp_path):
        return LocalStorage(tmp_path / "data")


def test_local_uri_is_file_uri(tmp_path):
    s = LocalStorage(tmp_path)
    assert s.uri() == tmp_path.resolve().as_uri()
```

Do not create `tests/__init__.py`. Shared test modules (`storage_contract`, `fakes`, `helpers`, …) are imported by bare name. To put `tests/` on `sys.path`, add this line to `[tool.pytest.ini_options]` in `pyproject.toml`:

```toml
pythonpath = ["tests"]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_storage_local.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'npd_loader.storage'`

- [ ] **Step 3: Implement `storage.py`**

```python
"""File storage behind a stream interface so object stores can replace LocalStorage later."""
from __future__ import annotations

import os
from pathlib import Path, PurePosixPath
from typing import BinaryIO, Protocol
from urllib.parse import quote


class Storage(Protocol):
    def open_write(self, rel_path: str) -> BinaryIO: ...
    def open_read(self, rel_path: str) -> BinaryIO: ...
    def exists(self, rel_path: str) -> bool: ...
    def size(self, rel_path: str) -> int: ...
    def delete(self, rel_path: str) -> None: ...
    def rename(self, src_rel: str, dst_rel: str) -> None: ...
    def list(self, prefix: str) -> list[str]: ...
    def uri(self, rel_path: str = "") -> str: ...


class LocalStorage:
    def __init__(self, root: str | os.PathLike[str]):
        self.root = Path(root).resolve()

    def _path(self, rel_path: str) -> Path:
        posix = PurePosixPath(rel_path)
        if not rel_path or posix.is_absolute() or rel_path.startswith("\\") or ".." in posix.parts:
            raise ValueError(f"unsafe storage path: {rel_path!r}")
        return self.root.joinpath(*posix.parts)

    def open_write(self, rel_path: str) -> BinaryIO:
        path = self._path(rel_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        return open(path, "wb")

    def open_read(self, rel_path: str) -> BinaryIO:
        return open(self._path(rel_path), "rb")

    def exists(self, rel_path: str) -> bool:
        return self._path(rel_path).is_file()

    def size(self, rel_path: str) -> int:
        return self._path(rel_path).stat().st_size

    def delete(self, rel_path: str) -> None:
        self._path(rel_path).unlink(missing_ok=True)

    def rename(self, src_rel: str, dst_rel: str) -> None:
        dst = self._path(dst_rel)
        dst.parent.mkdir(parents=True, exist_ok=True)
        os.replace(self._path(src_rel), dst)

    def list(self, prefix: str) -> list[str]:
        if not self.root.exists():
            return []
        found = []
        for path in self.root.rglob("*"):
            if path.is_file():
                rel = path.relative_to(self.root).as_posix()
                if rel.startswith(prefix):
                    found.append(rel)
        return sorted(found)

    def uri(self, rel_path: str = "") -> str:
        base = self.root.as_uri()
        if not rel_path:
            return base
        self._path(rel_path)
        return f"{base}/{quote(rel_path)}"
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_storage_local.py -v`
Expected: 12 passed

- [ ] **Step 5: Commit**

```bash
git add pyproject.toml src/npd_loader/storage.py tests/storage_contract.py tests/test_storage_local.py
git commit -m "feat: Storage interface with LocalStorage and shared contract tests"
```

---

### Task 3: Catalog interface, run XML, FakeCatalog

**Files:**
- Create: `src/npd_loader/catalog.py` (interface part), `src/npd_loader/runxml.py`, `tests/fakes.py`, `tests/catalog_contract.py`
- Test: `tests/test_runxml.py`, `tests/test_fake_catalog.py`

**Interfaces:**
- Produces in `runxml.py`: `build_config_xml(values: dict[str, object]) -> str` (root `<WAREHOUSE_RUN_CONFIG>`, one child per key, in order), `parse_release(config_xml: str | None) -> date | None`, `build_output_xml(items: list[dict[str, object]], item_tag: str) -> str` (root `<WAREHOUSE_RUN_OUTPUT>`, one element per item, values as attributes, `None` dropped).
- Produces in `catalog.py`: `SUCCESS = "Success"`, `FAILED = "Failed"` (meanings, not labels; the PG catalog maps them to config labels), `MAX_TEXT = 8000`, `truncate(text)`, `Run`, `DataFile`, `DATA_FILE_FIELDS`, `clean_fields(fields) -> dict`, `newest_run(runs, release) -> Run | None`, `releases_of(runs) -> list[date]`, and the `Catalog` Protocol:
  - `start_run(run_class: str, description: str, config_xml: str) -> Run`
  - `finish_run(run: Run, status: str, result: str | None = None, output_xml: str | None = None) -> None`
  - `add_data_file(run: Run, **fields) -> int`
  - `update_data_file(file_id: int, **fields) -> None`
  - `get_data_files(release: date, file_type: str, run_id: int | None = None) -> list[DataFile]` (ordered by id)
  - `last_successful_run(run_class: str, release: date | None = None) -> Run | None` (with `release=None`, returns the run with the newest release)
  - `successful_releases(run_class: str) -> list[date]` (ascending, unique)
- Produces in `tests/fakes.py`: `FakeCatalog`, with the protocol above plus test helpers `add_successful_run(run_class, release) -> Run` and attributes `runs: dict[int, dict]` and `files: dict[int, dict]`.

- [ ] **Step 1: Write failing tests**

`tests/test_runxml.py`:

```python
from datetime import date

from npd_loader.runxml import build_config_xml, build_output_xml, parse_release


def test_config_xml_roundtrip_release():
    xml = build_config_xml({"run_type": "National Provider Directory", "release_date": "2026-09-29",
                            "force": False, "note": "a<b&c"})
    assert xml.startswith("<WAREHOUSE_RUN_CONFIG><run_type>National Provider Directory</run_type>")
    assert "<force>False</force>" in xml
    assert "a&lt;b&amp;c" in xml
    assert parse_release(xml) == date(2026, 9, 29)


def test_parse_release_tolerates_missing_or_bad_xml():
    assert parse_release(None) is None
    assert parse_release("not xml") is None
    assert parse_release("<WAREHOUSE_RUN_CONFIG/>") is None


def test_output_xml_drops_none():
    xml = build_output_xml([{"name": "a", "size": 3, "etag": None}], item_tag="file")
    assert xml == '<WAREHOUSE_RUN_OUTPUT><file name="a" size="3" /></WAREHOUSE_RUN_OUTPUT>'
```

`tests/catalog_contract.py`:

```python
"""Behavior every Catalog implementation must have. Subclass and provide a `catalog` fixture."""
from datetime import date

import pytest

from npd_loader.catalog import FAILED, SUCCESS
from npd_loader.runxml import build_config_xml

R1 = date(2026, 9, 22)
R2 = date(2026, 9, 29)


def cfg(release: date) -> str:
    return build_config_xml({"run_type": "National Provider Directory", "release_date": release.isoformat()})


class CatalogContract:
    def test_start_run_parses_release(self, catalog):
        run = catalog.start_run("DOWNLOAD", "NPD FHIR Download 2026-09-29", cfg(R2))
        assert run.release_date == R2
        assert run.run_class == "DOWNLOAD"
        assert run.started_at.microsecond == 0
        second = catalog.start_run("DOWNLOAD", "again", cfg(R2))
        assert second.id > run.id

    def test_last_successful_run_ignores_failed_and_unfinished(self, catalog):
        failed = catalog.start_run("DOWNLOAD", "d", cfg(R2))
        catalog.finish_run(failed, FAILED, result="boom")
        catalog.start_run("DOWNLOAD", "d", cfg(R2))  # never finished
        assert catalog.last_successful_run("DOWNLOAD", R2) is None
        ok = catalog.start_run("DOWNLOAD", "d", cfg(R2))
        catalog.finish_run(ok, SUCCESS, output_xml="<WAREHOUSE_RUN_OUTPUT />")
        found = catalog.last_successful_run("DOWNLOAD", R2)
        assert found is not None and found.id == ok.id
        assert catalog.last_successful_run("IMPORT", R2) is None

    def test_newest_release_when_release_not_given(self, catalog):
        for release in (R2, R1):
            run = catalog.start_run("DOWNLOAD", "d", cfg(release))
            catalog.finish_run(run, SUCCESS)
        assert catalog.last_successful_run("DOWNLOAD").release_date == R2
        assert catalog.last_successful_run("DOWNLOAD", R1).release_date == R1
        assert catalog.successful_releases("DOWNLOAD") == [R1, R2]

    def test_data_files_filter_and_update(self, catalog):
        run1 = catalog.start_run("DOWNLOAD", "d", cfg(R2))
        run2 = catalog.start_run("DOWNLOAD", "d", cfg(R2))
        a = catalog.add_data_file(run1, file_type="ndjson.zst", source_version_num=R2.isoformat(),
                                  source_uri="https://x/01-Organization.ndjson.zst")
        b = catalog.add_data_file(run2, file_type="ndjson.zst", source_version_num=R2.isoformat(), parent_file=None)
        catalog.add_data_file(run2, file_type="ndjson", source_version_num=R2.isoformat(), parent_file=b)
        catalog.add_data_file(run2, file_type="ndjson.zst", source_version_num=R1.isoformat())
        assert [f.id for f in catalog.get_data_files(R2, "ndjson.zst")] == [a, b]
        assert [f.id for f in catalog.get_data_files(R2, "ndjson.zst", run_id=run2.id)] == [b]
        catalog.update_data_file(a, file_size=42, file_hash="ab" * 32, file_name="file_1_x")
        row = catalog.get_data_files(R2, "ndjson.zst", run_id=run1.id)[0]
        assert (row.file_size, row.file_hash, row.file_name, row.run_id) == (42, "ab" * 32, "file_1_x", run1.id)
        assert row.source_uri == "https://x/01-Organization.ndjson.zst"

    def test_unknown_field_rejected(self, catalog):
        run = catalog.start_run("DOWNLOAD", "d", cfg(R2))
        with pytest.raises(TypeError, match="unknown data_file fields"):
            catalog.add_data_file(run, file_type="x", bogus=1)

    def test_exceptions_truncated(self, catalog):
        run = catalog.start_run("DOWNLOAD", "d", cfg(R2))
        fid = catalog.add_data_file(run, file_type="ndjson", source_version_num=R2.isoformat())
        catalog.update_data_file(fid, exceptions="x" * 9000)
        assert len(catalog.get_data_files(R2, "ndjson")[0].exceptions) == 8000
```

`tests/test_fake_catalog.py`:

```python
import pytest

from catalog_contract import CatalogContract
from fakes import FakeCatalog


class TestFakeCatalog(CatalogContract):
    @pytest.fixture
    def catalog(self):
        return FakeCatalog()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_runxml.py tests/test_fake_catalog.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'npd_loader.runxml'`

- [ ] **Step 3: Implement `runxml.py`, the interface part of `catalog.py`, and `tests/fakes.py`**

`src/npd_loader/runxml.py`:

```python
"""Builders for master_warehouse_run.xml_config / xml_output."""
from __future__ import annotations

import xml.etree.ElementTree as ET
from datetime import date


def build_config_xml(values: dict[str, object]) -> str:
    root = ET.Element("WAREHOUSE_RUN_CONFIG")
    for key, value in values.items():
        ET.SubElement(root, key).text = "" if value is None else str(value)
    return ET.tostring(root, encoding="unicode")


def parse_release(config_xml: str | None) -> date | None:
    if not config_xml:
        return None
    try:
        root = ET.fromstring(config_xml)
    except ET.ParseError:
        return None
    el = root.find("release_date")
    if el is None or not el.text:
        return None
    try:
        return date.fromisoformat(el.text.strip())
    except ValueError:
        return None


def build_output_xml(items: list[dict[str, object]], item_tag: str) -> str:
    root = ET.Element("WAREHOUSE_RUN_OUTPUT")
    for item in items:
        ET.SubElement(root, item_tag, {k: str(v) for k, v in item.items() if v is not None})
    return ET.tostring(root, encoding="unicode")
```

`src/npd_loader/catalog.py` (interface part; Task 4 appends `CssCatalogPg`):

```python
"""Run and file tracking. `Catalog` is the interface; CssCatalogPg (below) writes css_catalog_local."""
from __future__ import annotations

from dataclasses import dataclass, fields as dc_fields
from datetime import date, datetime
from typing import Iterable, Protocol

SUCCESS = "Success"
FAILED = "Failed"
MAX_TEXT = 8000


def truncate(text: str | None) -> str | None:
    return None if text is None else text[:MAX_TEXT]


@dataclass(frozen=True)
class Run:
    id: int
    run_class: str
    description: str
    config_xml: str
    started_at: datetime
    release_date: date | None
    status: str | None = None


@dataclass(frozen=True)
class DataFile:
    id: int
    run_id: int
    file_type: str | None
    source_uri: str | None
    source_version_num: str | None
    file_name: str | None
    file_rel_path: str | None
    run_type_root_dir: str | None
    parent_file: int | None
    file_size: int | None
    file_hash: str | None
    date_modified: datetime | None
    date_created: datetime | None
    date_loaded: datetime | None
    exceptions: str | None


DATA_FILE_FIELDS: tuple[str, ...] = tuple(f.name for f in dc_fields(DataFile) if f.name not in ("id", "run_id"))


def clean_fields(fields: dict[str, object]) -> dict[str, object]:
    unknown = set(fields) - set(DATA_FILE_FIELDS)
    if unknown:
        raise TypeError(f"unknown data_file fields: {sorted(unknown)}")
    if isinstance(fields.get("exceptions"), str):
        fields = {**fields, "exceptions": truncate(fields["exceptions"])}
    return fields


def newest_run(runs: Iterable[Run], release: date | None) -> Run | None:
    candidates = [r for r in runs if r.release_date is not None and (release is None or r.release_date == release)]
    return max(candidates, key=lambda r: (r.release_date, r.id), default=None)


def releases_of(runs: Iterable[Run]) -> list[date]:
    return sorted({r.release_date for r in runs if r.release_date is not None})


class Catalog(Protocol):
    def start_run(self, run_class: str, description: str, config_xml: str) -> Run: ...
    def finish_run(self, run: Run, status: str, result: str | None = None, output_xml: str | None = None) -> None: ...
    def add_data_file(self, run: Run, **fields: object) -> int: ...
    def update_data_file(self, file_id: int, **fields: object) -> None: ...
    def get_data_files(self, release: date, file_type: str, run_id: int | None = None) -> list[DataFile]: ...
    def last_successful_run(self, run_class: str, release: date | None = None) -> Run | None: ...
    def successful_releases(self, run_class: str) -> list[date]: ...
```

`tests/fakes.py`:

```python
"""In-memory Catalog for stage unit tests."""
from __future__ import annotations

import itertools
from datetime import date, datetime

from npd_loader.catalog import (DATA_FILE_FIELDS, SUCCESS, DataFile, Run, clean_fields, newest_run,
                                releases_of, truncate)
from npd_loader.runxml import build_config_xml, parse_release


class FakeCatalog:
    def __init__(self) -> None:
        self.runs: dict[int, dict] = {}
        self.files: dict[int, dict] = {}
        self._run_ids = itertools.count(62000)
        self._file_ids = itertools.count(12000)

    def _run(self, run_id: int) -> Run:
        r = self.runs[run_id]
        return Run(id=run_id, run_class=r["run_class"], description=r["description"], config_xml=r["config_xml"],
                   started_at=r["started_at"], release_date=parse_release(r["config_xml"]), status=r["status"])

    def start_run(self, run_class: str, description: str, config_xml: str) -> Run:
        run_id = next(self._run_ids)
        self.runs[run_id] = {"run_class": run_class, "description": description, "config_xml": config_xml,
                             "started_at": datetime.now().replace(microsecond=0), "status": None,
                             "result": None, "output_xml": None}
        return self._run(run_id)

    def finish_run(self, run: Run, status: str, result: str | None = None, output_xml: str | None = None) -> None:
        self.runs[run.id].update(status=status, result=truncate(result), output_xml=output_xml)

    def add_data_file(self, run: Run, **fields: object) -> int:
        fields = clean_fields(fields)
        file_id = next(self._file_ids)
        self.files[file_id] = {name: None for name in DATA_FILE_FIELDS} | fields | {"id": file_id, "run_id": run.id}
        return file_id

    def update_data_file(self, file_id: int, **fields: object) -> None:
        self.files[file_id].update(clean_fields(fields))

    def get_data_files(self, release: date, file_type: str, run_id: int | None = None) -> list[DataFile]:
        rows = [f for f in self.files.values()
                if f["source_version_num"] == release.isoformat() and f["file_type"] == file_type
                and (run_id is None or f["run_id"] == run_id)]
        return [DataFile(**row) for row in sorted(rows, key=lambda f: f["id"])]

    def _successful(self, run_class: str) -> list[Run]:
        return [self._run(i) for i, r in self.runs.items() if r["run_class"] == run_class and r["status"] == SUCCESS]

    def last_successful_run(self, run_class: str, release: date | None = None) -> Run | None:
        return newest_run(self._successful(run_class), release)

    def successful_releases(self, run_class: str) -> list[date]:
        return releases_of(self._successful(run_class))

    # ---- test helpers -------------------------------------------------
    def add_successful_run(self, run_class: str, release: date) -> Run:
        run = self.start_run(run_class, "test", build_config_xml({"release_date": release.isoformat()}))
        self.finish_run(run, SUCCESS)
        return run
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_runxml.py tests/test_fake_catalog.py -v`
Expected: 9 passed

- [ ] **Step 5: Commit**

```bash
git add src/npd_loader/runxml.py src/npd_loader/catalog.py tests/fakes.py tests/catalog_contract.py tests/test_runxml.py tests/test_fake_catalog.py
git commit -m "feat: Catalog interface, run XML helpers, in-memory FakeCatalog"
```

---

### Task 4: CssCatalogPg and Postgres test infrastructure

**Files:**
- Create: `tests/conftest.py`, `tests/sql/css_catalog_schema.sql`
- Modify: `src/npd_loader/catalog.py` (append `CssCatalogPg`)
- Test: `tests/test_catalog_pg.py`

**Interfaces:**
- Consumes: everything from Task 3, plus `CatalogConfig` from Task 1.
- Produces: `CssCatalogPg(conninfo: str, cfg: CatalogConfig)` implementing `Catalog`. Pytest fixtures `pg_server` (session), `make_db` (factory returning a conninfo string for a brand-new empty database that is dropped after the test), and `catalog_db` (conninfo of a new database containing the catalog tables).

- [ ] **Step 1: Capture the real catalog DDL**

On a machine that can reach `192.10.0.6`, run:

```bash
pg_dump --schema-only --no-owner --no-privileges \
  -t public.master_warehouse_run -t public.data_file \
  -h 192.10.0.6 -U <catalog user> css_catalog_local > tests/sql/css_catalog_schema.sql
```

Open the file. The code below assumes these column names:
- `master_warehouse_run`: primary key `run_id`, plus `project`, `run_type`, `run_class`, `run_description`, `date_started`, `date_completed`, `completion_status`, `result`, `xml_config`, `xml_output`.
- `data_file`: primary key `id`, plus the §5.2 columns.

If the dump names any of these differently (e.g. the run PK is `id`, or the start column is `run_start`), change the matching identifiers in the `CssCatalogPg` SQL in Step 4. Then record the mapping in a comment at the top of `CssCatalogPg`. Delete any `SET`/`SELECT pg_catalog.set_config` lines and `ALTER ... OWNER` lines from the dump. Keep sequences, defaults, and constraints.

If the server is unreachable while you work, write this assumed DDL to `tests/sql/css_catalog_schema.sql` instead. Replace it with the real dump before deploying (Task 19 repeats this check).

```sql
CREATE TABLE public.master_warehouse_run (
    run_id            serial PRIMARY KEY,
    parent_run_id     integer,
    project           varchar(100),
    run_type          varchar(200),
    run_class         varchar(100),
    run_description   varchar(500),
    date_started      timestamp DEFAULT now(),
    date_completed    timestamp,
    completion_status varchar(50),
    result            text,
    xml_config        xml,
    xml_output        xml
);

CREATE TABLE public.data_file (
    id                  serial PRIMARY KEY,
    run_id              integer REFERENCES public.master_warehouse_run (run_id),
    file_set            varchar(100),
    file_type           varchar(50),
    source_uri          text,
    source_version_name varchar(200),
    source_version_num  varchar(100),
    file_name           text,
    file_rel_path       text,
    run_type_root_dir   text,
    parent_file         integer REFERENCES public.data_file (id),
    file_size           bigint,
    file_hash           varchar(128),
    date_modified       timestamp,
    date_created        timestamp,
    date_loaded         timestamp,
    exceptions          text
);
```

- [ ] **Step 2: Write `tests/conftest.py` and the failing test**

`tests/conftest.py`:

```python
import os
import uuid
from pathlib import Path

import psycopg
import pytest
from psycopg.conninfo import make_conninfo

PG_IMAGE = os.environ.get("NPD_TEST_PG_IMAGE", "postgres:16")
TESTS = Path(__file__).resolve().parent


@pytest.fixture(scope="session")
def pg_server():
    try:
        from testcontainers.postgres import PostgresContainer
        container = PostgresContainer(PG_IMAGE, username="test", password="test", dbname="postgres")
        container.start()
    except Exception as exc:  # Docker not running / not installed
        pytest.skip(f"Docker Postgres unavailable: {exc}")
    yield container
    container.stop()


def _dsn(container, dbname: str) -> str:
    return make_conninfo(host=container.get_container_host_ip(), port=int(container.get_exposed_port(5432)),
                         user="test", password="test", dbname=dbname)


@pytest.fixture
def make_db(pg_server):
    created: list[str] = []

    def factory() -> str:
        name = f"t_{uuid.uuid4().hex[:12]}"
        with psycopg.connect(_dsn(pg_server, "postgres"), autocommit=True) as conn:
            conn.execute(f'CREATE DATABASE "{name}"')
        created.append(name)
        return _dsn(pg_server, name)

    yield factory
    with psycopg.connect(_dsn(pg_server, "postgres"), autocommit=True) as conn:
        for name in created:
            conn.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')


@pytest.fixture
def catalog_db(make_db) -> str:
    info = make_db()
    with psycopg.connect(info, autocommit=True) as conn:
        conn.execute((TESTS / "sql" / "css_catalog_schema.sql").read_text())
    return info
```

`tests/test_catalog_pg.py`:

```python
from datetime import date

import psycopg
import pytest

from catalog_contract import CatalogContract, cfg
from npd_loader.catalog import FAILED, SUCCESS, CssCatalogPg
from npd_loader.config import CatalogConfig


def catalog_config() -> CatalogConfig:
    return CatalogConfig(backend="css_catalog_pg", host="unused", port=0, dbname="unused", user=None,
                         password=None, project="NPD", run_type="National Provider Directory", file_set="NPD_FHIR")


class TestCssCatalogPg(CatalogContract):
    @pytest.fixture
    def catalog(self, catalog_db):
        return CssCatalogPg(catalog_db, catalog_config())


def test_writes_configured_labels(catalog_db):
    catalog = CssCatalogPg(catalog_db, catalog_config())
    run = catalog.start_run("DOWNLOAD", "NPD FHIR Download 2026-09-29", cfg(date(2026, 9, 29)))
    fid = catalog.add_data_file(run, file_type="manifest", source_version_num="2026-09-29")
    catalog.finish_run(run, SUCCESS, result=None, output_xml="<WAREHOUSE_RUN_OUTPUT />")
    failed = catalog.start_run("IMPORT", "x", cfg(date(2026, 9, 29)))
    catalog.finish_run(failed, FAILED, result="y" * 9000)
    with psycopg.connect(catalog_db) as conn:
        row = conn.execute("SELECT project, run_type, run_class, run_description, completion_status, "
                           "date_completed IS NOT NULL, parent_run_id FROM master_warehouse_run WHERE run_id = %s",
                           (run.id,)).fetchone()
        assert row == ("NPD", "National Provider Directory", "DOWNLOAD", "NPD FHIR Download 2026-09-29",
                       "Success", True, None)
        assert conn.execute("SELECT completion_status, length(result) FROM master_warehouse_run WHERE run_id = %s",
                            (failed.id,)).fetchone() == ("Failed", 8000)
        assert conn.execute("SELECT file_set, source_version_name, run_id FROM data_file WHERE id = %s",
                            (fid,)).fetchone() == ("NPD_FHIR", "NPD release", run.id)


def test_other_projects_runs_are_ignored(catalog_db):
    with psycopg.connect(catalog_db) as conn:
        conn.execute("INSERT INTO master_warehouse_run (project, run_type, run_class, xml_config, completion_status) "
                     "VALUES ('OTHER', 'National Provider Directory', 'DOWNLOAD', %s, 'Success')",
                     (cfg(date(2026, 9, 29)),))
    catalog = CssCatalogPg(catalog_db, catalog_config())
    assert catalog.last_successful_run("DOWNLOAD") is None
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `python -m pytest tests/test_catalog_pg.py -v`
Expected: FAIL with `ImportError: cannot import name 'CssCatalogPg'`. If the run instead reports `SKIPPED ... Docker Postgres unavailable`, start Docker and re-run. Postgres tests must actually run before this task is done.

- [ ] **Step 4: Append `CssCatalogPg` to `src/npd_loader/catalog.py`**

Add these imports at the top of the file, next to the existing ones:

```python
import psycopg
from psycopg import sql
from psycopg.rows import dict_row

from npd_loader.config import CatalogConfig
from npd_loader.runxml import parse_release
```

Append at the end:

```python
def _qualified(name: str) -> sql.Identifier:
    return sql.Identifier(*name.split(".", 1))


class CssCatalogPg:
    """Catalog in css_catalog_local. Each call uses its own short autocommit connection, so catalog writes
    commit independently of data loads and survive their failures."""

    def __init__(self, conninfo: str, cfg: CatalogConfig):
        self._conninfo = conninfo
        self._cfg = cfg
        self._runs = _qualified(cfg.run_table)
        self._files = _qualified(cfg.file_table)

    def _connect(self) -> psycopg.Connection:
        return psycopg.connect(self._conninfo, autocommit=True)

    def start_run(self, run_class: str, description: str, config_xml: str) -> Run:
        started = datetime.now().replace(microsecond=0)
        query = sql.SQL("INSERT INTO {} (project, run_type, run_class, run_description, xml_config, date_started) "
                        "VALUES (%s, %s, %s, %s, %s, %s) RETURNING run_id").format(self._runs)
        with self._connect() as conn:
            run_id = conn.execute(query, (self._cfg.project, self._cfg.run_type, run_class, description,
                                          config_xml, started)).fetchone()[0]
        return Run(run_id, run_class, description, config_xml, started, parse_release(config_xml))

    def finish_run(self, run: Run, status: str, result: str | None = None, output_xml: str | None = None) -> None:
        label = {SUCCESS: self._cfg.status_success, FAILED: self._cfg.status_failed}[status]
        query = sql.SQL("UPDATE {} SET completion_status = %s, date_completed = %s, result = %s, xml_output = %s "
                        "WHERE run_id = %s").format(self._runs)
        with self._connect() as conn:
            conn.execute(query, (label, datetime.now().replace(microsecond=0), truncate(result), output_xml, run.id))

    def add_data_file(self, run: Run, **fields: object) -> int:
        fields = clean_fields(fields)
        cols = ["run_id", "file_set", "source_version_name", *fields]
        vals = [run.id, self._cfg.file_set, self._cfg.source_version_name, *fields.values()]
        query = sql.SQL("INSERT INTO {} ({}) VALUES ({}) RETURNING id").format(
            self._files, sql.SQL(", ").join(map(sql.Identifier, cols)),
            sql.SQL(", ").join([sql.Placeholder()] * len(cols)))
        with self._connect() as conn:
            return conn.execute(query, vals).fetchone()[0]

    def update_data_file(self, file_id: int, **fields: object) -> None:
        fields = clean_fields(fields)
        if not fields:
            return
        sets = sql.SQL(", ").join(sql.SQL("{} = %s").format(sql.Identifier(k)) for k in fields)
        query = sql.SQL("UPDATE {} SET {} WHERE id = %s").format(self._files, sets)
        with self._connect() as conn:
            conn.execute(query, [*fields.values(), file_id])

    def get_data_files(self, release: date, file_type: str, run_id: int | None = None) -> list[DataFile]:
        cols = sql.SQL(", ").join(map(sql.Identifier, ["id", "run_id", *DATA_FILE_FIELDS]))
        query = sql.SQL("SELECT {} FROM {} WHERE file_set = %s AND source_version_num = %s AND file_type = %s "
                        "AND (%s::integer IS NULL OR run_id = %s) ORDER BY id").format(cols, self._files)
        with self._connect() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query, (self._cfg.file_set, release.isoformat(), file_type, run_id, run_id))
            return [DataFile(**row) for row in cur.fetchall()]

    def _successful_runs(self, run_class: str) -> list[Run]:
        query = sql.SQL("SELECT run_id, run_class, run_description, xml_config::text, date_started, "
                        "completion_status FROM {} WHERE project = %s AND run_type = %s AND run_class = %s "
                        "AND completion_status = %s").format(self._runs)
        with self._connect() as conn:
            rows = conn.execute(query, (self._cfg.project, self._cfg.run_type, run_class,
                                        self._cfg.status_success)).fetchall()
        return [Run(r[0], r[1], r[2] or "", r[3] or "", r[4], parse_release(r[3]), SUCCESS) for r in rows]

    def last_successful_run(self, run_class: str, release: date | None = None) -> Run | None:
        return newest_run(self._successful_runs(run_class), release)

    def successful_releases(self, run_class: str) -> list[date]:
        return releases_of(self._successful_runs(run_class))
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `python -m pytest tests/test_catalog_pg.py tests/test_fake_catalog.py -v`
Expected: all passed (8 contract tests plus 2 PG-specific tests in `test_catalog_pg.py`, 6 in `test_fake_catalog.py`), none skipped.

- [ ] **Step 6: Commit**

```bash
git add src/npd_loader/catalog.py tests/conftest.py tests/sql/css_catalog_schema.sql tests/test_catalog_pg.py
git commit -m "feat: CssCatalogPg catalog backend with Docker Postgres tests"
```

---
### Task 5: Test fixtures and fake `directory.cms.gov`

**Files:**
- Create: `tests/fixture_data.py`, `tests/release_builder.py`, `tests/fake_cms.py`
- Modify: `tests/conftest.py` (add `cms` fixture)
- Test: `tests/test_fake_cms.py`

**Interfaces:**
- Produces in `fixture_data.py`: record constants `ORG1, ORG2, LOC1, ENDP1, HCS1, IP1, PRAC1, PRAC2, PR1, PR2, OA1`. Also `RECORDS: dict[str, list[dict]]`, keyed by manifest file name (`"01-Organization.ndjson"` … `"08-OrganizationAffiliation.ndjson"`), `ndjson_bytes(records) -> bytes`, and `all_records() -> list[dict]`. ORG1, LOC1, ENDP1, HCS1, IP1, PRAC1, PR1, and OA1 are the first record of each file in release 2026-09-29, copied verbatim. ORG2, PRAC2, and PR2 are synthetic and cover fields missing from those first records.
- Produces in `release_builder.py`: `BuiltRelease(release_date: str, manifest: bytes, files: dict[str, bytes], ndjson: dict[str, bytes])`, where `files` is keyed by `.zst` name and `ndjson` by `.ndjson` name. Also `build_release(release_date: str, records=None, raw_ndjson=None, manifest_overrides=None) -> BuiltRelease`.
- Produces in `fake_cms.py`: `FakeCms` with `start()`, `stop()`, `base_url`, `manifest_url`, `public_url(name)`, and `publish(release, later_manifest=None)`. It also has these knobs: `drops: dict[str, list[int]]` (each GET of that object drops the connection after N body bytes, consumed one per GET), `ignore_range: set[str]`, `expire_next: set[str]` (the next signed URL issued for that name answers 403), and `requests: list[tuple[str, str, str | None]]` (method, path, Range header).
- Produces in `conftest.py`: fixture `cms` → a started `FakeCms`.

- [ ] **Step 1: Write `tests/fixture_data.py`**

```python
"""FHIR sample records. Real ones are verbatim first records of release 2026-09-29; synthetic ones fill gaps."""
import json

NPI = "http://terminology.hl7.org/NamingSystem/npi"
V2 = "http://terminology.hl7.org/CodeSystem/v2-0203"
NDH = "http://hl7.org/fhir/us/ndh/StructureDefinition/"
VERIFIED = {"url": NDH + "base-ext-verification-status",
            "valueCodeableConcept": {"coding": [{"system": "http://hl7.org/fhir/us/ndh/CodeSystem/NdhVerificationStatusCS",
                                                 "code": "complete", "display": "Complete"}]}}
NPI_TYPE = {"coding": [{"system": V2, "code": "PRN", "display": "Provider number"}], "text": "NPI"}

ORG1 = {"resourceType": "Organization", "id": "Organization-1336200294",
        "meta": {"lastUpdated": "2026-09-29T04:29:05.411440Z"}, "extension": [VERIFIED],
        "identifier": [{"type": NPI_TYPE, "system": NPI, "use": "official", "value": "1336200294",
                        "period": {"start": "2006-12-13T00:00:00Z"}},
                       {"type": {"coding": [{"system": V2, "code": "TAX", "display": "Tax ID number"}]},
                        "system": "https://npd.cms.gov/fhir/sid/us-pseudo-ein", "use": "official",
                        "value": "6e5d8b3e-13d3-48be-9ed2-881d08f2c459"}],
        "active": True,
        "type": [{"coding": [{"system": "http://terminology.hl7.org/CodeSystem/organization-type", "code": "prov",
                              "display": "Healthcare Provider"}], "text": "Healthcare Provider"}],
        "name": "NEW MEXICO STATE UNIVERSITY STUDENT HEALTH CENTER",
        "telecom": [{"system": "fax", "value": "5056462692", "use": "work"},
                    {"system": "phone", "value": "5056461512", "use": "work"}]}

ORG2 = {"resourceType": "Organization", "id": "Organization-1902099112",
        "meta": {"lastUpdated": "2026-09-29T04:30:00Z"}, "extension": [VERIFIED],
        "identifier": [{"type": NPI_TYPE, "system": NPI, "use": "official", "value": "1902099112"}],
        "active": True,
        "type": [{"coding": [{"system": "http://terminology.hl7.org/CodeSystem/organization-type", "code": "prov",
                              "display": "Healthcare Provider"}], "text": "Healthcare Provider"}],
        "name": "EASTBLUFF MEDICAL GROUP",
        "telecom": [{"system": "phone", "value": "9496405050", "use": "work"}],
        "address": [{"line": ["2515 Eastbluff Dr", "Suite 100", "Building B"], "city": "Newport Beach",
                     "state": "CA", "postalCode": "92660", "country": "US", "type": "physical", "use": "work"}],
        "partOf": {"reference": "Organization/Organization-1336200294"},
        "endpoint": [{"reference": "Endpoint/Endpoint-000f410c-e1e9-4a78-a988-b6ce47d6a793"}]}

LOC1 = {"resourceType": "Location", "id": "Location-00027861-c380-4866-b677-4c28e4ceaf6b",
        "meta": {"lastUpdated": "2026-09-29T04:29:55.568276Z"}, "status": "active", "name": "2515 Eastbluff Dr",
        "description": "2515 Eastbluff Dr", "mode": "instance",
        "telecom": [{"system": "fax", "value": "9496405051", "use": "work"},
                    {"system": "phone", "value": "9496405050", "use": "work"}],
        "address": {"line": ["2515 Eastbluff Dr"], "city": "Newport Beach", "state": "CA", "postalCode": "92660",
                    "country": "US", "type": "physical"},
        "position": {"longitude": -117.87532, "latitude": 33.6399},
        "managingOrganization": {"reference": "Organization/Organization-1902099112"}}

ENDP1 = {"resourceType": "Endpoint", "id": "Endpoint-000f410c-e1e9-4a78-a988-b6ce47d6a793",
         "meta": {"lastUpdated": "2026-09-29T04:10:10.922997Z"}, "extension": [VERIFIED], "status": "active",
         "connectionType": {"system": "http://terminology.hl7.org/CodeSystem/endpoint-connection-type",
                            "code": "direct-project", "display": "Direct Project"},
         "name": "Direct Messaging Address", "address": "heather.bruneau.1@29651.direct.athenahealth.com",
         "payloadType": [{"coding": [{"system": "http://terminology.hl7.org/CodeSystem/data-absent-reason",
                                      "version": "1.0.0", "code": "not-applicable"}]}]}

HCS1 = {"resourceType": "HealthcareService", "id": "HealthcareService-cd52e7a4-79df-47ef-aa81-a89142e37ffe",
        "meta": {"lastUpdated": "2026-09-29T04:29:07.620230Z"}, "active": True,
        "providedBy": {"reference": "Organization/Organization-1295596195"},
        "location": [{"reference": "Location/Location-a1ab5e31-a038-4ec0-9439-ee9613d63e11"}],
        "extension": [{"url": NDH + "base-ext-network-reference",
                       "valueReference": {"reference": "Organization/Organization-ea579d05-454e-4359-8751-900c940a599a"}}]}

IP1 = {"resourceType": "InsurancePlan", "id": "InsurancePlan-0be2a43c-0c13-41fd-b97f-7f43394ec1fd",
       "meta": {"lastUpdated": "2026-09-29T04:03:43.071628Z"}, "status": "active",
       "name": "DEVOTED CHOICE GIVEBACK 002 SC (PPO)", "type": [{"text": "Medicare Advantage PPO Plan"}],
       "alias": ["H7028-002-000", "H7028"],
       "period": {"start": "2026-01-01T00:00:00Z", "end": "2026-12-31T00:00:00Z"},
       "ownedBy": {"reference": "Organization/Organization-242574ec-a550-43f6-80ae-592aa53c17c8"},
       "network": [{"reference": "Organization/Organization-c7d4aa30-a4c0-4733-aa2c-c8086e29159b"}]}

TAXONOMY = "http://hl7.org/fhir/us/ndh/ValueSet/HealthcareIndividualTaxonomyVS"

PRAC1 = {"resourceType": "Practitioner", "id": "Practitioner-1003000100",
         "meta": {"lastUpdated": "2026-09-29T04:34:00.724328Z"},
         "extension": [{"url": NDH + "base-ext-cms-identity-verified", "valueBoolean": False},
                       {"url": NDH + "base-ext-cms_medicare_enrollment", "valueBoolean": False},
                       {"url": NDH + "base-ext-hhs-in-exclusion-list", "valueBoolean": False},
                       {"url": NDH + "base-ext-cms_aligned_with_data_network", "valueBoolean": False}],
         "identifier": [{"type": NPI_TYPE, "system": NPI, "use": "official", "value": "1003000100",
                         "period": {"start": "2007-08-31T00:00:00Z"}}],
         "active": True, "name": [{"given": ["GERARDO"], "family": "GOMEZ", "use": "official"}],
         "telecom": [{"system": "fax", "value": "2133831280", "use": "work"},
                     {"system": "phone", "value": "2133657400", "use": "work"},
                     {"system": "phone", "value": "3107152020", "use": "work"}],
         "address": [{"line": ["108 W Victoria St"], "city": "Gardena", "state": "CA", "postalCode": "90248",
                      "country": "US", "type": "physical", "use": "work"},
                     {"line": ["680 S Wilton Pl"], "city": "Los Angeles", "state": "CA", "postalCode": "90005",
                      "country": "US", "type": "postal", "use": "billing"}],
         "gender": "male",
         "qualification": [{"code": {"coding": [{"system": TAXONOMY, "code": "171M00000X",
                                                 "display": "Case Manager/Care Coordinator"}],
                                     "text": "Case Manager/Care Coordinator"}},
                           {"code": {"coding": [{"system": TAXONOMY, "code": "225400000X",
                                                 "display": "Rehabilitation Practitioner"}],
                                     "text": "Rehabilitation Practitioner"}}]}

PRAC2 = {"resourceType": "Practitioner", "id": "Practitioner-1083687529",
         "meta": {"lastUpdated": "2026-09-29T04:35:00Z"},
         "extension": [{"url": NDH + "base-ext-cms-identity-verified", "valueBoolean": True},
                       {"url": NDH + "base-ext-cms_medicare_enrollment", "valueBoolean": True},
                       {"url": NDH + "base-ext-hhs-in-exclusion-list", "valueBoolean": False},
                       {"url": NDH + "base-ext-cms_aligned_with_data_network", "valueBoolean": True}],
         "identifier": [{"type": NPI_TYPE, "system": NPI, "use": "official", "value": "1083687529",
                         "period": {"start": "2005-05-23T00:00:00Z"}}],
         "active": True,
         "name": [{"family": "SMITH", "given": ["ANNA"], "use": "maiden", "period": {"start": "1990-01-01T00:00:00Z"}},
                  {"family": "JONES", "given": ["ANNA", "MARIE"], "prefix": ["DR."], "suffix": ["MD"],
                   "use": "official"}],
         "telecom": [{"system": "phone", "value": "2125551212", "use": "work"}],
         "gender": "female",
         "qualification": [{"identifier": [{"type": {"coding": [{"system": V2, "code": "MD",
                                                                 "display": "Medical License number"}]},
                                            "use": "official", "value": "A12345"}],
                            "code": {"coding": [{"system": TAXONOMY, "code": "207R00000X",
                                                 "display": "Internal Medicine Physician"}],
                                     "text": "Internal Medicine Physician"},
                            "issuer": {"reference": "Organization/Organization-NY-STATE-BOARD"}}]}

PR1 = {"resourceType": "PractitionerRole", "id": "PractitionerRole-00000990-37aa-428a-a1fd-d91bed7c789d",
       "meta": {"lastUpdated": "2026-09-29T04:29:22.708739Z"}, "active": True,
       "practitioner": {"reference": "Practitioner/Practitioner-1083687529"},
       "endpoint": [{"reference": "Endpoint/Endpoint-00000990-37aa-428a-a1fd-d91bed7c789d"}]}

PR2 = {"resourceType": "PractitionerRole", "id": "PractitionerRole-0f00aa11",
       "meta": {"lastUpdated": "2026-09-29T04:40:00Z"}, "active": True,
       "practitioner": {"reference": "Practitioner/Practitioner-1003000100"},
       "organization": {"reference": "Organization/Organization-1902099112"},
       "location": [{"reference": "Location/Location-00027861-c380-4866-b677-4c28e4ceaf6b"}],
       "code": [{"coding": [{"system": "http://hl7.org/fhir/us/ndh/CodeSystem/IndividualAndGroupSpecialtiesCS",
                             "code": "ph", "display": "Physician"}]}],
       "specialty": [{"coding": [{"system": "http://nucc.org/provider-taxonomy", "code": "207R00000X",
                                  "display": "Internal Medicine Physician"}], "text": "Internal Medicine"}],
       "period": {"start": "2020-01-01T00:00:00Z"}}

OA1 = {"resourceType": "OrganizationAffiliation", "id": "OrganizationAffiliation-00111700-8fc3-4ea1-a966-4c3d59b41921",
       "meta": {"lastUpdated": "2026-09-29T04:11:32.072722Z"}, "active": True,
       "organization": {"reference": "Organization/Organization-c618f893-235a-48ae-bbaa-1d60d5ac7ee9"},
       "participatingOrganization": {"reference": "Organization/Organization-1407192586"},
       "code": [{"coding": [{"system": "http://terminology.hl7.org/CodeSystem/organization-affiliation-role",
                             "code": "bt", "display": "Member Of"}], "text": "Member Of"}]}

RECORDS: dict[str, list[dict]] = {
    "01-Organization.ndjson": [ORG1, ORG2],
    "02-Location.ndjson": [LOC1],
    "03-Endpoint.ndjson": [ENDP1],
    "04-HealthcareService.ndjson": [HCS1],
    "05-InsurancePlan.ndjson": [IP1],
    "06-Practitioner.ndjson": [PRAC1, PRAC2],
    "07-PractitionerRole.ndjson": [PR1, PR2],
    "08-OrganizationAffiliation.ndjson": [OA1],
}


def ndjson_bytes(records: list[dict]) -> bytes:
    return b"".join(json.dumps(r).encode() + b"\n" for r in records)


def all_records() -> list[dict]:
    return [r for recs in RECORDS.values() for r in recs]
```

- [ ] **Step 2: Write `tests/release_builder.py`**

```python
"""Build a release (manifest + .zst bytes) in memory, in the same format directory.cms.gov uses."""
from __future__ import annotations

import json
from dataclasses import dataclass

import zstandard

import fixture_data


@dataclass
class BuiltRelease:
    release_date: str
    manifest: bytes
    files: dict[str, bytes]     # "06-Practitioner.ndjson.zst" -> compressed bytes
    ndjson: dict[str, bytes]    # "06-Practitioner.ndjson" -> uncompressed bytes


def build_release(release_date: str, records: dict[str, list[dict]] | None = None,
                  raw_ndjson: dict[str, bytes] | None = None,
                  manifest_overrides: dict[str, dict] | None = None) -> BuiltRelease:
    """`records` replaces the default fixture records; `raw_ndjson` adds or replaces files byte-for-byte
    (for bad-data variants); `manifest_overrides` patches a file's manifest entry, e.g. a wrong size."""
    raw = {name: fixture_data.ndjson_bytes(recs) for name, recs in (records or fixture_data.RECORDS).items()}
    raw.update(raw_ndjson or {})
    files: dict[str, bytes] = {}
    entries: dict[str, dict] = {}
    for name, data in sorted(raw.items()):
        compressed = zstandard.ZstdCompressor().compress(data)
        files[name + ".zst"] = compressed
        entries[name] = {"compressed_bytes": len(compressed), "compression_ratio_pct": 0.0,
                         "original_bytes": len(data)}
    for name, patch in (manifest_overrides or {}).items():
        entries[name].update(patch)
    manifest = {"compression_algorithm": "zstd", "files": entries, "generated_at": release_date,
                "totals": {"compressed_bytes": sum(e["compressed_bytes"] for e in entries.values()),
                           "original_bytes": sum(e["original_bytes"] for e in entries.values())}}
    return BuiltRelease(release_date, json.dumps(manifest, indent=2).encode(), files, raw)
```

- [ ] **Step 3: Write `tests/fake_cms.py`**

```python
"""A local imitation of directory.cms.gov: public URLs 302 to short-lived 'signed' URLs that support Range."""
from __future__ import annotations

import hashlib
import itertools
import socket
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

from release_builder import BuiltRelease


class FakeCms:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.manifests: list[bytes] = []
        self.drops: dict[str, list[int]] = {}
        self.ignore_range: set[str] = set()
        self.expire_next: set[str] = set()
        self.requests: list[tuple[str, str, str | None]] = []
        self._tokens: dict[str, tuple[str, bool]] = {}
        self._counter = itertools.count(1)
        self._server = ThreadingHTTPServer(("127.0.0.1", 0), _handler(self))
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)

    @property
    def base_url(self) -> str:
        host, port = self._server.server_address[:2]
        return f"http://{host}:{port}"

    @property
    def manifest_url(self) -> str:
        return self.base_url + "/downloads/manifest.json"

    def public_url(self, name: str) -> str:
        return f"{self.base_url}/downloads/{name}"

    def start(self) -> "FakeCms":
        self._thread.start()
        return self

    def stop(self) -> None:
        self._server.shutdown()
        self._server.server_close()

    def publish(self, release: BuiltRelease, later_manifest: bytes | None = None) -> None:
        """Serve `release`. If `later_manifest` is given, every manifest GET after the first returns it."""
        self.objects = dict(release.files)
        self.manifests = [release.manifest] + ([later_manifest] if later_manifest else [])

    # ---- internals used by the handler ---------------------------------
    def _issue(self, name: str) -> str:
        token = str(next(self._counter))
        valid = name not in self.expire_next
        self.expire_next.discard(name)
        self._tokens[token] = (name, valid)
        return token

    def _valid(self, name: str, token: str) -> bool:
        return self._tokens.get(token) == (name, True)

    def _body(self, name: str) -> bytes | None:
        if name == "manifest.json":
            body = self.manifests[0]
            if len(self.manifests) > 1:
                self.manifests.pop(0)
            return body
        return self.objects.get(name)


def _handler(cms: FakeCms) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *args) -> None:
            pass

        def _empty(self, status: int) -> None:
            self.send_response(status)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def do_HEAD(self) -> None:
            self._empty(403)  # like S3 signed GET URLs

        def do_GET(self) -> None:
            url = urlsplit(self.path)
            cms.requests.append(("GET", url.path, self.headers.get("Range")))
            if url.path.startswith("/downloads/"):
                name = url.path.removeprefix("/downloads/")
                if name != "manifest.json" and name not in cms.objects:
                    return self._empty(404)
                self.send_response(302)
                self.send_header("Location", f"/s3/{name}?token={cms._issue(name)}")
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            if url.path.startswith("/s3/"):
                name = url.path.removeprefix("/s3/")
                if not cms._valid(name, parse_qs(url.query).get("token", [""])[0]):
                    return self._empty(403)
                body = cms._body(name)
                if body is None:
                    return self._empty(404)
                return self._send(name, body)
            self._empty(404)

        def _send(self, name: str, body: bytes) -> None:
            start = 0
            rng = self.headers.get("Range")
            if rng and name not in cms.ignore_range:
                start = int(rng.removeprefix("bytes=").split("-")[0])
                self.send_response(206)
                self.send_header("Content-Range", f"bytes {start}-{len(body) - 1}/{len(body)}")
            else:
                self.send_response(200)
            part = body[start:]
            self.send_header("Content-Length", str(len(part)))
            self.send_header("Last-Modified", "Tue, 29 Sep 2026 04:00:00 GMT")
            self.send_header("ETag", f'"{hashlib.md5(body).hexdigest()}"')
            self.end_headers()
            drops = cms.drops.get(name)
            if drops:
                cut = drops.pop(0)
                self.wfile.write(part[:cut])
                self.wfile.flush()
                self.close_connection = True
                self.connection.shutdown(socket.SHUT_RDWR)
                return
            self.wfile.write(part)

    return Handler
```

Append to `tests/conftest.py`:

```python
@pytest.fixture
def cms():
    from fake_cms import FakeCms
    server = FakeCms().start()
    yield server
    server.stop()
```

- [ ] **Step 4: Write the self-test `tests/test_fake_cms.py`**

```python
import httpx
import pytest
import zstandard

from release_builder import build_release


def test_redirect_range_and_drop(cms):
    rel = build_release("2026-09-29")
    cms.publish(rel)
    name = "06-Practitioner.ndjson.zst"
    with httpx.Client() as client:
        first = client.get(cms.public_url(name), follow_redirects=False)
        assert first.status_code == 302 and "/s3/" in first.headers["location"]
        full = client.get(cms.public_url(name), follow_redirects=True)
        assert full.content == rel.files[name]
        assert zstandard.ZstdDecompressor().decompress(full.content) == rel.ndjson["06-Practitioner.ndjson"]
        part = client.get(cms.public_url(name), headers={"Range": "bytes=10-"}, follow_redirects=True)
        assert part.status_code == 206 and part.content == rel.files[name][10:]
        cms.drops[name] = [5]
        with pytest.raises(httpx.RemoteProtocolError):
            client.get(cms.public_url(name), follow_redirects=True)
        cms.expire_next.add(name)
        assert client.get(cms.public_url(name), follow_redirects=True).status_code == 403


def test_manifest_changes_after_first_fetch(cms):
    cms.publish(build_release("2026-09-29"), later_manifest=build_release("2026-10-06").manifest)
    with httpx.Client(follow_redirects=True) as client:
        assert b"2026-09-29" in client.get(cms.manifest_url).content
        assert b"2026-10-06" in client.get(cms.manifest_url).content
        assert b"2026-10-06" in client.get(cms.manifest_url).content
```

- [ ] **Step 5: Run the self-tests**

Run: `python -m pytest tests/test_fake_cms.py -v`
Expected: 2 passed

- [ ] **Step 6: Commit**

```bash
git add tests/fixture_data.py tests/release_builder.py tests/fake_cms.py tests/conftest.py tests/test_fake_cms.py
git commit -m "test: FHIR fixture records, release builder, fake directory.cms.gov server"
```

---

### Task 6: Manifest

**Files:**
- Create: `src/npd_loader/manifest.py`
- Test: `tests/test_manifest.py`

**Interfaces:**
- Produces: `ManifestError`. `ManifestFile(name, compressed_bytes, original_bytes)`, where `name` is like `"06-Practitioner.ndjson"`, with properties `zst_name` and `resource_type`. `Manifest(release_date: date, files: tuple[ManifestFile, ...], raw: bytes)` with `.file(name) -> ManifestFile`. Functions: `parse_manifest(raw: bytes) -> Manifest`, `fetch_manifest(client: httpx.Client, url: str) -> Manifest` (follows redirects), `resource_type_for(file_name: str) -> str` (accepts `.ndjson`, `.ndjson.zst`, or a `file_{id}_` prefix), and `file_url(manifest_url, zst_name) -> str`.

- [ ] **Step 1: Write the failing tests**

```python
import json
from datetime import date

import httpx
import pytest

from npd_loader.manifest import (ManifestError, fetch_manifest, file_url, parse_manifest,
                                 resource_type_for)
from release_builder import build_release

REAL = {"compression_algorithm": "zstd",
        "files": {"01-Organization.ndjson": {"compressed_bytes": 298129327, "compression_ratio_pct": 92.5,
                                             "original_bytes": 3973425929},
                  "06-Practitioner.ndjson": {"compressed_bytes": 844943714, "compression_ratio_pct": 94.95,
                                             "original_bytes": 16739630848}},
        "generated_at": "2026-09-29", "totals": {}}


def raw(d) -> bytes:
    return json.dumps(d).encode()


def test_parse_real_format():
    m = parse_manifest(raw(REAL))
    assert m.release_date == date(2026, 9, 29)
    assert [f.name for f in m.files] == ["01-Organization.ndjson", "06-Practitioner.ndjson"]
    p = m.file("06-Practitioner.ndjson")
    assert (p.compressed_bytes, p.original_bytes, p.zst_name, p.resource_type) == (
        844943714, 16739630848, "06-Practitioner.ndjson.zst", "Practitioner")
    assert m.raw == raw(REAL)


def test_generated_at_may_be_a_timestamp():
    assert parse_manifest(raw({**REAL, "generated_at": "2026-09-29T03:00:00Z"})).release_date == date(2026, 9, 29)


@pytest.mark.parametrize("patch,message", [
    ({"compression_algorithm": "gzip"}, "compression_algorithm"),
    ({"generated_at": "soon"}, "generated_at"),
    ({"files": {}}, "no files"),
    ({"files": {"Organization.json": {"compressed_bytes": 1, "original_bytes": 1}}}, "file name"),
    ({"files": {"01-Organization.ndjson": {"compressed_bytes": 1}}}, "original_bytes"),
    ({"files": {"01-Organization.ndjson": {"compressed_bytes": -1, "original_bytes": 1}}}, "compressed_bytes"),
])
def test_rejects_bad_manifests(patch, message):
    with pytest.raises(ManifestError, match=message):
        parse_manifest(raw({**REAL, **patch}))


def test_rejects_non_json():
    with pytest.raises(ManifestError, match="JSON"):
        parse_manifest(b"<html>")


def test_resource_type_for():
    assert resource_type_for("07-PractitionerRole.ndjson") == "PractitionerRole"
    assert resource_type_for("07-PractitionerRole.ndjson.zst") == "PractitionerRole"
    assert resource_type_for("file_12345_08-OrganizationAffiliation.ndjson") == "OrganizationAffiliation"
    with pytest.raises(ManifestError):
        resource_type_for("practitioner.csv")


def test_file_url():
    assert file_url("https://directory.cms.gov/downloads/manifest.json", "01-Organization.ndjson.zst") == \
        "https://directory.cms.gov/downloads/01-Organization.ndjson.zst"


def test_fetch_follows_redirect(cms):
    cms.publish(build_release("2026-09-29"))
    with httpx.Client() as client:
        m = fetch_manifest(client, cms.manifest_url)
    assert m.release_date == date(2026, 9, 29)
    assert len(m.files) == 8
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_manifest.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'npd_loader.manifest'`

- [ ] **Step 3: Implement `manifest.py`**

```python
"""The release manifest: release date (generated_at) and the expected files with their sizes."""
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import date, datetime

import httpx

FILE_NAME_RE = re.compile(r"^(?:file_\d+_)?\d+-([A-Za-z]+)\.ndjson(?:\.zst)?$")


class ManifestError(Exception):
    pass


def resource_type_for(file_name: str) -> str:
    m = FILE_NAME_RE.match(file_name)
    if not m:
        raise ManifestError(f"unexpected file name {file_name!r} (expected NN-ResourceType.ndjson)")
    return m.group(1)


@dataclass(frozen=True)
class ManifestFile:
    name: str
    compressed_bytes: int
    original_bytes: int

    @property
    def zst_name(self) -> str:
        return self.name + ".zst"

    @property
    def resource_type(self) -> str:
        return resource_type_for(self.name)


@dataclass(frozen=True)
class Manifest:
    release_date: date
    files: tuple[ManifestFile, ...]
    raw: bytes

    def file(self, name: str) -> ManifestFile:
        for f in self.files:
            if f.name == name:
                return f
        raise ManifestError(f"{name} is not in the manifest for {self.release_date}")


def _release_date(value: object) -> date:
    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError:
            try:
                return datetime.fromisoformat(value).date()
            except ValueError:
                pass
    raise ManifestError(f"generated_at {value!r} is not a date")


def _size(entry: dict, name: str, key: str) -> int:
    value = entry.get(key)
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ManifestError(f"{name}: {key} must be a positive integer, got {value!r}")
    return value


def parse_manifest(raw: bytes) -> Manifest:
    try:
        data = json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise ManifestError(f"manifest is not valid JSON: {exc}") from exc
    if not isinstance(data, dict):
        raise ManifestError("manifest is not a JSON object")
    if data.get("compression_algorithm") != "zstd":
        raise ManifestError(f"unsupported compression_algorithm {data.get('compression_algorithm')!r}")
    release = _release_date(data.get("generated_at"))
    entries = data.get("files")
    if not isinstance(entries, dict) or not entries:
        raise ManifestError("manifest lists no files")
    files = []
    for name in sorted(entries):
        if not FILE_NAME_RE.match(name) or name.endswith(".zst"):
            raise ManifestError(f"unexpected file name {name!r} in manifest")
        entry = entries[name]
        if not isinstance(entry, dict):
            raise ManifestError(f"{name}: entry is not an object")
        files.append(ManifestFile(name, _size(entry, name, "compressed_bytes"), _size(entry, name, "original_bytes")))
    return Manifest(release, tuple(files), raw)


def fetch_manifest(client: httpx.Client, url: str) -> Manifest:
    response = client.get(url, follow_redirects=True)
    response.raise_for_status()
    return parse_manifest(response.content)


def file_url(manifest_url: str, zst_name: str) -> str:
    return manifest_url.rsplit("/", 1)[0] + "/" + zst_name
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_manifest.py -v`
Expected: 12 passed

- [ ] **Step 5: Commit**

```bash
git add src/npd_loader/manifest.py tests/test_manifest.py
git commit -m "feat: manifest parsing and fetching"
```

---

### Task 7: Stage plumbing and the DOWNLOAD stage

**Files:**
- Create: `src/npd_loader/stages.py`, `src/npd_loader/download.py`, `tests/helpers.py`
- Test: `tests/test_download.py`

**Interfaces:**
- Consumes: `Catalog`, `SUCCESS`, and `FAILED` (Task 3); `Storage` (Task 2); `Config` (Task 1); `fetch_manifest`, `file_url`, and `Manifest` (Task 6); `build_output_xml` and `build_config_xml` (Task 3).
- Produces in `stages.py`:
  - `Context(config, catalog, storage, http, lock, npd_conninfo=None, sleep=time.sleep)`, where `lock` is a `Callable[[str], ContextManager[bool]]`.
  - `Outcome` (StrEnum: `SUCCESS`, `SKIPPED`, `LOCKED`) and `StageFailed(Exception)`.
  - Helpers: `no_lock(stage)` (context manager yielding True), `run_folder(run) -> str`, `prefixed_name(file_id, original) -> str`, `original_name(file_name) -> str`, `describe(ctx, stage_label, release) -> str`, `config_xml(ctx, description, release, **options) -> str`, `fail_run(ctx, run, exc) -> None`, `sha256_of(storage, rel) -> str`, `read_bytes(storage, rel) -> bytes`, `completed_child(parent_id, rows) -> DataFile | None` (the newest row with that `parent_file` and a non-null `file_hash`), and `now() -> datetime` (naive local time, whole seconds).
- Produces in `download.py`: `run_download(ctx, force=False) -> Outcome`, `fetch_file(ctx, public_url, part_rel, expected_size) -> FetchResult`, `resolve_signed_url(client, public_url) -> str`, `DownloadError`, and `FatalDownloadError`.
- Produces in `tests/helpers.py`: `config_data(storage_root, manifest_url, npd_conninfo=None, catalog_conninfo=None, keep_releases=5) -> dict`, `make_ctx(tmp_path, cms, catalog=None, npd_conninfo=None, **config_kw) -> Context`, and `to_toml(data) -> str`.

- [ ] **Step 1: Write `tests/helpers.py`**

```python
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
```

- [ ] **Step 2: Write the failing tests `tests/test_download.py`**

```python
import hashlib
import re
from contextlib import contextmanager
from datetime import date

import pytest

from npd_loader.catalog import FAILED, SUCCESS
from npd_loader.download import run_download
from npd_loader.stages import Outcome, StageFailed
from helpers import make_ctx
from release_builder import build_release

R = date(2026, 9, 29)
PRAC = "06-Practitioner.ndjson.zst"


@pytest.fixture
def release(cms):
    rel = build_release("2026-09-29")
    cms.publish(rel)
    return rel


def zst_rows(ctx):
    return ctx.catalog.get_data_files(R, "ndjson.zst")


def test_downloads_release_and_records_catalog_rows(tmp_path, cms, release):
    ctx = make_ctx(tmp_path, cms)
    assert run_download(ctx) is Outcome.SUCCESS
    [run] = ctx.catalog.runs.values()
    assert run["status"] == SUCCESS and run["run_class"] == "DOWNLOAD"
    assert run["description"] == "NPD FHIR Download 2026-09-29"
    assert "<release_date>2026-09-29</release_date>" in run["config_xml"]
    assert "<file_set>NPD_FHIR</file_set>" in run["config_xml"]
    [manifest] = ctx.catalog.get_data_files(R, "manifest")
    assert manifest.parent_file is None
    assert manifest.source_uri == cms.manifest_url
    assert re.fullmatch(rf"run_{manifest.run_id}_\d{{4}}-\d\d-\d\d-\d{{6}}/file_{manifest.id}_manifest\.json",
                        manifest.file_rel_path)
    rows = zst_rows(ctx)
    assert len(rows) == 8
    for row in rows:
        original = row.file_name.removeprefix(f"file_{row.id}_")
        data = release.files[original]
        assert row.parent_file == manifest.id
        assert row.source_uri == cms.public_url(original)  # public URL, never the signed /s3/ one
        assert "/s3/" not in row.source_uri
        assert row.file_rel_path == f"{manifest.file_rel_path.split('/')[0]}/{row.file_name}"
        assert (row.file_size, row.file_hash) == (len(data), hashlib.sha256(data).hexdigest())
        assert row.date_modified is not None and row.date_created is not None
        with ctx.storage.open_read(row.file_rel_path) as f:
            assert f.read() == data
    assert not [p for p in ctx.storage.list("") if p.endswith(".part")]


def test_second_run_is_skipped_unless_forced(tmp_path, cms, release):
    ctx = make_ctx(tmp_path, cms)
    run_download(ctx)
    assert run_download(ctx) is Outcome.SKIPPED
    assert len(ctx.catalog.runs) == 1
    assert run_download(ctx, force=True) is Outcome.SUCCESS
    assert len(ctx.catalog.runs) == 2
    assert len(zst_rows(ctx)) == 16


def test_resumes_after_dropped_connection(tmp_path, cms, release):
    cms.drops[PRAC] = [100]
    ctx = make_ctx(tmp_path, cms)
    assert run_download(ctx) is Outcome.SUCCESS
    assert ("GET", f"/s3/{PRAC}", "bytes=100-") in cms.requests
    row = next(r for r in zst_rows(ctx) if r.file_name.endswith(PRAC))
    assert row.file_hash == hashlib.sha256(release.files[PRAC]).hexdigest()


def test_resume_when_server_ignores_range(tmp_path, cms, release):
    cms.drops[PRAC] = [100]
    cms.ignore_range.add(PRAC)
    ctx = make_ctx(tmp_path, cms)
    assert run_download(ctx) is Outcome.SUCCESS
    row = next(r for r in zst_rows(ctx) if r.file_name.endswith(PRAC))
    with ctx.storage.open_read(row.file_rel_path) as f:
        assert f.read() == release.files[PRAC]


def test_expired_signed_url_gets_a_fresh_one(tmp_path, cms, release):
    cms.expire_next.add(PRAC)
    ctx = make_ctx(tmp_path, cms)
    assert run_download(ctx) is Outcome.SUCCESS
    assert sum(1 for r in cms.requests if r[1] == f"/downloads/{PRAC}") == 2


def test_gives_up_after_max_attempts(tmp_path, cms, release):
    cms.drops[PRAC] = [10] * 10
    ctx = make_ctx(tmp_path, cms)
    with pytest.raises(StageFailed, match="giving up after 3 attempts"):
        run_download(ctx)
    [run] = ctx.catalog.runs.values()
    assert run["status"] == FAILED and "giving up" in run["result"]
    row = next(r for r in zst_rows(ctx) if r.file_name.endswith(PRAC))
    assert "giving up" in row.exceptions and row.file_hash is None


def test_size_mismatch_fails(tmp_path, cms):
    cms.publish(build_release("2026-09-29", manifest_overrides={"06-Practitioner.ndjson": {"compressed_bytes": 5}}))
    ctx = make_ctx(tmp_path, cms)
    with pytest.raises(StageFailed, match="more bytes than the manifest"):
        run_download(ctx)


def test_manifest_changing_mid_run_fails(tmp_path, cms):
    cms.publish(build_release("2026-09-29"), later_manifest=build_release("2026-10-06").manifest)
    ctx = make_ctx(tmp_path, cms)
    with pytest.raises(StageFailed, match="generated_at changed"):
        run_download(ctx)
    [run] = ctx.catalog.runs.values()
    assert run["status"] == FAILED


def test_lock_held_elsewhere_exits_quietly(tmp_path, cms, release):
    @contextmanager
    def busy(stage):
        yield False
    ctx = make_ctx(tmp_path, cms)
    ctx.lock = busy
    assert run_download(ctx) is Outcome.LOCKED
    assert ctx.catalog.runs == {}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `python -m pytest tests/test_download.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'npd_loader.stages'`

- [ ] **Step 4: Implement `stages.py`**

```python
"""Shared plumbing for the DOWNLOAD / EXTRACT / IMPORT stages."""
from __future__ import annotations

import hashlib
import logging
import re
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import date, datetime
from enum import StrEnum
from typing import Callable, ContextManager, Iterator, Sequence

import httpx

from npd_loader.catalog import FAILED, Catalog, DataFile, Run
from npd_loader.config import Config
from npd_loader.runxml import build_config_xml
from npd_loader.storage import Storage

log = logging.getLogger(__name__)


class StageFailed(Exception):
    pass


class Outcome(StrEnum):
    SUCCESS = "success"
    SKIPPED = "skipped"
    LOCKED = "locked"


@dataclass
class Context:
    config: Config
    catalog: Catalog
    storage: Storage
    http: httpx.Client
    lock: Callable[[str], ContextManager[bool]]
    npd_conninfo: str | None = None
    sleep: Callable[[float], None] = field(default=time.sleep)


@contextmanager
def no_lock(stage: str) -> Iterator[bool]:
    yield True


def now() -> datetime:
    return datetime.now().replace(microsecond=0)


def run_folder(run: Run) -> str:
    return f"run_{run.id}_{run.started_at:%Y-%m-%d-%H%M%S}"


def prefixed_name(file_id: int, original: str) -> str:
    return f"file_{file_id}_{original}"


def original_name(file_name: str) -> str:
    return re.sub(r"^file_\d+_", "", file_name)


def describe(ctx: Context, stage_label: str, release: date) -> str:
    return ctx.config.catalog.description_template.format(stage=stage_label, release=release.isoformat())


def config_xml(ctx: Context, description: str, release: date, **options: object) -> str:
    cat = ctx.config.catalog
    return build_config_xml({"run_type": cat.run_type, "file_set": cat.file_set,
                             "data_store_base_path": ctx.storage.uri(), "run_description": description,
                             "release_date": release.isoformat(), **options})


def fail_run(ctx: Context, run: Run, exc: BaseException) -> None:
    message = f"{type(exc).__name__}: {exc}"
    log.error("%s run %s failed: %s", run.run_class, run.id, message)
    try:
        ctx.catalog.finish_run(run, FAILED, result=message)
    except Exception:
        log.exception("could not record the failure of run %s in the catalog", run.id)


def sha256_of(storage: Storage, rel_path: str, chunk: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with storage.open_read(rel_path) as f:
        while block := f.read(chunk):
            h.update(block)
    return h.hexdigest()


def read_bytes(storage: Storage, rel_path: str) -> bytes:
    with storage.open_read(rel_path) as f:
        return f.read()


def completed_child(parent_id: int, rows: Sequence[DataFile]) -> DataFile | None:
    """Newest row derived from `parent_id` that finished (has a hash). Rows from failed runs have no hash."""
    done = [r for r in rows if r.parent_file == parent_id and r.file_hash]
    return max(done, key=lambda r: r.id, default=None)
```

- [ ] **Step 5: Implement `download.py`**

```python
"""DOWNLOAD stage: manifest + every .zst file into a new run folder, with resume and retries."""
from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from urllib.parse import urljoin

import httpx

from npd_loader.catalog import SUCCESS, Run
from npd_loader.manifest import Manifest, fetch_manifest, file_url
from npd_loader.runxml import build_output_xml
from npd_loader.stages import (Context, Outcome, StageFailed, config_xml, describe, fail_run, now,
                               prefixed_name, run_folder)

log = logging.getLogger(__name__)


class DownloadError(Exception):
    """Retryable: connection problems, 403 on an expired signed URL, short bodies."""


class FatalDownloadError(Exception):
    """Not retryable: the data itself disagrees with the manifest."""


@dataclass
class FetchResult:
    size: int
    sha256: str
    last_modified: datetime | None
    etag: str | None
    attempts: int


def run_download(ctx: Context, force: bool = False) -> Outcome:
    cfg = ctx.config
    with ctx.lock("download") as acquired:
        if not acquired:
            log.info("another download is running; nothing to do")
            return Outcome.LOCKED
        manifest = fetch_manifest(ctx.http, cfg.source.manifest_url)
        release = manifest.release_date
        if not force and ctx.catalog.last_successful_run(cfg.catalog.run_class_download, release):
            log.info("release %s is already downloaded", release)
            return Outcome.SKIPPED
        description = describe(ctx, "Download", release)
        run = ctx.catalog.start_run(cfg.catalog.run_class_download, description,
                                    config_xml(ctx, description, release, force=force))
        log.info("download run %s started for release %s", run.id, release)
        try:
            summary = _download_release(ctx, run, manifest)
        except Exception as exc:
            fail_run(ctx, run, exc)
            raise StageFailed(f"download of release {release} failed: {exc}") from exc
        ctx.catalog.finish_run(run, SUCCESS, output_xml=build_output_xml(summary, item_tag="file"))
        log.info("download run %s finished", run.id)
        return Outcome.SUCCESS


def _download_release(ctx: Context, run: Run, manifest: Manifest) -> list[dict[str, object]]:
    cfg = ctx.config
    cat = cfg.catalog
    folder = run_folder(run)
    release = manifest.release_date.isoformat()
    root = ctx.storage.uri()

    manifest_id = ctx.catalog.add_data_file(run, file_type=cat.file_type_manifest, source_uri=cfg.source.manifest_url,
                                            source_version_num=release, run_type_root_dir=root)
    name = prefixed_name(manifest_id, "manifest.json")
    rel = f"{folder}/{name}"
    with ctx.storage.open_write(rel) as f:
        f.write(manifest.raw)
    ctx.catalog.update_data_file(manifest_id, file_name=name, file_rel_path=rel, file_size=len(manifest.raw),
                                 file_hash=hashlib.sha256(manifest.raw).hexdigest(), date_created=now())

    summary: list[dict[str, object]] = []
    for mf in manifest.files:
        url = file_url(cfg.source.manifest_url, mf.zst_name)
        file_id = ctx.catalog.add_data_file(run, file_type=cat.file_type_zst, source_uri=url,
                                            source_version_num=release, run_type_root_dir=root,
                                            parent_file=manifest_id)
        name = prefixed_name(file_id, mf.zst_name)
        rel = f"{folder}/{name}"
        ctx.catalog.update_data_file(file_id, file_name=name, file_rel_path=rel)
        started = datetime.now()
        try:
            result = fetch_file(ctx, url, rel + ".part", mf.compressed_bytes)
        except Exception as exc:
            ctx.catalog.update_data_file(file_id, exceptions=str(exc))
            raise
        ctx.storage.rename(rel + ".part", rel)
        ctx.catalog.update_data_file(file_id, file_size=result.size, file_hash=result.sha256,
                                     date_modified=result.last_modified, date_created=now())
        summary.append({"name": mf.zst_name, "file_id": file_id, "size": result.size, "sha256": result.sha256,
                        "etag": result.etag, "attempts": result.attempts,
                        "seconds": round((datetime.now() - started).total_seconds(), 1)})
        log.info("downloaded %s (%d bytes, %d attempt(s))", mf.zst_name, result.size, result.attempts)

    after = fetch_manifest(ctx.http, cfg.source.manifest_url)
    if after.release_date != manifest.release_date:
        raise FatalDownloadError(f"manifest generated_at changed during download: "
                                 f"{manifest.release_date} -> {after.release_date}")
    return summary


def resolve_signed_url(client: httpx.Client, public_url: str) -> str:
    """The public URL 302-redirects to a short-lived signed URL; fetch a fresh one for every attempt."""
    with client.stream("GET", public_url, follow_redirects=False) as response:
        if response.is_redirect:
            return urljoin(public_url, response.headers["location"])
        if response.status_code == 200:
            return public_url
        raise DownloadError(f"GET {public_url}: HTTP {response.status_code}")


def _last_modified(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return parsedate_to_datetime(value).astimezone(timezone.utc).replace(tzinfo=None)
    except (TypeError, ValueError):
        return None


def fetch_file(ctx: Context, public_url: str, part_rel: str, expected_size: int) -> FetchResult:
    dl = ctx.config.download
    hasher = hashlib.sha256()
    written = 0
    attempt = 0
    last_modified: datetime | None = None
    etag: str | None = None
    with ctx.storage.open_write(part_rel) as out:
        while True:
            attempt += 1
            try:
                signed = resolve_signed_url(ctx.http, public_url)
                headers = {"Range": f"bytes={written}-"} if written else {}
                with ctx.http.stream("GET", signed, headers=headers, timeout=dl.timeout_seconds) as response:
                    if response.status_code not in (200, 206):
                        raise DownloadError(f"GET {public_url}: HTTP {response.status_code}")
                    if response.status_code == 206 and not response.headers.get(
                            "content-range", "").startswith(f"bytes {written}-"):
                        raise DownloadError(f"unexpected Content-Range {response.headers.get('content-range')!r}")
                    skip = written if response.status_code == 200 else 0  # server ignored Range
                    last_modified = last_modified or _last_modified(response.headers.get("last-modified"))
                    etag = etag or response.headers.get("etag")
                    for chunk in response.iter_bytes(dl.chunk_bytes):
                        if skip:
                            if len(chunk) <= skip:
                                skip -= len(chunk)
                                continue
                            chunk = chunk[skip:]
                            skip = 0
                        written += len(chunk)
                        if written > expected_size:
                            raise FatalDownloadError(f"{public_url}: received more bytes than the manifest's "
                                                     f"compressed_bytes ({expected_size})")
                        hasher.update(chunk)
                        out.write(chunk)
                if written == expected_size:
                    return FetchResult(written, hasher.hexdigest(), last_modified, etag, attempt)
                raise DownloadError(f"connection ended at {written} of {expected_size} bytes")
            except FatalDownloadError:
                raise
            except (httpx.HTTPError, DownloadError) as exc:
                if attempt >= dl.max_attempts:
                    raise DownloadError(f"{public_url}: giving up after {attempt} attempts: {exc}") from exc
                delay = dl.backoff_seconds * 2 ** (attempt - 1)
                log.warning("%s: attempt %d failed (%s); retrying from byte %d in %.0fs",
                            public_url, attempt, exc, written, delay)
                ctx.sleep(delay)
```

Note: when a server ignores `Range` and returns `200`, the `skip` logic throws away the first `written` bytes of the body. The file on disk then stays a single continuous copy.

- [ ] **Step 6: Run tests to verify they pass**

Run: `python -m pytest tests/test_download.py -v`
Expected: 9 passed

- [ ] **Step 7: Commit**

```bash
git add src/npd_loader/stages.py src/npd_loader/download.py tests/helpers.py tests/test_download.py
git commit -m "feat: DOWNLOAD stage with signed-URL refresh, Range resume, retries"
```

---

### Task 8: EXTRACT stage

**Files:**
- Create: `src/npd_loader/extract.py`
- Test: `tests/test_extract.py`

**Interfaces:**
- Consumes: from `stages.py`: `Context`, `Outcome`, `StageFailed`, `completed_child`, `config_xml`, `describe`, `fail_run`, `now`, `original_name`, `prefixed_name`, `read_bytes`, `run_folder`, and `sha256_of`. Also `parse_manifest` (Task 6) and `run_download` (tests only).
- Produces: `run_extract(ctx, release: date | None = None, force: bool = False) -> Outcome`, `decompress(storage, src_rel, dst_rel) -> tuple[int, str]` (bytes written, SHA-256), and `ExtractError`.

- [ ] **Step 1: Write the failing tests `tests/test_extract.py`**

```python
from datetime import date

import pytest

from npd_loader.catalog import FAILED, SUCCESS
from npd_loader.download import run_download
from npd_loader.extract import run_extract
from npd_loader.stages import Outcome, StageFailed
from helpers import make_ctx
from release_builder import build_release

R = date(2026, 9, 29)


@pytest.fixture
def downloaded(tmp_path, cms):
    rel = build_release("2026-09-29")
    cms.publish(rel)
    ctx = make_ctx(tmp_path, cms)
    run_download(ctx)
    return ctx, rel


def ndjson_rows(ctx):
    return ctx.catalog.get_data_files(R, "ndjson")


def test_first_extract_creates_rows_and_files(downloaded):
    ctx, rel = downloaded
    assert run_extract(ctx) is Outcome.SUCCESS
    zsts = {z.id: z for z in ctx.catalog.get_data_files(R, "ndjson.zst")}
    rows = ndjson_rows(ctx)
    assert len(rows) == 8
    extract_run = max(ctx.catalog.runs)
    assert ctx.catalog.runs[extract_run]["run_class"] == "EXTRACT"
    assert ctx.catalog.runs[extract_run]["status"] == SUCCESS
    for row in rows:
        zst = zsts[row.parent_file]
        original = row.file_name.removeprefix(f"file_{row.id}_")
        assert zst.file_name.endswith(original + ".zst")
        assert row.run_id == extract_run
        assert row.source_uri == zst.source_uri
        assert row.file_rel_path.startswith(f"run_{extract_run}_")
        with ctx.storage.open_read(row.file_rel_path) as f:
            assert f.read() == rel.ndjson[original]
        assert row.file_size == len(rel.ndjson[original])


def test_second_extract_is_skipped(downloaded):
    ctx, _ = downloaded
    run_extract(ctx)
    runs = len(ctx.catalog.runs)
    assert run_extract(ctx) is Outcome.SKIPPED
    assert len(ctx.catalog.runs) == runs


def test_missing_file_is_restored_with_same_id(downloaded):
    ctx, rel = downloaded
    run_extract(ctx)
    before = ndjson_rows(ctx)
    victim = before[3]
    ctx.storage.delete(victim.file_rel_path)
    assert run_extract(ctx) is Outcome.SUCCESS
    after = ndjson_rows(ctx)
    assert [r.id for r in after] == [r.id for r in before]
    assert ctx.storage.exists(victim.file_rel_path)
    assert 'action="restored"' in ctx.catalog.runs[max(ctx.catalog.runs)]["output_xml"]


def test_restore_with_hash_mismatch_fails_and_keeps_rows(downloaded):
    ctx, _ = downloaded
    run_extract(ctx)
    victim = ndjson_rows(ctx)[0]
    ctx.storage.delete(victim.file_rel_path)
    ctx.catalog.update_data_file(victim.id, file_hash="0" * 64)
    with pytest.raises(StageFailed, match="does not match"):
        run_extract(ctx)
    assert ctx.catalog.runs[max(ctx.catalog.runs)]["status"] == FAILED
    assert len(ndjson_rows(ctx)) == 8
    assert not ctx.storage.exists(victim.file_rel_path)
    assert "does not match" in ctx.catalog.get_data_files(R, "ndjson")[0].exceptions


def test_incomplete_row_from_failed_run_is_ignored(downloaded):
    ctx, _ = downloaded
    zst = ctx.catalog.get_data_files(R, "ndjson.zst")[0]
    dead_run = ctx.catalog.start_run("EXTRACT", "died", "<WAREHOUSE_RUN_CONFIG/>")
    dead = ctx.catalog.add_data_file(dead_run, file_type="ndjson", source_version_num="2026-09-29",
                                     parent_file=zst.id, file_name="file_1_x.ndjson",
                                     file_rel_path="run_1_2026-01-01-000000/file_1_x.ndjson")
    assert run_extract(ctx) is Outcome.SUCCESS
    children = [r for r in ndjson_rows(ctx) if r.parent_file == zst.id]
    assert len(children) == 2
    completed = [r for r in children if r.file_hash]
    assert len(completed) == 1 and completed[0].id != dead


def test_original_bytes_mismatch_fails(tmp_path, cms):
    cms.publish(build_release("2026-09-29",
                              manifest_overrides={"02-Location.ndjson": {"original_bytes": 10}}))
    ctx = make_ctx(tmp_path, cms)
    run_download(ctx)
    with pytest.raises(StageFailed, match="original_bytes"):
        run_extract(ctx)


def test_no_download_fails(tmp_path, cms):
    ctx = make_ctx(tmp_path, cms)
    with pytest.raises(StageFailed, match="no successful download"):
        run_extract(ctx)


def test_force_reextracts_with_same_ids(downloaded):
    ctx, _ = downloaded
    run_extract(ctx)
    ids = [r.id for r in ndjson_rows(ctx)]
    assert run_extract(ctx, force=True) is Outcome.SUCCESS
    assert [r.id for r in ndjson_rows(ctx)] == ids
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_extract.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'npd_loader.extract'`

- [ ] **Step 3: Implement `extract.py`**

```python
"""EXTRACT stage: decompress each .zst of a release into a run folder, or restore a deleted .ndjson in place."""
from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
from datetime import date

import zstandard

from npd_loader.catalog import SUCCESS, DataFile, Run
from npd_loader.manifest import parse_manifest
from npd_loader.runxml import build_output_xml
from npd_loader.stages import (Context, Outcome, StageFailed, completed_child, config_xml, describe, fail_run, now,
                               original_name, prefixed_name, read_bytes, run_folder, sha256_of)
from npd_loader.storage import Storage

log = logging.getLogger(__name__)
CHUNK = 1 << 20


class ExtractError(Exception):
    pass


@dataclass
class ExtractItem:
    zst: DataFile
    ndjson: DataFile | None
    name: str
    original_bytes: int
    complete: bool


def decompress(storage: Storage, src_rel: str, dst_rel: str) -> tuple[int, str]:
    hasher = hashlib.sha256()
    size = 0
    with storage.open_read(src_rel) as src, storage.open_write(dst_rel) as dst:
        reader = zstandard.ZstdDecompressor().stream_reader(src, read_across_frames=True)
        while block := reader.read(CHUNK):
            hasher.update(block)
            dst.write(block)
            size += len(block)
    return size, hasher.hexdigest()


def _file_matches(storage: Storage, row: DataFile) -> bool:
    rel = row.file_rel_path
    return bool(rel) and storage.exists(rel) and storage.size(rel) == row.file_size \
        and sha256_of(storage, rel) == row.file_hash


def plan_extract(ctx: Context, release: date, download_run: Run) -> list[ExtractItem]:
    cat = ctx.config.catalog
    manifests = ctx.catalog.get_data_files(release, cat.file_type_manifest, run_id=download_run.id)
    if not manifests:
        raise ExtractError(f"download run {download_run.id} has no manifest row")
    manifest = parse_manifest(read_bytes(ctx.storage, manifests[0].file_rel_path))
    ndjsons = ctx.catalog.get_data_files(release, cat.file_type_ndjson)
    items = []
    for zst in ctx.catalog.get_data_files(release, cat.file_type_zst, run_id=download_run.id):
        name = original_name(zst.file_name).removesuffix(".zst")
        child = completed_child(zst.id, ndjsons)
        items.append(ExtractItem(zst, child, name, manifest.file(name).original_bytes,
                                 complete=child is not None and _file_matches(ctx.storage, child)))
    return items


def run_extract(ctx: Context, release: date | None = None, force: bool = False) -> Outcome:
    cat = ctx.config.catalog
    with ctx.lock("extract") as acquired:
        if not acquired:
            log.info("another extract is running; nothing to do")
            return Outcome.LOCKED
        download_run = ctx.catalog.last_successful_run(cat.run_class_download, release)
        if download_run is None:
            raise StageFailed(f"no successful download for release {release or '(any)'}")
        release = download_run.release_date
        items = plan_extract(ctx, release, download_run)
        if not force and all(item.complete for item in items):
            log.info("release %s is already extracted", release)
            return Outcome.SKIPPED
        description = describe(ctx, "Extract", release)
        run = ctx.catalog.start_run(cat.run_class_extract, description,
                                    config_xml(ctx, description, release, force=force,
                                               download_run_id=download_run.id))
        log.info("extract run %s started for release %s", run.id, release)
        try:
            summary = [_extract_one(ctx, run, release, item, force) for item in items]
        except Exception as exc:
            fail_run(ctx, run, exc)
            raise StageFailed(f"extract of release {release} failed: {exc}") from exc
        ctx.catalog.finish_run(run, SUCCESS, output_xml=build_output_xml(summary, item_tag="file"))
        return Outcome.SUCCESS


def _extract_one(ctx: Context, run: Run, release: date, item: ExtractItem, force: bool) -> dict[str, object]:
    cat = ctx.config.catalog
    if item.complete and not force:
        return {"name": item.name, "file_id": item.ndjson.id, "action": "present"}

    if item.ndjson is None:
        file_id = ctx.catalog.add_data_file(run, file_type=cat.file_type_ndjson, source_uri=item.zst.source_uri,
                                            source_version_num=release.isoformat(),
                                            run_type_root_dir=ctx.storage.uri(), parent_file=item.zst.id)
        name = prefixed_name(file_id, item.name)
        rel = f"{run_folder(run)}/{name}"
        ctx.catalog.update_data_file(file_id, file_name=name, file_rel_path=rel)
        size, digest = decompress(ctx.storage, item.zst.file_rel_path, rel + ".part")
        if size != item.original_bytes:
            ctx.storage.delete(rel + ".part")
            message = f"{item.name}: decompressed {size} bytes but the manifest's original_bytes is {item.original_bytes}"
            ctx.catalog.update_data_file(file_id, exceptions=message)
            raise ExtractError(message)
        ctx.storage.rename(rel + ".part", rel)
        ctx.catalog.update_data_file(file_id, file_size=size, file_hash=digest, date_created=now())
        log.info("extracted %s (%d bytes)", rel, size)
        return {"name": item.name, "file_id": file_id, "action": "created", "size": size}

    row = item.ndjson
    rel = row.file_rel_path
    size, digest = decompress(ctx.storage, item.zst.file_rel_path, rel + ".part")
    if size != row.file_size or digest != row.file_hash:
        ctx.storage.delete(rel + ".part")
        message = (f"re-extracted {rel} does not match data_file {row.id} "
                   f"(size {size} vs {row.file_size}, sha256 {digest} vs {row.file_hash})")
        ctx.catalog.update_data_file(row.id, exceptions=message)
        raise ExtractError(message)
    ctx.storage.rename(rel + ".part", rel)
    log.info("restored %s as data_file %s", rel, row.id)
    return {"name": item.name, "file_id": row.id, "action": "restored", "size": size}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_extract.py -v`
Expected: 8 passed

- [ ] **Step 5: Commit**

```bash
git add src/npd_loader/extract.py tests/test_extract.py
git commit -m "feat: EXTRACT stage with in-place restore that reuses data_file ids"
```

---

### Task 9: JSON path profiler

**Files:**
- Create: `src/npd_loader/profile.py`, `src/npd_loader/sql/mapped_paths.txt`
- Test: `tests/test_profile.py`

**Interfaces:**
- Produces: `leaf_paths(obj, prefix="") -> Iterator[str]`. Arrays become `[]`, and an extension element becomes `.extension[<url>]` with its `url` key dropped. Also `Profile`, a class with `resources: Counter[str]` (records per type) and `paths: dict[str, Counter[str]]` (resource type → path → occurrences); `profile_records(records: Iterable[dict]) -> Profile`; `profile_file(path: str | Path, limit: int | None = None) -> Profile` (handles `.ndjson` or `.zst`); `load_mapped_paths() -> set[tuple[str, str]]`; `unmapped(profile, mapped) -> list[tuple[str, str, int]]`; and `format_report(profile, rows=None) -> str`.
- `mapped_paths.txt` format: one `<ResourceType or *> <path>` per line, with `#` comments. Each listed path is either consumed by a transform or deliberately kept raw-only (raw-only lines sit under a `# raw-only` comment). Tasks 12–14 add lines.

- [ ] **Step 1: Write the failing tests**

```python
import json

import zstandard

from npd_loader.profile import leaf_paths, load_mapped_paths, profile_file, profile_records, unmapped
from fixture_data import PRAC1


def test_leaf_paths_arrays_and_extensions():
    paths = list(leaf_paths({"resourceType": "Practitioner", "name": [{"given": ["A", "B"]}],
                             "extension": [{"url": "http://x/flag", "valueBoolean": True}]}))
    assert paths == [".resourceType", ".name[].given[]", ".name[].given[]", ".extension[http://x/flag].valueBoolean"]


def test_profile_counts_per_type():
    prof = profile_records([PRAC1, PRAC1, {"resourceType": "Location", "id": "L"}])
    assert prof.resources == {"Practitioner": 2, "Location": 1}
    assert prof.paths["Practitioner"][".telecom[].value"] == 6
    assert prof.paths["Location"][".id"] == 1


def test_profile_reads_zst(tmp_path):
    path = tmp_path / "06-Practitioner.ndjson.zst"
    path.write_bytes(zstandard.ZstdCompressor().compress((json.dumps(PRAC1) + "\n").encode() * 3))
    assert profile_file(path).resources == {"Practitioner": 3}
    assert profile_file(path, limit=1).resources == {"Practitioner": 1}


def test_unmapped_honours_wildcards():
    prof = profile_records([{"resourceType": "Location", "id": "L", "status": "active", "mode": "x"}])
    mapped = {("*", ".id"), ("*", ".resourceType"), ("Location", ".status")}
    assert unmapped(prof, mapped) == [("Location", ".mode", 1)]


def test_mapped_paths_file_loads():
    mapped = load_mapped_paths()
    assert ("*", ".id") in mapped and ("*", ".meta.lastUpdated") in mapped
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_profile.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'npd_loader.profile'`

- [ ] **Step 3: Implement `profile.py` and the initial `mapped_paths.txt`**

`src/npd_loader/sql/mapped_paths.txt`:

```text
# JSON paths that the sql/transform scripts flatten, or that are deliberately left raw-only.
# Format: <ResourceType or *> <path>. Paths use [] for arrays and .extension[<url>] for extensions.
# `npd-loader profile FILE --unmapped` lists every path in FILE that is not listed here.
* .id
* .resourceType
* .meta.lastUpdated
```

`src/npd_loader/profile.py`:

```python
"""Profile the JSON paths in an NDJSON(.zst) file, and compare them with what the transforms map."""
from __future__ import annotations

import io
import json
from collections import Counter
from dataclasses import dataclass, field
from importlib import resources
from pathlib import Path
from typing import BinaryIO, Iterable, Iterator

import zstandard


def leaf_paths(obj: object, prefix: str = "") -> Iterator[str]:
    if isinstance(obj, dict):
        for key, value in obj.items():
            if key == "extension" and isinstance(value, list):
                for ext in value:
                    if isinstance(ext, dict) and isinstance(ext.get("url"), str):
                        rest = {k: v for k, v in ext.items() if k != "url"}
                        yield from leaf_paths(rest, f"{prefix}.extension[{ext['url']}]")
                    else:
                        yield from leaf_paths(ext, f"{prefix}.extension[]")
            else:
                yield from leaf_paths(value, f"{prefix}.{key}")
    elif isinstance(obj, list):
        for value in obj:
            yield from leaf_paths(value, prefix + "[]")
    else:
        yield prefix


@dataclass
class Profile:
    resources: Counter = field(default_factory=Counter)
    paths: dict[str, Counter] = field(default_factory=dict)

    def add(self, record: dict) -> None:
        rtype = str(record.get("resourceType"))
        self.resources[rtype] += 1
        self.paths.setdefault(rtype, Counter()).update(leaf_paths(record))


def profile_records(records: Iterable[dict]) -> Profile:
    prof = Profile()
    for record in records:
        prof.add(record)
    return prof


def _open(path: Path) -> BinaryIO:
    raw = open(path, "rb")
    if path.suffix == ".zst":
        return io.BufferedReader(zstandard.ZstdDecompressor().stream_reader(raw, read_across_frames=True,
                                                                            closefd=True))
    return raw


def profile_file(path: str | Path, limit: int | None = None) -> Profile:
    prof = Profile()
    with _open(Path(path)) as f:
        for n, line in enumerate(f):
            if limit is not None and n >= limit:
                break
            if line.strip():
                prof.add(json.loads(line))
    return prof


def load_mapped_paths() -> set[tuple[str, str]]:
    text = (resources.files("npd_loader") / "sql" / "mapped_paths.txt").read_text(encoding="utf-8")
    mapped = set()
    for line in text.splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            rtype, path = line.split(maxsplit=1)
            mapped.add((rtype, path))
    return mapped


def unmapped(prof: Profile, mapped: set[tuple[str, str]]) -> list[tuple[str, str, int]]:
    return sorted((rtype, path, count) for rtype, counter in prof.paths.items() for path, count in counter.items()
                  if (rtype, path) not in mapped and ("*", path) not in mapped)


def format_report(prof: Profile, rows: list[tuple[str, str, int]] | None = None) -> str:
    lines = []
    for rtype in sorted(prof.resources):
        lines.append(f"== {rtype}: {prof.resources[rtype]} resources")
        selected = sorted(prof.paths[rtype].items()) if rows is None else \
            [(path, count) for t, path, count in rows if t == rtype]
        lines.extend(f"{count:>12} {path}" for path, count in selected)
    return "\n".join(lines)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_profile.py -v`
Expected: 5 passed

- [ ] **Step 5: Commit**

```bash
git add src/npd_loader/profile.py src/npd_loader/sql/mapped_paths.txt tests/test_profile.py
git commit -m "feat: JSON path profiler and mapped-path coverage list"
```

---
### Task 10: `npd` database objects and DB helpers

**Files:**
- Create: `src/npd_loader/db.py`, `src/npd_loader/schema.py`, `src/npd_loader/sql/init/001_schemas.sql`, `src/npd_loader/sql/init/002_raw.sql`, `src/npd_loader/sql/init/003_tables.sql`
- Modify: `tests/conftest.py` (add `npd_db` fixture)
- Test: `tests/test_schema.py`

**Interfaces:**
- Produces in `db.py`:
  - `render_sql(text, tokens: Mapping[str, sql.Composable], conn) -> str` replaces `<<name>>` / `<<t:name>>` and raises `KeyError` on unknown tokens.
  - `standalone_name(table, release, run_id) -> str` returns `"{table}__{yyyymmdd}__r{run_id}"` and raises `ValueError` if longer than 63 characters.
  - `list_parent_tables(conn, schema) -> list[str]` returns top-level partitioned tables, sorted.
  - `list_release_partitions(conn, schema, parent) -> dict[date, str]` maps release date to partition table name.
  - `clone_parent_indexes(conn, schema, parent, target: sql.Composable) -> None`.
  - `advisory_lock(conninfo, name) -> ContextManager[bool]`.
  - `published_releases(conninfo, schema) -> list[date]`.
- Produces in `schema.py`: `init_db(conninfo, raw_schema, schema) -> None` (idempotent).
- SQL helper functions in schema `npd`: `ref_id(text)`, `ext(jsonb, url text) -> jsonb`, `join_text(jsonb, sep text) -> text`, `identifier_value(jsonb, systems text[]) -> text`, `fhir_ts(text) -> timestamptz`.
- Parent tables (all `PARTITION BY LIST (release_date)`): `npd_raw.resource`, plus the 24 tables in `NPD_TABLES` below. Also non-partitioned `npd.release`. There is a view `v_<table>` for each parent in its own schema.
- Fixture `npd_db` → conninfo of a new database with `init_db(…, "npd_raw", "npd")` applied.

- [ ] **Step 1: Write the init SQL**

`src/npd_loader/sql/init/001_schemas.sql`:

```sql
CREATE SCHEMA IF NOT EXISTS <<raw_schema>>;
CREATE SCHEMA IF NOT EXISTS <<schema>>;

-- One row per published release; written in the same transaction that attaches its partitions.
CREATE TABLE IF NOT EXISTS <<schema>>.release (
    release_date   date        PRIMARY KEY,
    import_run_id  integer     NOT NULL,
    published_at   timestamptz NOT NULL DEFAULT now()
);

-- "Organization/Organization-123" -> "Organization-123"
CREATE OR REPLACE FUNCTION <<schema>>.ref_id(ref text) RETURNS text
LANGUAGE sql IMMUTABLE AS $$ SELECT substring(ref from '([^/]+)$') $$;

-- First extension element with the given url, or NULL.
CREATE OR REPLACE FUNCTION <<schema>>.ext(resource jsonb, url text) RETURNS jsonb
LANGUAGE sql IMMUTABLE AS $$
    SELECT e FROM jsonb_array_elements(coalesce(resource->'extension', '[]'::jsonb)) AS e
    WHERE e->>'url' = url LIMIT 1
$$;

-- Join a JSON array of strings; NULL when empty.
CREATE OR REPLACE FUNCTION <<schema>>.join_text(arr jsonb, sep text) RETURNS text
LANGUAGE sql IMMUTABLE AS $$
    SELECT nullif(string_agg(t.v, sep ORDER BY t.ord), '')
    FROM jsonb_array_elements_text(coalesce(arr, '[]'::jsonb)) WITH ORDINALITY AS t(v, ord)
$$;

-- Value of the first identifier whose system is in `systems`.
CREATE OR REPLACE FUNCTION <<schema>>.identifier_value(resource jsonb, systems text[]) RETURNS text
LANGUAGE sql IMMUTABLE AS $$
    SELECT i.e->>'value'
    FROM jsonb_array_elements(coalesce(resource->'identifier', '[]'::jsonb)) WITH ORDINALITY AS i(e, ord)
    WHERE i.e->>'system' = ANY (systems)
    ORDER BY i.ord LIMIT 1
$$;

-- FHIR dateTime, which may be partial ("2020", "2020-05"), as timestamptz.
CREATE OR REPLACE FUNCTION <<schema>>.fhir_ts(v text) RETURNS timestamptz
LANGUAGE sql STABLE AS $$
    SELECT CASE
        WHEN v IS NULL THEN NULL
        WHEN v ~ '^\d{4}$' THEN (v || '-01-01')::timestamptz
        WHEN v ~ '^\d{4}-\d{2}$' THEN (v || '-01')::timestamptz
        ELSE v::timestamptz
    END
$$;
```

`src/npd_loader/sql/init/002_raw.sql`:

```sql
-- Each release partition is itself partitioned BY LIST (resource_type); see raw_load.py.
CREATE TABLE IF NOT EXISTS <<raw_schema>>.resource (
    release_date    date        NOT NULL,
    resource_type   text        NOT NULL,
    resource_id     text        NOT NULL,
    last_updated    timestamptz,
    ndjson_file_id  integer     NOT NULL,
    zst_file_id     integer     NOT NULL,
    line_number     bigint      NOT NULL,
    resource        jsonb       NOT NULL
) PARTITION BY LIST (release_date);

CREATE UNIQUE INDEX IF NOT EXISTS resource_key ON <<raw_schema>>.resource (release_date, resource_type, resource_id);
```

`src/npd_loader/sql/init/003_tables.sql`:

```sql
-- Every table: release_date, resource_id, ndjson_file_id, zst_file_id first; child tables add seq (1-based
-- position in the repeating element). Column lists come from profiling release 2026-09-29.

CREATE TABLE IF NOT EXISTS <<schema>>.practitioner (
    release_date date NOT NULL, resource_id text NOT NULL, ndjson_file_id integer NOT NULL, zst_file_id integer NOT NULL,
    last_updated timestamptz,
    npi text,
    active boolean,
    gender text,
    name_family text,
    name_given text,
    name_prefix text,
    name_suffix text,
    identity_verified boolean,
    medicare_enrolled boolean,
    in_hhs_exclusion_list boolean,
    aligned_with_data_network boolean
) PARTITION BY LIST (release_date);
CREATE UNIQUE INDEX IF NOT EXISTS practitioner_key ON <<schema>>.practitioner (release_date, resource_id);
CREATE INDEX IF NOT EXISTS practitioner_npi ON <<schema>>.practitioner (release_date, npi);

CREATE TABLE IF NOT EXISTS <<schema>>.practitioner_name (
    release_date date NOT NULL, resource_id text NOT NULL, ndjson_file_id integer NOT NULL, zst_file_id integer NOT NULL,
    seq integer NOT NULL,
    use text, family text, given text, prefix text, suffix text,
    period_start timestamptz, period_end timestamptz
) PARTITION BY LIST (release_date);
CREATE UNIQUE INDEX IF NOT EXISTS practitioner_name_key ON <<schema>>.practitioner_name (release_date, resource_id, seq);

CREATE TABLE IF NOT EXISTS <<schema>>.practitioner_address (
    release_date date NOT NULL, resource_id text NOT NULL, ndjson_file_id integer NOT NULL, zst_file_id integer NOT NULL,
    seq integer NOT NULL,
    use text, type text, line1 text, line2 text, extra_lines text,
    city text, state text, postal_code text, country text
) PARTITION BY LIST (release_date);
CREATE UNIQUE INDEX IF NOT EXISTS practitioner_address_key ON <<schema>>.practitioner_address (release_date, resource_id, seq);

CREATE TABLE IF NOT EXISTS <<schema>>.practitioner_telecom (
    release_date date NOT NULL, resource_id text NOT NULL, ndjson_file_id integer NOT NULL, zst_file_id integer NOT NULL,
    seq integer NOT NULL,
    system text, use text, value text
) PARTITION BY LIST (release_date);
CREATE UNIQUE INDEX IF NOT EXISTS practitioner_telecom_key ON <<schema>>.practitioner_telecom (release_date, resource_id, seq);

CREATE TABLE IF NOT EXISTS <<schema>>.practitioner_qualification (
    release_date date NOT NULL, resource_id text NOT NULL, ndjson_file_id integer NOT NULL, zst_file_id integer NOT NULL,
    seq integer NOT NULL,
    code_system text, code text, code_display text, code_text text,
    identifier_value text, identifier_type_code text, issuer_organization_id text
) PARTITION BY LIST (release_date);
CREATE UNIQUE INDEX IF NOT EXISTS practitioner_qualification_key ON <<schema>>.practitioner_qualification (release_date, resource_id, seq);
CREATE INDEX IF NOT EXISTS practitioner_qualification_code ON <<schema>>.practitioner_qualification (release_date, code);

CREATE TABLE IF NOT EXISTS <<schema>>.organization (
    release_date date NOT NULL, resource_id text NOT NULL, ndjson_file_id integer NOT NULL, zst_file_id integer NOT NULL,
    last_updated timestamptz,
    npi text,
    pseudo_ein text,
    name text,
    active boolean,
    type_code text,
    type_display text,
    part_of_organization_id text,
    verification_status text
) PARTITION BY LIST (release_date);
CREATE UNIQUE INDEX IF NOT EXISTS organization_key ON <<schema>>.organization (release_date, resource_id);
CREATE INDEX IF NOT EXISTS organization_npi ON <<schema>>.organization (release_date, npi);

CREATE TABLE IF NOT EXISTS <<schema>>.organization_address (
    release_date date NOT NULL, resource_id text NOT NULL, ndjson_file_id integer NOT NULL, zst_file_id integer NOT NULL,
    seq integer NOT NULL,
    use text, type text, line1 text, line2 text, extra_lines text,
    city text, state text, postal_code text, country text
) PARTITION BY LIST (release_date);
CREATE UNIQUE INDEX IF NOT EXISTS organization_address_key ON <<schema>>.organization_address (release_date, resource_id, seq);

CREATE TABLE IF NOT EXISTS <<schema>>.organization_telecom (
    release_date date NOT NULL, resource_id text NOT NULL, ndjson_file_id integer NOT NULL, zst_file_id integer NOT NULL,
    seq integer NOT NULL,
    system text, use text, value text
) PARTITION BY LIST (release_date);
CREATE UNIQUE INDEX IF NOT EXISTS organization_telecom_key ON <<schema>>.organization_telecom (release_date, resource_id, seq);

CREATE TABLE IF NOT EXISTS <<schema>>.organization_endpoint (
    release_date date NOT NULL, resource_id text NOT NULL, ndjson_file_id integer NOT NULL, zst_file_id integer NOT NULL,
    seq integer NOT NULL,
    endpoint_id text
) PARTITION BY LIST (release_date);
CREATE UNIQUE INDEX IF NOT EXISTS organization_endpoint_key ON <<schema>>.organization_endpoint (release_date, resource_id, seq);

CREATE TABLE IF NOT EXISTS <<schema>>.location (
    release_date date NOT NULL, resource_id text NOT NULL, ndjson_file_id integer NOT NULL, zst_file_id integer NOT NULL,
    last_updated timestamptz,
    status text, name text, description text, mode text,
    address_use text, address_type text, line1 text, line2 text, extra_lines text,
    city text, state text, postal_code text, country text,
    latitude double precision, longitude double precision,
    managing_organization_id text
) PARTITION BY LIST (release_date);
CREATE UNIQUE INDEX IF NOT EXISTS location_key ON <<schema>>.location (release_date, resource_id);
CREATE INDEX IF NOT EXISTS location_managing_organization ON <<schema>>.location (release_date, managing_organization_id);

CREATE TABLE IF NOT EXISTS <<schema>>.location_telecom (
    release_date date NOT NULL, resource_id text NOT NULL, ndjson_file_id integer NOT NULL, zst_file_id integer NOT NULL,
    seq integer NOT NULL,
    system text, use text, value text
) PARTITION BY LIST (release_date);
CREATE UNIQUE INDEX IF NOT EXISTS location_telecom_key ON <<schema>>.location_telecom (release_date, resource_id, seq);

CREATE TABLE IF NOT EXISTS <<schema>>.endpoint (
    release_date date NOT NULL, resource_id text NOT NULL, ndjson_file_id integer NOT NULL, zst_file_id integer NOT NULL,
    last_updated timestamptz,
    status text, name text, address text,
    connection_type_system text, connection_type_code text,
    payload_type_system text, payload_type_code text,
    managing_organization_id text,
    verification_status text
) PARTITION BY LIST (release_date);
CREATE UNIQUE INDEX IF NOT EXISTS endpoint_key ON <<schema>>.endpoint (release_date, resource_id);

CREATE TABLE IF NOT EXISTS <<schema>>.practitioner_role (
    release_date date NOT NULL, resource_id text NOT NULL, ndjson_file_id integer NOT NULL, zst_file_id integer NOT NULL,
    last_updated timestamptz,
    active boolean,
    practitioner_id text,
    organization_id text,
    period_start timestamptz, period_end timestamptz
) PARTITION BY LIST (release_date);
CREATE UNIQUE INDEX IF NOT EXISTS practitioner_role_key ON <<schema>>.practitioner_role (release_date, resource_id);
CREATE INDEX IF NOT EXISTS practitioner_role_practitioner ON <<schema>>.practitioner_role (release_date, practitioner_id);
CREATE INDEX IF NOT EXISTS practitioner_role_organization ON <<schema>>.practitioner_role (release_date, organization_id);

CREATE TABLE IF NOT EXISTS <<schema>>.practitioner_role_endpoint (
    release_date date NOT NULL, resource_id text NOT NULL, ndjson_file_id integer NOT NULL, zst_file_id integer NOT NULL,
    seq integer NOT NULL,
    endpoint_id text
) PARTITION BY LIST (release_date);
CREATE UNIQUE INDEX IF NOT EXISTS practitioner_role_endpoint_key ON <<schema>>.practitioner_role_endpoint (release_date, resource_id, seq);

CREATE TABLE IF NOT EXISTS <<schema>>.practitioner_role_location (
    release_date date NOT NULL, resource_id text NOT NULL, ndjson_file_id integer NOT NULL, zst_file_id integer NOT NULL,
    seq integer NOT NULL,
    location_id text
) PARTITION BY LIST (release_date);
CREATE UNIQUE INDEX IF NOT EXISTS practitioner_role_location_key ON <<schema>>.practitioner_role_location (release_date, resource_id, seq);

CREATE TABLE IF NOT EXISTS <<schema>>.practitioner_role_specialty (
    release_date date NOT NULL, resource_id text NOT NULL, ndjson_file_id integer NOT NULL, zst_file_id integer NOT NULL,
    seq integer NOT NULL,
    system text, code text, display text, text text
) PARTITION BY LIST (release_date);
CREATE UNIQUE INDEX IF NOT EXISTS practitioner_role_specialty_key ON <<schema>>.practitioner_role_specialty (release_date, resource_id, seq);

CREATE TABLE IF NOT EXISTS <<schema>>.practitioner_role_code (
    release_date date NOT NULL, resource_id text NOT NULL, ndjson_file_id integer NOT NULL, zst_file_id integer NOT NULL,
    seq integer NOT NULL,
    system text, code text, display text, text text
) PARTITION BY LIST (release_date);
CREATE UNIQUE INDEX IF NOT EXISTS practitioner_role_code_key ON <<schema>>.practitioner_role_code (release_date, resource_id, seq);

CREATE TABLE IF NOT EXISTS <<schema>>.organization_affiliation (
    release_date date NOT NULL, resource_id text NOT NULL, ndjson_file_id integer NOT NULL, zst_file_id integer NOT NULL,
    last_updated timestamptz,
    active boolean,
    organization_id text,
    participating_organization_id text,
    role_code text, role_display text, role_text text,
    period_start timestamptz, period_end timestamptz
) PARTITION BY LIST (release_date);
CREATE UNIQUE INDEX IF NOT EXISTS organization_affiliation_key ON <<schema>>.organization_affiliation (release_date, resource_id);

CREATE TABLE IF NOT EXISTS <<schema>>.healthcare_service (
    release_date date NOT NULL, resource_id text NOT NULL, ndjson_file_id integer NOT NULL, zst_file_id integer NOT NULL,
    last_updated timestamptz,
    active boolean,
    name text,
    provided_by_organization_id text,
    network_organization_id text
) PARTITION BY LIST (release_date);
CREATE UNIQUE INDEX IF NOT EXISTS healthcare_service_key ON <<schema>>.healthcare_service (release_date, resource_id);

CREATE TABLE IF NOT EXISTS <<schema>>.healthcare_service_location (
    release_date date NOT NULL, resource_id text NOT NULL, ndjson_file_id integer NOT NULL, zst_file_id integer NOT NULL,
    seq integer NOT NULL,
    location_id text
) PARTITION BY LIST (release_date);
CREATE UNIQUE INDEX IF NOT EXISTS healthcare_service_location_key ON <<schema>>.healthcare_service_location (release_date, resource_id, seq);

CREATE TABLE IF NOT EXISTS <<schema>>.insurance_plan (
    release_date date NOT NULL, resource_id text NOT NULL, ndjson_file_id integer NOT NULL, zst_file_id integer NOT NULL,
    last_updated timestamptz,
    status text, name text,
    type_code text, type_text text,
    period_start timestamptz, period_end timestamptz,
    owned_by_organization_id text,
    administered_by_organization_id text
) PARTITION BY LIST (release_date);
CREATE UNIQUE INDEX IF NOT EXISTS insurance_plan_key ON <<schema>>.insurance_plan (release_date, resource_id);

CREATE TABLE IF NOT EXISTS <<schema>>.insurance_plan_alias (
    release_date date NOT NULL, resource_id text NOT NULL, ndjson_file_id integer NOT NULL, zst_file_id integer NOT NULL,
    seq integer NOT NULL,
    alias text
) PARTITION BY LIST (release_date);
CREATE UNIQUE INDEX IF NOT EXISTS insurance_plan_alias_key ON <<schema>>.insurance_plan_alias (release_date, resource_id, seq);

CREATE TABLE IF NOT EXISTS <<schema>>.insurance_plan_network (
    release_date date NOT NULL, resource_id text NOT NULL, ndjson_file_id integer NOT NULL, zst_file_id integer NOT NULL,
    seq integer NOT NULL,
    network_organization_id text
) PARTITION BY LIST (release_date);
CREATE UNIQUE INDEX IF NOT EXISTS insurance_plan_network_key ON <<schema>>.insurance_plan_network (release_date, resource_id, seq);

-- Every identifier from every resource type (join key for NPI etc.).
CREATE TABLE IF NOT EXISTS <<schema>>.identifier (
    release_date date NOT NULL, resource_id text NOT NULL, ndjson_file_id integer NOT NULL, zst_file_id integer NOT NULL,
    resource_type text NOT NULL,
    seq integer NOT NULL,
    system text, value text, use text, type_code text, type_text text,
    period_start timestamptz, period_end timestamptz
) PARTITION BY LIST (release_date);
CREATE UNIQUE INDEX IF NOT EXISTS identifier_key ON <<schema>>.identifier (release_date, resource_type, resource_id, seq);
CREATE INDEX IF NOT EXISTS identifier_value ON <<schema>>.identifier (release_date, system, value);
```

- [ ] **Step 2: Write the failing tests**

Append to `tests/conftest.py`:

```python
@pytest.fixture
def npd_db(make_db) -> str:
    from npd_loader.schema import init_db
    info = make_db()
    init_db(info, "npd_raw", "npd")
    return info
```

`tests/test_schema.py`:

```python
from datetime import date

import psycopg
import pytest
from psycopg import sql

from npd_loader.db import (advisory_lock, list_parent_tables, list_release_partitions, published_releases,
                           render_sql, standalone_name)
from npd_loader.schema import init_db

NPD_TABLES = [
    "endpoint", "healthcare_service", "healthcare_service_location", "identifier", "insurance_plan",
    "insurance_plan_alias", "insurance_plan_network", "location", "location_telecom", "organization",
    "organization_address", "organization_affiliation", "organization_endpoint", "organization_telecom",
    "practitioner", "practitioner_address", "practitioner_name", "practitioner_qualification",
    "practitioner_role", "practitioner_role_code", "practitioner_role_endpoint", "practitioner_role_location",
    "practitioner_role_specialty", "practitioner_telecom",
]


def test_init_db_is_idempotent_and_creates_parents_and_views(npd_db):
    init_db(npd_db, "npd_raw", "npd")
    with psycopg.connect(npd_db) as conn:
        assert list_parent_tables(conn, "npd") == NPD_TABLES
        assert list_parent_tables(conn, "npd_raw") == ["resource"]
        views = {r[0] for r in conn.execute(
            "SELECT schemaname || '.' || viewname FROM pg_views WHERE schemaname IN ('npd', 'npd_raw')")}
        assert views == {f"npd.v_{t}" for t in NPD_TABLES} | {"npd_raw.v_resource"}


def test_helper_functions(npd_db):
    with psycopg.connect(npd_db) as conn:
        def one(q, *args):
            return conn.execute(q, args).fetchone()[0]
        assert one("SELECT npd.ref_id('Organization/Organization-123')") == "Organization-123"
        assert one("SELECT npd.ref_id(NULL)") is None
        res = '{"extension": [{"url": "u1", "valueBoolean": true}], ' \
              '"identifier": [{"system": "a", "value": "1"}, {"system": "b", "value": "2"}]}'
        assert one("SELECT npd.ext(%s::jsonb, 'u1')->>'valueBoolean'", res) == "true"
        assert one("SELECT npd.ext(%s::jsonb, 'nope')", res) is None
        assert one("SELECT npd.identifier_value(%s::jsonb, ARRAY['b', 'a'])", res) == "1"
        assert one("SELECT npd.join_text('[\"A\", \"B\"]'::jsonb, ' ')") == "A B"
        assert one("SELECT npd.join_text('[]'::jsonb, ' ')") is None
        conn.execute("SET TIME ZONE 'UTC'")
        assert one("SELECT npd.fhir_ts('2020')::text") == "2020-01-01 00:00:00+00"
        assert one("SELECT npd.fhir_ts('2020-05')::text") == "2020-05-01 00:00:00+00"
        assert one("SELECT npd.fhir_ts('2007-08-31T00:00:00Z')::text") == "2007-08-31 00:00:00+00"


def test_release_partitions_are_discovered(npd_db):
    with psycopg.connect(npd_db) as conn:
        conn.execute("CREATE TABLE npd.endpoint__20260929__r1 (LIKE npd.endpoint INCLUDING DEFAULTS)")
        conn.execute("ALTER TABLE npd.endpoint ATTACH PARTITION npd.endpoint__20260929__r1 "
                     "FOR VALUES IN ('2026-09-29')")
        assert list_release_partitions(conn, "npd", "endpoint") == {date(2026, 9, 29): "endpoint__20260929__r1"}
        assert list_release_partitions(conn, "npd", "location") == {}
        conn.execute("INSERT INTO npd.release (release_date, import_run_id) VALUES ('2026-09-29', 1)")
    assert published_releases(npd_db, "npd") == [date(2026, 9, 29)]


def test_render_sql(npd_db):
    with psycopg.connect(npd_db) as conn:
        text = "SELECT * FROM <<t:practitioner>> WHERE release_date = <<release>>"
        out = render_sql(text, {"t:practitioner": sql.Identifier("npd", "p"),
                                "release": sql.Literal(date(2026, 9, 29))}, conn)
        assert out == "SELECT * FROM \"npd\".\"p\" WHERE release_date = '2026-09-29'::date"
        with pytest.raises(KeyError, match="<<raw>>"):
            render_sql("SELECT <<raw>>", {}, conn)


def test_standalone_names_fit_postgres_limit():
    longest = max(NPD_TABLES + ["resource"], key=len)
    assert standalone_name(longest, date(2026, 9, 29), 999_999_999).endswith("__20260929__r999999999")
    assert len(standalone_name("resource", date(2026, 9, 29), 999_999_999) + "__organizationaffiliation") <= 63
    with pytest.raises(ValueError):
        standalone_name("x" * 60, date(2026, 9, 29), 1)


def test_advisory_lock_is_exclusive(npd_db):
    with advisory_lock(npd_db, "import") as first:
        assert first is True
        with advisory_lock(npd_db, "import") as second:
            assert second is False
        with advisory_lock(npd_db, "download") as other:
            assert other is True
    with advisory_lock(npd_db, "import") as again:
        assert again is True
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `python -m pytest tests/test_schema.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'npd_loader.schema'`

- [ ] **Step 4: Implement `db.py` and `schema.py`**

`src/npd_loader/db.py`:

```python
"""Postgres helpers for the npd database."""
from __future__ import annotations

import hashlib
import re
from contextlib import contextmanager
from datetime import date
from typing import Iterator, Mapping

import psycopg
from psycopg import sql

TOKEN_RE = re.compile(r"<<([a-z_]+(?::[a-z_]+)?)>>")
BOUND_RE = re.compile(r"FOR VALUES IN \('(\d{4}-\d{2}-\d{2})'\)")
MAX_IDENTIFIER = 63


def render_sql(text: str, tokens: Mapping[str, sql.Composable], conn: psycopg.Connection) -> str:
    def substitute(match: re.Match) -> str:
        key = match.group(1)
        if key not in tokens:
            raise KeyError(f"unknown SQL token <<{key}>>")
        return tokens[key].as_string(conn)
    return TOKEN_RE.sub(substitute, text)


def standalone_name(table: str, release: date, run_id: int) -> str:
    name = f"{table}__{release:%Y%m%d}__r{run_id}"
    if len(name) > MAX_IDENTIFIER:
        raise ValueError(f"table name {name!r} is longer than {MAX_IDENTIFIER} characters")
    return name


def list_parent_tables(conn: psycopg.Connection, schema: str) -> list[str]:
    rows = conn.execute(
        "SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname = %s AND c.relkind = 'p' AND NOT c.relispartition ORDER BY c.relname", (schema,)).fetchall()
    return [r[0] for r in rows]


def list_release_partitions(conn: psycopg.Connection, schema: str, parent: str) -> dict[date, str]:
    rows = conn.execute(
        "SELECT c.relname, pg_get_expr(c.relpartbound, c.oid) FROM pg_inherits i "
        "JOIN pg_class c ON c.oid = i.inhrelid JOIN pg_class p ON p.oid = i.inhparent "
        "JOIN pg_namespace n ON n.oid = p.relnamespace WHERE n.nspname = %s AND p.relname = %s",
        (schema, parent)).fetchall()
    found = {}
    for name, bound in rows:
        m = BOUND_RE.search(bound or "")
        if m:
            found[date.fromisoformat(m.group(1))] = name
    return found


def clone_parent_indexes(conn: psycopg.Connection, schema: str, parent: str, target: sql.Composable) -> None:
    """Build the parent's indexes on a standalone table so ATTACH PARTITION reuses them instead of building
    them while holding the parent lock."""
    rows = conn.execute(
        "SELECT pg_get_indexdef(i.indexrelid), i.indisunique FROM pg_index i "
        "JOIN pg_class c ON c.oid = i.indrelid JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname = %s AND c.relname = %s ORDER BY i.indexrelid", (schema, parent)).fetchall()
    for indexdef, unique in rows:
        tail = indexdef[indexdef.index(" USING "):]
        conn.execute(sql.SQL("CREATE {}INDEX ON {}{}").format(
            sql.SQL("UNIQUE " if unique else ""), target, sql.SQL(tail)))


def _lock_key(name: str) -> int:
    return int.from_bytes(hashlib.sha256(f"npd_loader:{name}".encode()).digest()[:8], "big", signed=True)


@contextmanager
def advisory_lock(conninfo: str, name: str) -> Iterator[bool]:
    """Session-level advisory lock held on its own connection for the whole stage."""
    key = _lock_key(name)
    with psycopg.connect(conninfo, autocommit=True) as conn:
        acquired = conn.execute("SELECT pg_try_advisory_lock(%s)", (key,)).fetchone()[0]
        try:
            yield acquired
        finally:
            if acquired:
                conn.execute("SELECT pg_advisory_unlock(%s)", (key,))


def published_releases(conninfo: str, schema: str) -> list[date]:
    with psycopg.connect(conninfo) as conn:
        rows = conn.execute(sql.SQL("SELECT release_date FROM {}.release ORDER BY release_date")
                            .format(sql.Identifier(schema))).fetchall()
    return [r[0] for r in rows]
```

`src/npd_loader/schema.py`:

```python
"""init-db: create schemas, parent tables, helper functions and latest-release views. Safe to re-run."""
from __future__ import annotations

from importlib import resources

import psycopg
from psycopg import sql

from npd_loader.db import list_parent_tables, render_sql


def _init_scripts() -> list[str]:
    folder = resources.files("npd_loader") / "sql" / "init"
    return [p.read_text(encoding="utf-8") for p in sorted(folder.iterdir(), key=lambda p: p.name)
            if p.name.endswith(".sql")]


def _create_view(conn: psycopg.Connection, view_schema: str, table: str, release_schema: str) -> None:
    conn.execute(sql.SQL(
        "CREATE OR REPLACE VIEW {view} AS SELECT * FROM {table} "
        "WHERE release_date = (SELECT max(release_date) FROM {release})").format(
        view=sql.Identifier(view_schema, f"v_{table}"), table=sql.Identifier(view_schema, table),
        release=sql.Identifier(release_schema, "release")))


def init_db(conninfo: str, raw_schema: str, schema: str) -> None:
    with psycopg.connect(conninfo) as conn:
        tokens = {"raw_schema": sql.Identifier(raw_schema), "schema": sql.Identifier(schema)}
        for text in _init_scripts():
            conn.execute(render_sql(text, tokens, conn))
        for s in (raw_schema, schema):
            for table in list_parent_tables(conn, s):
                _create_view(conn, s, table, schema)
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `python -m pytest tests/test_schema.py -v`
Expected: 6 passed

- [ ] **Step 6: Commit**

```bash
git add src/npd_loader/db.py src/npd_loader/schema.py src/npd_loader/sql/init tests/conftest.py tests/test_schema.py
git commit -m "feat: npd schemas, partitioned parent tables, views, DB helpers, advisory lock"
```

---

### Task 11: Raw load (IMPORT, raw part)

**Files:**
- Create: `src/npd_loader/raw_load.py`, `tests/pg_helpers.py`
- Test: `tests/test_raw_load.py`

**Interfaces:**
- Consumes: `standalone_name`, `clone_parent_indexes` (Task 10); `Storage` (Task 2).
- Produces:
  - `NdjsonInput(file_id: int, zst_file_id: int, rel_path: str, resource_type: str, name: str)` (frozen dataclass).
  - `RawLoadResult(table: str, rows: dict[str, int])`, where `table` is the unqualified standalone name in the raw schema and `rows` maps resource type to row count.
  - `RawLoadError`.
  - `iter_lines(f) -> Iterator[tuple[int, str]]`.
  - `validate_line(text, line_number, expected_type) -> tuple[str, str | None]` returns `(id, lastUpdated)`.
  - `load_raw(conn, storage, raw_schema, release, run_id, inputs) -> RawLoadResult`.
- Produces in `tests/pg_helpers.py`: `write_inputs(storage, ndjson: dict[str, bytes], first_id=500) -> list[NdjsonInput]`, plus `load_fixture_raw(conn, storage, ndjson=None, release=R, run_id=7) -> RawLoadResult`, which loads `release_builder.build_release(...).ndjson` by default.

- [ ] **Step 1: Write `tests/pg_helpers.py` and the failing tests**

`tests/pg_helpers.py`:

```python
from datetime import date

from npd_loader.manifest import resource_type_for
from npd_loader.raw_load import NdjsonInput, load_raw
from release_builder import build_release

R = date(2026, 9, 29)


def write_inputs(storage, ndjson: dict[str, bytes], first_id: int = 500) -> list[NdjsonInput]:
    inputs = []
    for i, (name, data) in enumerate(sorted(ndjson.items())):
        file_id = first_id + 2 * i
        rel = f"run_1_2026-09-29-000000/file_{file_id}_{name}"
        with storage.open_write(rel) as f:
            f.write(data)
        inputs.append(NdjsonInput(file_id=file_id, zst_file_id=file_id + 1, rel_path=rel,
                                  resource_type=resource_type_for(name), name=name))
    return inputs


def load_fixture_raw(conn, storage, ndjson=None, release=R, run_id=7):
    ndjson = ndjson if ndjson is not None else build_release(release.isoformat()).ndjson
    return load_raw(conn, storage, "npd_raw", release, run_id, write_inputs(storage, ndjson))
```

`tests/test_raw_load.py`:

```python
import io
import json
from datetime import datetime, timezone

import psycopg
import pytest

from npd_loader.raw_load import RawLoadError, iter_lines, load_raw
from npd_loader.storage import LocalStorage
from fixture_data import ORG1, PRAC1, PRAC2, ndjson_bytes
from pg_helpers import R, load_fixture_raw, write_inputs


@pytest.fixture
def storage(tmp_path):
    return LocalStorage(tmp_path / "data")


def test_loads_every_type_with_lineage(npd_db, storage):
    with psycopg.connect(npd_db) as conn:
        result = load_fixture_raw(conn, storage)
        assert result.table == "resource__20260929__r7"
        assert result.rows == {"Endpoint": 1, "HealthcareService": 1, "InsurancePlan": 1, "Location": 1,
                               "Organization": 2, "OrganizationAffiliation": 1, "Practitioner": 2,
                               "PractitionerRole": 2}
        row = conn.execute(
            "SELECT release_date, resource_type, last_updated, ndjson_file_id, zst_file_id, line_number, resource "
            "FROM npd_raw.resource__20260929__r7 WHERE resource_id = %s", (PRAC2["id"],)).fetchone()
        assert row[:2] == (R, "Practitioner")
        assert row[2] == datetime(2026, 9, 29, 4, 35, tzinfo=timezone.utc)
        assert row[5] == 2
        assert row[6] == PRAC2
        assert row[4] == row[3] + 1
        # leaves are attached to the standalone release table, which is NOT yet attached to npd_raw.resource
        leaves = conn.execute("SELECT count(*) FROM pg_inherits i JOIN pg_class p ON p.oid = i.inhparent "
                              "WHERE p.relname = 'resource__20260929__r7'").fetchone()[0]
        assert leaves == 8
        assert conn.execute("SELECT count(*) FROM npd_raw.resource").fetchone()[0] == 0


def test_crlf_and_missing_final_newline(npd_db, storage):
    data = (json.dumps(PRAC1) + "\r\n" + json.dumps(PRAC2)).encode()
    with psycopg.connect(npd_db) as conn:
        result = load_fixture_raw(conn, storage, {"06-Practitioner.ndjson": data})
    assert result.rows == {"Practitioner": 2}


def test_iter_lines_blank_lines():
    assert list(iter_lines(io.BytesIO(b'{"a":1}\n\n\n'))) == [(1, '{"a":1}')]
    with pytest.raises(RawLoadError, match="line 2: blank line"):
        list(iter_lines(io.BytesIO(b'{"a":1}\n\n{"b":2}\n')))


@pytest.mark.parametrize("lines,message", [
    ([json.dumps(PRAC1), "{not json"], "line 2: invalid JSON"),
    ([json.dumps(PRAC1), json.dumps(ORG1)], "line 2: resourceType 'Organization', expected 'Practitioner'"),
    ([json.dumps({**PRAC1, "id": ""})], "line 1: missing id"),
    ([json.dumps(PRAC1), json.dumps(PRAC1)], "duplicate"),
    ([json.dumps({**PRAC1, "gender": "x\u0000y"})], "line 1: contains \\\\u0000"),
])
def test_strict_validation(npd_db, storage, lines, message):
    data = ("\n".join(lines) + "\n").encode()
    with psycopg.connect(npd_db) as conn:
        with pytest.raises(RawLoadError, match=message) as info:
            load_fixture_raw(conn, storage, {"06-Practitioner.ndjson": data})
    assert info.value.file_id is not None or "duplicate" in message


def test_duplicate_error_names_the_id(npd_db, storage):
    data = ndjson_bytes([PRAC1, PRAC2, PRAC1])
    with psycopg.connect(npd_db) as conn:
        with pytest.raises(RawLoadError, match="Practitioner-1003000100 .*lines \\[1, 3\\]"):
            load_fixture_raw(conn, storage, {"06-Practitioner.ndjson": data})


def test_unknown_resource_type_loads_raw(npd_db, storage):
    med = {"resourceType": "Medication", "id": "Medication-1", "meta": {"lastUpdated": "2026-09-29T00:00:00Z"}}
    ndjson = {"06-Practitioner.ndjson": ndjson_bytes([PRAC1]), "09-Medication.ndjson": ndjson_bytes([med])}
    with psycopg.connect(npd_db) as conn:
        result = load_fixture_raw(conn, storage, ndjson)
        assert result.rows == {"Medication": 1, "Practitioner": 1}
        assert conn.execute("SELECT resource_id FROM npd_raw.resource__20260929__r7 "
                            "WHERE resource_type = 'Medication'").fetchone()[0] == "Medication-1"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_raw_load.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'npd_loader.raw_load'`

- [ ] **Step 3: Implement `raw_load.py`**

```python
"""IMPORT, raw part: stream each .ndjson into a standalone raw table with COPY, strictly validated.

Layout built here (not visible to readers until publish.py attaches it):
  {raw}.resource__YYYYMMDD__rRUN                 PARTITION BY LIST (resource_type)
  {raw}.resource__YYYYMMDD__rRUN__practitioner   one leaf per file, CHECKed on release_date and resource_type
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import date
from typing import BinaryIO, Iterator

import psycopg
from psycopg import sql

from npd_loader.db import MAX_IDENTIFIER, clone_parent_indexes, standalone_name
from npd_loader.storage import Storage

log = logging.getLogger(__name__)
RAW_PARENT = "resource"
COLUMNS = ("release_date", "resource_type", "resource_id", "last_updated", "ndjson_file_id", "zst_file_id",
           "line_number", "resource")


class RawLoadError(Exception):
    def __init__(self, message: str, file_id: int | None = None):
        super().__init__(message)
        self.file_id = file_id


@dataclass(frozen=True)
class NdjsonInput:
    file_id: int
    zst_file_id: int
    rel_path: str
    resource_type: str
    name: str


@dataclass
class RawLoadResult:
    table: str
    rows: dict[str, int]


def iter_lines(f: BinaryIO) -> Iterator[tuple[int, str]]:
    """Yield (line number, text). Accepts LF or CRLF and a missing final newline. Blank lines are only
    allowed at the very end of the file."""
    blank_at: int | None = None
    for number, raw in enumerate(f, start=1):
        line = raw.rstrip(b"\r\n")
        if not line.strip():
            blank_at = blank_at or number
            continue
        if blank_at is not None:
            raise RawLoadError(f"line {blank_at}: blank line")
        try:
            yield number, line.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise RawLoadError(f"line {number}: not UTF-8: {exc}") from exc


def validate_line(text: str, number: int, expected_type: str) -> tuple[str, str | None]:
    if "\\u0000" in text:
        raise RawLoadError(f"line {number}: contains \\u0000, which Postgres jsonb cannot store")
    try:
        obj = json.loads(text)
    except json.JSONDecodeError as exc:
        raise RawLoadError(f"line {number}: invalid JSON: {exc}") from exc
    if not isinstance(obj, dict):
        raise RawLoadError(f"line {number}: not a JSON object")
    if obj.get("resourceType") != expected_type:
        raise RawLoadError(f"line {number}: resourceType {obj.get('resourceType')!r}, expected {expected_type!r}")
    resource_id = obj.get("id")
    if not isinstance(resource_id, str) or not resource_id:
        raise RawLoadError(f"line {number}: missing id")
    meta = obj.get("meta")
    last_updated = meta.get("lastUpdated") if isinstance(meta, dict) else None
    return resource_id, last_updated if isinstance(last_updated, str) else None


def _create_release_table(conn: psycopg.Connection, raw_schema: str, release: date, run_id: int) -> str:
    name = standalone_name(RAW_PARENT, release, run_id)
    conn.execute(sql.SQL("CREATE TABLE {} (LIKE {} INCLUDING DEFAULTS) PARTITION BY LIST (resource_type)").format(
        sql.Identifier(raw_schema, name), sql.Identifier(raw_schema, RAW_PARENT)))
    conn.commit()
    return name


def _load_file(conn: psycopg.Connection, storage: Storage, raw_schema: str, table: str, release: date,
               inp: NdjsonInput) -> int:
    leaf = f"{table}__{inp.resource_type.lower()}"
    if len(leaf) > MAX_IDENTIFIER:
        raise RawLoadError(f"table name {leaf!r} is too long", inp.file_id)
    leaf_id = sql.Identifier(raw_schema, leaf)
    conn.execute(sql.SQL("CREATE TABLE {} (LIKE {} INCLUDING DEFAULTS)").format(
        leaf_id, sql.Identifier(raw_schema, RAW_PARENT)))
    conn.execute(sql.SQL("ALTER TABLE {} ADD CHECK (release_date = {} AND resource_type = {})").format(
        leaf_id, sql.Literal(release), sql.Literal(inp.resource_type)))
    lines = 0
    copy_sql = sql.SQL("COPY {} ({}) FROM STDIN").format(leaf_id, sql.SQL(", ").join(map(sql.Identifier, COLUMNS)))
    try:
        with storage.open_read(inp.rel_path) as f, conn.cursor() as cur, cur.copy(copy_sql) as copy:
            for number, text in iter_lines(f):
                resource_id, last_updated = validate_line(text, number, inp.resource_type)
                copy.write_row((release, inp.resource_type, resource_id, last_updated, inp.file_id,
                                inp.zst_file_id, number, text))
                lines += 1
    except RawLoadError as exc:
        conn.rollback()
        raise RawLoadError(f"{inp.name}: {exc}", inp.file_id) from exc
    except psycopg.Error as exc:
        conn.rollback()
        raise RawLoadError(f"{inp.name}: COPY failed: {exc}", inp.file_id) from exc
    count = conn.execute(sql.SQL("SELECT count(*) FROM {}").format(leaf_id)).fetchone()[0]
    if count != lines:
        raise RawLoadError(f"{inp.name}: read {lines} lines but loaded {count} rows", inp.file_id)
    conn.execute(sql.SQL("ALTER TABLE {} ATTACH PARTITION {} FOR VALUES IN ({})").format(
        sql.Identifier(raw_schema, table), leaf_id, sql.Literal(inp.resource_type)))
    conn.commit()
    log.info("loaded %d %s resources from %s", lines, inp.resource_type, inp.rel_path)
    return lines


def _index_and_check_duplicates(conn: psycopg.Connection, raw_schema: str, table: str) -> None:
    target = sql.Identifier(raw_schema, table)
    try:
        clone_parent_indexes(conn, raw_schema, RAW_PARENT, target)
        conn.commit()
    except psycopg.errors.UniqueViolation:
        conn.rollback()
        dups = conn.execute(sql.SQL(
            "SELECT resource_type, resource_id, array_agg(line_number ORDER BY line_number) FROM {} "
            "GROUP BY 1, 2 HAVING count(*) > 1 ORDER BY 1, 2 LIMIT 20").format(target)).fetchall()
        detail = "; ".join(f"{t} {i} at lines {lines}" for t, i, lines in dups)
        raise RawLoadError(f"duplicate resource ids: {detail}")


def load_raw(conn: psycopg.Connection, storage: Storage, raw_schema: str, release: date, run_id: int,
             inputs: list[NdjsonInput]) -> RawLoadResult:
    table = _create_release_table(conn, raw_schema, release, run_id)
    rows = {inp.resource_type: _load_file(conn, storage, raw_schema, table, release, inp) for inp in inputs}
    _index_and_check_duplicates(conn, raw_schema, table)
    return RawLoadResult(table, rows)
```

Note: `UniqueViolation` from `CREATE UNIQUE INDEX` is how duplicate `(resource_type, resource_id)` pairs are detected (spec §8.3 step 5). The `GROUP BY` runs only on that failure path, to name the offenders.

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_raw_load.py -v`
Expected: 10 passed

- [ ] **Step 5: Commit**

```bash
git add src/npd_loader/raw_load.py tests/pg_helpers.py tests/test_raw_load.py
git commit -m "feat: strict streaming raw load into standalone release partitions"
```

---

### Task 12: Transform runner, Practitioner tables, identifier table

**Files:**
- Create: `src/npd_loader/transform.py`, `src/npd_loader/sql/transform/010_practitioner.sql`, `src/npd_loader/sql/transform/090_identifier.sql`
- Modify: `src/npd_loader/sql/mapped_paths.txt`, `tests/pg_helpers.py`
- Test: `tests/test_transform_practitioner.py`, `tests/test_mapped_paths.py`

**Interfaces:**
- Consumes: `load_fixture_raw` (Task 11); `render_sql`, `standalone_name`, `list_parent_tables`, and `clone_parent_indexes` (Task 10).
- Produces: `TransformResult(tables: dict[str, str], counts: dict[str, int])`, mapping parent table to unqualified standalone name and parent table to row count. Also `run_transforms(conn, raw_schema, raw_table, schema, release, run_id) -> TransformResult`. SQL tokens available to transform scripts are `<<raw>>`, `<<release>>`, `<<schema>>`, and `<<t:TABLE>>` for every parent table in the schema.
- Produces in `tests/pg_helpers.py`: `transformed(conn, storage) -> TransformResult` (raw load + transforms of the default fixture release), and `rows(conn, result, table, columns: str) -> list[tuple]` (selects `columns` from the standalone table, ordered by `resource_id` plus `seq` when present).

- [ ] **Step 1: Write the failing tests**

Append to `tests/pg_helpers.py`:

```python
from psycopg import sql as _sql

from npd_loader.transform import run_transforms


def transformed(conn, storage, run_id=7):
    raw = load_fixture_raw(conn, storage, run_id=run_id)
    return run_transforms(conn, "npd_raw", raw.table, "npd", R, run_id)


def rows(conn, result, table: str, columns: str) -> list[tuple]:
    has_seq = conn.execute("SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'npd' "
                           "AND table_name = %s AND column_name = 'seq')", (table,)).fetchone()[0]
    order = "resource_id, seq" if has_seq else "resource_id"
    query = _sql.SQL("SELECT {} FROM {} ORDER BY {}").format(
        _sql.SQL(columns), _sql.Identifier("npd", result.tables[table]), _sql.SQL(order))
    return conn.execute(query).fetchall()
```

`tests/test_transform_practitioner.py`:

```python
from datetime import datetime, timezone

import psycopg
import pytest

from npd_loader.storage import LocalStorage
from pg_helpers import R, rows, transformed

NPI = "http://terminology.hl7.org/NamingSystem/npi"
TAX = "http://hl7.org/fhir/us/ndh/ValueSet/HealthcareIndividualTaxonomyVS"
P1, P2 = "Practitioner-1003000100", "Practitioner-1083687529"


def utc(*args):
    return datetime(*args, tzinfo=timezone.utc)


@pytest.fixture
def result(npd_db, tmp_path):
    conn = psycopg.connect(npd_db)
    res = transformed(conn, LocalStorage(tmp_path / "data"))
    yield conn, res
    conn.close()


def test_standalone_tables_and_lineage(result):
    conn, res = result
    assert res.tables["practitioner"] == "practitioner__20260929__r7"
    assert res.counts["practitioner"] == 2
    lineage = conn.execute(
        "SELECT p.release_date, p.ndjson_file_id = r.ndjson_file_id, p.zst_file_id = r.zst_file_id "
        "FROM npd.practitioner__20260929__r7 p JOIN npd_raw.resource__20260929__r7 r "
        "ON r.resource_type = 'Practitioner' AND r.resource_id = p.resource_id").fetchall()
    assert lineage == [(R, True, True), (R, True, True)]
    # not yet visible through the parent
    assert conn.execute("SELECT count(*) FROM npd.practitioner").fetchone()[0] == 0


def test_practitioner(result):
    conn, res = result
    assert rows(conn, res, "practitioner",
                "resource_id, last_updated, npi, active, gender, name_family, name_given, name_prefix, name_suffix, "
                "identity_verified, medicare_enrolled, in_hhs_exclusion_list, aligned_with_data_network") == [
        (P1, utc(2026, 9, 29, 4, 34, 0, 724328), "1003000100", True, "male", "GOMEZ", "GERARDO", None, None,
         False, False, False, False),
        (P2, utc(2026, 9, 29, 4, 35), "1083687529", True, "female", "JONES", "ANNA MARIE", "DR.", "MD",
         True, True, False, True),
    ]


def test_practitioner_name(result):
    conn, res = result
    assert rows(conn, res, "practitioner_name",
                "resource_id, seq, use, family, given, prefix, suffix, period_start, period_end") == [
        (P1, 1, "official", "GOMEZ", "GERARDO", None, None, None, None),
        (P2, 1, "maiden", "SMITH", "ANNA", None, None, utc(1990, 1, 1), None),
        (P2, 2, "official", "JONES", "ANNA MARIE", "DR.", "MD", None, None),
    ]


def test_practitioner_address_and_telecom(result):
    conn, res = result
    assert rows(conn, res, "practitioner_address",
                "resource_id, seq, use, type, line1, line2, extra_lines, city, state, postal_code, country") == [
        (P1, 1, "work", "physical", "108 W Victoria St", None, None, "Gardena", "CA", "90248", "US"),
        (P1, 2, "billing", "postal", "680 S Wilton Pl", None, None, "Los Angeles", "CA", "90005", "US"),
    ]
    assert rows(conn, res, "practitioner_telecom", "resource_id, seq, system, use, value") == [
        (P1, 1, "fax", "work", "2133831280"), (P1, 2, "phone", "work", "2133657400"),
        (P1, 3, "phone", "work", "3107152020"), (P2, 1, "phone", "work", "2125551212"),
    ]


def test_practitioner_qualification(result):
    conn, res = result
    assert rows(conn, res, "practitioner_qualification",
                "resource_id, seq, code_system, code, code_display, code_text, identifier_value, "
                "identifier_type_code, issuer_organization_id") == [
        (P1, 1, TAX, "171M00000X", "Case Manager/Care Coordinator", "Case Manager/Care Coordinator",
         None, None, None),
        (P1, 2, TAX, "225400000X", "Rehabilitation Practitioner", "Rehabilitation Practitioner", None, None, None),
        (P2, 1, TAX, "207R00000X", "Internal Medicine Physician", "Internal Medicine Physician",
         "A12345", "MD", "Organization-NY-STATE-BOARD"),
    ]


def test_identifier_covers_all_types(result):
    conn, res = result
    got = conn.execute(
        "SELECT resource_type, resource_id, seq, system, value, use, type_code, type_text, period_start "
        "FROM npd.identifier__20260929__r7 ORDER BY resource_type, resource_id, seq").fetchall()
    assert got == [
        ("Organization", "Organization-1336200294", 1, NPI, "1336200294", "official", "PRN", "NPI", utc(2006, 12, 13)),
        ("Organization", "Organization-1336200294", 2, "https://npd.cms.gov/fhir/sid/us-pseudo-ein",
         "6e5d8b3e-13d3-48be-9ed2-881d08f2c459", "official", "TAX", None, None),
        ("Organization", "Organization-1902099112", 1, NPI, "1902099112", "official", "PRN", "NPI", None),
        ("Practitioner", P1, 1, NPI, "1003000100", "official", "PRN", "NPI", utc(2007, 8, 31)),
        ("Practitioner", P2, 1, NPI, "1083687529", "official", "PRN", "NPI", utc(2005, 5, 23)),
    ]


def test_indexes_cloned_onto_standalone(result):
    conn, _ = result
    defs = [r[0] for r in conn.execute(
        "SELECT indexdef FROM pg_indexes WHERE schemaname = 'npd' AND tablename = 'practitioner__20260929__r7'")]
    assert any("UNIQUE" in d and "(release_date, resource_id)" in d for d in defs)
    assert any("(release_date, npi)" in d for d in defs)
```

`tests/test_mapped_paths.py`:

```python
import pytest

from npd_loader.profile import load_mapped_paths, profile_records, unmapped
from fixture_data import all_records

COVERED_TYPES = ["Practitioner"]


@pytest.mark.parametrize("rtype", COVERED_TYPES)
def test_every_fixture_path_is_mapped_or_raw_only(rtype):
    missing = [(t, p) for t, p, _ in unmapped(profile_records(all_records()), load_mapped_paths()) if t == rtype]
    assert missing == []
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_transform_practitioner.py tests/test_mapped_paths.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'npd_loader.transform'`. The mapped-paths test fails, listing unmapped Practitioner paths.

- [ ] **Step 3: Implement `transform.py`**

```python
"""IMPORT, table part: run sql/transform/*.sql (in name order) into standalone tables for one release."""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date
from importlib import resources

import psycopg
from psycopg import sql

from npd_loader.db import clone_parent_indexes, list_parent_tables, render_sql, standalone_name

log = logging.getLogger(__name__)


@dataclass
class TransformResult:
    tables: dict[str, str]
    counts: dict[str, int]


def _scripts() -> list[tuple[str, str]]:
    folder = resources.files("npd_loader") / "sql" / "transform"
    return [(p.name, p.read_text(encoding="utf-8")) for p in sorted(folder.iterdir(), key=lambda p: p.name)
            if p.name.endswith(".sql")]


def run_transforms(conn: psycopg.Connection, raw_schema: str, raw_table: str, schema: str, release: date,
                   run_id: int) -> TransformResult:
    conn.execute("SET TIME ZONE 'UTC'")
    tables: dict[str, str] = {}
    for parent in list_parent_tables(conn, schema):
        name = standalone_name(parent, release, run_id)
        target = sql.Identifier(schema, name)
        conn.execute(sql.SQL("CREATE TABLE {} (LIKE {} INCLUDING DEFAULTS)").format(
            target, sql.Identifier(schema, parent)))
        conn.execute(sql.SQL("ALTER TABLE {} ADD CHECK (release_date = {})").format(target, sql.Literal(release)))
        tables[parent] = name
    conn.commit()

    tokens: dict[str, sql.Composable] = {
        "raw": sql.Identifier(raw_schema, raw_table),
        "release": sql.Literal(release),
        "schema": sql.Identifier(schema),
        **{f"t:{parent}": sql.Identifier(schema, name) for parent, name in tables.items()},
    }
    for script_name, text in _scripts():
        log.info("running transform %s", script_name)
        conn.execute(render_sql(text, tokens, conn))
        conn.commit()

    for parent, name in tables.items():
        clone_parent_indexes(conn, schema, parent, sql.Identifier(schema, name))
    conn.commit()
    counts = {parent: conn.execute(sql.SQL("SELECT count(*) FROM {}").format(sql.Identifier(schema, name)))
              .fetchone()[0] for parent, name in tables.items()}
    return TransformResult(tables, counts)
```

- [ ] **Step 4: Write `010_practitioner.sql` and `090_identifier.sql`**

`src/npd_loader/sql/transform/010_practitioner.sql`:

```sql
INSERT INTO <<t:practitioner>> (
    release_date, resource_id, ndjson_file_id, zst_file_id, last_updated,
    npi, active, gender, name_family, name_given, name_prefix, name_suffix,
    identity_verified, medicare_enrolled, in_hhs_exclusion_list, aligned_with_data_network)
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, r.last_updated,
       <<schema>>.identifier_value(r.resource, ARRAY['http://terminology.hl7.org/NamingSystem/npi',
                                                    'http://hl7.org/fhir/sid/us-npi']),
       (r.resource->>'active')::boolean,
       r.resource->>'gender',
       n.e->>'family',
       <<schema>>.join_text(n.e->'given', ' '),
       <<schema>>.join_text(n.e->'prefix', ' '),
       <<schema>>.join_text(n.e->'suffix', ' '),
       (<<schema>>.ext(r.resource, 'http://hl7.org/fhir/us/ndh/StructureDefinition/base-ext-cms-identity-verified')->>'valueBoolean')::boolean,
       (<<schema>>.ext(r.resource, 'http://hl7.org/fhir/us/ndh/StructureDefinition/base-ext-cms_medicare_enrollment')->>'valueBoolean')::boolean,
       (<<schema>>.ext(r.resource, 'http://hl7.org/fhir/us/ndh/StructureDefinition/base-ext-hhs-in-exclusion-list')->>'valueBoolean')::boolean,
       (<<schema>>.ext(r.resource, 'http://hl7.org/fhir/us/ndh/StructureDefinition/base-ext-cms_aligned_with_data_network')->>'valueBoolean')::boolean
FROM <<raw>> r
LEFT JOIN LATERAL (
    -- the official name, else the first name
    SELECT x.e FROM jsonb_array_elements(coalesce(r.resource->'name', '[]'::jsonb)) WITH ORDINALITY AS x(e, ord)
    ORDER BY (x.e->>'use' = 'official') DESC NULLS LAST, x.ord
    LIMIT 1
) n ON true
WHERE r.resource_type = 'Practitioner';

INSERT INTO <<t:practitioner_name>> (
    release_date, resource_id, ndjson_file_id, zst_file_id, seq,
    use, family, given, prefix, suffix, period_start, period_end)
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, x.ord,
       x.e->>'use', x.e->>'family',
       <<schema>>.join_text(x.e->'given', ' '),
       <<schema>>.join_text(x.e->'prefix', ' '),
       <<schema>>.join_text(x.e->'suffix', ' '),
       <<schema>>.fhir_ts(x.e->'period'->>'start'),
       <<schema>>.fhir_ts(x.e->'period'->>'end')
FROM <<raw>> r
CROSS JOIN LATERAL jsonb_array_elements(coalesce(r.resource->'name', '[]'::jsonb)) WITH ORDINALITY AS x(e, ord)
WHERE r.resource_type = 'Practitioner';

INSERT INTO <<t:practitioner_address>> (
    release_date, resource_id, ndjson_file_id, zst_file_id, seq,
    use, type, line1, line2, extra_lines, city, state, postal_code, country)
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, x.ord,
       x.e->>'use', x.e->>'type', x.e->'line'->>0, x.e->'line'->>1,
       <<schema>>.join_text((x.e->'line') - 0 - 0, ', '),
       x.e->>'city', x.e->>'state', x.e->>'postalCode', x.e->>'country'
FROM <<raw>> r
CROSS JOIN LATERAL jsonb_array_elements(coalesce(r.resource->'address', '[]'::jsonb)) WITH ORDINALITY AS x(e, ord)
WHERE r.resource_type = 'Practitioner';

INSERT INTO <<t:practitioner_telecom>> (
    release_date, resource_id, ndjson_file_id, zst_file_id, seq, system, use, value)
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, x.ord,
       x.e->>'system', x.e->>'use', x.e->>'value'
FROM <<raw>> r
CROSS JOIN LATERAL jsonb_array_elements(coalesce(r.resource->'telecom', '[]'::jsonb)) WITH ORDINALITY AS x(e, ord)
WHERE r.resource_type = 'Practitioner';

INSERT INTO <<t:practitioner_qualification>> (
    release_date, resource_id, ndjson_file_id, zst_file_id, seq,
    code_system, code, code_display, code_text, identifier_value, identifier_type_code, issuer_organization_id)
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, x.ord,
       x.e->'code'->'coding'->0->>'system',
       x.e->'code'->'coding'->0->>'code',
       x.e->'code'->'coding'->0->>'display',
       x.e->'code'->>'text',
       x.e->'identifier'->0->>'value',
       x.e->'identifier'->0->'type'->'coding'->0->>'code',
       <<schema>>.ref_id(x.e->'issuer'->>'reference')
FROM <<raw>> r
CROSS JOIN LATERAL jsonb_array_elements(coalesce(r.resource->'qualification', '[]'::jsonb)) WITH ORDINALITY AS x(e, ord)
WHERE r.resource_type = 'Practitioner';
```

`src/npd_loader/sql/transform/090_identifier.sql`:

```sql
INSERT INTO <<t:identifier>> (
    release_date, resource_id, ndjson_file_id, zst_file_id, resource_type, seq,
    system, value, use, type_code, type_text, period_start, period_end)
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, r.resource_type, x.ord,
       x.e->>'system', x.e->>'value', x.e->>'use',
       x.e->'type'->'coding'->0->>'code',
       x.e->'type'->>'text',
       <<schema>>.fhir_ts(x.e->'period'->>'start'),
       <<schema>>.fhir_ts(x.e->'period'->>'end')
FROM <<raw>> r
CROSS JOIN LATERAL jsonb_array_elements(coalesce(r.resource->'identifier', '[]'::jsonb)) WITH ORDINALITY AS x(e, ord);
```

- [ ] **Step 5: Append the Practitioner and identifier lines to `src/npd_loader/sql/mapped_paths.txt`**

```text

# identifier (090_identifier.sql), all resource types
* .identifier[].system
* .identifier[].value
* .identifier[].use
* .identifier[].type.coding[].code
* .identifier[].type.text
* .identifier[].period.start
* .identifier[].period.end
# raw-only
* .identifier[].type.coding[].system
* .identifier[].type.coding[].display

# Practitioner (010_practitioner.sql)
Practitioner .active
Practitioner .gender
Practitioner .extension[http://hl7.org/fhir/us/ndh/StructureDefinition/base-ext-cms-identity-verified].valueBoolean
Practitioner .extension[http://hl7.org/fhir/us/ndh/StructureDefinition/base-ext-cms_medicare_enrollment].valueBoolean
Practitioner .extension[http://hl7.org/fhir/us/ndh/StructureDefinition/base-ext-hhs-in-exclusion-list].valueBoolean
Practitioner .extension[http://hl7.org/fhir/us/ndh/StructureDefinition/base-ext-cms_aligned_with_data_network].valueBoolean
Practitioner .name[].use
Practitioner .name[].family
Practitioner .name[].given[]
Practitioner .name[].prefix[]
Practitioner .name[].suffix[]
Practitioner .name[].period.start
Practitioner .name[].period.end
Practitioner .address[].use
Practitioner .address[].type
Practitioner .address[].line[]
Practitioner .address[].city
Practitioner .address[].state
Practitioner .address[].postalCode
Practitioner .address[].country
Practitioner .telecom[].system
Practitioner .telecom[].use
Practitioner .telecom[].value
Practitioner .qualification[].code.coding[].system
Practitioner .qualification[].code.coding[].code
Practitioner .qualification[].code.coding[].display
Practitioner .qualification[].code.text
Practitioner .qualification[].identifier[].value
Practitioner .qualification[].identifier[].type.coding[].code
Practitioner .qualification[].issuer.reference
# raw-only
Practitioner .qualification[].identifier[].type.coding[].system
Practitioner .qualification[].identifier[].type.coding[].display
Practitioner .qualification[].identifier[].use
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `python -m pytest tests/test_transform_practitioner.py tests/test_mapped_paths.py -v`
Expected: 8 passed

- [ ] **Step 7: Commit**

```bash
git add src/npd_loader/transform.py src/npd_loader/sql/transform src/npd_loader/sql/mapped_paths.txt tests/pg_helpers.py tests/test_transform_practitioner.py tests/test_mapped_paths.py
git commit -m "feat: transform runner with practitioner and identifier tables"
```

---

### Task 13: Organization, Location, Endpoint tables

**Files:**
- Create: `src/npd_loader/sql/transform/020_organization.sql`, `030_location.sql`, `040_endpoint.sql`
- Modify: `src/npd_loader/sql/mapped_paths.txt`, `tests/test_mapped_paths.py`
- Test: `tests/test_transform_organization.py`

**Interfaces:**
- Consumes: `transformed`, `rows`, `R` (from `tests/pg_helpers.py`); the parent tables from `003_tables.sql`; and the SQL helper functions from Task 10.

- [ ] **Step 1: Write the failing tests**

`tests/test_transform_organization.py`:

```python
from datetime import datetime, timezone

import psycopg
import pytest

from npd_loader.storage import LocalStorage
from pg_helpers import rows, transformed

O1, O2 = "Organization-1336200294", "Organization-1902099112"
L1 = "Location-00027861-c380-4866-b677-4c28e4ceaf6b"
E1 = "Endpoint-000f410c-e1e9-4a78-a988-b6ce47d6a793"


@pytest.fixture
def result(npd_db, tmp_path):
    conn = psycopg.connect(npd_db)
    res = transformed(conn, LocalStorage(tmp_path / "data"))
    yield conn, res
    conn.close()


def test_organization(result):
    conn, res = result
    assert rows(conn, res, "organization",
                "resource_id, last_updated, npi, pseudo_ein, name, active, type_code, type_display, "
                "part_of_organization_id, verification_status") == [
        (O1, datetime(2026, 9, 29, 4, 29, 5, 411440, tzinfo=timezone.utc), "1336200294",
         "6e5d8b3e-13d3-48be-9ed2-881d08f2c459", "NEW MEXICO STATE UNIVERSITY STUDENT HEALTH CENTER", True,
         "prov", "Healthcare Provider", None, "complete"),
        (O2, datetime(2026, 9, 29, 4, 30, tzinfo=timezone.utc), "1902099112", None, "EASTBLUFF MEDICAL GROUP", True,
         "prov", "Healthcare Provider", O1, "complete"),
    ]


def test_organization_children(result):
    conn, res = result
    assert rows(conn, res, "organization_address",
                "resource_id, seq, use, type, line1, line2, extra_lines, city, state, postal_code, country") == [
        (O2, 1, "work", "physical", "2515 Eastbluff Dr", "Suite 100", "Building B", "Newport Beach", "CA",
         "92660", "US"),
    ]
    assert rows(conn, res, "organization_telecom", "resource_id, seq, system, use, value") == [
        (O1, 1, "fax", "work", "5056462692"), (O1, 2, "phone", "work", "5056461512"),
        (O2, 1, "phone", "work", "9496405050"),
    ]
    assert rows(conn, res, "organization_endpoint", "resource_id, seq, endpoint_id") == [(O2, 1, E1)]


def test_location(result):
    conn, res = result
    assert rows(conn, res, "location",
                "resource_id, status, name, description, mode, address_use, address_type, line1, line2, "
                "extra_lines, city, state, postal_code, country, latitude, longitude, managing_organization_id") == [
        (L1, "active", "2515 Eastbluff Dr", "2515 Eastbluff Dr", "instance", None, "physical", "2515 Eastbluff Dr",
         None, None, "Newport Beach", "CA", "92660", "US", 33.6399, -117.87532, O2),
    ]
    assert rows(conn, res, "location_telecom", "resource_id, seq, system, use, value") == [
        (L1, 1, "fax", "work", "9496405051"), (L1, 2, "phone", "work", "9496405050"),
    ]


def test_endpoint(result):
    conn, res = result
    assert rows(conn, res, "endpoint",
                "resource_id, status, name, address, connection_type_system, connection_type_code, "
                "payload_type_system, payload_type_code, managing_organization_id, verification_status") == [
        (E1, "active", "Direct Messaging Address", "heather.bruneau.1@29651.direct.athenahealth.com",
         "http://terminology.hl7.org/CodeSystem/endpoint-connection-type", "direct-project",
         "http://terminology.hl7.org/CodeSystem/data-absent-reason", "not-applicable", None, "complete"),
    ]
```

In `tests/test_mapped_paths.py`, change the `COVERED_TYPES` line to:

```python
COVERED_TYPES = ["Practitioner", "Organization", "Location", "Endpoint"]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_transform_organization.py tests/test_mapped_paths.py -v`
Expected: FAIL. The organization, location, and endpoint row lists are empty (`[] == [...]`), and the mapped-path tests for Organization, Location, and Endpoint list unmapped paths.

- [ ] **Step 3: Write the SQL**

`src/npd_loader/sql/transform/020_organization.sql`:

```sql
INSERT INTO <<t:organization>> (
    release_date, resource_id, ndjson_file_id, zst_file_id, last_updated,
    npi, pseudo_ein, name, active, type_code, type_display, part_of_organization_id, verification_status)
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, r.last_updated,
       <<schema>>.identifier_value(r.resource, ARRAY['http://terminology.hl7.org/NamingSystem/npi',
                                                    'http://hl7.org/fhir/sid/us-npi']),
       <<schema>>.identifier_value(r.resource, ARRAY['https://npd.cms.gov/fhir/sid/us-pseudo-ein']),
       r.resource->>'name',
       (r.resource->>'active')::boolean,
       r.resource->'type'->0->'coding'->0->>'code',
       r.resource->'type'->0->'coding'->0->>'display',
       <<schema>>.ref_id(r.resource->'partOf'->>'reference'),
       <<schema>>.ext(r.resource, 'http://hl7.org/fhir/us/ndh/StructureDefinition/base-ext-verification-status')
           ->'valueCodeableConcept'->'coding'->0->>'code'
FROM <<raw>> r
WHERE r.resource_type = 'Organization';

INSERT INTO <<t:organization_address>> (
    release_date, resource_id, ndjson_file_id, zst_file_id, seq,
    use, type, line1, line2, extra_lines, city, state, postal_code, country)
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, x.ord,
       x.e->>'use', x.e->>'type', x.e->'line'->>0, x.e->'line'->>1,
       <<schema>>.join_text((x.e->'line') - 0 - 0, ', '),
       x.e->>'city', x.e->>'state', x.e->>'postalCode', x.e->>'country'
FROM <<raw>> r
CROSS JOIN LATERAL jsonb_array_elements(coalesce(r.resource->'address', '[]'::jsonb)) WITH ORDINALITY AS x(e, ord)
WHERE r.resource_type = 'Organization';

INSERT INTO <<t:organization_telecom>> (
    release_date, resource_id, ndjson_file_id, zst_file_id, seq, system, use, value)
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, x.ord,
       x.e->>'system', x.e->>'use', x.e->>'value'
FROM <<raw>> r
CROSS JOIN LATERAL jsonb_array_elements(coalesce(r.resource->'telecom', '[]'::jsonb)) WITH ORDINALITY AS x(e, ord)
WHERE r.resource_type = 'Organization';

INSERT INTO <<t:organization_endpoint>> (
    release_date, resource_id, ndjson_file_id, zst_file_id, seq, endpoint_id)
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, x.ord,
       <<schema>>.ref_id(x.e->>'reference')
FROM <<raw>> r
CROSS JOIN LATERAL jsonb_array_elements(coalesce(r.resource->'endpoint', '[]'::jsonb)) WITH ORDINALITY AS x(e, ord)
WHERE r.resource_type = 'Organization';
```

`src/npd_loader/sql/transform/030_location.sql`:

```sql
INSERT INTO <<t:location>> (
    release_date, resource_id, ndjson_file_id, zst_file_id, last_updated,
    status, name, description, mode, address_use, address_type, line1, line2, extra_lines,
    city, state, postal_code, country, latitude, longitude, managing_organization_id)
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, r.last_updated,
       r.resource->>'status', r.resource->>'name', r.resource->>'description', r.resource->>'mode',
       r.resource->'address'->>'use', r.resource->'address'->>'type',
       r.resource->'address'->'line'->>0, r.resource->'address'->'line'->>1,
       <<schema>>.join_text((r.resource->'address'->'line') - 0 - 0, ', '),
       r.resource->'address'->>'city', r.resource->'address'->>'state',
       r.resource->'address'->>'postalCode', r.resource->'address'->>'country',
       (r.resource->'position'->>'latitude')::double precision,
       (r.resource->'position'->>'longitude')::double precision,
       <<schema>>.ref_id(r.resource->'managingOrganization'->>'reference')
FROM <<raw>> r
WHERE r.resource_type = 'Location';

INSERT INTO <<t:location_telecom>> (
    release_date, resource_id, ndjson_file_id, zst_file_id, seq, system, use, value)
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, x.ord,
       x.e->>'system', x.e->>'use', x.e->>'value'
FROM <<raw>> r
CROSS JOIN LATERAL jsonb_array_elements(coalesce(r.resource->'telecom', '[]'::jsonb)) WITH ORDINALITY AS x(e, ord)
WHERE r.resource_type = 'Location';
```

`src/npd_loader/sql/transform/040_endpoint.sql`:

```sql
INSERT INTO <<t:endpoint>> (
    release_date, resource_id, ndjson_file_id, zst_file_id, last_updated,
    status, name, address, connection_type_system, connection_type_code,
    payload_type_system, payload_type_code, managing_organization_id, verification_status)
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, r.last_updated,
       r.resource->>'status', r.resource->>'name', r.resource->>'address',
       r.resource->'connectionType'->>'system', r.resource->'connectionType'->>'code',
       r.resource->'payloadType'->0->'coding'->0->>'system',
       r.resource->'payloadType'->0->'coding'->0->>'code',
       <<schema>>.ref_id(r.resource->'managingOrganization'->>'reference'),
       <<schema>>.ext(r.resource, 'http://hl7.org/fhir/us/ndh/StructureDefinition/base-ext-verification-status')
           ->'valueCodeableConcept'->'coding'->0->>'code'
FROM <<raw>> r
WHERE r.resource_type = 'Endpoint';
```

- [ ] **Step 4: Append to `src/npd_loader/sql/mapped_paths.txt`**

```text

# Organization (020_organization.sql)
Organization .active
Organization .name
Organization .type[].coding[].code
Organization .type[].coding[].display
Organization .partOf.reference
Organization .endpoint[].reference
Organization .extension[http://hl7.org/fhir/us/ndh/StructureDefinition/base-ext-verification-status].valueCodeableConcept.coding[].code
Organization .address[].use
Organization .address[].type
Organization .address[].line[]
Organization .address[].city
Organization .address[].state
Organization .address[].postalCode
Organization .address[].country
Organization .telecom[].system
Organization .telecom[].use
Organization .telecom[].value
# raw-only
Organization .type[].coding[].system
Organization .type[].text
Organization .extension[http://hl7.org/fhir/us/ndh/StructureDefinition/base-ext-verification-status].valueCodeableConcept.coding[].system
Organization .extension[http://hl7.org/fhir/us/ndh/StructureDefinition/base-ext-verification-status].valueCodeableConcept.coding[].display

# Location (030_location.sql)
Location .status
Location .name
Location .description
Location .mode
Location .address.use
Location .address.type
Location .address.line[]
Location .address.city
Location .address.state
Location .address.postalCode
Location .address.country
Location .position.latitude
Location .position.longitude
Location .managingOrganization.reference
Location .telecom[].system
Location .telecom[].use
Location .telecom[].value

# Endpoint (040_endpoint.sql)
Endpoint .status
Endpoint .name
Endpoint .address
Endpoint .connectionType.system
Endpoint .connectionType.code
Endpoint .payloadType[].coding[].system
Endpoint .payloadType[].coding[].code
Endpoint .managingOrganization.reference
Endpoint .extension[http://hl7.org/fhir/us/ndh/StructureDefinition/base-ext-verification-status].valueCodeableConcept.coding[].code
# raw-only
Endpoint .connectionType.display
Endpoint .payloadType[].coding[].version
Endpoint .extension[http://hl7.org/fhir/us/ndh/StructureDefinition/base-ext-verification-status].valueCodeableConcept.coding[].system
Endpoint .extension[http://hl7.org/fhir/us/ndh/StructureDefinition/base-ext-verification-status].valueCodeableConcept.coding[].display
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `python -m pytest tests/test_transform_organization.py tests/test_mapped_paths.py -v`
Expected: 8 passed

- [ ] **Step 6: Commit**

```bash
git add src/npd_loader/sql/transform src/npd_loader/sql/mapped_paths.txt tests/test_transform_organization.py tests/test_mapped_paths.py
git commit -m "feat: organization, location, endpoint transforms"
```

---

### Task 14: PractitionerRole, OrganizationAffiliation, HealthcareService, InsurancePlan tables

**Files:**
- Create: `src/npd_loader/sql/transform/050_practitioner_role.sql`, `060_organization_affiliation.sql`, `070_healthcare_service.sql`, `080_insurance_plan.sql`
- Modify: `src/npd_loader/sql/mapped_paths.txt`, `tests/test_mapped_paths.py`
- Test: `tests/test_transform_roles.py`

**Interfaces:**
- Consumes: `transformed`, `rows` (from `tests/pg_helpers.py`), and the Task 10 helpers.

- [ ] **Step 1: Write the failing tests**

`tests/test_transform_roles.py`:

```python
from datetime import datetime, timezone

import psycopg
import pytest

from npd_loader.storage import LocalStorage
from pg_helpers import rows, transformed

PR1 = "PractitionerRole-00000990-37aa-428a-a1fd-d91bed7c789d"
PR2 = "PractitionerRole-0f00aa11"
HS1 = "HealthcareService-cd52e7a4-79df-47ef-aa81-a89142e37ffe"
IP1 = "InsurancePlan-0be2a43c-0c13-41fd-b97f-7f43394ec1fd"
OA1 = "OrganizationAffiliation-00111700-8fc3-4ea1-a966-4c3d59b41921"


def utc(*args):
    return datetime(*args, tzinfo=timezone.utc)


@pytest.fixture
def result(npd_db, tmp_path):
    conn = psycopg.connect(npd_db)
    res = transformed(conn, LocalStorage(tmp_path / "data"))
    yield conn, res
    conn.close()


def test_practitioner_role(result):
    conn, res = result
    assert rows(conn, res, "practitioner_role",
                "resource_id, active, practitioner_id, organization_id, period_start, period_end") == [
        (PR1, True, "Practitioner-1083687529", None, None, None),
        (PR2, True, "Practitioner-1003000100", "Organization-1902099112", utc(2020, 1, 1), None),
    ]
    assert rows(conn, res, "practitioner_role_endpoint", "resource_id, seq, endpoint_id") == [
        (PR1, 1, "Endpoint-00000990-37aa-428a-a1fd-d91bed7c789d")]
    assert rows(conn, res, "practitioner_role_location", "resource_id, seq, location_id") == [
        (PR2, 1, "Location-00027861-c380-4866-b677-4c28e4ceaf6b")]
    assert rows(conn, res, "practitioner_role_specialty", "resource_id, seq, system, code, display, text") == [
        (PR2, 1, "http://nucc.org/provider-taxonomy", "207R00000X", "Internal Medicine Physician",
         "Internal Medicine")]
    assert rows(conn, res, "practitioner_role_code", "resource_id, seq, system, code, display, text") == [
        (PR2, 1, "http://hl7.org/fhir/us/ndh/CodeSystem/IndividualAndGroupSpecialtiesCS", "ph", "Physician", None)]


def test_organization_affiliation(result):
    conn, res = result
    assert rows(conn, res, "organization_affiliation",
                "resource_id, active, organization_id, participating_organization_id, role_code, role_display, "
                "role_text, period_start, period_end") == [
        (OA1, True, "Organization-c618f893-235a-48ae-bbaa-1d60d5ac7ee9", "Organization-1407192586", "bt",
         "Member Of", "Member Of", None, None)]


def test_healthcare_service(result):
    conn, res = result
    assert rows(conn, res, "healthcare_service",
                "resource_id, active, name, provided_by_organization_id, network_organization_id") == [
        (HS1, True, None, "Organization-1295596195", "Organization-ea579d05-454e-4359-8751-900c940a599a")]
    assert rows(conn, res, "healthcare_service_location", "resource_id, seq, location_id") == [
        (HS1, 1, "Location-a1ab5e31-a038-4ec0-9439-ee9613d63e11")]


def test_insurance_plan(result):
    conn, res = result
    assert rows(conn, res, "insurance_plan",
                "resource_id, status, name, type_code, type_text, period_start, period_end, "
                "owned_by_organization_id, administered_by_organization_id") == [
        (IP1, "active", "DEVOTED CHOICE GIVEBACK 002 SC (PPO)", None, "Medicare Advantage PPO Plan",
         utc(2026, 1, 1), utc(2026, 12, 31), "Organization-242574ec-a550-43f6-80ae-592aa53c17c8", None)]
    assert rows(conn, res, "insurance_plan_alias", "resource_id, seq, alias") == [
        (IP1, 1, "H7028-002-000"), (IP1, 2, "H7028")]
    assert rows(conn, res, "insurance_plan_network", "resource_id, seq, network_organization_id") == [
        (IP1, 1, "Organization-c7d4aa30-a4c0-4733-aa2c-c8086e29159b")]


def test_every_table_has_rows(result):
    _, res = result
    assert [t for t, n in res.counts.items() if n == 0] == []
```

In `tests/test_mapped_paths.py`, change the `COVERED_TYPES` line to:

```python
COVERED_TYPES = ["Practitioner", "Organization", "Location", "Endpoint", "PractitionerRole",
                 "OrganizationAffiliation", "HealthcareService", "InsurancePlan"]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_transform_roles.py tests/test_mapped_paths.py -v`
Expected: FAIL. The row lists are empty, and the new types report unmapped paths.

- [ ] **Step 3: Write the SQL**

`src/npd_loader/sql/transform/050_practitioner_role.sql`:

```sql
INSERT INTO <<t:practitioner_role>> (
    release_date, resource_id, ndjson_file_id, zst_file_id, last_updated,
    active, practitioner_id, organization_id, period_start, period_end)
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, r.last_updated,
       (r.resource->>'active')::boolean,
       <<schema>>.ref_id(r.resource->'practitioner'->>'reference'),
       <<schema>>.ref_id(r.resource->'organization'->>'reference'),
       <<schema>>.fhir_ts(r.resource->'period'->>'start'),
       <<schema>>.fhir_ts(r.resource->'period'->>'end')
FROM <<raw>> r
WHERE r.resource_type = 'PractitionerRole';

INSERT INTO <<t:practitioner_role_endpoint>> (
    release_date, resource_id, ndjson_file_id, zst_file_id, seq, endpoint_id)
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, x.ord, <<schema>>.ref_id(x.e->>'reference')
FROM <<raw>> r
CROSS JOIN LATERAL jsonb_array_elements(coalesce(r.resource->'endpoint', '[]'::jsonb)) WITH ORDINALITY AS x(e, ord)
WHERE r.resource_type = 'PractitionerRole';

INSERT INTO <<t:practitioner_role_location>> (
    release_date, resource_id, ndjson_file_id, zst_file_id, seq, location_id)
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, x.ord, <<schema>>.ref_id(x.e->>'reference')
FROM <<raw>> r
CROSS JOIN LATERAL jsonb_array_elements(coalesce(r.resource->'location', '[]'::jsonb)) WITH ORDINALITY AS x(e, ord)
WHERE r.resource_type = 'PractitionerRole';

INSERT INTO <<t:practitioner_role_specialty>> (
    release_date, resource_id, ndjson_file_id, zst_file_id, seq, system, code, display, text)
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, x.ord,
       x.e->'coding'->0->>'system', x.e->'coding'->0->>'code', x.e->'coding'->0->>'display', x.e->>'text'
FROM <<raw>> r
CROSS JOIN LATERAL jsonb_array_elements(coalesce(r.resource->'specialty', '[]'::jsonb)) WITH ORDINALITY AS x(e, ord)
WHERE r.resource_type = 'PractitionerRole';

INSERT INTO <<t:practitioner_role_code>> (
    release_date, resource_id, ndjson_file_id, zst_file_id, seq, system, code, display, text)
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, x.ord,
       x.e->'coding'->0->>'system', x.e->'coding'->0->>'code', x.e->'coding'->0->>'display', x.e->>'text'
FROM <<raw>> r
CROSS JOIN LATERAL jsonb_array_elements(coalesce(r.resource->'code', '[]'::jsonb)) WITH ORDINALITY AS x(e, ord)
WHERE r.resource_type = 'PractitionerRole';
```

`src/npd_loader/sql/transform/060_organization_affiliation.sql`:

```sql
INSERT INTO <<t:organization_affiliation>> (
    release_date, resource_id, ndjson_file_id, zst_file_id, last_updated,
    active, organization_id, participating_organization_id, role_code, role_display, role_text,
    period_start, period_end)
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, r.last_updated,
       (r.resource->>'active')::boolean,
       <<schema>>.ref_id(r.resource->'organization'->>'reference'),
       <<schema>>.ref_id(r.resource->'participatingOrganization'->>'reference'),
       r.resource->'code'->0->'coding'->0->>'code',
       r.resource->'code'->0->'coding'->0->>'display',
       r.resource->'code'->0->>'text',
       <<schema>>.fhir_ts(r.resource->'period'->>'start'),
       <<schema>>.fhir_ts(r.resource->'period'->>'end')
FROM <<raw>> r
WHERE r.resource_type = 'OrganizationAffiliation';
```

`src/npd_loader/sql/transform/070_healthcare_service.sql`:

```sql
INSERT INTO <<t:healthcare_service>> (
    release_date, resource_id, ndjson_file_id, zst_file_id, last_updated,
    active, name, provided_by_organization_id, network_organization_id)
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, r.last_updated,
       (r.resource->>'active')::boolean,
       r.resource->>'name',
       <<schema>>.ref_id(r.resource->'providedBy'->>'reference'),
       <<schema>>.ref_id(<<schema>>.ext(r.resource,
           'http://hl7.org/fhir/us/ndh/StructureDefinition/base-ext-network-reference')->'valueReference'->>'reference')
FROM <<raw>> r
WHERE r.resource_type = 'HealthcareService';

INSERT INTO <<t:healthcare_service_location>> (
    release_date, resource_id, ndjson_file_id, zst_file_id, seq, location_id)
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, x.ord, <<schema>>.ref_id(x.e->>'reference')
FROM <<raw>> r
CROSS JOIN LATERAL jsonb_array_elements(coalesce(r.resource->'location', '[]'::jsonb)) WITH ORDINALITY AS x(e, ord)
WHERE r.resource_type = 'HealthcareService';
```

`src/npd_loader/sql/transform/080_insurance_plan.sql`:

```sql
INSERT INTO <<t:insurance_plan>> (
    release_date, resource_id, ndjson_file_id, zst_file_id, last_updated,
    status, name, type_code, type_text, period_start, period_end,
    owned_by_organization_id, administered_by_organization_id)
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, r.last_updated,
       r.resource->>'status', r.resource->>'name',
       r.resource->'type'->0->'coding'->0->>'code',
       r.resource->'type'->0->>'text',
       <<schema>>.fhir_ts(r.resource->'period'->>'start'),
       <<schema>>.fhir_ts(r.resource->'period'->>'end'),
       <<schema>>.ref_id(r.resource->'ownedBy'->>'reference'),
       <<schema>>.ref_id(r.resource->'administeredBy'->>'reference')
FROM <<raw>> r
WHERE r.resource_type = 'InsurancePlan';

INSERT INTO <<t:insurance_plan_alias>> (
    release_date, resource_id, ndjson_file_id, zst_file_id, seq, alias)
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, x.ord, x.v
FROM <<raw>> r
CROSS JOIN LATERAL jsonb_array_elements_text(coalesce(r.resource->'alias', '[]'::jsonb)) WITH ORDINALITY AS x(v, ord)
WHERE r.resource_type = 'InsurancePlan';

INSERT INTO <<t:insurance_plan_network>> (
    release_date, resource_id, ndjson_file_id, zst_file_id, seq, network_organization_id)
SELECT r.release_date, r.resource_id, r.ndjson_file_id, r.zst_file_id, x.ord, <<schema>>.ref_id(x.e->>'reference')
FROM <<raw>> r
CROSS JOIN LATERAL jsonb_array_elements(coalesce(r.resource->'network', '[]'::jsonb)) WITH ORDINALITY AS x(e, ord)
WHERE r.resource_type = 'InsurancePlan';
```

- [ ] **Step 4: Append to `src/npd_loader/sql/mapped_paths.txt`**

```text

# PractitionerRole (050_practitioner_role.sql)
PractitionerRole .active
PractitionerRole .practitioner.reference
PractitionerRole .organization.reference
PractitionerRole .period.start
PractitionerRole .period.end
PractitionerRole .endpoint[].reference
PractitionerRole .location[].reference
PractitionerRole .specialty[].coding[].system
PractitionerRole .specialty[].coding[].code
PractitionerRole .specialty[].coding[].display
PractitionerRole .specialty[].text
PractitionerRole .code[].coding[].system
PractitionerRole .code[].coding[].code
PractitionerRole .code[].coding[].display
PractitionerRole .code[].text

# OrganizationAffiliation (060_organization_affiliation.sql)
OrganizationAffiliation .active
OrganizationAffiliation .organization.reference
OrganizationAffiliation .participatingOrganization.reference
OrganizationAffiliation .code[].coding[].code
OrganizationAffiliation .code[].coding[].display
OrganizationAffiliation .code[].text
OrganizationAffiliation .period.start
OrganizationAffiliation .period.end
# raw-only
OrganizationAffiliation .code[].coding[].system

# HealthcareService (070_healthcare_service.sql)
HealthcareService .active
HealthcareService .name
HealthcareService .providedBy.reference
HealthcareService .location[].reference
HealthcareService .extension[http://hl7.org/fhir/us/ndh/StructureDefinition/base-ext-network-reference].valueReference.reference

# InsurancePlan (080_insurance_plan.sql)
InsurancePlan .status
InsurancePlan .name
InsurancePlan .type[].coding[].code
InsurancePlan .type[].text
InsurancePlan .period.start
InsurancePlan .period.end
InsurancePlan .ownedBy.reference
InsurancePlan .administeredBy.reference
InsurancePlan .alias[]
InsurancePlan .network[].reference
```

- [ ] **Step 5: Run the whole transform suite**

Run: `python -m pytest tests/test_transform_practitioner.py tests/test_transform_organization.py tests/test_transform_roles.py tests/test_mapped_paths.py -v`
Expected: all passed (7 + 4 + 5 + 8 = 24)

- [ ] **Step 6: Commit**

```bash
git add src/npd_loader/sql/transform src/npd_loader/sql/mapped_paths.txt tests/test_transform_roles.py tests/test_mapped_paths.py
git commit -m "feat: practitioner role, affiliation, healthcare service, insurance plan transforms"
```

---
### Task 15: Atomic publication and the IMPORT stage

**Files:**
- Create: `src/npd_loader/publish.py`, `src/npd_loader/import_stage.py`
- Test: `tests/test_import.py`

**Interfaces:**
- Consumes: `load_raw`, `NdjsonInput`, `RawLoadError`, and `RAW_PARENT` (Task 11); `run_transforms` (Task 12); `list_parent_tables` and `list_release_partitions` (Task 10); `run_extract` (Task 8); and the stage helpers (Task 7).
- Produces in `publish.py`: `PublishConflict`, `publish_release(conn, raw_schema, raw_table, schema, tables: dict[str, str], release, run_id, force) -> None`, and `drop_release(conn, raw_schema, schema, release) -> list[str]`, which returns the dropped partitions as `"schema.name"`.
- Produces in `import_stage.py`: `ImportInputs(inputs: list[NdjsonInput], extract_run_ids: list[int])`, `find_inputs(ctx, release, download_run) -> ImportInputs | None`, `drop_standalone_tables(conninfo, schemas, run_id) -> None`, and `run_import(ctx, release=None, force=False) -> Outcome`.

- [ ] **Step 1: Write the failing tests `tests/test_import.py`**

```python
from datetime import date

import psycopg
import pytest

from npd_loader.catalog import FAILED, SUCCESS
from npd_loader.db import list_release_partitions
from npd_loader.download import run_download
from npd_loader.import_stage import run_import
from npd_loader.stages import Outcome, StageFailed
from helpers import make_ctx
from release_builder import build_release

R = date(2026, 9, 29)


def downloaded_ctx(tmp_path, cms, npd_db, release):
    cms.publish(release)
    ctx = make_ctx(tmp_path, cms, npd_conninfo=npd_db)
    run_download(ctx)
    return ctx


@pytest.fixture
def ctx(tmp_path, cms, npd_db):
    return downloaded_ctx(tmp_path, cms, npd_db, build_release("2026-09-29"))


def one(conninfo, query, *args):
    with psycopg.connect(conninfo) as conn:
        return conn.execute(query, args).fetchone()[0]


def test_import_extracts_loads_and_publishes(ctx):
    assert run_import(ctx) is Outcome.SUCCESS
    runs = ctx.catalog.runs
    assert [r["run_class"] for r in runs.values()] == ["DOWNLOAD", "EXTRACT", "IMPORT"]
    download_id, extract_id, import_id = sorted(runs)
    imp = runs[import_id]
    assert imp["status"] == SUCCESS
    assert imp["description"] == "NPD FHIR Import 2026-09-29"
    assert f"<download_run_id>{download_id}</download_run_id>" in imp["config_xml"]
    assert f"<extract_run_ids>{extract_id}</extract_run_ids>" in imp["config_xml"]
    assert '<table table="npd.practitioner" rows="2" />' in imp["output_xml"]
    assert one(ctx.npd_conninfo, "SELECT count(*) FROM npd_raw.resource WHERE release_date = %s", R) == 11
    assert one(ctx.npd_conninfo, "SELECT import_run_id FROM npd.release WHERE release_date = %s", R) == import_id
    assert one(ctx.npd_conninfo, "SELECT count(*) FROM npd.v_practitioner") == 2
    ndjson = {r.id: r for r in ctx.catalog.get_data_files(R, "ndjson")}
    with psycopg.connect(ctx.npd_conninfo) as conn:
        pairs = conn.execute("SELECT DISTINCT ndjson_file_id, zst_file_id FROM npd_raw.resource").fetchall()
    assert sorted(pairs) == sorted((r.id, r.parent_file) for r in ndjson.values())
    assert all(r.date_loaded is not None for r in ndjson.values())


def test_second_import_is_skipped(ctx):
    run_import(ctx)
    assert run_import(ctx) is Outcome.SKIPPED


def test_force_replaces_the_release(ctx):
    run_import(ctx)
    assert run_import(ctx, force=True) is Outcome.SUCCESS
    second = max(ctx.catalog.runs)
    with psycopg.connect(ctx.npd_conninfo) as conn:
        assert list_release_partitions(conn, "npd", "practitioner") == {R: f"practitioner__20260929__r{second}"}
        assert list_release_partitions(conn, "npd_raw", "resource") == {R: f"resource__20260929__r{second}"}
    assert one(ctx.npd_conninfo, "SELECT import_run_id FROM npd.release") == second
    assert one(ctx.npd_conninfo, "SELECT count(*) FROM npd_raw.resource") == 11
    assert one(ctx.npd_conninfo, "SELECT count(*) FROM npd.v_practitioner") == 2


def test_bad_data_fails_cleanly(tmp_path, cms, npd_db):
    bad = b'{"resourceType": "Practitioner", "id": "P1"}\n{oops\n'
    ctx = downloaded_ctx(tmp_path, cms, npd_db,
                         build_release("2026-09-29", raw_ndjson={"06-Practitioner.ndjson": bad}))
    with pytest.raises(StageFailed, match="line 2: invalid JSON"):
        run_import(ctx)
    imp = ctx.catalog.runs[max(ctx.catalog.runs)]
    assert imp["run_class"] == "IMPORT" and imp["status"] == FAILED and "line 2" in imp["result"]
    prac = next(r for r in ctx.catalog.get_data_files(R, "ndjson") if r.file_name.endswith("_06-Practitioner.ndjson"))
    assert "line 2" in prac.exceptions and prac.date_loaded is None
    assert one(npd_db, "SELECT count(*) FROM pg_class WHERE relname ~ '__r[0-9]+'") == 0
    assert one(npd_db, "SELECT count(*) FROM npd.release") == 0


def test_published_without_catalog_success_requires_force(ctx):
    run_import(ctx)
    ctx.catalog.runs[max(ctx.catalog.runs)]["status"] = FAILED  # crash after the publish commit
    with pytest.raises(StageFailed, match="--force"):
        run_import(ctx)
    assert one(ctx.npd_conninfo, "SELECT count(*) FROM npd.v_practitioner") == 2  # still live
    assert run_import(ctx, force=True) is Outcome.SUCCESS


def test_no_download_fails(tmp_path, cms, npd_db):
    ctx = make_ctx(tmp_path, cms, npd_conninfo=npd_db)
    with pytest.raises(StageFailed, match="no successful download"):
        run_import(ctx)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_import.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'npd_loader.import_stage'`

- [ ] **Step 3: Implement `publish.py`**

```python
"""Make a release visible (or invisible) in one transaction."""
from __future__ import annotations

import logging
from datetime import date

import psycopg
from psycopg import sql

from npd_loader.db import list_parent_tables, list_release_partitions
from npd_loader.raw_load import RAW_PARENT

log = logging.getLogger(__name__)


class PublishConflict(Exception):
    pass


def _detach_and_drop(conn: psycopg.Connection, schema: str, parent: str, name: str) -> None:
    conn.execute(sql.SQL("ALTER TABLE {} DETACH PARTITION {}").format(
        sql.Identifier(schema, parent), sql.Identifier(schema, name)))
    conn.execute(sql.SQL("DROP TABLE {}").format(sql.Identifier(schema, name)))


def publish_release(conn: psycopg.Connection, raw_schema: str, raw_table: str, schema: str,
                    tables: dict[str, str], release: date, run_id: int, force: bool) -> None:
    targets = [(raw_schema, RAW_PARENT, raw_table)] + [(schema, p, n) for p, n in sorted(tables.items())]
    conn.commit()
    with conn.transaction():
        for s, parent, new in targets:
            existing = list_release_partitions(conn, s, parent).get(release)
            if existing is not None:
                if not force:
                    raise PublishConflict(f"release {release} is already published in {s}.{parent}; "
                                          f"rerun with --force to replace it")
                _detach_and_drop(conn, s, parent, existing)
            conn.execute(sql.SQL("ALTER TABLE {} ATTACH PARTITION {} FOR VALUES IN ({})").format(
                sql.Identifier(s, parent), sql.Identifier(s, new), sql.Literal(release)))
        conn.execute(sql.SQL(
            "INSERT INTO {} (release_date, import_run_id, published_at) VALUES (%s, %s, now()) "
            "ON CONFLICT (release_date) DO UPDATE SET import_run_id = EXCLUDED.import_run_id, published_at = now()"
        ).format(sql.Identifier(schema, "release")), (release, run_id))
    log.info("published release %s (%d tables)", release, len(targets))
    for s, _, new in targets:
        conn.execute(sql.SQL("ANALYZE {}").format(sql.Identifier(s, new)))
    conn.commit()


def drop_release(conn: psycopg.Connection, raw_schema: str, schema: str, release: date) -> list[str]:
    """Detach and drop every partition of `release`, in one transaction. Checks what actually exists first."""
    dropped: list[str] = []
    conn.commit()
    with conn.transaction():
        for s in (raw_schema, schema):
            for parent in list_parent_tables(conn, s):
                name = list_release_partitions(conn, s, parent).get(release)
                if name is not None:
                    _detach_and_drop(conn, s, parent, name)
                    dropped.append(f"{s}.{name}")
        conn.execute(sql.SQL("DELETE FROM {} WHERE release_date = %s").format(
            sql.Identifier(schema, "release")), (release,))
    return dropped
```

- [ ] **Step 4: Implement `import_stage.py`**

```python
"""IMPORT stage: raw load + transforms into standalone tables, then one-transaction publication."""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date

import psycopg
from psycopg import sql

from npd_loader.catalog import SUCCESS, Run
from npd_loader.db import list_release_partitions
from npd_loader.extract import run_extract
from npd_loader.manifest import resource_type_for
from npd_loader.publish import PublishConflict, publish_release
from npd_loader.raw_load import RAW_PARENT, NdjsonInput, RawLoadError, load_raw
from npd_loader.runxml import build_output_xml
from npd_loader.stages import (Context, Outcome, StageFailed, completed_child, config_xml, describe, fail_run, now,
                               original_name)
from npd_loader.transform import run_transforms

log = logging.getLogger(__name__)


@dataclass
class ImportInputs:
    inputs: list[NdjsonInput]
    extract_run_ids: list[int]


def find_inputs(ctx: Context, release: date, download_run: Run) -> ImportInputs | None:
    """The .ndjson file for every .zst of `download_run`, or None if any is missing (extract first)."""
    cat = ctx.config.catalog
    ndjsons = ctx.catalog.get_data_files(release, cat.file_type_ndjson)
    inputs, run_ids = [], set()
    for zst in ctx.catalog.get_data_files(release, cat.file_type_zst, run_id=download_run.id):
        row = completed_child(zst.id, ndjsons)
        if row is None or not ctx.storage.exists(row.file_rel_path) \
                or ctx.storage.size(row.file_rel_path) != row.file_size:
            return None
        name = original_name(row.file_name)
        inputs.append(NdjsonInput(file_id=row.id, zst_file_id=zst.id, rel_path=row.file_rel_path,
                                  resource_type=resource_type_for(name), name=name))
        run_ids.add(row.run_id)
    return ImportInputs(inputs, sorted(run_ids))


def drop_standalone_tables(conninfo: str, schemas: list[str], run_id: int) -> None:
    with psycopg.connect(conninfo, autocommit=True) as conn:
        rows = conn.execute(
            "SELECT n.nspname, c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE n.nspname = ANY(%s) AND c.relkind IN ('r', 'p') AND NOT c.relispartition AND c.relname ~ %s",
            (schemas, rf"__r{run_id}(__[a-z]+)?$")).fetchall()
        for schema, name in rows:
            conn.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier(schema, name)))


def _import(ctx: Context, run: Run, release: date, inputs: list[NdjsonInput], force: bool) -> list[dict]:
    db = ctx.config.npd_db
    with psycopg.connect(ctx.npd_conninfo) as conn:
        if not force and list_release_partitions(conn, db.raw_schema, RAW_PARENT).get(release):
            raise PublishConflict(f"release {release} is already published in {db.raw_schema}.{RAW_PARENT} "
                                  f"but the catalog has no successful import; rerun with --force to replace it")
        raw = load_raw(conn, ctx.storage, db.raw_schema, release, run.id, inputs)
        transformed = run_transforms(conn, db.raw_schema, raw.table, db.schema, release, run.id)
        publish_release(conn, db.raw_schema, raw.table, db.schema, transformed.tables, release, run.id, force)
    return ([{"table": f"{db.raw_schema}.{RAW_PARENT}", "resource_type": t, "rows": n} for t, n in raw.rows.items()]
            + [{"table": f"{db.schema}.{t}", "rows": n} for t, n in transformed.counts.items()])


def _finish(ctx: Context, run: Run, release: date, summary: list[dict]) -> None:
    ctx.catalog.finish_run(run, SUCCESS, output_xml=build_output_xml(summary, item_tag="table"))


def run_import(ctx: Context, release: date | None = None, force: bool = False) -> Outcome:
    cfg = ctx.config
    cat = cfg.catalog
    with ctx.lock("import") as acquired:
        if not acquired:
            log.info("another import is running; nothing to do")
            return Outcome.LOCKED
        download_run = ctx.catalog.last_successful_run(cat.run_class_download, release)
        if download_run is None:
            raise StageFailed(f"no successful download for release {release or '(any)'}")
        release = download_run.release_date
        if not force and ctx.catalog.last_successful_run(cat.run_class_import, release):
            log.info("release %s is already imported", release)
            return Outcome.SKIPPED
        found = find_inputs(ctx, release, download_run)
        if found is None:
            log.info("some .ndjson files for %s are missing; running extract first", release)
            if run_extract(ctx, release=release) is Outcome.LOCKED:
                raise StageFailed("an extract is running in another process; try again later")
            found = find_inputs(ctx, release, download_run)
            if found is None:
                raise StageFailed(f".ndjson files for {release} are still missing after extract")

        description = describe(ctx, "Import", release)
        run = ctx.catalog.start_run(cat.run_class_import, description, config_xml(
            ctx, description, release, force=force, download_run_id=download_run.id,
            extract_run_ids=",".join(map(str, found.extract_run_ids))))
        log.info("import run %s started for release %s", run.id, release)
        try:
            summary = _import(ctx, run, release, found.inputs, force)
        except Exception as exc:
            if isinstance(exc, RawLoadError) and exc.file_id is not None:
                ctx.catalog.update_data_file(exc.file_id, exceptions=str(exc))
            try:
                drop_standalone_tables(ctx.npd_conninfo, [cfg.npd_db.raw_schema, cfg.npd_db.schema], run.id)
            except Exception:
                log.exception("could not drop standalone tables of run %s", run.id)
            fail_run(ctx, run, exc)
            raise StageFailed(f"import of release {release} failed: {exc}") from exc
        loaded = now()
        for inp in found.inputs:
            ctx.catalog.update_data_file(inp.file_id, date_loaded=loaded)
        _finish(ctx, run, release, summary)
        log.info("import run %s finished", run.id)
        return Outcome.SUCCESS
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `python -m pytest tests/test_import.py -v`
Expected: 6 passed

- [ ] **Step 6: Commit**

```bash
git add src/npd_loader/publish.py src/npd_loader/import_stage.py tests/test_import.py
git commit -m "feat: IMPORT stage with one-transaction partition publication"
```

---

### Task 16: Retention

**Files:**
- Create: `src/npd_loader/retention.py`
- Modify: `src/npd_loader/import_stage.py` (`_finish` runs retention)
- Test: `tests/test_retention.py`

**Interfaces:**
- Consumes: `drop_release` (Task 15); `list_parent_tables` and `list_release_partitions` (Task 10).
- Produces: `apply_retention(ctx, just_imported: date) -> list[str]`, which returns warnings and never raises.

- [ ] **Step 1: Write the failing tests `tests/test_retention.py`**

```python
from datetime import date, timedelta

import psycopg
import pytest
from psycopg import sql

from npd_loader.db import list_parent_tables, list_release_partitions
from npd_loader.retention import apply_retention
from helpers import make_ctx

BASE = date(2026, 8, 4)
RELEASES = [BASE + timedelta(weeks=k) for k in range(7)]


def publish_empty(conn, release: date, run_id: int) -> None:
    for schema in ("npd_raw", "npd"):
        for parent in list_parent_tables(conn, schema):
            name = f"{parent}__{release:%Y%m%d}__r{run_id}"
            conn.execute(sql.SQL("CREATE TABLE {} (LIKE {} INCLUDING DEFAULTS)").format(
                sql.Identifier(schema, name), sql.Identifier(schema, parent)))
            conn.execute(sql.SQL("ALTER TABLE {} ATTACH PARTITION {} FOR VALUES IN ({})").format(
                sql.Identifier(schema, parent), sql.Identifier(schema, name), sql.Literal(release)))
    conn.execute("INSERT INTO npd.release (release_date, import_run_id) VALUES (%s, %s)", (release, run_id))
    conn.commit()


def seed(ctx, release: date) -> tuple[str, str]:
    run = ctx.catalog.add_successful_run("IMPORT", release)
    zst_rel = f"run_1_{release}/file_1_06-Practitioner.ndjson.zst"
    nd_rel = f"run_2_{release}/file_2_06-Practitioner.ndjson"
    zst = ctx.catalog.add_data_file(run, file_type="ndjson.zst", source_version_num=release.isoformat(),
                                    file_rel_path=zst_rel)
    ctx.catalog.add_data_file(run, file_type="ndjson", source_version_num=release.isoformat(),
                              file_rel_path=nd_rel, parent_file=zst, file_hash="x")
    for rel in (zst_rel, nd_rel):
        with ctx.storage.open_write(rel) as f:
            f.write(b"data")
    return zst_rel, nd_rel


@pytest.fixture
def ctx(tmp_path, cms, npd_db):
    c = make_ctx(tmp_path, cms, npd_conninfo=npd_db)
    c.paths = {}
    with psycopg.connect(npd_db) as conn:
        for i, release in enumerate(RELEASES):
            publish_empty(conn, release, 100 + i)
            c.paths[release] = seed(c, release)
    return c


def published(ctx):
    with psycopg.connect(ctx.npd_conninfo) as conn:
        raw = sorted(list_release_partitions(conn, "npd_raw", "resource"))
        npd = sorted(list_release_partitions(conn, "npd", "practitioner"))
        rel = [r[0] for r in conn.execute("SELECT release_date FROM npd.release ORDER BY 1")]
    assert raw == npd == rel
    return raw


def test_keeps_newest_five(ctx):
    files_before = len(ctx.catalog.files)
    assert apply_retention(ctx, RELEASES[-1]) == []
    assert published(ctx) == RELEASES[2:]
    for release in RELEASES:
        zst_rel, nd_rel = ctx.paths[release]
        assert ctx.storage.exists(zst_rel)                         # .zst never deleted
        assert ctx.storage.exists(nd_rel) == (release in RELEASES[2:])
    assert len(ctx.catalog.files) == files_before                  # catalog rows never deleted


def test_just_imported_old_release_is_kept(ctx):
    assert apply_retention(ctx, RELEASES[0]) == []
    assert published(ctx) == [RELEASES[0]] + RELEASES[2:]
    assert ctx.storage.exists(ctx.paths[RELEASES[0]][1])
    assert not ctx.storage.exists(ctx.paths[RELEASES[1]][1])


def test_already_dropped_release_is_not_an_error(ctx):
    apply_retention(ctx, RELEASES[-1])
    with ctx.storage.open_write(ctx.paths[RELEASES[0]][1]) as f:   # e.g. someone re-extracted it
        f.write(b"again")
    assert apply_retention(ctx, RELEASES[-1]) == []
    assert not ctx.storage.exists(ctx.paths[RELEASES[0]][1])


def test_errors_become_warnings(ctx, monkeypatch):
    def broken(rel):
        raise OSError("disk unavailable")
    monkeypatch.setattr(ctx.storage, "delete", broken)
    warnings = apply_retention(ctx, RELEASES[-1])
    assert len(warnings) == 2 and all("disk unavailable" in w for w in warnings)
    assert published(ctx) == RELEASES[2:]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_retention.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'npd_loader.retention'`

- [ ] **Step 3: Implement `retention.py`**

```python
"""Keep the newest N imported releases: drop older partitions and their .ndjson files.
Never touches the release just imported, .zst/manifest files, or catalog rows."""
from __future__ import annotations

import logging
from datetime import date

import psycopg

from npd_loader.db import list_parent_tables, list_release_partitions
from npd_loader.publish import drop_release
from npd_loader.stages import Context

log = logging.getLogger(__name__)


def _delete_ndjson(ctx: Context, release: date) -> None:
    for row in ctx.catalog.get_data_files(release, ctx.config.catalog.file_type_ndjson):
        if not row.file_rel_path:
            continue
        for rel in (row.file_rel_path, row.file_rel_path + ".part"):
            if ctx.storage.exists(rel):
                ctx.storage.delete(rel)
                log.info("retention deleted %s", rel)


def apply_retention(ctx: Context, just_imported: date) -> list[str]:
    cfg = ctx.config
    db = cfg.npd_db
    warnings: list[str] = []
    try:
        with psycopg.connect(ctx.npd_conninfo) as conn:
            published: set[date] = set()
            for schema in (db.raw_schema, db.schema):
                for parent in list_parent_tables(conn, schema):
                    published |= set(list_release_partitions(conn, schema, parent))
            imported = set(ctx.catalog.successful_releases(cfg.catalog.run_class_import)) | published | {just_imported}
            keep = set(sorted(imported, reverse=True)[:cfg.retention.keep_releases]) | {just_imported}
            for release in sorted(imported - keep):
                try:
                    if release in published:
                        dropped = drop_release(conn, db.raw_schema, db.schema, release)
                        log.info("retention dropped %d partitions of release %s", len(dropped), release)
                    _delete_ndjson(ctx, release)
                except Exception as exc:
                    conn.rollback()
                    warnings.append(f"retention of release {release}: {exc}")
    except Exception as exc:
        warnings.append(f"retention: {exc}")
    for warning in warnings:
        log.warning(warning)
    return warnings
```

- [ ] **Step 4: Call retention from the import stage**

In `src/npd_loader/import_stage.py`, add the import:

```python
from npd_loader.retention import apply_retention
```

and replace `_finish` with:

```python
def _finish(ctx: Context, run: Run, release: date, summary: list[dict]) -> None:
    warnings = apply_retention(ctx, release)
    ctx.catalog.finish_run(run, SUCCESS, result="; ".join(warnings) or None,
                           output_xml=build_output_xml(summary, item_tag="table"))
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `python -m pytest tests/test_retention.py tests/test_import.py -v`
Expected: 10 passed

- [ ] **Step 6: Commit**

```bash
git add src/npd_loader/retention.py src/npd_loader/import_stage.py tests/test_retention.py
git commit -m "feat: retention of the newest N imported releases"
```

---

### Task 17: CLI

**Files:**
- Create: `src/npd_loader/cli.py`
- Test: `tests/test_cli.py`

**Interfaces:**
- Consumes: every stage (`run_download`, `run_extract`, `run_import`), `init_db`, `advisory_lock`, `published_releases`, `CssCatalogPg`, `LocalStorage`, `load_config`, `conninfo`, and the profile functions.
- Produces: `main(argv: list[str] | None = None) -> int`, `build_parser()`, `build_context(config) -> Context`, and `format_status(catalog, cat_cfg, published: list[date], limit=10) -> str`. Commands: `download [--force]`, `extract [--release D] [--force]`, `import [--release D] [--force]`, `run`, `status`, `init-db`, and `profile PATH [--unmapped] [--limit N]`. `profile --unmapped` exits 1 when unmapped paths exist. The global option is `--config`, which defaults to env `NPD_LOADER_CONFIG` or `/etc/npd-loader/config.toml`.

- [ ] **Step 1: Write the failing tests `tests/test_cli.py`**

```python
import json
from datetime import date
from types import SimpleNamespace

import zstandard

from npd_loader import cli
from npd_loader.config import parse_config
from npd_loader.stages import Outcome, StageFailed
from fakes import FakeCatalog
from fixture_data import PRAC1, PRAC2
from helpers import config_data


def write_zst(path, records):
    path.write_bytes(zstandard.ZstdCompressor().compress(
        b"".join(json.dumps(r).encode() + b"\n" for r in records)))


def test_profile_command(tmp_path, capsys):
    path = tmp_path / "06-Practitioner.ndjson.zst"
    write_zst(path, [PRAC1, PRAC2])
    assert cli.main(["profile", str(path)]) == 0
    out = capsys.readouterr().out
    assert "== Practitioner: 2 resources" in out and ".name[].family" in out
    assert cli.main(["profile", str(path), "--unmapped"]) == 0
    write_zst(path, [{**PRAC1, "birthDate": "1970-01-01"}])
    assert cli.main(["profile", str(path), "--unmapped"]) == 1
    assert ".birthDate" in capsys.readouterr().out


def test_missing_config_exits_nonzero(tmp_path, caplog):
    assert cli.main(["--config", str(tmp_path / "nope.toml"), "status"]) == 1
    assert "config file not found" in caplog.text


def fake_context(monkeypatch, tmp_path, calls, outcomes):
    config = parse_config(config_data(tmp_path, "https://example.test/downloads/manifest.json"))
    monkeypatch.setattr(cli, "load_config", lambda path: config)
    monkeypatch.setattr(cli, "build_context",
                        lambda cfg: SimpleNamespace(http=SimpleNamespace(close=lambda: calls.append("close"))))

    def stage(name):
        def run(ctx, *args, **kwargs):
            calls.append((name, args, kwargs))
            result = outcomes.get(name, Outcome.SUCCESS)
            if isinstance(result, Exception):
                raise result
            return result
        return run
    for name in ("download", "extract", "import"):
        monkeypatch.setattr(cli, f"run_{name}", stage(name))


def test_run_is_download_then_import(monkeypatch, tmp_path):
    calls = []
    fake_context(monkeypatch, tmp_path, calls, {})
    assert cli.main(["--config", "x", "run"]) == 0
    assert [c[0] for c in calls[:-1]] == ["download", "import"] and calls[-1] == "close"


def test_options_are_passed(monkeypatch, tmp_path):
    calls = []
    fake_context(monkeypatch, tmp_path, calls, {})
    assert cli.main(["--config", "x", "import", "--release", "2026-09-29", "--force"]) == 0
    assert calls[0] == ("import", (), {"release": date(2026, 9, 29), "force": True})
    assert cli.main(["--config", "x", "download", "--force"]) == 0
    assert calls[2] == ("download", (), {"force": True})


def test_failures_and_locks(monkeypatch, tmp_path):
    calls = []
    fake_context(monkeypatch, tmp_path, calls, {"import": StageFailed("boom"), "download": Outcome.LOCKED})
    assert cli.main(["--config", "x", "download"]) == 0
    assert cli.main(["--config", "x", "import"]) == 1


def test_format_status():
    catalog = FakeCatalog()
    r1, r2 = date(2026, 9, 22), date(2026, 9, 29)
    d1 = catalog.add_successful_run("DOWNLOAD", r1)
    i1 = catalog.add_successful_run("IMPORT", r1)
    d2 = catalog.add_successful_run("DOWNLOAD", r2)
    cfg = parse_config(config_data("/tmp", "https://x/manifest.json")).catalog
    text = cli.format_status(catalog, cfg, published=[r1])
    lines = text.splitlines()
    assert lines[0].split() == ["release", "download", "extract", "import", "published"]
    assert lines[1].split() == ["2026-09-29", str(d2.id), "-", "-", "no"]
    assert lines[2].split() == ["2026-09-22", str(d1.id), "-", str(i1.id), "yes"]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_cli.py -v`
Expected: FAIL with `ImportError: cannot import name 'cli'`

- [ ] **Step 3: Implement `cli.py`**

```python
"""npd-loader command line."""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import date

import httpx

from npd_loader.catalog import Catalog, CssCatalogPg
from npd_loader.config import CatalogConfig, Config, ConfigError, load_config
from npd_loader.credentials import conninfo
from npd_loader.db import advisory_lock, published_releases
from npd_loader.download import run_download
from npd_loader.extract import run_extract
from npd_loader.import_stage import run_import
from npd_loader.profile import format_report, load_mapped_paths, profile_file, unmapped
from npd_loader.schema import init_db
from npd_loader.stages import Context, StageFailed
from npd_loader.storage import LocalStorage

log = logging.getLogger("npd_loader")
DEFAULT_CONFIG = "/etc/npd-loader/config.toml"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="npd-loader", description="Load the CMS NPD FHIR release into Postgres")
    parser.add_argument("--config", default=os.environ.get("NPD_LOADER_CONFIG", DEFAULT_CONFIG))
    parser.add_argument("-v", "--verbose", action="store_true")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("download", help="download the current release").add_argument("--force", action="store_true")
    for name in ("extract", "import"):
        p = sub.add_parser(name, help=f"{name} a release (default: newest downloaded)")
        p.add_argument("--release", type=date.fromisoformat)
        p.add_argument("--force", action="store_true")
    sub.add_parser("run", help="download, then import (extracting as needed)")
    sub.add_parser("status", help="show recent releases")
    sub.add_parser("init-db", help="create npd database objects")
    p = sub.add_parser("profile", help="list JSON paths in an .ndjson or .ndjson.zst file")
    p.add_argument("path")
    p.add_argument("--unmapped", action="store_true", help="only paths not in mapped_paths.txt; exit 1 if any")
    p.add_argument("--limit", type=int)
    return parser


def build_context(config: Config) -> Context:
    npd = conninfo(config, "npd_db")
    return Context(
        config=config,
        catalog=CssCatalogPg(conninfo(config, "catalog"), config.catalog),
        storage=LocalStorage(config.storage.root),
        http=httpx.Client(timeout=config.download.timeout_seconds, headers={"User-Agent": "npd-loader/0.1"}),
        lock=lambda stage: advisory_lock(npd, stage),
        npd_conninfo=npd,
    )


def format_status(catalog: Catalog, cat: CatalogConfig, published: list[date], limit: int = 10) -> str:
    classes = [cat.run_class_download, cat.run_class_extract, cat.run_class_import]
    releases = set(published)
    for run_class in classes:
        releases |= set(catalog.successful_releases(run_class))
    lines = [f"{'release':<12}{'download':>10}{'extract':>10}{'import':>10}  published"]
    for release in sorted(releases, reverse=True)[:limit]:
        ids = []
        for run_class in classes:
            run = catalog.last_successful_run(run_class, release)
            ids.append(str(run.id) if run else "-")
        lines.append(f"{release.isoformat():<12}{ids[0]:>10}{ids[1]:>10}{ids[2]:>10}  "
                     f"{'yes' if release in published else 'no'}")
    return "\n".join(lines)


def _profile(args: argparse.Namespace) -> int:
    prof = profile_file(args.path, limit=args.limit)
    if args.unmapped:
        rows = unmapped(prof, load_mapped_paths())
        print(format_report(prof, rows))
        return 1 if rows else 0
    print(format_report(prof))
    return 0


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO, stream=sys.stderr,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    try:
        if args.command == "profile":
            return _profile(args)
        config = load_config(args.config)
        if args.command == "init-db":
            init_db(conninfo(config, "npd_db"), config.npd_db.raw_schema, config.npd_db.schema)
            log.info("npd database objects are up to date")
            return 0
        ctx = build_context(config)
        try:
            if args.command == "download":
                outcome = run_download(ctx, force=args.force)
            elif args.command == "extract":
                outcome = run_extract(ctx, release=args.release, force=args.force)
            elif args.command == "import":
                outcome = run_import(ctx, release=args.release, force=args.force)
            elif args.command == "run":
                run_download(ctx)
                outcome = run_import(ctx)
            else:  # status
                print(format_status(ctx.catalog, config.catalog,
                                    published_releases(ctx.npd_conninfo, config.npd_db.schema)))
                return 0
        finally:
            ctx.http.close()
        log.info("%s: %s", args.command, outcome)
        return 0
    except (StageFailed, ConfigError) as exc:
        log.error("%s", exc)
        return 1
    except Exception:
        log.exception("unexpected error")
        return 1
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_cli.py -v`
Expected: 6 passed

- [ ] **Step 5: Commit**

```bash
git add src/npd_loader/cli.py tests/test_cli.py
git commit -m "feat: npd-loader CLI"
```

---

### Task 18: End-to-end test across six releases

**Files:**
- Test: `tests/test_e2e.py`

**Interfaces:**
- Consumes: `cli.main`, `CssCatalogPg`, `LocalStorage`, `list_release_partitions`, `config_data`/`to_toml` (Task 7 helpers), `build_release`, `fixture_data.RECORDS`, and fixtures `cms`, `make_db`, and `catalog_db`.

- [ ] **Step 1: Write the test**

```python
import copy
from datetime import date, timedelta

import psycopg

from npd_loader.catalog import CssCatalogPg
from npd_loader.cli import main
from npd_loader.config import parse_config
from npd_loader.db import list_release_partitions
from npd_loader.storage import LocalStorage
import fixture_data
from helpers import config_data, to_toml
from release_builder import build_release

BASE = date(2026, 8, 4)
ORG1 = fixture_data.ORG1["id"]


def publish(cms, week: int) -> date:
    release = BASE + timedelta(weeks=week)
    records = copy.deepcopy(fixture_data.RECORDS)
    records["01-Organization.ndjson"][0]["name"] = f"ORG {release}"
    cms.publish(build_release(release.isoformat(), records=records))
    return release


def test_six_releases_end_to_end(tmp_path, cms, make_db, catalog_db):
    npd = make_db()
    data = config_data(tmp_path / "data", cms.manifest_url, npd_conninfo=npd, catalog_conninfo=catalog_db,
                       keep_releases=5)
    config_path = tmp_path / "config.toml"
    config_path.write_text(to_toml(data))
    catalog = CssCatalogPg(catalog_db, parse_config(data).catalog)
    storage = LocalStorage(tmp_path / "data")

    def cli(*args):
        return main(["--config", str(config_path), *args])

    def raw_pairs():
        with psycopg.connect(npd) as conn:
            return set(conn.execute("SELECT DISTINCT ndjson_file_id, zst_file_id FROM npd_raw.resource "
                                    "WHERE release_date = %s", (r1,)).fetchall())

    def run_count():
        with psycopg.connect(catalog_db) as conn:
            return conn.execute("SELECT count(*) FROM master_warehouse_run").fetchone()[0]

    assert cli("init-db") == 0

    # release 1: download -> extract -> import, lineage matches the catalog
    r1 = publish(cms, 0)
    assert cli("run") == 0
    assert run_count() == 3
    ndjson = catalog.get_data_files(r1, "ndjson")
    zst = catalog.get_data_files(r1, "ndjson.zst")
    assert len(ndjson) == 8 and {n.parent_file for n in ndjson} == {z.id for z in zst}
    assert raw_pairs() == {(n.id, n.parent_file) for n in ndjson}

    # nothing to do the second time
    assert cli("run") == 0
    assert run_count() == 3

    # forced import replaces the partitions
    assert cli("import", "--force") == 0
    with psycopg.connect(npd) as conn:
        assert list(list_release_partitions(conn, "npd", "practitioner")) == [r1]

    # deleted .ndjson is re-extracted in place with the same data_file id
    victim = ndjson[0]
    storage.delete(victim.file_rel_path)
    assert cli("import", "--release", r1.isoformat(), "--force") == 0
    assert [n.id for n in catalog.get_data_files(r1, "ndjson")] == [n.id for n in ndjson]
    assert storage.exists(victim.file_rel_path)
    assert raw_pairs() == {(n.id, n.parent_file) for n in ndjson}

    # releases 2..6: release 1 falls out of the newest 5
    releases = [r1] + [None] * 5
    for week in range(1, 6):
        releases[week] = publish(cms, week)
        assert cli("run") == 0
    with psycopg.connect(npd) as conn:
        assert sorted(list_release_partitions(conn, "npd_raw", "resource")) == releases[1:]
        assert sorted(list_release_partitions(conn, "npd", "practitioner")) == releases[1:]
        assert [r[0] for r in conn.execute("SELECT release_date FROM npd.release ORDER BY 1")] == releases[1:]
        assert conn.execute("SELECT name FROM npd.v_organization WHERE resource_id = %s",
                            (ORG1,)).fetchone()[0] == f"ORG {releases[-1]}"
    old_ndjson = catalog.get_data_files(r1, "ndjson")
    assert old_ndjson and not any(storage.exists(n.file_rel_path) for n in old_ndjson)
    assert all(storage.exists(z.file_rel_path) for z in catalog.get_data_files(r1, "ndjson.zst"))
    assert all(storage.exists(m.file_rel_path) for m in catalog.get_data_files(r1, "manifest"))
    for release in releases[1:]:
        assert all(storage.exists(n.file_rel_path) for n in catalog.get_data_files(release, "ndjson"))

    assert cli("status") == 0
```

- [ ] **Step 2: Run it**

Run: `python -m pytest tests/test_e2e.py -v`
Expected: 1 passed. A failure here points at an integration seam the unit tests missed. Fix that seam in the module that owns it, add a unit test there, and re-run.

- [ ] **Step 3: Run the whole suite**

Run: `python -m pytest -v`
Expected: all passed and **no** skipped tests. A skip means Docker was unavailable, and the Postgres paths were not verified.

- [ ] **Step 4: Commit**

```bash
git add tests/test_e2e.py
git commit -m "test: end-to-end download/extract/import/retention across six releases"
```

---

### Task 19: Full-release profiling, catalog DDL check, and deployment docs

**Files:**
- Create: `docs/profile/2026-09-29.md` (or the release current when this runs), `README.md`
- Modify (only if profiling finds unmapped paths): `src/npd_loader/sql/init/003_tables.sql`, `src/npd_loader/sql/transform/*.sql`, `src/npd_loader/sql/mapped_paths.txt`, `tests/fixture_data.py`, `tests/test_transform_*.py`
- Modify (only if Task 4 used the assumed DDL): `tests/sql/css_catalog_schema.sql`, `src/npd_loader/catalog.py`

This task finalizes column lists against one full release (spec §7.2). The tables so far are based on the first 3,000 records of each file. For example, those PractitionerRole records only carry `practitioner` and `endpoint`.

- [ ] **Step 1: Download the current release's files for profiling**

On `192.10.0.7` (about 2.4 GB):

```bash
mkdir -p /data/npd/profile && cd /data/npd/profile
curl -sSL -o manifest.json https://directory.cms.gov/downloads/manifest.json
for f in $(python3 -c "import json; print(' '.join(json.load(open('manifest.json'))['files']))"); do
  curl -sSL --retry 5 -o "$f.zst" "https://directory.cms.gov/downloads/$f.zst"
done
ls -l
```

- [ ] **Step 2: Profile every file and list unmapped paths**

```bash
mkdir -p ~/npd_fhir_loader/docs/profile
for z in /data/npd/profile/*.zst; do npd-loader profile "$z"; done > /tmp/profile-full.txt
for z in /data/npd/profile/*.zst; do npd-loader profile "$z" --unmapped; done > /tmp/profile-unmapped.txt
```

Practitioner (16.7 GB) and PractitionerRole (11.1 GB) take the longest; expect tens of minutes in total. Write `docs/profile/<release>.md` containing the `generated_at` date, the contents of `/tmp/profile-full.txt` in a code block, and a "Decisions" section that starts empty.

- [ ] **Step 3: Decide every unmapped path**

For each line in `/tmp/profile-unmapped.txt`, choose one of these and record it in the Decisions section:
- **Flatten** paths analysts will filter or join on: references, codes, identifiers, names, addresses, telecom, dates, flags.
- **Raw-only** paths that duplicate a flattened value: `coding[].system` when the code is kept, `display` text, `version`. Raw-only paths stay queryable in `npd_raw.resource`.

For **raw-only**, add the path to `mapped_paths.txt` under that resource's `# raw-only` comment.

For **flatten**, use the same pattern as Tasks 12–14. Worked example: if `PractitionerRole .telecom[].value` (with `.system` and `.use`) appears:
1. In `003_tables.sql`, add a `practitioner_role_telecom` table with the same columns and key index as `practitioner_telecom`. This is before first deployment, so editing the file is enough. After deployment, use `ALTER TABLE` on the parent, which propagates to partitions.
2. In `050_practitioner_role.sql`, append a copy of the `practitioner_telecom` INSERT from `010_practitioner.sql`, with `<<t:practitioner_telecom>>` changed to `<<t:practitioner_role_telecom>>` and `'Practitioner'` changed to `'PractitionerRole'`.
3. Add the three paths to `mapped_paths.txt` under `# PractitionerRole`.
4. In `tests/fixture_data.py`, add `"telecom": [{"system": "phone", "value": "5551234567", "use": "work"}]` to `PR2`. In `test_transform_roles.py`, add `assert rows(conn, res, "practitioner_role_telecom", "resource_id, seq, system, use, value") == [(PR2, 1, "phone", "work", "5551234567")]`.
5. Add the new table to `NPD_TABLES` in `tests/test_schema.py`.

Repeat until this exits 0 for every file:

```bash
for z in /data/npd/profile/*.zst; do npd-loader profile "$z" --unmapped || echo "UNMAPPED in $z"; done
```

Run `python -m pytest -v`. Expected: all passed, none skipped.

- [ ] **Step 4: Confirm the catalog DDL**

If `tests/sql/css_catalog_schema.sql` still holds the assumed DDL from Task 4, replace it with the real `pg_dump` output (Task 4 Step 1). Adjust any column names in `CssCatalogPg`, then run `python -m pytest tests/test_catalog_pg.py tests/test_e2e.py -v`. Expected: all passed.

- [ ] **Step 5: Write `README.md`**

````markdown
# npd-loader

Loads the CMS National Provider Directory FHIR bulk release (`https://directory.cms.gov/downloads/`) into
Postgres: raw JSONB in `npd_raw.resource`, flattened tables in `npd`, one partition per release, the newest
5 releases kept. Every run and file is recorded in `css_catalog_local` (`master_warehouse_run`, `data_file`).
Design: `docs/superpowers/specs/2026-10-03-npd-fhir-loader-design.md`.

## Install (192.10.0.7)

```bash
sudo mkdir -p /opt/npd-loader /etc/npd-loader /data/npd
git clone <repo> /opt/npd-loader/src
python3.12 -m venv /opt/npd-loader/.venv
/opt/npd-loader/.venv/bin/pip install /opt/npd-loader/src
sudo cp /opt/npd-loader/src/config.example.toml /etc/npd-loader/config.toml
sudo chmod 600 /etc/npd-loader/config.toml   # then fill in users and passwords
/opt/npd-loader/.venv/bin/npd-loader init-db
```

`init-db` is idempotent. The `npd` database must already exist, and the `npd_db` user needs CREATE on it.
The `catalog` user needs INSERT/UPDATE/SELECT on `master_warehouse_run` and `data_file`.

## Commands

| Command | What it does |
|---|---|
| `npd-loader run` | download the current release, then import it (extracting first if needed) |
| `npd-loader download [--force]` | DOWNLOAD run: manifest + `.zst` files |
| `npd-loader extract [--release YYYY-MM-DD] [--force]` | EXTRACT run: `.zst` → `.ndjson`; restores deleted files in place |
| `npd-loader import [--release YYYY-MM-DD] [--force]` | IMPORT run: load, transform, publish atomically, apply retention |
| `npd-loader status` | recent releases, their successful runs, and whether each is published |
| `npd-loader profile FILE [--unmapped]` | JSON paths in an `.ndjson[.zst]` file; `--unmapped` exits 1 if any path is unmapped |

Exit code 0 means success, nothing to do, or another run holds the lock; anything else is a failure. Logs go to stderr.

## Schedule

systemd (`/etc/systemd/system/npd-loader.service` and `.timer`):

```ini
[Unit]
Description=Load the CMS NPD FHIR release

[Service]
Type=oneshot
User=npd
ExecStart=/opt/npd-loader/.venv/bin/npd-loader --config /etc/npd-loader/config.toml run
```

```ini
[Unit]
Description=Daily NPD FHIR load

[Timer]
OnCalendar=*-*-* 06:00:00
Persistent=true

[Install]
WantedBy=timers.target
```

```bash
sudo systemctl daemon-reload && sudo systemctl enable --now npd-loader.timer
journalctl -u npd-loader.service
```

or cron: `0 6 * * * /opt/npd-loader/.venv/bin/npd-loader --config /etc/npd-loader/config.toml run >> /var/log/npd-loader.log 2>&1`

## Querying

`npd.v_<table>` views show the newest published release. Query the base tables with `release_date = ...` for older
ones. Every row has `ndjson_file_id` and `zst_file_id`, which are `data_file.id` values in `css_catalog_local`.

## Tests

```bash
python -m pip install -e ".[test]"
python -m pytest -v          # needs Docker; NPD_TEST_PG_IMAGE=postgres:<server major> to match the server
```
````

- [ ] **Step 6: Deployment smoke run on `192.10.0.7`**

Before running, confirm the free space listed in spec §15: about 175 GB for NDJSON and about 2.35 GB per release for `.zst` on `192.10.0.7`, and 250–400 GB on `192.10.0.6`. Then:

```bash
npd-loader --config /etc/npd-loader/config.toml init-db
npd-loader --config /etc/npd-loader/config.toml run      # first run: hours
npd-loader --config /etc/npd-loader/config.toml status
psql -h 192.10.0.6 -d npd -c "SELECT resource_type, count(*) FROM npd_raw.v_resource GROUP BY 1 ORDER BY 1"
psql -h 192.10.0.6 -d npd -c "SELECT count(*), count(npi) FROM npd.v_practitioner"
```

Expected: `status` shows the release with download, extract, and import run ids and `published yes`. The raw counts match the number of lines in each `.ndjson` (`wc -l`). Then enable the timer (README).

- [ ] **Step 7: Commit**

```bash
git add README.md docs/profile src/npd_loader/sql tests
git commit -m "docs: full-release profile decisions, README, deployment"
```
