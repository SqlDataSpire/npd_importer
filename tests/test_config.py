from pathlib import Path

import pytest

from npd_loader.config import ConfigError, load_config, parse_config

EXAMPLE = Path(__file__).resolve().parents[1] / "config.example.toml"


def minimal() -> dict:
    return {
        "source": {"manifest_url": "https://example.test/downloads/manifest.json"},
        "storage": {"backend": "local", "root": "/data/npd"},
        "databases": {"env_file": "database.env"},
        "npd_db": {"connection": "data"},
        "catalog": {"connection": "catalog", "project": "NPD", "run_type": "National Provider Directory",
                    "file_set": "NPD_FHIR"},
    }


def test_example_config_loads():
    cfg = load_config(EXAMPLE)
    assert cfg.source.manifest_url == "https://directory.cms.gov/downloads/manifest.json"
    assert cfg.storage.root == "D:\\npd"
    assert cfg.catalog.run_table == "dbo.MASTER_WAREHOUSE_RUN"
    assert cfg.npd_db.raw_schema == "npd_raw"
    assert cfg.npd_db.schema == "npd"
    assert cfg.catalog.file_set == "NPD_FHIR"
    assert cfg.retention.keep_releases == 1
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


def test_lock_timeout_default_and_override():
    assert parse_config(minimal()).npd_db.lock_timeout_seconds == 30.0
    assert load_config(EXAMPLE).npd_db.lock_timeout_seconds == 30.0
    data = minimal()
    data["npd_db"]["lock_timeout_seconds"] = 2
    assert parse_config(data).npd_db.lock_timeout_seconds == 2.0
    for bad in ("soon", 0):
        data["npd_db"]["lock_timeout_seconds"] = bad
        with pytest.raises(ConfigError, match="lock_timeout_seconds"):
            parse_config(data)


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


@pytest.mark.parametrize("section,value", [("storage", "s3")])
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


def test_connections_are_named():
    cfg = parse_config(minimal())
    assert cfg.databases.env_file == "database.env"
    assert (cfg.npd_db.connection, cfg.catalog.connection) == ("data", "catalog")


@pytest.mark.parametrize("section,key", [("npd_db", "host"), ("npd_db", "password"), ("catalog", "user"),
                                         ("catalog", "backend")])
def test_legacy_connection_keys_are_rejected(section, key):
    data = minimal()
    data[section][key] = "x"
    with pytest.raises(ConfigError, match="database.env"):
        parse_config(data)


def test_raw_schema_must_differ_from_schema():
    data = minimal()
    data["npd_db"] = {"connection": "data", "raw_schema": "npd", "schema": "npd"}
    with pytest.raises(ConfigError, match="raw_schema"):
        parse_config(data)
