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
