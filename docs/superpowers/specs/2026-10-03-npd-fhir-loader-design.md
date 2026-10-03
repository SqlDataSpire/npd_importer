# NPD FHIR Loader — Design

**Date:** 2026-10-03
**Status:** Draft for review
**Owner:** eellsworth

## 1. Purpose

Load the CMS National Provider Directory (NPD) FHIR bulk release from
`directory.cms.gov` into Postgres, release by release, with full traceability
from every database row back to the exact file it came from.

Use cases, in priority order:

1. **Raw landing (C)** — every FHIR resource stored as JSON in Postgres, retained
   even after it has been flattened into tables.
2. **Analyst tables (A)** — relational tables for querying practitioners,
   organizations, locations, roles, etc.
3. **Joins with other Checkbook data (B)** — tables expose standard keys (NPI,
   FHIR ids) so they can be joined to other datasets.

Every raw and table row is labeled with the **release date** and with the
`data_file` ids of the files it was loaded from.

## 2. Source

- Manifest: `https://directory.cms.gov/downloads/manifest.json`
  - `generated_at` (e.g. `2026-09-29`) is the **release date**.
  - `files` lists 8 files with `compressed_bytes` and `original_bytes`.
  - `compression_algorithm` is `zstd`.
- Files: `https://directory.cms.gov/downloads/{name}.zst`, e.g.
  `01-Organization.ndjson.zst`. The URL 302-redirects to a short-lived
  pre-signed S3 URL (`npd-east-prod-bulk-site.s3.amazonaws.com`) that supports
  HTTP `Range` requests and returns `Last-Modified` and `ETag`. HEAD requests to
  the signed URL return 403; GET works.
- Files (release 2026-09-29): Organization, Location, Endpoint,
  HealthcareService, InsurancePlan, Practitioner, PractitionerRole,
  OrganizationAffiliation. About 2.35 GB compressed, 34.6 GB uncompressed
  (Practitioner 16.7 GB, PractitionerRole 11.1 GB).
- Each NDJSON line is one FHIR resource with `resourceType`, `id`, and
  `meta.lastUpdated`. References look like
  `{"reference": "Organization/Organization-1336200294"}`.
- A `.zst` file is a single compressed stream (not an archive), so it always
  decompresses to exactly one NDJSON file. This is verified by checking the
  decompressed size against `original_bytes` and by parsing every line.

## 3. Environment

| Item | Value |
|---|---|
| Script host | Linux, `192.10.0.7`, Python 3.12.3 |
| Postgres server | `192.10.0.6:5432` |
| Data database | `npd` (new), schemas `npd_raw` and `npd` |
| Catalog database | `css_catalog_local` (existing), tables `public.master_warehouse_run`, `public.data_file` |
| File storage | `npd_data_folder`, a local directory on `192.10.0.7` for now |
| Dependencies | `psycopg` (v3), `zstandard`, `httpx`; `tomllib` (stdlib) |
| Test dependencies | `pytest`, `testcontainers` (Docker Postgres), a local HTTP test server |

All database names, schema names, paths, and catalog labels are config values,
never hard-coded, so the `npd` schemas can later move into a shared database
with `pg_dump`/`pg_restore` and a config change.

## 4. Architecture

A single Python package, `npd_loader`, with a CLI. Three pieces are
**swappable backends** chosen in config, so later migrations do not touch the
pipeline logic:

| Concern | Interface | Now | Later |
|---|---|---|---|
| Credentials | `get_db_credentials(config)` | config file | 1Password SDK |
| Run and file tracking | `Catalog` | `CssCatalogPg` (`css_catalog_local`) | `AzureApiCatalog` |
| File storage | `Storage` | `LocalStorage` | `S3Storage` / `AzureBlobStorage` |

Only the "now" implementations are built in this project.

### 4.1 Modules

| Module | Responsibility |
|---|---|
| `cli.py` | Commands: `download`, `extract`, `import`, `run`, `status`, `init-db` |
| `config.py` | Load and validate TOML config |
| `credentials.py` | `get_db_credentials()`; config-file backend |
| `catalog.py` | `Catalog` interface and `CssCatalogPg` |
| `storage.py` | `Storage` interface and `LocalStorage` |
| `manifest.py` | `fetch_manifest()` → release date plus expected files and sizes |
| `download.py` | DOWNLOAD stage |
| `extract.py` | EXTRACT stage |
| `raw_load.py` | IMPORT stage, raw part: stream NDJSON into `npd_raw` |
| `transform.py` + `sql/` | IMPORT stage, table part: numbered SQL scripts that build `npd` tables |
| `retention.py` | Drop data and `.ndjson` files for releases beyond the newest 5 |

### 4.2 `Catalog` interface

```python
class Catalog(Protocol):
    def start_run(self, run_class, description, config_xml) -> Run: ...
    def finish_run(self, run, status, result=None, output_xml=None) -> None: ...
    def add_data_file(self, run, **fields) -> int: ...          # returns data_file.id
    def update_data_file(self, file_id, **fields) -> None: ...
    def get_data_files(self, release, file_type) -> list[DataFile]: ...
    def last_successful_run(self, run_class, release) -> Run | None: ...
```

### 4.3 `Storage` interface

Stream-based so that object stores can implement it without changing callers:

```python
class Storage(Protocol):
    def open_write(self, rel_path) -> BinaryIO: ...
    def open_read(self, rel_path) -> BinaryIO: ...
    def exists(self, rel_path) -> bool: ...
    def size(self, rel_path) -> int: ...
    def delete(self, rel_path) -> None: ...
    def rename(self, src_rel, dst_rel) -> None: ...
    def list(self, prefix) -> list[str]: ...
    def uri(self, rel_path="") -> str: ...   # file:///data/npd/... later s3://...
```

## 5. Catalog conventions (`css_catalog_local`)

Existing usage (from recent rows): `parent_run_id` is unused, status columns are
NULL in practice, and newer runs use a `<WAREHOUSE_RUN_CONFIG>` block in
`xml_config`. This loader follows the `<WAREHOUSE_RUN_CONFIG>` style and fills in
completion fields itself.

### 5.1 `master_warehouse_run`

One top-level run per stage execution. No child runs.

| Column | Value |
|---|---|
| `project` | `NPD` |
| `run_type` | `National Provider Directory` |
| `run_class` | `DOWNLOAD`, `EXTRACT`, or `IMPORT` |
| `run_description` | e.g. `NPD FHIR Download 2026-09-29` / `NPD FHIR Extract 2026-09-29` / `NPD FHIR Import 2026-09-29` |
| `xml_config` | `<WAREHOUSE_RUN_CONFIG>` with `run_type`, `file_set`, `data_store_base_path`, `run_description`, `release_date`, options (e.g. `force`), and for EXTRACT/IMPORT the source run ids |
| `completion_status` | NULL while running, then `Success` or `Failed` |
| `date_completed` | set on finish |
| `result` | short error or warning summary |
| `xml_output` | per-file or per-table summary (sizes, counts, timings) |

All of these labels are config values.

### 5.2 `data_file`

| Column | `.zst` (and manifest) row | `.ndjson` row |
|---|---|---|
| `run_id` | DOWNLOAD run | EXTRACT run that first created it |
| `file_set` | `NPD_FHIR` | `NPD_FHIR` |
| `file_type` | `ndjson.zst` / `manifest` | `ndjson` |
| `source_uri` | public URL, e.g. `https://directory.cms.gov/downloads/06-Practitioner.ndjson.zst` (never the signed S3 URL) | same as parent |
| `source_version_name` | `NPD release` | `NPD release` |
| `source_version_num` | release date, e.g. `2026-09-29` | same |
| `file_name` | `file_{id}_06-Practitioner.ndjson.zst` | `file_{id}_06-Practitioner.ndjson` |
| `file_rel_path` | `run_{runid}_{ts}/file_{id}_….ndjson.zst` | `run_{runid}_{ts}/file_{id}_….ndjson` |
| `run_type_root_dir` | `Storage.uri()` of the data folder | same |
| `parent_file` | NULL for the manifest; the manifest's id for the 8 `.zst` rows | the `.zst` row's id |
| `file_size`, `file_hash` | bytes, SHA-256 | bytes, SHA-256 |
| `date_modified` | S3 `Last-Modified` | — |
| `date_created` | download finished | first extraction finished |
| `date_loaded` | — | most recent successful import |
| `exceptions` | validation or download errors (truncated to 8000 chars) | validation errors |

Catalog rows are never deleted by the loader.

## 6. File layout and retention

```
{npd_data_folder}/
  run_62018_2026-06-16-150657/                 ← DOWNLOAD run folder
    file_12344_manifest.json
    file_12345_01-Organization.ndjson.zst
    ...
  run_62019_2026-06-16-160102/                 ← EXTRACT run folder
    file_12353_01-Organization.ndjson
    ...
```

- Run folder name: `run_{run id}_{start time as yyyy-mm-dd-hhmmss}`.
- File names: `file_{data_file.id}_` prepended to the original file name.
- In-progress downloads use a `.part` suffix and are renamed on verification.
- `.zst` files and the manifest are **never deleted** by the loader; cleaning up
  old files is handled separately.
- `.ndjson` files are kept for the **newest 5 imported releases** and deleted
  by retention after that. Their `data_file` rows stay.

## 7. Postgres data model (`npd` database)

### 7.1 Raw layer: `npd_raw.resource`

```sql
release_date    date        NOT NULL,
resource_type   text        NOT NULL,
resource_id     text        NOT NULL,
last_updated    timestamptz,
ndjson_file_id  integer     NOT NULL,
zst_file_id     integer     NOT NULL,
line_number     bigint      NOT NULL,
resource        jsonb       NOT NULL
-- PARTITION BY LIST (release_date), each sub-partitioned BY LIST (resource_type)
-- unique (release_date, resource_type, resource_id)
```

### 7.2 Table layer: schema `npd`

Partitioned by `release_date` like the raw layer. Every row carries
`release_date`, `ndjson_file_id`, `zst_file_id`, and `resource_id`.
References are stored as plain ids (`"Organization/Organization-123"` becomes
`organization_id = 'Organization-123'`).

| Main table | Child tables (repeating elements) |
|---|---|
| `practitioner`: NPI, gender, active, official name, CMS flags (identity verified, Medicare enrolled, HHS exclusion list, aligned with data network) | `practitioner_name`, `practitioner_address`, `practitioner_telecom`, `practitioner_qualification` |
| `organization`: NPI, pseudo-EIN, name, type, `part_of` organization, verification status | `organization_address`, `organization_telecom` |
| `location`: address, latitude/longitude, managing organization | `location_telecom` |
| `practitioner_role`: practitioner, organization, location, active | `practitioner_role_endpoint` (plus specialty and other fields found by profiling) |
| `organization_affiliation`, `endpoint`, `healthcare_service`, `insurance_plan` | `insurance_plan_alias`, `insurance_plan_network` |
| `identifier`: every identifier from every resource | — |

Latest-release views (`npd.v_practitioner`, and so on) filter to the newest
imported release.

**Column lists are finalized by profiling one full release** (the first
implementation task). The initial samples only covered the start of each file.

### 7.3 Atomic publication

Each import builds raw and table data in standalone tables, indexes them,
validates them, and then attaches every partition in **one transaction**.
With `--force`, the existing partitions for that release are detached and
dropped in the same transaction. Readers see a release completely or not at
all.

## 8. Run flow

`npd-loader run` performs download → extract → import. Each stage is its own
catalog run and its own command, and each is idempotent.

### 8.1 Download (`run_class = DOWNLOAD`)

1. Fetch the manifest; the release is `generated_at`.
2. If a successful DOWNLOAD run exists for the release and `--force` is not
   set, log and exit 0.
3. Start the run; create the run folder.
4. Add the manifest's `data_file` row and save the manifest as
   `file_{id}_manifest.json`.
5. For each listed file: add its `data_file` row; GET it to `.part` (follow
   the redirect, resume with `Range` on interruption, retry with backoff, get
   a fresh signed URL on each retry); verify the size equals
   `compressed_bytes`; compute SHA-256; rename the file; update the row.
6. Re-fetch the manifest. If `generated_at` changed, fail the run.
7. Finish with `Success` and a per-file summary.

### 8.2 Extract (`run_class = EXTRACT`)

1. Choose the release (default: newest successful download; or `--release`).
   Use the `.zst` files from the newest successful DOWNLOAD run for it.
2. If every `.ndjson` exists and matches its recorded size and hash, and
   `--force` is not set, exit 0.
3. Start the run; create the run folder.
4. For each `.zst`:
   - **No `.ndjson` row yet:** add a row (`parent_file` = the `.zst` id),
     decompress into the run folder, verify the size equals `original_bytes`,
     compute SHA-256, update the row.
   - **Row exists, file missing (re-extraction):** decompress to the row's
     recorded `file_rel_path`, verify the size and hash match the row, and
     **reuse the same id**. On a mismatch, fail the run and do not reuse the
     id.
5. Finish with `Success`; `xml_output` lists files created and restored.

### 8.3 Import (`run_class = IMPORT`)

1. Choose the release (default: newest successful download; or `--release`).
   If a successful IMPORT exists and `--force` is not set, exit 0.
2. If any `.ndjson` for the release is missing, run the extract stage first
   (as its own EXTRACT run).
3. Start the run; `xml_config` records the release and the download and
   extract run ids.
4. For each `.ndjson`: stream lines into a standalone raw table with
   `COPY`. Each line must parse as JSON, have `id`, and have a `resourceType`
   matching the file. The number of rows loaded must match the number of
   lines read.
5. Build indexes and check for duplicate `(resource_type, resource_id)`.
6. Run the `sql/` transforms into standalone table partitions.
7. Attach all partitions in one transaction (Section 7.3), then `ANALYZE`.
8. Set `date_loaded` on the `.ndjson` rows; finish with `Success` and row
   counts per table.
9. Retention (Section 9).

## 9. Retention

After a successful import, for imported releases older than the newest 5
(`keep_releases = 5`, configurable):

- detach and drop their raw and table partitions;
- delete their `.ndjson` files from storage.

Retention never touches the release just imported, any `.zst` or manifest
file, or any catalog row. Before dropping anything, it confirms against the
`npd` database which release partitions actually exist. A retention error is
recorded as a warning in `result` and does not fail the import.

## 10. Error handling

- **Strict validation:** invalid JSON, a wrong or missing `resourceType`, a
  missing `id`, a size or hash mismatch, or a duplicate id fails the stage.
  Details go to `data_file.exceptions` and `master_warehouse_run.result`.
- **Unexpected errors:** the run is marked `Failed` and standalone tables are
  dropped. Published data is untouched.
- **Catalog independence:** catalog writes use their own connection and
  commit independently of data loads, so failed runs are always recorded.
- **Concurrency:** a Postgres advisory lock per stage prevents overlapping
  runs. A second run that cannot take the lock logs this and exits 0.
- **Retries:** failed runs are ignored by idempotency checks; the next run
  starts fresh (new run id and folder). Download resume happens only within
  a run.
- **Exit codes:** 0 for success or nothing to do, non-zero for failure. Logs
  go to stdout/stderr for cron or systemd to capture. Alerting is out of
  scope.

## 11. Configuration

`/etc/npd-loader/config.toml` (mode 600, never committed;
`config.example.toml` is in the repo):

```toml
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

[retention]
keep_releases = 5
```

## 12. Testing

- **Fixtures:** small `.ndjson.zst` files per resource type, built from real
  sample records, plus a matching manifest. Bad-data variants cover invalid
  JSON, a wrong `resourceType`, a duplicate id, and a size mismatch.
- **HTTP:** a local test server imitates `directory.cms.gov`: the redirect to
  a signed URL, `Range` resume, dropped connections, expired URLs, and a
  manifest that changes mid-run.
- **Catalog:** stages are unit-tested against an in-memory fake `Catalog`.
  `CssCatalogPg` is tested against Docker Postgres with copies of
  `master_warehouse_run` and `data_file`, never the live catalog.
- **Storage:** a shared test suite that every `Storage` implementation must
  pass, run against `LocalStorage` on a temp directory.
- **SQL transforms:** fixture records in, exact expected table rows out.
- **End-to-end:** download → extract → import → forced import → retention
  across 6 small releases. It checks partition drops, `.ndjson` deletion,
  `.zst` retention, re-extraction with id reuse, and correct
  `ndjson_file_id`/`zst_file_id` on rows.
- Postgres tests use `testcontainers` with an image matching the server
  version. Tests run on Python 3.12.

## 13. Deployment

- Git repo; installed into a Python 3.12 virtual environment on `192.10.0.7`.
- `npd-loader init-db` creates the `npd` database objects (schemas, parent
  tables, views).
- A daily cron job or systemd timer runs `npd-loader run`.

## 14. Out of scope

- 1Password, Azure catalog API, and S3/Azure Blob storage implementations
  (interfaces only).
- Deleting `.zst` files or catalog rows.
- Alerting or notifications.
- Moving the `npd` schemas into a shared database.

## 15. Open items

- Postgres server version on `192.10.0.6` (the design assumes 13 or later;
  it also sets the Docker test image).
- Free space on `192.10.0.7` for `npd_data_folder` (about 175 GB of NDJSON
  plus about 2.35 GB per release of `.zst`) and on `192.10.0.6` (about
  250–400 GB for 5 releases).
- Database logins: one that can create objects in `npd`, and one that can
  insert into and update `master_warehouse_run` and `data_file`.
