# Full-release import test — release 2026-09-29

A full `npd-loader run` against `https://directory.cms.gov/downloads/` on 2026-10-04, into empty databases. This
measures what the first production import will cost. It is not a production run.

## Environment

- Windows 11 dev workstation, Python 3.14 (production target is 3.12).
- PostgreSQL 16.4 (EDB portable binaries), local disk, default settings except `max_wal_size = 8GB`,
  `fsync = off`, and `shared_buffers = 128MB`.
- Catalog: a local copy of the **assumed** `css_catalog_local` DDL (`tests/sql/css_catalog_schema.sql`), not the
  real server.

## Result

Exit code 0. The catalog recorded three `Success` runs, and all 8 `.ndjson` `data_file` rows have `date_loaded`.

| Stage | Start | End | Duration |
|---|---|---|---|
| DOWNLOAD (2.35 GB) | 14:06:52 | 14:08:15 | 1 min 23 s |
| EXTRACT (34.6 GB) | 14:08:17 | 14:09:47 | 1 min 30 s |
| IMPORT, raw load | 14:09:48 | 17:16:46 | 3 h 7 min |
| IMPORT, transforms | 17:16:46 | 18:14:43 | 58 min |
| IMPORT, publish + ANALYZE + retention | 18:14:43 | 18:16:20 | 1.6 min |
| **Total** | | | **4 h 9 min (14,969 s)** |

Raw load per file: Organization took 5 min, Practitioner (16.7 GB) 19 min, and PractitionerRole (11.1 GB) 2 h 37 min.
PractitionerRole was far slower per byte than Practitioner. The workstation also stalled during an earlier test run,
so treat that figure as an outlier until it is measured on the server. The slowest transform was
`010_practitioner.sql` at about 38 min: four `ext()` calls plus `identifier_value()` per row over 7.5M rows.

Database size after publication: **73 GB**. The raw layer is 35 GB; the flattened tables are about 38 GB.

## Row counts

| Table | Rows |
|---|---|
| npd_raw.resource | 24,852,222 |
| — Practitioner | 7,481,906 |
| — PractitionerRole | 11,059,682 |
| — Location | 2,558,069 |
| — Organization | 2,058,139 |
| — Endpoint | 1,140,616 |
| — OrganizationAffiliation | 493,222 |
| — HealthcareService | 54,445 |
| — InsurancePlan | 6,143 |
| npd.practitioner_role_telecom | 26,330,884 |
| npd.practitioner_telecom | 24,723,185 |
| npd.practitioner_role_location | 24,572,175 |
| npd.practitioner_qualification | 21,249,125 |
| npd.practitioner_address | 18,150,026 |
| npd.organization_telecom | 14,457,692 |
| npd.identifier | 11,416,254 |
| npd.practitioner_name | 9,334,812 |
| npd.organization_address | 4,765,490 |
| npd.location_telecom | 3,790,079 |
| npd.practitioner_role_specialty | 3,229,314 |
| npd.practitioner_role_endpoint | 1,282,898 |
| npd.organization_endpoint | 156,616 |
| npd.organization_affiliation_network | 17,483 |
| npd.insurance_plan_alias | 12,286 |
| npd.healthcare_service_location | 273 |
| npd.insurance_plan_network | 233 |
| npd.practitioner_role_code | 0 (the release has no `PractitionerRole.code`) |

Every main table has one row per raw resource of its type. All 7,481,906 practitioners have an NPI.

## Implications for deployment

- Spec §15 estimated 250–400 GB of database space for 5 releases. At 73 GB per release, 5 releases need about
  365 GB, plus room for one release being built in standalone tables while 5 are live (about 440 GB peak).
- A first import takes several hours. The daily `run` is a no-op until CMS publishes a new `generated_at`, so this
  cost is paid once per release.
- If transform time matters, `010_practitioner.sql` is the place to optimise. For example, extract the four
  extension flags in one lateral pass instead of four `ext()` calls.
