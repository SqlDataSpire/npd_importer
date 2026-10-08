# Phase 3: refinements (stub)

Items parked during Phase 2 (`docs/superpowers/specs/2026-10-06-phase2-python-flatten-upsert-design.md`). Not
designed yet; each needs its own decision before work starts.

## Before production

- `READ_COMMITTED_SNAPSHOT` is already ON in the production `npd` database (checked 2026-10-08; it is still empty).
  Size tempdb for one apply's version store.
  The first weekly update, with the old hash, used ~28 GB of version store and 205 GB of log. A first load used
  ~66 GB of log.
- Find out whether anything outside the loader reads the Phase 1 `npd.v_*` views in production. They are removed
  below.

## Schema and indexing

- **Remove the `v_*` views.** In Phase 2 each one is `SELECT * FROM npd.<table>` (the data is one merged dataset, so
  there is no "latest release" to select). Stop creating them in `init_db` (`dialect/mssql.py`), drop existing ones in
  `900_migrations.sql`, and update the README's Querying section.
- **Index reverse references.** Only 3 of the 19 `*_key` reference columns are indexed: `practitioner_role`
  practitioner and organization, `location.managing_organization_key`. Candidates, by expected use:
  - `practitioner_role_location.location_key` (24.6 M rows)
  - `organization.part_of_organization_key`
  - `organization_affiliation.organization_key` and `participating_organization_key`
  - `endpoint.managing_organization_key`, `practitioner_role_endpoint.endpoint_key`, `organization_endpoint.endpoint_key`
  - `practitioner_role.network_organization_key`

  Skip unless asked: `practitioner_qualification.issuer_organization_key` (21 M rows), and the small tables
  (insurance_plan, healthcare_service: under 60 k rows).
- **Drop `resource_state_last_seen`.** Every apply updates `last_seen` on all ~24.8 M rows, so the index is rewritten
  weekly. The "which resources disappeared" query can scan `resource_state` (3.5 GB). See also `missing_since` below.
- **Turn on Query Store on `npd`** and add search indexes from real queries (address state/postal code, specialty
  code, names) instead of guessing.
- **Watch fragmentation.** Changed resources are deleted and reinserted under the same key (~9% of rows per week).
  After a few weekly loads, check `sys.dm_db_index_physical_stats`. If needed, set FILLFACTOR 90 on the large tables
  or add a periodic `ALTER INDEX … REORGANIZE`.
- Not planned: columnstore indexes (only for heavy aggregate reporting; the weekly deletes need maintenance), and
  foreign keys to `resource_state` (valid, but they slow the apply and protect nothing the loader doesn't already
  guarantee).
- Natural-key indexes beyond `resource_state.ux_resource_state_id` and the NPI indexes (deferred when surrogate keys
  were chosen).

## Pipeline

- **Classify before bcp.** Export the stored hashes (`bcp queryout`, bucketed by id to fit TonyServer's memory), hash
  each line while reading, and flatten only new and changed resources. Stage the full id + hash list so not-seen
  detection still works. This saves ~20 of the ~26 minutes of flatten + bcp, and ~30 GB of staging. The user
  accepted the current cost (2026-10-08), so this is optional.
- **Replace the weekly `last_seen` update with `missing_since_release`** (NULL while present). The apply then touches
  only new, changed and missing resources.

## Open questions from reviews

- Unprefixed ids cannot be told apart from ids that came without a `Type-` prefix (relevant for any views that
  rebuild FHIR ids).
- A `SPEC_VERSION` bump rewrites every resource (≈ a first load in time and ~2× in log). Consider a reload path
  (truncate and load) for spec bumps.
- Hash edge cases (parked as minor): `meta: {}` hashes differently from no `meta`; no test pins key-order sensitivity.
- Check whether the 1.4 M weekly Practitioner `qualification` and 430 k Location `managingOrganization` changes are
  real data changes or reorderings.
