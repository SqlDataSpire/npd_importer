"""IMPORT stage: raw load + transforms into standalone tables, then one-transaction publication."""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date

import psycopg
from psycopg import sql

from npd_loader.catalog import SUCCESS, Run
from npd_loader.dialect import PublishConflict
from npd_loader.extract import run_extract
from npd_loader.manifest import resource_type_for
from npd_loader.raw_load import RAW_PARENT, NdjsonInput, RawLoadError
from npd_loader.retention import apply_retention
from npd_loader.runxml import build_output_xml
from npd_loader.stages import (Context, Outcome, StageFailed, completed_child, config_xml, describe, fail_run, now,
                               original_name)

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


def drop_standalone_tables(conninfo: str, schemas: list[str], run_id: int | None = None) -> list[str]:
    """Drop the unpublished (non-partition) tables of import run `run_id`, or of any run when run_id is None.
    Only safe with the import lock held. Returns the dropped tables."""
    run = str(run_id) if run_id is not None else r"\d+"
    dropped = []
    with psycopg.connect(conninfo, autocommit=True) as conn:
        rows = conn.execute(
            "SELECT n.nspname, c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE n.nspname = ANY(%s) AND c.relkind IN ('r', 'p') AND NOT c.relispartition AND c.relname ~ %s "
            "ORDER BY 1, 2", (schemas, rf"__r{run}(__[a-z]+)?$")).fetchall()
        for schema, name in rows:
            conn.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier(schema, name)))
            dropped.append(f"{schema}.{name}")
    return dropped


def _drop_orphans(ctx: Context) -> None:
    """With the import lock held no import is running, so any standalone table left by an import that was
    killed (SIGTERM/SIGKILL/OOM) before its cleanup ran is an orphan."""
    try:
        dropped = ctx.dialect.drop_standalone_tables()
    except Exception:
        log.exception("could not drop orphaned standalone tables")
        return
    if dropped:
        log.warning("dropped %d orphaned standalone tables of earlier interrupted imports: %s",
                    len(dropped), ", ".join(dropped))


def _import(ctx: Context, run: Run, release: date, inputs: list[NdjsonInput], force: bool) -> list[dict]:
    db = ctx.config.npd_db
    d = ctx.dialect
    if not force and d.is_published(release):
        raise PublishConflict(f"release {release} is already published in {db.raw_schema}.{RAW_PARENT} "
                              f"but the catalog has no successful import; rerun with --force to replace it")
    with d.session():
        raw = d.load_raw(ctx.storage, release, run.id, inputs)
        transformed = d.run_transforms(raw.table, release, run.id)
        d.publish(raw.table, transformed.tables, release, run.id, force)
    return ([{"table": f"{db.raw_schema}.{RAW_PARENT}", "resource_type": t, "rows": n} for t, n in raw.rows.items()]
            + [{"table": f"{db.schema}.{t}", "rows": n} for t, n in transformed.counts.items()])


def _finish(ctx: Context, run: Run, release: date, summary: list[dict]) -> None:
    warnings = apply_retention(ctx, release)
    ctx.catalog.finish_run(run, SUCCESS, result="; ".join(warnings) or None,
                           output_xml=build_output_xml(summary, item_tag="table"))


def run_import(ctx: Context, release: date | None = None, force: bool = False) -> Outcome:
    cfg = ctx.config
    cat = cfg.catalog
    with ctx.lock("import") as acquired:
        if not acquired:
            log.info("another import is running; nothing to do")
            return Outcome.LOCKED
        _drop_orphans(ctx)
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
                ctx.dialect.drop_standalone_tables(run.id)
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
