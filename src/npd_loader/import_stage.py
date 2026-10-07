"""IMPORT stage: flatten + stage the release, then apply the delta to the current dataset."""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date

from npd_loader.catalog import SUCCESS, Run
from npd_loader.dialect import DeltaResult, StageResult
from npd_loader.extract import run_extract
from npd_loader.manifest import resource_type_for
from npd_loader.flatten.stagefiles import FlattenError
from npd_loader.raw_load import NdjsonInput
from npd_loader.retention import apply_retention
from npd_loader.runxml import build_output_xml
from npd_loader.stages import (Context, Outcome, StageFailed, close_interrupted_runs, completed_child, config_xml,
                               describe, fail_run, now, original_name)

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


def _summary(staged: StageResult, delta: DeltaResult, schema: str) -> list[dict]:
    items: list[dict] = [{"resource_type": t, **k} for t, k in sorted(delta.kinds.items())]
    items += [{"table": f"{schema}.{t}", "inserted": delta.inserted.get(t, 0), "replaced": delta.replaced.get(t, 0),
               "staged": staged.rows.get(t, 0)} for t in sorted(delta.inserted)]
    return items


def _import(ctx: Context, run: Run, release: date, inputs: list[NdjsonInput], force: bool) -> list[dict]:
    d = ctx.dialect
    newest = max(d.published_releases(), default=None)
    if newest is not None and release < newest and not force:
        raise StageFailed(f"release {release} is older than the current release {newest}; applying it would roll "
                          f"the data back. Rerun with --force to apply it anyway")
    staged = d.stage_release(ctx.storage, release, run.id, inputs)
    delta = d.apply_delta(release, run.id)
    return _summary(staged, delta, ctx.config.npd_db.schema)


def _finish(ctx: Context, run: Run, release: date, summary: list[dict]) -> None:
    warnings = apply_retention(ctx, release)
    items = [{"_tag": "delta", **i} if "resource_type" in i else i for i in summary]
    ctx.catalog.finish_run(run, SUCCESS, result="; ".join(warnings) or None,
                           output_xml=build_output_xml(items, item_tag="table"))


def run_import(ctx: Context, release: date | None = None, force: bool = False) -> Outcome:
    cfg = ctx.config
    cat = cfg.catalog
    with ctx.lock("import") as acquired:
        if not acquired:
            log.info("another import is running; nothing to do")
            return Outcome.LOCKED
        close_interrupted_runs(ctx, cat.run_class_import)
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
            if isinstance(exc, FlattenError) and exc.file_id is not None:
                ctx.catalog.update_data_file(exc.file_id, exceptions=str(exc))
            fail_run(ctx, run, exc)
            raise StageFailed(f"import of release {release} failed: {exc}") from exc
        loaded = now()
        for inp in found.inputs:
            ctx.catalog.update_data_file(inp.file_id, date_loaded=loaded)
        _finish(ctx, run, release, summary)
        log.info("import run %s finished", run.id)
        return Outcome.SUCCESS
