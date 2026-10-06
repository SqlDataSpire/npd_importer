"""Keep the newest N published releases: drop older partitions and their .ndjson files, and the .ndjson files of
extracted-but-unpublished releases older than every kept release. Never touches the release just imported, .zst/manifest files, or catalog rows."""
from __future__ import annotations

import logging
from datetime import date

from npd_loader.dialect import LockUnavailable
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
    warnings: list[str] = []
    try:
        published = ctx.dialect.partitioned_releases()
        # Keep slots go to releases published in this database (plus the one just imported) only.
        newest = sorted(published | {just_imported}, reverse=True)[:cfg.retention.keep_releases]
        keep = set(newest) | {just_imported}
        # .ndjson only: releases extracted (or imported per the catalog) but not published here, older than
        # the oldest kept release, e.g. an extract whose import failed and was never retried.
        extracted = set(ctx.catalog.successful_releases(cfg.catalog.run_class_extract)) \
            | set(ctx.catalog.successful_releases(cfg.catalog.run_class_import))
        unpublished = {r for r in extracted - published - keep if r < min(newest)}
        locked_out = False
        for release in sorted((published - keep) | unpublished):
            try:
                if release in published:
                    if locked_out:  # the same parents are still locked; don't wait out the timeout again
                        warnings.append(f"retention of release {release}: skipped, a table lock was not "
                                        f"available (retried on the next run)")
                        continue
                    dropped = ctx.dialect.drop_release(release)
                    log.info("retention dropped %d partitions of release %s", len(dropped), release)
                _delete_ndjson(ctx, release)
            except LockUnavailable as exc:
                locked_out = True
                warnings.append(f"retention of release {release}: gave up waiting for a table lock "
                                f"({exc}); retried on the next run")
            except Exception as exc:
                warnings.append(f"retention of release {release}: {exc}")
    except Exception as exc:
        warnings.append(f"retention: {exc}")
    for warning in warnings:
        log.warning(warning)
    return warnings
