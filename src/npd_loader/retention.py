"""Keep the .ndjson files of the newest keep_releases releases (extracted or imported per the catalog); delete older
ones. The data itself is one current dataset (no releases to drop). Never touches .zst/manifest files or catalog rows."""
from __future__ import annotations

import logging
from datetime import date

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
        releases = set(ctx.catalog.successful_releases(cfg.catalog.run_class_extract)) \
            | set(ctx.catalog.successful_releases(cfg.catalog.run_class_import)) | {just_imported}
        keep = set(sorted(releases, reverse=True)[:cfg.retention.keep_releases]) | {just_imported}
        for release in sorted(releases - keep):
            try:
                _delete_ndjson(ctx, release)
            except Exception as exc:
                warnings.append(f"retention of release {release}: {exc}")
    except Exception as exc:
        warnings.append(f"retention: {exc}")
    for warning in warnings:
        log.warning(warning)
    return warnings
