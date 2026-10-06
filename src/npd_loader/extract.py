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
from npd_loader.stages import (Context, Outcome, StageFailed, close_interrupted_runs, completed_child, config_xml,
                               describe, fail_run, now, original_name, prefixed_name, read_bytes, run_folder,
                               sha256_of)
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
        close_interrupted_runs(ctx, cat.run_class_extract)
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
