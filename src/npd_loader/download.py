"""DOWNLOAD stage: manifest + every .zst file into a new run folder, with resume and retries."""
from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from urllib.parse import urljoin

import httpx

from npd_loader.catalog import SUCCESS, Run
from npd_loader.manifest import Manifest, fetch_manifest, file_url
from npd_loader.runxml import build_output_xml
from npd_loader.stages import (Context, Outcome, StageFailed, close_interrupted_runs, config_xml, describe,
                               fail_run, now, prefixed_name, run_folder)

log = logging.getLogger(__name__)


class DownloadError(Exception):
    """Retryable: connection problems, 403 on an expired signed URL, short bodies."""


class FatalDownloadError(Exception):
    """Not retryable: the data itself disagrees with the manifest."""


@dataclass
class FetchResult:
    size: int
    sha256: str
    last_modified: datetime | None
    etag: str | None
    attempts: int


def run_download(ctx: Context, force: bool = False) -> Outcome:
    cfg = ctx.config
    with ctx.lock("download") as acquired:
        if not acquired:
            log.info("another download is running; nothing to do")
            return Outcome.LOCKED
        close_interrupted_runs(ctx, cfg.catalog.run_class_download)
        manifest = fetch_manifest(ctx.http, cfg.source.manifest_url)
        release = manifest.release_date
        if not force and ctx.catalog.last_successful_run(cfg.catalog.run_class_download, release):
            log.info("release %s is already downloaded", release)
            return Outcome.SKIPPED
        description = describe(ctx, "Download", release)
        run = ctx.catalog.start_run(cfg.catalog.run_class_download, description,
                                    config_xml(ctx, description, release, force=force))
        log.info("download run %s started for release %s", run.id, release)
        try:
            summary = _download_release(ctx, run, manifest)
        except Exception as exc:
            fail_run(ctx, run, exc)
            raise StageFailed(f"download of release {release} failed: {exc}") from exc
        ctx.catalog.finish_run(run, SUCCESS, output_xml=build_output_xml(summary, item_tag="file"))
        log.info("download run %s finished", run.id)
        return Outcome.SUCCESS


def _download_release(ctx: Context, run: Run, manifest: Manifest) -> list[dict[str, object]]:
    cfg = ctx.config
    cat = cfg.catalog
    folder = run_folder(run)
    release = manifest.release_date.isoformat()
    root = ctx.storage.uri()

    manifest_id = ctx.catalog.add_data_file(run, file_type=cat.file_type_manifest, source_uri=cfg.source.manifest_url,
                                            source_version_num=release, run_type_root_dir=root)
    name = prefixed_name(manifest_id, "manifest.json")
    rel = f"{folder}/{name}"
    with ctx.storage.open_write(rel) as f:
        f.write(manifest.raw)
    ctx.catalog.update_data_file(manifest_id, file_name=name, file_rel_path=rel, file_size=len(manifest.raw),
                                 file_hash=hashlib.sha256(manifest.raw).hexdigest(), date_created=now())

    summary: list[dict[str, object]] = []
    for mf in manifest.files:
        url = file_url(cfg.source.manifest_url, mf.zst_name)
        file_id = ctx.catalog.add_data_file(run, file_type=cat.file_type_zst, source_uri=url,
                                            source_version_num=release, run_type_root_dir=root,
                                            parent_file=manifest_id)
        name = prefixed_name(file_id, mf.zst_name)
        rel = f"{folder}/{name}"
        ctx.catalog.update_data_file(file_id, file_name=name, file_rel_path=rel)
        started = datetime.now()
        try:
            result = fetch_file(ctx, url, rel + ".part", mf.compressed_bytes)
        except Exception as exc:
            ctx.catalog.update_data_file(file_id, exceptions=str(exc))
            raise
        ctx.storage.rename(rel + ".part", rel)
        ctx.catalog.update_data_file(file_id, file_size=result.size, file_hash=result.sha256,
                                     date_modified=result.last_modified, date_created=now())
        summary.append({"name": mf.zst_name, "file_id": file_id, "size": result.size, "sha256": result.sha256,
                        "etag": result.etag, "attempts": result.attempts,
                        "seconds": round((datetime.now() - started).total_seconds(), 1)})
        log.info("downloaded %s (%d bytes, %d attempt(s))", mf.zst_name, result.size, result.attempts)

    after = fetch_manifest(ctx.http, cfg.source.manifest_url)
    if after.release_date != manifest.release_date:
        raise FatalDownloadError(f"manifest generated_at changed during download: "
                                 f"{manifest.release_date} -> {after.release_date}")
    return summary


def resolve_signed_url(client: httpx.Client, public_url: str) -> str:
    """The public URL 302-redirects to a short-lived signed URL; fetch a fresh one for every attempt."""
    with client.stream("GET", public_url, follow_redirects=False) as response:
        if response.is_redirect:
            return urljoin(public_url, response.headers["location"])
        if response.status_code == 200:
            return public_url
        raise DownloadError(f"GET {public_url}: HTTP {response.status_code}")


def _last_modified(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return parsedate_to_datetime(value).astimezone(timezone.utc).replace(tzinfo=None)
    except (TypeError, ValueError):
        return None


def fetch_file(ctx: Context, public_url: str, part_rel: str, expected_size: int) -> FetchResult:
    dl = ctx.config.download
    hasher = hashlib.sha256()
    written = 0
    attempt = 0
    last_modified: datetime | None = None
    etag: str | None = None
    with ctx.storage.open_write(part_rel) as out:
        while True:
            attempt += 1
            try:
                signed = resolve_signed_url(ctx.http, public_url)
                headers = {"Range": f"bytes={written}-"} if written else {}
                with ctx.http.stream("GET", signed, headers=headers, timeout=dl.timeout_seconds) as response:
                    if response.status_code not in (200, 206):
                        raise DownloadError(f"GET {public_url}: HTTP {response.status_code}")
                    if response.status_code == 206 and not response.headers.get(
                            "content-range", "").startswith(f"bytes {written}-"):
                        raise DownloadError(f"unexpected Content-Range {response.headers.get('content-range')!r}")
                    skip = written if response.status_code == 200 else 0  # server ignored Range
                    last_modified = last_modified or _last_modified(response.headers.get("last-modified"))
                    etag = etag or response.headers.get("etag")
                    # No chunk_size: httpx's ByteChunker only flushes its buffer when the stream ends
                    # normally, so a size hint here would swallow already-received bytes whenever the
                    # connection drops mid-chunk, defeating Range-based resume. Letting it default to
                    # None makes each network read forward immediately, which is what resume needs.
                    for chunk in response.iter_bytes():
                        if skip:
                            if len(chunk) <= skip:
                                skip -= len(chunk)
                                continue
                            chunk = chunk[skip:]
                            skip = 0
                        written += len(chunk)
                        if written > expected_size:
                            raise FatalDownloadError(f"{public_url}: received more bytes than the manifest's "
                                                     f"compressed_bytes ({expected_size})")
                        hasher.update(chunk)
                        out.write(chunk)
                if written == expected_size:
                    return FetchResult(written, hasher.hexdigest(), last_modified, etag, attempt)
                raise DownloadError(f"connection ended at {written} of {expected_size} bytes")
            except FatalDownloadError:
                raise
            except (httpx.HTTPError, DownloadError) as exc:
                if attempt >= dl.max_attempts:
                    raise DownloadError(f"{public_url}: giving up after {attempt} attempts: {exc}") from exc
                delay = dl.backoff_seconds * 2 ** (attempt - 1)
                log.warning("%s: attempt %d failed (%s); retrying from byte %d in %.0fs",
                            public_url, attempt, exc, written, delay)
                ctx.sleep(delay)
