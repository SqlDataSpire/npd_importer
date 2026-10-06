"""npd-loader command line."""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import date

import httpx

from npd_loader.catalog import Catalog, CssCatalogPg
from npd_loader.config import CatalogConfig, Config, ConfigError, load_config
from npd_loader.connections import open_connection, pg_conninfo
from npd_loader.db import advisory_lock, published_releases
from npd_loader.download import run_download
from npd_loader.extract import run_extract
from npd_loader.import_stage import run_import
from npd_loader.profile import format_report, load_mapped_paths, profile_file, unmapped
from npd_loader.schema import init_db
from npd_loader.stages import Context, StageFailed
from npd_loader.storage import LocalStorage

log = logging.getLogger("npd_loader")
DEFAULT_CONFIG = "/etc/npd-loader/config.toml"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="npd-loader", description="Load the CMS NPD FHIR release into Postgres")
    parser.add_argument("--config", default=os.environ.get("NPD_LOADER_CONFIG", DEFAULT_CONFIG))
    parser.add_argument("-v", "--verbose", action="store_true")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("download", help="download the current release").add_argument("--force", action="store_true")
    for name in ("extract", "import"):
        p = sub.add_parser(name, help=f"{name} a release (default: newest downloaded)")
        p.add_argument("--release", type=date.fromisoformat)
        p.add_argument("--force", action="store_true")
    sub.add_parser("run", help="download, then import (extracting as needed)")
    sub.add_parser("status", help="show recent releases")
    sub.add_parser("init-db", help="create npd database objects")
    p = sub.add_parser("profile", help="list JSON paths in an .ndjson or .ndjson.zst file")
    p.add_argument("path")
    p.add_argument("--unmapped", action="store_true", help="only paths not in mapped_paths.txt; exit 1 if any")
    p.add_argument("--limit", type=int)
    return parser


def build_context(config: Config) -> Context:
    npd = pg_conninfo(open_connection(config, "npd_db"))
    return Context(
        config=config,
        catalog=CssCatalogPg(pg_conninfo(open_connection(config, "catalog")), config.catalog),
        storage=LocalStorage(config.storage.root),
        http=httpx.Client(timeout=config.download.timeout_seconds, headers={"User-Agent": "npd-loader/0.1"}),
        lock=lambda stage: advisory_lock(npd, stage),
        npd_conninfo=npd,
    )


def format_status(catalog: Catalog, cat: CatalogConfig, published: list[date], limit: int = 10) -> str:
    classes = [cat.run_class_download, cat.run_class_extract, cat.run_class_import]
    releases = set(published)
    for run_class in classes:
        releases |= set(catalog.successful_releases(run_class))
    lines = [f"{'release':<12}{'download':>10}{'extract':>10}{'import':>10}  published"]
    for release in sorted(releases, reverse=True)[:limit]:
        ids = []
        for run_class in classes:
            run = catalog.last_successful_run(run_class, release)
            ids.append(str(run.id) if run else "-")
        lines.append(f"{release.isoformat():<12}{ids[0]:>10}{ids[1]:>10}{ids[2]:>10}  "
                     f"{'yes' if release in published else 'no'}")
    return "\n".join(lines)


def _profile(args: argparse.Namespace) -> int:
    prof = profile_file(args.path, limit=args.limit)
    if args.unmapped:
        rows = unmapped(prof, load_mapped_paths())
        print(format_report(prof, rows))
        return 1 if rows else 0
    print(format_report(prof))
    return 0


def _run(ctx: Context) -> int:
    """Download, then import even if the download failed (an earlier download may still need importing).
    Exit 1 if either stage failed."""
    failed = False
    for name, stage in (("download", run_download), ("import", run_import)):
        try:
            log.info("run: %s: %s", name, stage(ctx))
        except StageFailed as exc:
            log.error("run: %s failed: %s", name, exc)
            failed = True
        except Exception:
            log.exception("run: %s failed with an unexpected error", name)
            failed = True
    return 1 if failed else 0


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO, stream=sys.stderr,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    try:
        if args.command == "profile":
            return _profile(args)
        config = load_config(args.config)
        if args.command == "init-db":
            init_db(pg_conninfo(open_connection(config, "npd_db")), config.npd_db.raw_schema, config.npd_db.schema)
            log.info("npd database objects are up to date")
            return 0
        ctx = build_context(config)
        try:
            if args.command == "download":
                outcome = run_download(ctx, force=args.force)
            elif args.command == "extract":
                outcome = run_extract(ctx, release=args.release, force=args.force)
            elif args.command == "import":
                outcome = run_import(ctx, release=args.release, force=args.force)
            elif args.command == "run":
                return _run(ctx)
            else:  # status
                print(format_status(ctx.catalog, config.catalog,
                                    published_releases(ctx.npd_conninfo, config.npd_db.schema)))
                return 0
        finally:
            ctx.http.close()
        log.info("%s: %s", args.command, outcome)
        return 0
    except (StageFailed, ConfigError) as exc:
        log.error("%s", exc)
        return 1
    except Exception:
        log.exception("unexpected error")
        return 1
