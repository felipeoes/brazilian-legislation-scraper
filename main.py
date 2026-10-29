"""Run scrapers and import/export their SQLite archives."""

import argparse
import asyncio
import sys
from contextlib import AsyncExitStack
from pathlib import Path

from loguru import logger

from src.config import (
    LLM_BATCH_SIZE,
    LLM_CONCURRENCY,
    LLM_MODEL,
    LLM_PDF_INPUT,
    build_proxy_config,
)
from src.core.browser import BrowserPool
from src.core.http import HttpClient
from src.core.ocr import OCR
from src.core.proxy import ProxyService
from src.core.store import Store
from src.registry import SCRAPERS, ScraperSpec


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("list", help="List scraper names and aliases")
    run = commands.add_parser("run", help="Scrape legislation")
    select = run.add_mutually_exclusive_group(required=True)
    select.add_argument("-s", "--scrapers", nargs="+")
    select.add_argument("--all", action="store_true")
    run.add_argument("--year", type=int)
    run.add_argument("--overwrite", action="store_true")
    run.add_argument("-v", "--verbose", action="store_true")
    export = commands.add_parser("export", help="Stream SQLite documents to JSON/JSONL")
    export.add_argument("-s", "--scrapers", nargs="+", required=True)
    export.add_argument("--format", choices=("jsonl", "json"), default="jsonl")
    export.add_argument("--out", type=Path)
    migration = commands.add_parser(
        "import-json", help="Import data.json and JSON shards without deleting inputs"
    )
    migration.add_argument("-s", "--scrapers", nargs="+", required=True)
    return parser.parse_args(argv)


def select_scrapers(names: list[str] | None) -> list[ScraperSpec]:
    if names is None:
        return SCRAPERS
    requested = {name.lower() for name in names}
    unknown = requested - set().union(*(spec.names for spec in SCRAPERS))
    if unknown:
        raise ValueError(f"Unknown scrapers: {', '.join(sorted(unknown))}")
    return [spec for spec in SCRAPERS if spec.names & requested]


async def run_scraper(
    spec: ScraperSpec, args: argparse.Namespace, ocr: OCR | None
) -> bool:
    name = spec.cls.name
    try:
        proxy_config = build_proxy_config() if spec.needs_proxy else None
        http = HttpClient(
            rps=spec.rps,
            max_retries=spec.max_retries,
            disable_cookies=spec.disable_cookies,
            proxy_service=ProxyService(proxy_config) if proxy_config else None,
        )
        years = range(args.year, args.year + 1) if args.year is not None else spec.years
        scraper = spec.cls(
            store=Store(spec.database),
            http=http,
            ocr=ocr if spec.needs_ocr else None,
            browser=BrowserPool(spec.browser_pages, limiter=http.limiter),
            years=years,
            workers=spec.workers,
            overwrite=args.overwrite,
        )
        async with scraper:
            result = await scraper.run()
            total = sum(row["documents"] for row in await scraper.store.summary())
        print(
            f"{name}: {result.docs} saved, {result.skipped} skipped, {result.errors} errors; {total} total"
        )
        return result.errors == 0
    except Exception:
        logger.exception(f"{name} failed")
        return False


async def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logger.remove()
    logger.add(
        sys.stderr, level="DEBUG" if getattr(args, "verbose", False) else "WARNING"
    )
    if args.command == "list":
        for spec in SCRAPERS:
            print(
                f"{spec.cls.__name__.removesuffix('Scraper')} [{', '.join(spec.aliases)}] {spec.years.start}–{spec.years.stop - 1}"
            )
        return 0
    try:
        specs = select_scrapers(None if getattr(args, "all", False) else args.scrapers)
        if args.command == "run":
            async with AsyncExitStack() as stack:
                ocr = None
                if LLM_MODEL and any(spec.needs_ocr for spec in specs):
                    ocr = OCR(
                        LLM_MODEL,
                        batch_size=LLM_BATCH_SIZE,
                        concurrency=LLM_CONCURRENCY,
                        pdf_input=LLM_PDF_INPUT,
                    )
                    await stack.enter_async_context(ocr.agent)
                results = await asyncio.gather(
                    *(run_scraper(spec, args, ocr) for spec in specs)
                )
            return 0 if all(results) else 1
        failed = False
        for spec in specs:
            try:
                if args.command == "export" and not spec.database.exists():
                    raise FileNotFoundError(spec.database)
                async with Store(spec.database) as store:
                    if args.command == "import-json":
                        count = await store.import_json(spec.database.parent)
                    else:
                        out = args.out or spec.database.parent / "export"
                        if args.out and len(specs) > 1:
                            out /= spec.cls.name
                        count = await store.export(out, args.format)
                print(f"{spec.cls.name}: {count} documents {args.command}")
            except Exception:
                logger.exception(f"{spec.cls.name}: {args.command} failed")
                failed = True
        return int(failed)
    except Exception:
        logger.exception("Command failed")
        return 1


if __name__ == "__main__":
    try:
        sys.exit(asyncio.run(main()))
    except KeyboardInterrupt:
        sys.exit(130)
