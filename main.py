"""Main script to run all scrapers related to Brazilian legislation.

Note: I'm not using https://leisestaduais.com.br because it's explicitly forbidden to scrape their data, vide https://leisestaduais.com.br/robots.txt
"""

import argparse
import asyncio
import sys
from dataclasses import dataclass, field
from loguru import logger
from src.scraper.base.scraper import BaseScraper
from src.services.ocr.config import LLMConfig
from src.scraper.federal_legislation.scrape import CamaraDepScraper
from src.scraper.conama.scrape import ConamaScraper
from src.scraper.icmbio.scrape import ICMBioScraper
from src.scraper.state_legislation import (
    AcreLegisScraper,
    AlagoasSefazScraper,
    LegislaAMScraper,
    AmapaAlapScraper,
    BahiaLegislaScraper,
    CearaAleceScraper,
    DFSinjScraper,
    ESAlesScraper,
    LegislaGoias,
    MaranhaoAlemaScraper,
    MSAlemsScraper,
    MTAlmtScraper,
    MGAlmgScraper,
    ParaAlepaScraper,
    ParaibaAlpbScraper,
    ParanaCVScraper,
    PernambucoAlepeScraper,
    PiauiAlepiScraper,
    RJAlerjScraper,
    RNAlrnScraper,
    RSAlrsScraper,
    RondoniaCotelScraper,
    RoraimaAlerScraper,
    SantaCatarinaScraper,
    SaoPauloAlespScraper,
    SergipeLegsonScraper,
    TocantinsScraper,
)
from src.config import (
    SAVE_DIR,
    SPECIFIC_LEGISLATION_SAVE_DIR,
    build_proxy_config,
)


@dataclass
class ScraperConfig:
    """Configuration for a scraper instance."""

    scraper: type[BaseScraper]
    name: str | None = None
    params: dict = field(default_factory=dict)
    run: bool = False
    needs_proxy: bool = False


def _sc(
    scraper: type[BaseScraper],
    *,
    name: str | None = None,
    run: bool = False,
    needs_proxy: bool = False,
    **params,
) -> ScraperConfig:
    """Shorthand for creating a ScraperConfig."""
    return ScraperConfig(
        scraper=scraper,
        name=name or scraper.__name__.removesuffix("Scraper"),
        params=params,
        run=run,
        needs_proxy=needs_proxy,
    )


def _get_scraper_names(cfg: ScraperConfig) -> set[str]:
    """Return accepted CLI names for a scraper."""
    names = {cfg.scraper.__name__.lower()}
    if cfg.name:
        names.add(cfg.name.lower())
    return names


def build_scraper_configs(llm_config: LLMConfig | None) -> list[ScraperConfig]:
    """Build the list of scraper configurations."""
    configs = [
        # --- Federal / regulatory ---
        _sc(CamaraDepScraper, year_start=1808, year_end=2026, rps=200),
        _sc(
            ConamaScraper,
            year_start=1984,
            docs_save_dir=SPECIFIC_LEGISLATION_SAVE_DIR or SAVE_DIR,
            llm_config=llm_config,
        ),
        _sc(
            ICMBioScraper,
            year_start=2016,
            docs_save_dir=SPECIFIC_LEGISLATION_SAVE_DIR or SAVE_DIR,
            disable_cookies=True,
        ),
        # --- State scrapers ---
        _sc(AcreLegisScraper, year_start=1963),
        _sc(
            AlagoasSefazScraper,
            year_start=1942,
            llm_config=llm_config,
            rps=5,
        ),
        _sc(LegislaAMScraper, year_start=1956, llm_config=llm_config),
        _sc(AmapaAlapScraper, year_start=1991),
        _sc(BahiaLegislaScraper, year_start=1891, rps=5),
        _sc(CearaAleceScraper, year_start=1968, llm_config=llm_config),
        _sc(DFSinjScraper, year_start=1922, llm_config=llm_config),
        _sc(ESAlesScraper, year_start=1958, llm_config=llm_config),
        _sc(LegislaGoias, year_start=1978, year_end=1978, llm_config=llm_config),
        _sc(
            MaranhaoAlemaScraper,
            year_start=1906,
            llm_config=llm_config,
        ),
        _sc(MSAlemsScraper, year_start=1979),
        _sc(MTAlmtScraper, year_start=2017, llm_config=llm_config, rps=5),
        _sc(MGAlmgScraper, year_start=1831, llm_config=llm_config),
        _sc(ParaAlepaScraper, llm_config=llm_config, rps=5),
        _sc(ParaibaAlpbScraper, year_start=1924, llm_config=llm_config),
        _sc(
            ParanaCVScraper,
            year_start=2025,
            max_workers=2,
            max_retries=6,
            rps=1,
            needs_proxy=True,
        ),
        _sc(PernambucoAlepeScraper, year_start=1835),
        _sc(PiauiAlepiScraper, year_start=1922, llm_config=llm_config),
        _sc(RJAlerjScraper),
        _sc(RNAlrnScraper, year_start=1971, llm_config=llm_config),
        _sc(RSAlrsScraper, llm_config=llm_config),
        _sc(RondoniaCotelScraper, year_start=1981, llm_config=llm_config),
        _sc(RoraimaAlerScraper, year_start=1991, llm_config=llm_config),
        _sc(SantaCatarinaScraper, year_start=1946),
        _sc(SaoPauloAlespScraper, year_start=1835, llm_config=llm_config),
        _sc(SergipeLegsonScraper, year_start=1940, llm_config=llm_config),
        _sc(TocantinsScraper, year_start=1989, llm_config=llm_config),
    ]

    proxy_config = build_proxy_config()
    if proxy_config:
        for cfg in configs:
            if cfg.needs_proxy:
                cfg.params.setdefault("proxy_config", proxy_config)

    return configs


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Brazilian legislation scraper")
    parser.add_argument(
        "--scrapers",
        "-s",
        nargs="+",
        help=(
            "Names of scrapers to run. Accepts short names and class names "
            "(e.g., MTAlmt Conama or MTAlmtScraper ConamaScraper). "
            "If not specified, runs enabled scrapers in config."
        ),
    )
    parser.add_argument(
        "--list",
        "-l",
        action="store_true",
        help="List all available scraper names and exit.",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose logging for all scrapers.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Re-scrape all documents, ignoring resume/skip logic.",
    )
    parser.add_argument(
        "--year",
        "-y",
        type=int,
        help="Specific year to scrape. If provided, overrides year_start and year_end in scraper configs.",
    )
    return parser.parse_args()


async def main():
    args = parse_args()

    # Configure loguru: WARNING+ by default, DEBUG+ with --verbose.
    # loguru's default sink (stderr at DEBUG) must be removed first.
    logger.remove()
    log_level = "DEBUG" if args.verbose else "WARNING"
    logger.add(sys.stderr, level=log_level)

    running_scrapers: list[BaseScraper] = []
    llm_config: LLMConfig | None = None

    try:
        if args.list:
            configs = build_scraper_configs(None)
            for cfg in configs:
                status = "ON" if cfg.run else "OFF"
                display_name = cfg.name or cfg.scraper.__name__
                if display_name == cfg.scraper.__name__:
                    logger.info(f"  [{status}] {display_name}")
                else:
                    logger.info(f"  [{status}] {display_name} ({cfg.scraper.__name__})")
            return

        llm_config = LLMConfig.from_env()
        configs = build_scraper_configs(llm_config)

        if args.scrapers:
            selected = {name.lower() for name in args.scrapers}
            for cfg in configs:
                cfg.run = bool(_get_scraper_names(cfg) & selected)

        if args.verbose:
            for cfg in configs:
                cfg.params["verbose"] = True

        if args.overwrite:
            for cfg in configs:
                cfg.params["overwrite"] = True

        if args.year:
            for cfg in configs:
                cfg.params["year_start"] = args.year
                cfg.params["year_end"] = args.year

        enabled_configs = [cfg for cfg in configs if cfg.run]
        if not enabled_configs:
            logger.warning("No scrapers enabled. Use --scrapers or enable in config.")
            return

        logger.info(f"Running {len(enabled_configs)} scrapers in parallel...")

        async def run_scraper(cfg: ScraperConfig):
            """Run a single scraper and handle its lifecycle."""
            scraper_instance = None
            scraper_name = cfg.scraper.__name__
            try:
                scraper_instance = cfg.scraper(**cfg.params)
                running_scrapers.append(scraper_instance)
                count = await scraper_instance.scrape()
                logger.info(f"✓ {scraper_name} completed: {count} items")
                return scraper_name, count, None
            except Exception as e:
                logger.error(f"✗ {scraper_name} failed: {e}")
                return scraper_name, 0, str(e)
            finally:
                if scraper_instance:
                    try:
                        await scraper_instance.cleanup()
                    except Exception as cleanup_err:
                        logger.error(f"Cleanup error for {scraper_name}: {cleanup_err}")

        results = await asyncio.gather(
            *[run_scraper(cfg) for cfg in enabled_configs],
            return_exceptions=True,
        )

        logger.info("=" * 60)
        logger.info("Scraping Summary:")
        for result in results:
            if isinstance(result, BaseException):
                logger.error(f"  ERROR: {result}")
            else:
                name, count, error = result
                if error:
                    logger.info(f"  ✗ {name}: FAILED ({error})")
                else:
                    logger.info(f"  ✓ {name}: {count} documents")
        logger.info("=" * 60)

    except KeyboardInterrupt:
        logger.warning("KeyboardInterrupt: Exiting...")
    finally:
        if llm_config:
            try:
                await llm_config.cleanup()
            except Exception as cleanup_err:
                logger.error(f"LLM client cleanup error: {cleanup_err}")

    logger.info("Exiting...")


if __name__ == "__main__":
    asyncio.run(main())
