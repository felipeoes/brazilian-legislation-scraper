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
    aliases: list[str] = field(default_factory=list)


def _get_scraper_names(cfg: ScraperConfig) -> set[str]:
    """Return accepted CLI names for a scraper (class name, short name, and aliases)."""
    names = {cfg.scraper.__name__.lower()}
    if cfg.name:
        names.add(cfg.name.lower())
    for alias in cfg.aliases:
        names.add(alias.lower())
    return names


def build_scraper_configs(
    llm_config: LLMConfig | None,
    *,
    run_names: set[str] | None = None,
    verbose: bool = False,
    overwrite: bool = False,
    year: int | None = None,
) -> list[ScraperConfig]:
    """Build the list of scraper configurations.

    CLI flags are baked into each config at construction time so that no
    post-construction mutation of ``params`` is required.
    """
    proxy_config = build_proxy_config()

    cli_extras: dict = {}
    if verbose:
        cli_extras["verbose"] = True
    if overwrite:
        cli_extras["overwrite"] = True
    if year is not None:
        cli_extras["year_start"] = year
        cli_extras["year_end"] = year

    def _sc(
        scraper: type[BaseScraper],
        *,
        needs_proxy: bool = False,
        aliases: list[str] | None = None,
        **params,
    ) -> ScraperConfig:
        merged = {**params, **cli_extras}
        if needs_proxy and proxy_config:
            merged.setdefault("proxy_config", proxy_config)
        cfg = ScraperConfig(
            scraper=scraper,
            name=scraper.__name__.removesuffix("Scraper"),
            params=merged,
            needs_proxy=needs_proxy,
            aliases=aliases or [],
        )
        if run_names is not None:
            cfg.run = bool(_get_scraper_names(cfg) & run_names)
        return cfg

    return [
        # --- Federal / regulatory ---
        _sc(
            CamaraDepScraper,
            year_start=1808,
            year_end=2026,
            rps=100,
        ),
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
        _sc(AcreLegisScraper, aliases=["Acre", "AC"], year_start=1963),
        _sc(
            AlagoasSefazScraper,
            aliases=["Alagoas", "AL"],
            year_start=1942,
            llm_config=llm_config,
        ),
        _sc(
            LegislaAMScraper,
            aliases=["Amazonas", "AM"],
            year_start=1956,
            rps=50,
        ),
        _sc(AmapaAlapScraper, aliases=["Amapa", "AP"], year_start=1991),
        _sc(BahiaLegislaScraper, aliases=["Bahia", "BA"], year_start=1891, rps=5),
        _sc(
            CearaAleceScraper,
            aliases=["Ceara", "CE"],
            year_start=1968,
            llm_config=llm_config,
        ),
        _sc(
            DFSinjScraper,
            aliases=["DF", "DistritoFederal"],
            year_start=1922,
            llm_config=llm_config,
            rps=50,
        ),
        _sc(
            ESAlesScraper,
            aliases=["ES", "EspiritoSanto"],
            year_start=1958,
            llm_config=llm_config,
        ),
        _sc(
            LegislaGoias,
            aliases=["Goias", "GO"],
            year_start=1978,
            year_end=1978,
            llm_config=llm_config,
        ),
        _sc(
            MaranhaoAlemaScraper,
            aliases=["Maranhao", "MA"],
            year_start=1906,
            llm_config=llm_config,
        ),
        _sc(MSAlemsScraper, aliases=["MS", "MatoGrossoDoSul"], year_start=1979),
        _sc(
            MTAlmtScraper,
            aliases=["MT", "MatoGrosso"],
            year_start=2017,
            llm_config=llm_config,
            rps=5,
        ),
        _sc(
            MGAlmgScraper,
            aliases=["MG", "MinasGerais"],
            year_start=1831,
            llm_config=llm_config,
        ),
        _sc(ParaAlepaScraper, aliases=["Para", "PA"], llm_config=llm_config, rps=5),
        _sc(
            ParaibaAlpbScraper,
            aliases=["Paraiba", "PB"],
            year_start=1924,
            llm_config=llm_config,
        ),
        _sc(
            ParanaCVScraper,
            aliases=["Parana", "PR"],
            year_start=2025,
            max_workers=2,
            max_retries=6,
            rps=1,
            needs_proxy=True,
        ),
        _sc(PernambucoAlepeScraper, aliases=["Pernambuco", "PE"], year_start=1835),
        _sc(
            PiauiAlepiScraper,
            aliases=["Piaui", "PI"],
            year_start=1922,
            llm_config=llm_config,
        ),
        _sc(RJAlerjScraper, aliases=["RJ", "RioDeJaneiro"]),
        _sc(
            RNAlrnScraper,
            aliases=["RN", "RioGrandeDoNorte"],
            year_start=1971,
            llm_config=llm_config,
        ),
        _sc(RSAlrsScraper, aliases=["RS", "RioGrandeDoSul"], llm_config=llm_config),
        _sc(
            RondoniaCotelScraper,
            aliases=["Rondonia", "RO"],
            year_start=1981,
            llm_config=llm_config,
        ),
        _sc(
            RoraimaAlerScraper,
            aliases=["Roraima", "RR"],
            year_start=1991,
            llm_config=llm_config,
        ),
        _sc(SantaCatarinaScraper, aliases=["SC"], year_start=1946),
        _sc(
            SaoPauloAlespScraper,
            aliases=["SaoPaulo", "SP"],
            year_start=1835,
            llm_config=llm_config,
        ),
        _sc(
            SergipeLegsonScraper,
            aliases=["Sergipe", "SE"],
            year_start=1940,
            llm_config=llm_config,
        ),
        _sc(TocantinsScraper, aliases=["TO"], year_start=1989, llm_config=llm_config),
    ]


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
                aliases_str = f" [{', '.join(cfg.aliases)}]" if cfg.aliases else ""
                if display_name == cfg.scraper.__name__:
                    print(f"  [{status}] {display_name}{aliases_str}")
                else:
                    print(
                        f"  [{status}] {display_name} ({cfg.scraper.__name__}){aliases_str}"
                    )
            return

        llm_config = LLMConfig.from_env()
        run_names = {name.lower() for name in args.scrapers} if args.scrapers else None
        configs = build_scraper_configs(
            llm_config,
            run_names=run_names,
            verbose=args.verbose,
            overwrite=args.overwrite,
            year=args.year,
        )

        enabled_configs = [cfg for cfg in configs if cfg.run]
        if not enabled_configs:
            logger.warning("No scrapers enabled. Use --scrapers or enable in config.")
            return

        print(f"Running {len(enabled_configs)} scrapers in parallel...")

        async def run_scraper(cfg: ScraperConfig):
            """Run a single scraper and handle its lifecycle."""
            scraper_instance = None
            scraper_name = cfg.scraper.__name__
            try:
                scraper_instance = cfg.scraper(**cfg.params)
                running_scrapers.append(scraper_instance)
                count = await scraper_instance.scrape()
                with logger.contextualize(scraper=scraper_instance.name):
                    logger.info(f"{scraper_name} completed: {count} items")
                print(f"✓ {scraper_name} completed: {count} items")
                return scraper_name, count, None
            except Exception as e:
                if scraper_instance:
                    with logger.contextualize(scraper=scraper_instance.name):
                        logger.error(f"{scraper_name} failed: {e}")
                print(f"✗ {scraper_name} failed: {e}")
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

        print("=" * 60)
        print("Scraping Summary:")
        for result in results:
            if isinstance(result, BaseException):
                print(f"  ERROR: {result}")
            else:
                name, count, error = result
                if error:
                    print(f"  ✗ {name}: FAILED ({error})")
                else:
                    print(f"  ✓ {name}: {count} documents")
        print("=" * 60)

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
