"""Main script to run all scrapers related to Brazilian legislation.

Note: I'm not using https://leisestaduais.com.br because it's explicitly forbidden to scrape their data, vide https://leisestaduais.com.br/robots.txt
"""

import argparse
import asyncio
import os
from dataclasses import dataclass, field
from loguru import logger
from openai import AsyncOpenAI
from src.scraper.base.concurrency import RateLimiter
from src.scraper.base.scraper import BaseScraper
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
    PiauiAlpbScraper,
    RJAlerjScraper,
    RNAlrnScraper,
    RSAlrsScraper,
    RondoniaCotelScraper,
    RoraimaAlpbScraper,
    SantaCatarinaScraper,
    SaoPauloAlespScraper,
    SergipeLegsonScraper,
    TocantinsScraper,
)
from src.services.ocr.bedrock import BedrockClient
from src.services.ocr.openai_client import OpenAIClient
from src.services.ocr.snowflake import SnowflakeClient
from src.services.request.service import RequestService
from dotenv import load_dotenv

load_dotenv()

SPECIFIC_LEGISLATION_SAVE_DIR = os.environ.get("SPECIFIC_LEGISLATION_SAVE_DIR")


@dataclass
class ScraperConfig:
    """Configuration for a scraper instance."""

    scraper: type[BaseScraper]
    params: dict = field(default_factory=dict)
    run: bool = False
    needs_proxy: bool = False


def _sc(
    scraper: type[BaseScraper],
    *,
    run: bool = False,
    needs_proxy: bool = False,
    **params,
) -> ScraperConfig:
    """Shorthand for creating a ScraperConfig."""
    return ScraperConfig(
        scraper=scraper, params=params, run=run, needs_proxy=needs_proxy
    )


def build_scraper_configs(llm_config: dict) -> list[ScraperConfig]:
    """Build the list of scraper configurations."""
    configs = [
        # --- Federal / regulatory ---
        _sc(CamaraDepScraper, year_start=1807, year_end=2026, rps=200),
        _sc(
            ConamaScraper,
            year_start=1984,
            docs_save_dir=SPECIFIC_LEGISLATION_SAVE_DIR,
            llm_config=llm_config,
        ),
        _sc(
            ICMBioScraper,
            year_start=2016,
            docs_save_dir=SPECIFIC_LEGISLATION_SAVE_DIR,
            verbose=True,
            max_workers=16,
            disable_cookies=True,
        ),
        # --- State scrapers ---
        _sc(AcreLegisScraper, year_start=1963),
        _sc(
            AlagoasSefazScraper,
            year_start=2019,
            llm_config=llm_config,
            rps=5,
            verbose=True,
        ),
        _sc(LegislaAMScraper, year_start=1956, llm_config=llm_config),
        _sc(AmapaAlapScraper, year_start=1991, verbose=True),
        _sc(BahiaLegislaScraper, year_start=1891, rps=5),
        _sc(CearaAleceScraper, year_start=1968, verbose=True, llm_config=llm_config),
        _sc(DFSinjScraper, year_start=1922, llm_config=llm_config),
        _sc(ESAlesScraper, year_start=1958, llm_config=llm_config),
        _sc(LegislaGoias, year_start=1978, year_end=1978, llm_config=llm_config),
        _sc(
            MaranhaoAlemaScraper,
            year_start=1948,
            use_browser=True,
            llm_config=llm_config,
        ),
        _sc(MSAlemsScraper, year_start=1979),
        _sc(MTAlmtScraper, year_start=2017, llm_config=llm_config),
        _sc(MGAlmgScraper, year_start=1831, llm_config=llm_config),
        _sc(ParaAlepaScraper, llm_config=llm_config),
        _sc(ParaibaAlpbScraper, year_start=1924, llm_config=llm_config),
        _sc(
            ParanaCVScraper,
            year_start=2025,
            max_workers=2,
            max_retries=6,
            rps=1,
            needs_proxy=True,
        ),
        _sc(PernambucoAlepeScraper, year_start=1835, max_workers=32),
        _sc(PiauiAlpbScraper, year_start=1922, llm_config=llm_config),
        _sc(RJAlerjScraper),
        _sc(RNAlrnScraper, year_start=1971, llm_config=llm_config),
        _sc(RSAlrsScraper, llm_config=llm_config),
        _sc(RondoniaCotelScraper, year_start=1981, llm_config=llm_config),
        _sc(RoraimaAlpbScraper, year_start=1991, llm_config=llm_config),
        _sc(SantaCatarinaScraper, year_start=1946),
        _sc(SaoPauloAlespScraper, year_start=1835, llm_config=llm_config),
        _sc(SergipeLegsonScraper, year_start=1940, llm_config=llm_config),
        _sc(TocantinsScraper, year_start=1989, llm_config=llm_config),
    ]

    proxy_config = None
    if os.environ.get("PROXY_FILE_PATH"):
        proxy_config = {"file_path": os.environ.get("PROXY_FILE_PATH")}
    elif os.environ.get("PROXY_ENDPOINT"):
        proxy_config = {"endpoint": os.environ.get("PROXY_ENDPOINT")}

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
        help="Names of scrapers to run (e.g., MTAlmt CONAMA). If not specified, runs enabled scrapers in config.",
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


def _build_llm_config() -> dict:
    """Build LLM configuration from environment variables."""
    llm_provider = os.environ.get("LLM_PROVIDER", "openai").lower()
    api_key = os.environ.get("LLM_API_KEY", "")
    base_url = os.environ.get("PROVIDER_BASE_URL", "")
    model = os.environ.get("LLM_MODEL", "")

    if llm_provider == "bedrock":
        bedrock_request_service = RequestService()
        client = BedrockClient(
            base_url=base_url,
            api_key=api_key,
            request_service=bedrock_request_service,
            inference_config={"maxTokens": 32768},
            performance_config={"latency": "standard"},
        )
        logger.info(f"Using Bedrock provider | Model: {model} | Base URL: {base_url}")
        llm_rps = 1
        return {
            "llm_client": client,
            "llm_model": model,
            "llm_rps": llm_rps,
            "llm_rate_limiter": RateLimiter(llm_rps),
            "llm_raw": True,
        }

    if llm_provider == "snowflake":
        client = SnowflakeClient(
            account=os.environ.get("SNOWFLAKE_ACCOUNT", ""),
            user=os.environ.get("SNOWFLAKE_USER", ""),
            token=api_key,
            database=os.environ.get("SNOWFLAKE_DATABASE", ""),
            schema=os.environ.get("SNOWFLAKE_SCHEMA", "PUBLIC"),
            stage=os.environ.get("SNOWFLAKE_STAGE", ""),
        )
        logger.info(
            f"Using Snowflake provider | Model: {model} | Account: {client.account}"
        )
        llm_rps = 2
        return {
            "llm_client": client,
            "llm_model": model,
            "llm_rps": llm_rps,
            "llm_rate_limiter": RateLimiter(llm_rps),
        }

    # Default: OpenAI-compatible
    raw_client = AsyncOpenAI(api_key=api_key, base_url=base_url)
    logger.info(
        f"Using OpenAI provider | Model: {model} | Base URL: {raw_client.base_url}"
    )
    llm_rps = 2
    openai_kwargs = {
        "max_completion_tokens": 32768,
        "extra_body": {"media_resolution": "MEDIA_RESOLUTION_HIGH"},
    }
    return {
        "llm_client": OpenAIClient(raw_client, **openai_kwargs),
        "llm_model": model,
        "llm_rps": llm_rps,
        "llm_rate_limiter": RateLimiter(llm_rps),
    }


async def main():
    args = parse_args()
    running_scrapers: list[BaseScraper] = []

    try:
        llm_config = _build_llm_config()
        configs = build_scraper_configs(llm_config)

        if args.list:
            for cfg in configs:
                status = "ON" if cfg.run else "OFF"
                logger.info(f"  [{status}] {cfg.scraper.__name__}")
            return

        if args.scrapers:
            selected = {name.lower() for name in args.scrapers}
            for cfg in configs:
                cfg.run = cfg.scraper.__name__.lower() in selected

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
            if isinstance(result, Exception):
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
        for scraper in running_scrapers:
            await scraper.cleanup()

    logger.info("Exiting...")


if __name__ == "__main__":
    asyncio.run(main())
