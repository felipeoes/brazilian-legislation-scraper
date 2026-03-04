"""Main script to run all scrapers related to Brazilian legislation.

Note: I'm not using https://leisestaduais.com.br because it's explicitly forbidden to scrape their data, vide https://leisestaduais.com.br/robots.txt
"""

import argparse
import asyncio
import os
from dataclasses import dataclass, field
from openai import AsyncOpenAI
from loguru import logger
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
from dotenv import load_dotenv

load_dotenv()

SPECIFIC_LEGISLATION_SAVE_DIR = os.environ.get("SPECIFIC_LEGISLATION_SAVE_DIR")


@dataclass
class ScraperConfig:
    """Configuration for a scraper instance."""

    scraper: type[BaseScraper]
    params: dict = field(default_factory=dict)
    run: bool = False


def build_scraper_configs(llm_config: dict) -> list[ScraperConfig]:
    """Build the list of scraper configurations."""
    proxy_config = None
    if os.environ.get("PROXY_FILE_PATH"):
        proxy_config = {"file_path": os.environ.get("PROXY_FILE_PATH")}
    elif os.environ.get("PROXY_ENDPOINT"):
        proxy_config = {"endpoint": os.environ.get("PROXY_ENDPOINT")}

    configs = [
        ScraperConfig(
            scraper=CamaraDepScraper,
            params={
                "year_start": 1807,
                "year_end": 2026,
                "rps": 200,
                "verbose": True,
            },
            run=False,
        ),
        ScraperConfig(
            scraper=ConamaScraper,
            params={
                "year_start": 1984,
                "docs_save_dir": SPECIFIC_LEGISLATION_SAVE_DIR,
                "llm_config": llm_config,
            },
            run=False,
        ),
        ScraperConfig(
            scraper=ICMBioScraper,
            params={
                "year_start": 2016,  # starts from 2016
                "headless": False,
                "docs_save_dir": SPECIFIC_LEGISLATION_SAVE_DIR,
                "verbose": True,
                "max_workers": 4,  # using 4 workers only to avoid 403 errors from in.gov.br
            },
            run=False,
        ),
        ScraperConfig(
            scraper=AcreLegisScraper,
            params={
                "year_start": 1963,  # starts from 1963
            },
            run=False
        ),
        ScraperConfig(
            scraper=AlagoasSefazScraper,
            params={
                "year_start": 2019,  # starts at 1900
                "llm_config": llm_config,
                "rps": 5,
                "verbose": True,
            },
            run=True,
        ),
        ScraperConfig(
            scraper=LegislaAMScraper,
            params={
                "year_start": 1956,  # starts from 1956
                "llm_config": llm_config,
            },
            run=False,
        ),
        ScraperConfig(
            scraper=AmapaAlapScraper,
            params={
                "year_start": 1991,  # starts from 1991
                "verbose": True,
            },
            run=False,
        ),
        ScraperConfig(
            scraper=BahiaLegislaScraper,
            params={
                "year_start": 1891,  # starts from 1891
                "rps": 1,  # lower RPS to avoid 500 and 504 errors from https://www.legislabahia.ba.gov.br
            },
            run=False,
        ),
        ScraperConfig(
            scraper=CearaAleceScraper,
            params={
                "year_start": 1968,  # starts from 1968
                "verbose": True,
                "llm_config": llm_config,
            },
            run=False,
        ),
        ScraperConfig(
            scraper=DFSinjScraper,
            params={
                "year_start": 1922,
                "use_requests_session": True,
                "llm_config": llm_config,
            },
        ),
        ScraperConfig(
            scraper=ESAlesScraper,
            params={
                "year_start": 1958,
                "llm_config": llm_config,
            },
        ),
        ScraperConfig(
            scraper=LegislaGoias,
            params={
                "year_start": 1978,
                "year_end": 1978,
                "llm_config": llm_config,
            },
        ),
        ScraperConfig(
            scraper=MaranhaoAlemaScraper,
            params={
                "year_start": 1948,
                "use_browser": True,
                "use_requests_session": True,
                "llm_config": llm_config,
            },
        ),
        ScraperConfig(
            scraper=MSAlemsScraper,
            params={
                "year_start": 1979,  # starts from 1979
            },
        ),
        ScraperConfig(
            scraper=MTAlmtScraper,
            params={
                "year_start": 2017,
                "llm_config": llm_config,
            },
            run=False,
        ),
        ScraperConfig(
            scraper=MGAlmgScraper,
            params={
                "year_start": 1831,
                "llm_config": llm_config,
            },
        ),
        ScraperConfig(
            scraper=ParaAlepaScraper,
            params={
                "llm_config": llm_config,
            },
        ),
        ScraperConfig(
            scraper=ParaibaAlpbScraper,
            params={
                "year_start": 1924,
                "llm_config": llm_config,
            },
        ),
        ScraperConfig(
            scraper=ParanaCVScraper,
            params={
                "year_start": 1854,
                "use_browser": True,
                "use_browser_vpn": True,
                "vpn_extension_path": "src/extensions/vee_vpn/veevpn_3_7_0_0",
                "vpn_extension_page": "chrome-extension://majdfhpaihoncoakbjgbdhglocklcgno/src/popup/popup.html",
            },
        ),
        ScraperConfig(
            scraper=PernambucoAlepeScraper,
            params={
                "year_start": 1835,
                "max_workers": 32,
                "use_browser": True,
            },
        ),
        ScraperConfig(
            scraper=PiauiAlpbScraper,
            params={
                "year_start": 1922,
                "llm_config": llm_config,
            },
        ),
        ScraperConfig(
            scraper=RJAlerjScraper,
        ),
        ScraperConfig(
            scraper=RNAlrnScraper,
            params={
                "year_start": 1971,  # oldest Lei Complementar from 1971
                "llm_config": llm_config,
            },
        ),
        ScraperConfig(
            scraper=RSAlrsScraper,
            params={
                "llm_config": llm_config,
            },
        ),
        ScraperConfig(
            scraper=RondoniaCotelScraper,
            params={
                "year_start": 1981,
                "llm_config": llm_config,
            },
        ),
        ScraperConfig(
            scraper=RoraimaAlpbScraper,
            params={
                "year_start": 1991,
                "llm_config": llm_config,
            },
        ),
        ScraperConfig(
            scraper=SantaCatarinaScraper,
            params={
                "year_start": 1946,
                "use_requests_session": True,
            },
        ),
        ScraperConfig(
            scraper=SaoPauloAlespScraper,
            params={
                "year_start": 1835,
                "llm_config": llm_config,
            },
        ),
        ScraperConfig(
            scraper=SergipeLegsonScraper,
            params={
                "year_start": 1940,
                "llm_config": llm_config,
            },
        ),
        ScraperConfig(
            scraper=TocantinsScraper,
            params={
                "year_start": 1989,
                "llm_config": llm_config,
            },
        ),
    ]
    return configs


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Brazilian legislation scraper")
    parser.add_argument(
        "--scrapers",
        "-s",
        nargs="+",
        help="Names of scrapers to run (e.g., MTAlmt CONAMA). If not specified, runs scrapers with run=True in config.",
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
    return parser.parse_args()


async def main():
    args = parse_args()

    running_scrapers = []

    try:
        llm_provider = os.environ.get("LLM_PROVIDER", "openai").lower()
        api_key = os.environ.get("LLM_API_KEY", "")
        base_url = os.environ.get("PROVIDER_BASE_URL", "")
        model = os.environ.get("LLM_MODEL", "")

        if llm_provider == "bedrock":
            from src.services.ocr.bedrock import BedrockClient
            from src.services.request.service import RequestService

            # Shared RequestService for Bedrock HTTP calls
            bedrock_request_service = RequestService()

            bedrock_client = BedrockClient(
                base_url=base_url,
                api_key=api_key,
                request_service=bedrock_request_service,
                inference_config={"maxTokens": 32768},
                performance_config={"latency": "standard"},
            )

            logger.info(
                f"Using Bedrock provider | Model: {model} | Base URL: {base_url}"
            )

            llm_config = {
                "llm_client": bedrock_client,
                "llm_model": model,
                "llm_rps": 1,
                "llm_raw": True,  # send PDF bytes directly; handled by BedrockClient
            }

        else:
            client = AsyncOpenAI(api_key=api_key, base_url=base_url)
            logger.info(
                f"Using OpenAI provider | Model: {model} | Base URL: {client.base_url}"
            )

            llm_config = {
                "llm_client": client,
                "llm_model": model,
                "llm_rps": 2,
                "llm_kwargs": {
                    "max_completion_tokens": 32768,
                    "extra_body": {"media_resolution": "MEDIA_RESOLUTION_HIGH"},
                },  # using stream=True for fireworks AI to send multiple images
            }

        configs = build_scraper_configs(llm_config)

        if args.list:
            for cfg in configs:
                status = "ON" if cfg.run else "OFF"
                logger.info(f"  [{status}] {cfg.scraper.__name__}")
            return

        # If scrapers specified via CLI, override run flags
        if args.scrapers:
            selected = {name.lower() for name in args.scrapers}
            for cfg in configs:
                cfg.run = cfg.scraper.__name__.lower() in selected

        # Apply --verbose flag to all scraper configs
        if args.verbose:
            for cfg in configs:
                cfg.params["verbose"] = True

        # Build list of enabled scrapers
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
                data = await scraper_instance.scrape()
                logger.info(f"✓ {scraper_name} completed: {len(data)} items")
                return scraper_name, len(data), None
            except Exception as e:
                logger.error(f"✗ {scraper_name} failed: {e}")
                return scraper_name, 0, str(e)
            finally:
                if scraper_instance:
                    try:
                        await scraper_instance.cleanup()
                    except Exception as cleanup_err:
                        logger.error(f"Cleanup error for {scraper_name}: {cleanup_err}")

        # Run all scrapers in parallel
        results = await asyncio.gather(
            *[run_scraper(cfg) for cfg in enabled_configs],
            return_exceptions=True,
        )

        # Log summary
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
