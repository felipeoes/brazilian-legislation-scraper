"""Main script to run all scrapers realted to Brazilian legislation.

Note: I'm not using https://leisestaduais.com.br because it's explicitly forbidden to scrape their data, vide https://leisestaduais.com.br/robots.txt
"""

import os
from openai import OpenAI
from typing import List, Dict, Any
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
    SaoPauloAlespScraper,
    RJAlerjScraper,
    RNAlrnScraper,
    RSAlrsScraper,
)
from dotenv import load_dotenv

load_dotenv()

ONEDRIVE_SPECIFIC_LEGISLATION_SAVE_DIR = os.environ.get(
    "ONEDRIVE_SPECIFIC_LEGISLATION_SAVE_DIR"
)

OPENVPN_USERNAME = os.environ.get("OPENVPN_USERNAME")
OPENVPN_PASSWORD = os.environ.get("OPENVPN_PASSWORD")

if __name__ == "__main__":
    running_scrapers = (
        []
    )  # Initialize outside try block to ensure it's always available

    try:
        client = OpenAI(
            api_key=os.environ.get("LLM_API_KEY"),
            base_url=os.environ.get("PROVIDER_BASE_URL"),
        )
        model = os.environ.get("LLM_MODEL")

        print(f"Using LLM model: {model} with client: {client}")

        scrapers: List[Dict[str, Any]] = [
            {
                "scraper": CamaraDepScraper,
                "params": {
                    "verbose": False,
                    "year_start": 1808,
                    "max_workers": 32,
                },
                "name": "Camara dos Deputados",
                "run": False,
            },
            {
                "scraper": ConamaScraper,
                "params": {
                    "year_start": 1984,  # 1984 is the earliest year available
                    "docs_save_dir": ONEDRIVE_SPECIFIC_LEGISLATION_SAVE_DIR,
                    "verbose": True,
                },
                "name": "CONAMA",
                "run": False,
            },
            {
                "scraper": ICMBioScraper,
                "params": {
                    "year_start": 1800,
                    "use_selenium": True,
                    "docs_save_dir": ONEDRIVE_SPECIFIC_LEGISLATION_SAVE_DIR,
                    "verbose": True,
                },
                "name": "ICMBio",
                "run": False,
            },
            {
                "scraper": AcreLegisScraper,
                "params": {
                    "year_start": 1963,  # 1963 is the earliest year available
                    "verbose": True,
                    "max_workers": 32,
                },
                "name": "ACLegis",
                "run": False,
            },
            {
                "scraper": AlagoasSefazScraper,
                "params": {
                    "year_start": 1900,  # 1900 is the earliest year available
                    "llm_client": client,  # we have pdf image extraction
                    "llm_model": model,
                    "verbose": True,
                    "max_workers": 48,
                },
                "name": "ALSefaz",
                "run": False,
            },
            {
                "scraper": LegislaAMScraper,
                "params": {
                    "year_start": 1953,  # 1953 is the earliest year available
                    "verbose": True,
                    "max_workers": 32,
                },
                "name": "LegislaAM",
                "run": False,
            },
            {
                "scraper": AmapaAlapScraper,
                "params": {
                    "year_start": 1991,  # 1991 is the earliest year available
                    "verbose": True,
                    "max_workers": 32,
                },
                "name": "APAlap",
                "run": False,
            },
            {
                "scraper": BahiaLegislaScraper,
                "params": {
                    "year_start": 1891,  # 1891 is the earliest year available
                    "verbose": True,
                    "max_workers": 32,
                },
                "name": "BALegisla",
                "run": False,
            },
            {
                "scraper": CearaAleceScraper,
                "params": {
                    "verbose": True,
                    "max_workers": 32,
                },
                "name": "CEAlece",
                "run": False,
            },
            {
                "scraper": DFSinjScraper,
                "params": {
                    "year_start": 1922,  # 1922 is the earliest year available
                    "use_requests_session": True,  # needs to use in order to maintain session ID across requests
                    "llm_client": client,  # we have pdf image extraction
                    "llm_model": model,
                    "verbose": True,
                },
                "name": "DFSinj",
                "run": False,
            },
            {
                "scraper": ESAlesScraper,
                "params": {
                    "year_start": 1943,  # 1943 is the earliest year available
                    "verbose": True,
                    "llm_client": client,  # we have pdf image extraction
                    "llm_model": model,
                },
                "name": "ESAles",
                "run": False,
            },
            {
                "scraper": LegislaGoias,
                "params": {
                    "year_start": 1887,  # 1887 is the earliest year available
                    "use_selenium": True,  # needs to use selenium to get html content
                    "llm_client": client,  # we have pdf image extraction
                    "llm_model": model,
                    "verbose": False,
                },
                "name": "LegislaGoias",
                "run": False,
            },
            {
                "scraper": MaranhaoAlemaScraper,
                "params": {
                    "year_start": 1948,  # 1948 is the earliest year available
                    "use_selenium": True,  # needs to use selenium to get html content
                    "use_requests_session": True,  # needs to use in order to maintain session ID across requests
                    "verbose": True,
                },
                "name": "MAAlema",
                "run": False,
            },
            {
                "scraper": MSAlemsScraper,
                "params": {
                    "year_start": 1979,  # 1979 is the earliest year available
                    "verbose": True,
                    "max_workers": 32,
                },
                "name": "MSAlems",
                "run": False,
            },
            {
                "scraper": MTAlmtScraper,
                "params": {
                    "year_start": 1835,  # 1835 is the earliest year available (historical data)
                    "verbose": True,
                    "llm_client": client,  # we have pdf image extraction
                    "llm_model": model,
                },
                "name": "MTAlmt",
                "run": False,
            },
            {
                "scraper": MGAlmgScraper,
                "params": {
                    "year_start": 1831,  # 1831 is the earliest year available
                    "verbose": True,
                    "max_workers": 32,
                },
                "name": "MGAlmg",
                "run": False,
            },
            {
                "scraper": ParaAlepaScraper,
                "params": {
                    "year_start": 1947,  # 1947 is the earliest year available
                    "verbose": True,
                    "max_workers": 32,
                },
                "name": "PAAlepa",
                "run": False,
            },
            {
                "scraper": ParaibaAlpbScraper,
                "params": {
                    "year_start": 1924,  # 1924 is the earliest year available
                    "llm_client": client,  # we have pdf image extraction
                    "llm_model": model,
                    "verbose": True,
                },
                "name": "PBAlpb",
                "run": False,
            },
            # OBS: using Selenium for PARANA SCRAPER because the website blocks the requests for a while after just a few requests. Selenium works fine
            {
                "scraper": ParanaCVScraper,
                "params": {
                    "year_start": 1854,  # 1854 is the earliest year available
                    "verbose": True,
                    "use_selenium": True,
                    "use_selenium_vpn": True,
                    "multiple_drivers": True,
                    "vpn_extension_path": r"src\extensions\vee_vpn\veevpn_3_4_3_0.crx",
                    "vpn_extension_page": "chrome-extension://majdfhpaihoncoakbjgbdhglocklcgno/src/popup/popup.html",
                    # "max_workers": 20,
                    # "use_openvpn": True,
                    # "config_files": [
                    #     r"C:\Users\Docker\OpenVPN\config\us-free-2.protonvpn.udp.ovpn",
                    #     r"C:\Users\Docker\OpenVPN\config\us-free-3.protonvpn.udp.ovpn",
                    #     r"C:\Users\Docker\OpenVPN\config\us-free-4.protonvpn.udp.ovpn",
                    #     r"C:\Users\Docker\OpenVPN\config\us-free-5.protonvpn.udp.ovpn",
                    #     r"C:\Users\Docker\OpenVPN\config\us-free-6.protonvpn.udp.ovpn",
                    #     r"C:\Users\Docker\OpenVPN\config\us-free-7.protonvpn.udp.ovpn",
                    #     r"C:\Users\Docker\OpenVPN\config\us-free-16.protonvpn.udp.ovpn",
                    #     r"C:\Users\Docker\OpenVPN\config\us-free-23.protonvpn.udp.ovpn",
                    #     r"C:\Users\Docker\OpenVPN\config\us-free-35.protonvpn.udp.ovpn",
                    #     r"C:\Users\Docker\OpenVPN\config\us-free-59.protonvpn.udp.ovpn",
                    # ],
                    # "openvpn_credentials_map": {
                    #     "us-free-2.protonvpn.udp": (
                    #         OPENVPN_USERNAME,
                    #         OPENVPN_PASSWORD,
                    #     ),
                    #     "us-free-3.protonvpn.udp": (
                    #         OPENVPN_USERNAME,
                    #         OPENVPN_PASSWORD,
                    #     ),
                    #     "us-free-4.protonvpn.udp": (
                    #         OPENVPN_USERNAME,
                    #         OPENVPN_PASSWORD,
                    #     ),
                    #     "us-free-5.protonvpn.udp": (
                    #         OPENVPN_USERNAME,
                    #         OPENVPN_PASSWORD,
                    #     ),
                    #     "us-free-6.protonvpn.udp": (
                    #         OPENVPN_USERNAME,
                    #         OPENVPN_PASSWORD,
                    #     ),
                    #     "us-free-7.protonvpn.udp": (
                    #         OPENVPN_USERNAME,
                    #         OPENVPN_PASSWORD,
                    #     ),
                    #     "us-free-16.protonvpn.udp": (
                    #         OPENVPN_USERNAME,
                    #         OPENVPN_PASSWORD,
                    #     ),
                    #     "us-free-23.protonvpn.udp": (
                    #         OPENVPN_USERNAME,
                    #         OPENVPN_PASSWORD,
                    #     ),
                    #     "us-free-35.protonvpn.udp": (
                    #         OPENVPN_USERNAME,
                    #         OPENVPN_PASSWORD,
                    #     ),
                    #     "us-free-59.protonvpn.udp": (
                    #         OPENVPN_USERNAME,
                    #         OPENVPN_PASSWORD,
                    #     ),
                    # },
                },
                "name": "PRCV",
                "run": False,
            },
            {
                "scraper": RJAlerjScraper,
                "params": {
                    "year_start": 1968,  # 1968 is the earliest year available
                    "verbose": True,
                    "max_workers": 32,
                },
                "name": "RJAlerj",
                "run": False,
            },
            {
                "scraper": RNAlrnScraper,
                "params": {
                    "llm_client": client,  # we have pdf image extraction
                    "llm_model": model,
                    "verbose": True,
                },
                "name": "RNAlrn",
                "run": False,
            },
            {
                "scraper": RSAlrsScraper,
                "params": {
                    "year_start": 1950,  # 1830 is the earliest year available
                    "verbose": True,
                    "max_workers": 32,
                },
                "name": "RSAlrs",
                "run": False,
            },
            {
                "scraper": PernambucoAlepeScraper,
                "params": {
                    "year_start": 1965,  # 1835 is the earliest year available
                    "verbose": True,
                    "max_workers": 32,
                    "use_selenium": True,  # needs to use selenium to get html content
                    # "use_requests_session": True,  # needs to use in order to maintain aspx state
                },
                "name": "PEAlepe",
                "run": True,
            },
            {
                "scraper": SaoPauloAlespScraper,
                "params": {},
                "name": "SPAlesp",
                "run": False,
            },
        ]

        for scraper in scrapers:
            if scraper["run"]:
                scraper_instance = scraper["scraper"](**scraper["params"])
                running_scrapers.append(scraper_instance)
                data = scraper_instance.scrape()
                # data = scraper["scraper"](**scraper["params"]).scrape()
                print(f"Scraped {len(data)} data for {scraper['name']}")

    except KeyboardInterrupt:
        for scraper in running_scrapers:
            scraper.saver.running = False
            scraper.saver.join()

        print("KeyboardInterrupt: Exiting...")

    print("Exiting...")
    exit(0)
