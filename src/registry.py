"""Scraper registration and site-specific runtime defaults."""

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from src.config import (
    SAVE_DIR,
    SPECIFIC_LEGISLATION_SAVE_DIR,
    STATE_LEGISLATION_SAVE_DIR,
)
from src.core.scraper import BaseScraper
from src.scrapers.environment.conama import ConamaScraper
from src.scrapers.environment.icmbio import ICMBioScraper
from src.scrapers.federal.camara import CamaraDepScraper
from src.scrapers.states.ac import AcreLegisScraper
from src.scrapers.states.al import AlagoasSefazScraper
from src.scrapers.states.am import LegislaAMScraper
from src.scrapers.states.ap import AmapaAlapScraper
from src.scrapers.states.ba import BahiaLegislaScraper
from src.scrapers.states.ce import CearaAleceScraper
from src.scrapers.states.df import DFSinjScraper
from src.scrapers.states.es import ESAlesScraper
from src.scrapers.states.go import LegislaGoias
from src.scrapers.states.ma import MaranhaoAlemaScraper
from src.scrapers.states.mg import MGAlmgScraper
from src.scrapers.states.ms import MSAlemsScraper
from src.scrapers.states.mt import MTAlmtScraper
from src.scrapers.states.pa import ParaAlepaScraper
from src.scrapers.states.pb import ParaibaAlpbScraper
from src.scrapers.states.pe import PernambucoAlepeScraper
from src.scrapers.states.pi import PiauiAlepiScraper
from src.scrapers.states.pr import ParanaCVScraper
from src.scrapers.states.rj import RJAlerjScraper
from src.scrapers.states.rn import RNAlrnScraper
from src.scrapers.states.ro import RondoniaCotelScraper
from src.scrapers.states.rr import RoraimaAlerScraper
from src.scrapers.states.rs import RSAlrsScraper
from src.scrapers.states.sc import SantaCatarinaScraper
from src.scrapers.states.se import SergipeLegsonScraper
from src.scrapers.states.sp import SaoPauloAlespScraper
from src.scrapers.states.to import TocantinsScraper


@dataclass(frozen=True)
class ScraperSpec:
    cls: type[BaseScraper]
    aliases: tuple[str, ...]
    years: range
    rps: float = 10
    workers: int = 16
    needs_proxy: bool = False
    needs_ocr: bool = False
    browser_pages: int = 4
    save_dir: Path = SAVE_DIR
    max_retries: int = 6
    disable_cookies: bool = False

    @property
    def names(self) -> set[str]:
        return {
            self.cls.__name__.lower(),
            self.cls.__name__.removesuffix("Scraper").lower(),
            self.cls.name.lower(),
            *(alias.lower() for alias in self.aliases),
        }

    @property
    def database(self) -> Path:
        return self.save_dir / self.cls.name / "documents.db"


SCRAPERS = [
    ScraperSpec(CamaraDepScraper, (), range(1808, datetime.now().year + 1), rps=100),
    ScraperSpec(
        ConamaScraper,
        (),
        range(1984, datetime.now().year + 1),
        needs_ocr=True,
        save_dir=SPECIFIC_LEGISLATION_SAVE_DIR or SAVE_DIR,
    ),
    ScraperSpec(
        ICMBioScraper,
        (),
        range(2016, datetime.now().year + 1),
        disable_cookies=True,
        save_dir=SPECIFIC_LEGISLATION_SAVE_DIR or SAVE_DIR,
    ),
    ScraperSpec(
        AcreLegisScraper,
        ("Acre", "AC"),
        range(1963, datetime.now().year + 1),
        save_dir=STATE_LEGISLATION_SAVE_DIR or SAVE_DIR,
    ),
    ScraperSpec(
        AlagoasSefazScraper,
        ("Alagoas", "AL"),
        range(1942, datetime.now().year + 1),
        needs_ocr=True,
        rps=5,
        save_dir=STATE_LEGISLATION_SAVE_DIR or SAVE_DIR,
    ),
    ScraperSpec(
        LegislaAMScraper,
        ("Amazonas", "AM"),
        range(1956, datetime.now().year + 1),
        rps=50,
        save_dir=STATE_LEGISLATION_SAVE_DIR or SAVE_DIR,
    ),
    ScraperSpec(
        AmapaAlapScraper,
        ("Amapa", "AP"),
        range(1991, datetime.now().year + 1),
        save_dir=STATE_LEGISLATION_SAVE_DIR or SAVE_DIR,
    ),
    ScraperSpec(
        BahiaLegislaScraper,
        ("Bahia", "BA"),
        range(1891, datetime.now().year + 1),
        max_retries=7,
        save_dir=STATE_LEGISLATION_SAVE_DIR or SAVE_DIR,
    ),
    ScraperSpec(
        CearaAleceScraper,
        ("Ceara", "CE"),
        range(1968, datetime.now().year + 1),
        needs_ocr=True,
        save_dir=STATE_LEGISLATION_SAVE_DIR or SAVE_DIR,
    ),
    ScraperSpec(
        DFSinjScraper,
        ("DF", "DistritoFederal"),
        range(1922, datetime.now().year + 1),
        needs_ocr=True,
        rps=100,
        workers=100,
        save_dir=STATE_LEGISLATION_SAVE_DIR or SAVE_DIR,
    ),
    ScraperSpec(
        ESAlesScraper,
        ("ES", "EspiritoSanto"),
        range(1958, datetime.now().year + 1),
        needs_ocr=True,
        rps=50,
        save_dir=STATE_LEGISLATION_SAVE_DIR or SAVE_DIR,
    ),
    ScraperSpec(
        LegislaGoias,
        ("Goias", "GO"),
        range(1887, datetime.now().year + 1),
        needs_ocr=True,
        rps=100,
        save_dir=STATE_LEGISLATION_SAVE_DIR or SAVE_DIR,
    ),
    ScraperSpec(
        MaranhaoAlemaScraper,
        ("Maranhao", "MA"),
        range(1906, datetime.now().year + 1),
        needs_ocr=True,
        save_dir=STATE_LEGISLATION_SAVE_DIR or SAVE_DIR,
    ),
    ScraperSpec(
        MSAlemsScraper,
        ("MS", "MatoGrossoDoSul"),
        range(1979, datetime.now().year + 1),
        save_dir=STATE_LEGISLATION_SAVE_DIR or SAVE_DIR,
    ),
    ScraperSpec(
        MTAlmtScraper,
        ("MT", "MatoGrosso"),
        range(2017, datetime.now().year + 1),
        needs_ocr=True,
        rps=5,
        save_dir=STATE_LEGISLATION_SAVE_DIR or SAVE_DIR,
    ),
    ScraperSpec(
        MGAlmgScraper,
        ("MG", "MinasGerais"),
        range(1831, datetime.now().year + 1),
        needs_ocr=True,
        save_dir=STATE_LEGISLATION_SAVE_DIR or SAVE_DIR,
    ),
    ScraperSpec(
        ParaAlepaScraper,
        ("Para", "PA"),
        range(1990, datetime.now().year + 1),
        needs_ocr=True,
        rps=5,
        save_dir=STATE_LEGISLATION_SAVE_DIR or SAVE_DIR,
    ),
    ScraperSpec(
        ParaibaAlpbScraper,
        ("Paraiba", "PB"),
        range(1924, datetime.now().year + 1),
        needs_ocr=True,
        workers=2,
        rps=1,
        save_dir=STATE_LEGISLATION_SAVE_DIR or SAVE_DIR,
    ),
    ScraperSpec(
        ParanaCVScraper,
        ("Parana", "PR"),
        range(2025, datetime.now().year + 1),
        rps=1,
        workers=2,
        needs_proxy=True,
        max_retries=6,
        save_dir=STATE_LEGISLATION_SAVE_DIR or SAVE_DIR,
    ),
    ScraperSpec(
        PernambucoAlepeScraper,
        ("Pernambuco", "PE"),
        range(1835, datetime.now().year + 1),
        save_dir=STATE_LEGISLATION_SAVE_DIR or SAVE_DIR,
    ),
    ScraperSpec(
        PiauiAlepiScraper,
        ("Piaui", "PI"),
        range(1922, datetime.now().year + 1),
        needs_ocr=True,
        save_dir=STATE_LEGISLATION_SAVE_DIR or SAVE_DIR,
    ),
    ScraperSpec(
        RJAlerjScraper,
        ("RJ", "RioDeJaneiro"),
        range(1891, datetime.now().year + 1),
        save_dir=STATE_LEGISLATION_SAVE_DIR or SAVE_DIR,
    ),
    ScraperSpec(
        RNAlrnScraper,
        ("RN", "RioGrandeDoNorte"),
        range(1971, datetime.now().year + 1),
        needs_ocr=True,
        save_dir=STATE_LEGISLATION_SAVE_DIR or SAVE_DIR,
    ),
    ScraperSpec(
        RSAlrsScraper,
        ("RS", "RioGrandeDoSul"),
        range(1830, datetime.now().year + 1),
        needs_ocr=True,
        save_dir=STATE_LEGISLATION_SAVE_DIR or SAVE_DIR,
    ),
    ScraperSpec(
        RondoniaCotelScraper,
        ("Rondonia", "RO"),
        range(1981, datetime.now().year + 1),
        needs_ocr=True,
        save_dir=STATE_LEGISLATION_SAVE_DIR or SAVE_DIR,
    ),
    ScraperSpec(
        RoraimaAlerScraper,
        ("Roraima", "RR"),
        range(1991, datetime.now().year + 1),
        needs_ocr=True,
        save_dir=STATE_LEGISLATION_SAVE_DIR or SAVE_DIR,
    ),
    ScraperSpec(
        SantaCatarinaScraper,
        ("SC",),
        range(1946, datetime.now().year + 1),
        save_dir=STATE_LEGISLATION_SAVE_DIR or SAVE_DIR,
    ),
    ScraperSpec(
        SaoPauloAlespScraper,
        ("SaoPaulo", "SP"),
        range(1835, datetime.now().year + 1),
        needs_ocr=True,
        save_dir=STATE_LEGISLATION_SAVE_DIR or SAVE_DIR,
    ),
    ScraperSpec(
        SergipeLegsonScraper,
        ("Sergipe", "SE"),
        range(1940, datetime.now().year + 1),
        needs_ocr=True,
        save_dir=STATE_LEGISLATION_SAVE_DIR or SAVE_DIR,
    ),
    ScraperSpec(
        TocantinsScraper,
        ("TO",),
        range(1989, datetime.now().year + 1),
        needs_ocr=True,
        save_dir=STATE_LEGISLATION_SAVE_DIR or SAVE_DIR,
    ),
]
