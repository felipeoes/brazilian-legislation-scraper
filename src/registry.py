"""Scraper registration and site-specific runtime defaults."""

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

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
    start_year: int
    rps: float = 10
    workers: int = 16
    needs_proxy: bool = False
    needs_ocr: bool = False
    browser_pages: int = 4
    save_dir: Path = SAVE_DIR
    max_retries: int = 6
    pause_on_403: float = 0
    disable_cookies: bool = False

    @property
    def years(self) -> range:
        return range(self.start_year, datetime.now().year + 1)

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


def state(*args: Any, **kwargs: Any) -> ScraperSpec:
    return ScraperSpec(*args, save_dir=STATE_LEGISLATION_SAVE_DIR or SAVE_DIR, **kwargs)


SCRAPERS = [
    ScraperSpec(CamaraDepScraper, (), 1808, rps=100),
    ScraperSpec(
        ConamaScraper,
        (),
        1984,
        needs_ocr=True,
        save_dir=SPECIFIC_LEGISLATION_SAVE_DIR or SAVE_DIR,
    ),
    ScraperSpec(
        ICMBioScraper,
        (),
        2016,
        disable_cookies=True,
        save_dir=SPECIFIC_LEGISLATION_SAVE_DIR or SAVE_DIR,
    ),
    state(AcreLegisScraper, ("Acre", "AC"), 1963),
    state(AlagoasSefazScraper, ("Alagoas", "AL"), 1942, needs_ocr=True, rps=5),
    state(LegislaAMScraper, ("Amazonas", "AM"), 1956, rps=50),
    state(AmapaAlapScraper, ("Amapa", "AP"), 1991),
    state(BahiaLegislaScraper, ("Bahia", "BA"), 1891, max_retries=7, rps=1),
    state(CearaAleceScraper, ("Ceara", "CE"), 1968, needs_ocr=True),
    state(
        DFSinjScraper,
        ("DF", "DistritoFederal"),
        1922,
        needs_ocr=True,
        rps=100,
        workers=100,
    ),
    state(ESAlesScraper, ("ES", "EspiritoSanto"), 1958, needs_ocr=True, rps=50),
    state(LegislaGoias, ("Goias", "GO"), 1887, needs_ocr=True, rps=100),
    state(MaranhaoAlemaScraper, ("Maranhao", "MA"), 1906, needs_ocr=True),
    state(MSAlemsScraper, ("MS", "MatoGrossoDoSul"), 1979),
    state(MTAlmtScraper, ("MT", "MatoGrosso"), 2017, needs_ocr=True, rps=5),
    state(MGAlmgScraper, ("MG", "MinasGerais"), 1831, needs_ocr=True),
    state(ParaAlepaScraper, ("Para", "PA"), 1990, needs_ocr=True, rps=5),
    state(
        ParaibaAlpbScraper, ("Paraiba", "PB"), 1924, needs_ocr=True, workers=2, rps=1
    ),
    state(
        ParanaCVScraper,
        ("Parana", "PR"),
        2025,
        # The site allows ~20 requests per rolling minute per IP; beyond that it answers
        # 403 and blocks the client for a fixed 21-30 min. 0.3 rps (18/min) stays under
        # it (180 requests tested clean), and a block pauses every direct request
        # through it. With PROXY_FILE_PATH set, each proxy gets its own 0.3 rps and a
        # blocked or failing one rests, so load on the site is proxies x 0.3 rps.
        # shortcut: a direct 403 while every proxy rests holds the workers that fell
        # back to direct for the whole pause, even after proxies return; seen 0 times.
        rps=0.3,
        pause_on_403=35 * 60,
        # In one session, a year listing after another type's listing can come back
        # empty; without cookies every request starts a fresh session.
        disable_cookies=True,
        # 20 free proxies at 0.3 rps each need ~64 in flight (pages take ~10 s through
        # them; 32 workers reached half the rate). Without proxies they just queue.
        workers=64,
        needs_proxy=True,
        # A retry after a proxy failure goes through the next proxy.
        max_retries=4,
    ),
    state(PernambucoAlepeScraper, ("Pernambuco", "PE"), 1835),
    state(PiauiAlepiScraper, ("Piaui", "PI"), 1922, needs_ocr=True),
    # RJ documents use a 300 s timeout; 3 attempts caps one document at 15 min.
    state(RJAlerjScraper, ("RJ", "RioDeJaneiro"), 1891, max_retries=3),
    state(RNAlrnScraper, ("RN", "RioGrandeDoNorte"), 1971, needs_ocr=True),
    state(RSAlrsScraper, ("RS", "RioGrandeDoSul"), 1830, needs_ocr=True),
    state(RondoniaCotelScraper, ("Rondonia", "RO"), 1981, needs_ocr=True),
    state(RoraimaAlerScraper, ("Roraima", "RR"), 1991, needs_ocr=True),
    state(SantaCatarinaScraper, ("SC",), 1946),
    state(SaoPauloAlespScraper, ("SaoPaulo", "SP"), 1835, needs_ocr=True),
    state(SergipeLegsonScraper, ("Sergipe", "SE"), 1940, needs_ocr=True),
    state(TocantinsScraper, ("TO",), 1989, needs_ocr=True),
]
