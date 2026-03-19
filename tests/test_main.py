"""Tests for main CLI scraper selection helpers."""

import importlib
from pathlib import Path

import src.config as config_module
from main import _get_scraper_names, build_scraper_configs
from src.scraper.conama.scrape import ConamaScraper
from src.scraper.state_legislation import AlagoasSefazScraper, LegislaGoias
from src.services.ocr.config import LLMConfig


class TestScraperCliNames:
    def test_log_dir_reads_env_value(self, monkeypatch):
        with monkeypatch.context() as context:
            context.setenv("LOG_DIR", "logs/legislation")
            context.delenv("ERROR_LOG_DIR", raising=False)

            importlib.reload(config_module)

            assert config_module.LOG_DIR == Path("logs/legislation")

        importlib.reload(config_module)

    def test_short_name_defaults_from_scraper_class(self):
        configs = build_scraper_configs(None)
        cfg = next(c for c in configs if c.scraper is ConamaScraper)

        assert cfg.name == "Conama"
        assert _get_scraper_names(cfg) == {"conama", "conamascraper"}

    def test_scrapers_without_suffix_keep_class_name(self):
        configs = build_scraper_configs(None)
        cfg = next(c for c in configs if c.scraper is LegislaGoias)

        assert cfg.name == "LegislaGoias"
        assert _get_scraper_names(cfg) == {"legislagoias", "goias", "go"}

    def test_build_configs_assigns_names_to_all_scrapers(self):
        configs = build_scraper_configs(None)

        assert configs
        assert all(cfg.name for cfg in configs)

    def test_short_name_selection_matches_conama(self):
        configs = build_scraper_configs(None, run_names={"conama"})

        enabled = [cfg.scraper.__name__ for cfg in configs if cfg.run]
        assert enabled == ["ConamaScraper"]

    def test_class_name_selection_still_matches_conama(self):
        configs = build_scraper_configs(None, run_names={"conamascraper"})

        enabled = [cfg.scraper.__name__ for cfg in configs if cfg.run]
        assert enabled == ["ConamaScraper"]

    def test_case_insensitive_selection_matches_goias(self):
        configs = build_scraper_configs(None, run_names={"legislagoias"})

        enabled = [cfg.scraper.__name__ for cfg in configs if cfg.run]
        assert enabled == ["LegislaGoias"]

    def test_state_alias_goias_matches_goias_scraper(self):
        configs = build_scraper_configs(None, run_names={"goias"})

        enabled = [cfg.scraper.__name__ for cfg in configs if cfg.run]
        assert enabled == ["LegislaGoias"]

    def test_state_alias_alagoas_matches_alagoas_scraper(self):
        configs = build_scraper_configs(None, run_names={"alagoas"})

        enabled = [cfg.scraper.__name__ for cfg in configs if cfg.run]
        assert enabled == ["AlagoasSefazScraper"]

    def test_state_alias_case_insensitive(self):
        configs = build_scraper_configs(None, run_names={"alagoas"})
        cfg = next(c for c in configs if c.scraper is AlagoasSefazScraper)

        assert cfg.run is True

    def test_all_state_scrapers_have_aliases(self):
        configs = build_scraper_configs(None)
        federal_scrapers = {"CamaraDepScraper", "ConamaScraper", "ICMBioScraper"}

        state_configs = [
            cfg for cfg in configs if cfg.scraper.__name__ not in federal_scrapers
        ]
        assert all(cfg.aliases for cfg in state_configs), (
            "All state scrapers must declare at least one alias. "
            f"Missing: {[cfg.scraper.__name__ for cfg in state_configs if not cfg.aliases]}"
        )

    def test_build_llm_config_infers_snowflake_when_provider_unset(self, monkeypatch):
        captured_snowflake_kwargs = {}

        class DummySnowflakeClient:
            def __init__(self, **kwargs):
                captured_snowflake_kwargs.update(kwargs)
                self.account = kwargs["account"]

        monkeypatch.delenv("LLM_PROVIDER", raising=False)
        monkeypatch.setenv("LLM_API_KEY", "test-token")
        monkeypatch.setenv(
            "PROVIDER_BASE_URL",
            "https://account.snowflakecomputing.com/api/v2/statements/",
        )
        monkeypatch.setenv("LLM_MODEL", "claude-sonnet")
        monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "account")
        monkeypatch.setenv("SNOWFLAKE_USER", "user")
        monkeypatch.setenv("SNOWFLAKE_DATABASE", "database")
        monkeypatch.setenv("SNOWFLAKE_SCHEMA", "PUBLIC")
        monkeypatch.setenv("SNOWFLAKE_STAGE", "stage")
        monkeypatch.setattr(
            "src.services.ocr.clients.SnowflakeClient", DummySnowflakeClient
        )

        config = LLMConfig.from_env()

        assert config is not None
        assert captured_snowflake_kwargs == {
            "account": "account",
            "user": "user",
            "token": "test-token",
            "database": "database",
            "schema": "PUBLIC",
            "stage": "stage",
        }
