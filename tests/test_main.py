"""Tests for main CLI scraper selection helpers."""

from main import _get_scraper_names, _sc, build_scraper_configs
from src.services.ocr.config import LLMConfig
from src.scraper.conama.scrape import ConamaScraper
from src.scraper.state_legislation import LegislaGoias


class TestScraperCliNames:
    def test_short_name_defaults_from_scraper_class(self):
        cfg = _sc(ConamaScraper)

        assert cfg.name == "Conama"
        assert _get_scraper_names(cfg) == {"conama", "conamascraper"}

    def test_scrapers_without_suffix_keep_class_name(self):
        cfg = _sc(LegislaGoias)

        assert cfg.name == "LegislaGoias"
        assert _get_scraper_names(cfg) == {"legislagoias"}

    def test_build_configs_assigns_names_to_all_scrapers(self):
        configs = build_scraper_configs(None)

        assert configs
        assert all(cfg.name for cfg in configs)

    def test_short_name_selection_matches_conama(self):
        configs = build_scraper_configs(None)
        selected = {"conama"}

        for cfg in configs:
            cfg.run = bool(_get_scraper_names(cfg) & selected)

        enabled = [cfg.scraper.__name__ for cfg in configs if cfg.run]
        assert enabled == ["ConamaScraper"]

    def test_class_name_selection_still_matches_conama(self):
        configs = build_scraper_configs(None)
        selected = {"conamascraper"}

        for cfg in configs:
            cfg.run = bool(_get_scraper_names(cfg) & selected)

        enabled = [cfg.scraper.__name__ for cfg in configs if cfg.run]
        assert enabled == ["ConamaScraper"]

    def test_case_insensitive_selection_matches_goias(self):
        configs = build_scraper_configs(None)
        selected = {"legislagoias"}

        for cfg in configs:
            cfg.run = bool(_get_scraper_names(cfg) & selected)

        enabled = [cfg.scraper.__name__ for cfg in configs if cfg.run]
        assert enabled == ["LegislaGoias"]

    def test_build_llm_config_disables_openai_sdk_retries(self, monkeypatch):
        captured_async_openai_kwargs = {}
        captured_openai_client_kwargs = {}

        class DummyAsyncOpenAI:
            def __init__(self, **kwargs):
                captured_async_openai_kwargs.update(kwargs)
                self.base_url = kwargs.get("base_url")

        class DummyOpenAIClient:
            def __init__(self, raw_client, **kwargs):
                captured_openai_client_kwargs.update(kwargs)
                self.raw_client = raw_client

        monkeypatch.setenv("LLM_PROVIDER", "openai")
        monkeypatch.setenv("LLM_API_KEY", "test-key")
        monkeypatch.setenv(
            "PROVIDER_BASE_URL", "https://example.invalid/v1beta/openai/"
        )
        monkeypatch.setenv("LLM_MODEL", "google.gemini-2.5-flash")
        monkeypatch.setattr("openai.AsyncOpenAI", DummyAsyncOpenAI)
        monkeypatch.setattr("src.services.ocr.clients.OpenAIClient", DummyOpenAIClient)

        config = LLMConfig.from_env()

        assert config is not None
        assert captured_async_openai_kwargs == {
            "api_key": "test-key",
            "base_url": "https://example.invalid/v1beta/openai/",
            "max_retries": 0,
        }
        assert captured_openai_client_kwargs == {
            "max_completion_tokens": 32768,
            "extra_body": {"media_resolution": "MEDIA_RESOLUTION_HIGH"},
        }

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
