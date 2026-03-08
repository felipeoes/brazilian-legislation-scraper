"""Tests for LLM-related functionality.

Covers:
- LLMConfig dataclass
- LLM usage accumulation and extraction (openai, bedrock, snowflake)
- _llm_usage_totals, _format_llm_usage
- BaseScraper._save_summary with LLM usage
"""

import json
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.scraper.base.concurrency import RateLimiter
from src.services.ocr.config import LLMConfig


# =========================================================================
# LLMConfig
# =========================================================================


class TestLLMConfig:
    def test_creates_rate_limiter_automatically(self):
        mock_client = MagicMock()
        config = LLMConfig(client=mock_client, model="test-model", rps=5)
        assert config.rate_limiter is not None
        assert isinstance(config.rate_limiter, RateLimiter)

    def test_uses_provided_rate_limiter(self):
        mock_client = MagicMock()
        limiter = RateLimiter(10)
        config = LLMConfig(client=mock_client, model="test-model", rate_limiter=limiter)
        assert config.rate_limiter is limiter

    def test_preserves_all_fields(self):
        mock_client = MagicMock()
        config = LLMConfig(
            client=mock_client,
            model="gpt-4o",
            rps=3,
            batch_size=10,
            raw=True,
        )
        assert config.client is mock_client
        assert config.model == "gpt-4o"
        assert config.rps == 3
        assert config.batch_size == 10
        assert config.raw is True
        assert config.rate_limiter is not None

    def test_defaults(self):
        mock_client = MagicMock()
        config = LLMConfig(client=mock_client, model="test")
        assert config.rps == 10
        assert config.batch_size == 5
        assert config.raw is False

    @pytest.mark.asyncio
    async def test_cleanup_awaits_async_client_close(self):
        close = MagicMock()

        async def async_close():
            close()

        mock_client = MagicMock()
        mock_client.close = async_close
        config = LLMConfig(client=mock_client, model="test")

        await config.cleanup()

        close.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_cleanup_calls_close(self):
        mock_client = MagicMock()
        mock_client.close = AsyncMock()
        config = LLMConfig(client=mock_client, model="test")

        await config.cleanup()

        mock_client.close.assert_called_once()


# =========================================================================
# LLM usage totals (_llm_usage_totals)
# =========================================================================


class TestLLMUsageTotals:
    def test_llm_usage_totals(self):
        from src.scraper.base.scraper import _llm_usage_totals

        totals = _llm_usage_totals(
            {
                "model-a": {
                    "requests": 2,
                    "successful_requests": 2,
                    "failed_requests": 0,
                    "input_tokens": 100,
                    "cached_tokens": 10,
                    "output_tokens": 40,
                    "reasoning_tokens": 5,
                },
                "model-b": {
                    "requests": 1,
                    "successful_requests": 1,
                    "failed_requests": 0,
                    "input_tokens": 50,
                    "cached_tokens": 0,
                    "output_tokens": 20,
                    "reasoning_tokens": 3,
                },
            }
        )

        assert totals == {
            "requests": 3,
            "successful_requests": 3,
            "failed_requests": 0,
            "input_tokens": 150,
            "cached_tokens": 10,
            "output_tokens": 60,
            "reasoning_tokens": 8,
        }


# =========================================================================
# LLM usage formatting (_format_llm_usage)
# =========================================================================


class TestLLMUsageFormatting:
    def test_format_llm_usage_includes_model_breakdown(self):
        from src.scraper.base.scraper import _format_llm_usage

        formatted = _format_llm_usage(
            {
                "model-b": {
                    "requests": 1,
                    "successful_requests": 1,
                    "failed_requests": 0,
                    "input_tokens": 50,
                    "cached_tokens": 0,
                    "output_tokens": 20,
                    "reasoning_tokens": 3,
                },
                "model-a": {
                    "requests": 2,
                    "successful_requests": 1,
                    "failed_requests": 1,
                    "input_tokens": 100,
                    "cached_tokens": 10,
                    "output_tokens": 40,
                    "reasoning_tokens": 5,
                },
            }
        )

        assert formatted.startswith(
            "LLM total 3 reqs (2 ok, 1 failed), 150 input, 10 cached, 60 output, 8 reasoning |"
        )
        assert (
            "model-a: 2 reqs (1 ok, 1 failed), 100 input, 10 cached, 40 output, 5 reasoning"
            in formatted
        )
        assert (
            "model-b: 1 reqs (1 ok, 0 failed), 50 input, 0 cached, 20 output, 3 reasoning"
            in formatted
        )

    def test_format_llm_usage_empty(self):
        from src.scraper.base.scraper import _format_llm_usage

        assert (
            _format_llm_usage({})
            == "LLM total 0 reqs (0 ok, 0 failed), 0 input, 0 cached, 0 output, 0 reasoning"
        )


# =========================================================================
# BaseScraper._save_summary with LLM usage
# =========================================================================


class TestSaveSummaryLLMUsage:
    @pytest.mark.asyncio
    async def test_save_summary_includes_llm_usage(self):
        from src.database.saver import FileSaver
        from src.scraper.base.scraper import BaseScraper

        with tempfile.TemporaryDirectory() as tmp:
            scraper = BaseScraper.__new__(BaseScraper)
            scraper.saver = FileSaver(save_dir=Path(tmp), flush_interval=100)
            scraper.year_start = 2024
            scraper.year_end = 2025
            scraper.count = 7
            scraper.error_count = 1
            scraper._scrape_start = None
            scraper._types_summary = {"Lei": {"total": 7, "situations": {"Vigente": 7}}}
            scraper.ocr_service = MagicMock()
            scraper.ocr_service.usage_stats = {
                "gpt-4o": {
                    "requests": 2,
                    "successful_requests": 2,
                    "failed_requests": 0,
                    "input_tokens": 120,
                    "cached_tokens": 30,
                    "output_tokens": 60,
                    "reasoning_tokens": 4,
                }
            }

            await scraper._save_summary()

            summary = json.loads(Path(tmp, "summary.json").read_text())
            assert summary["llm_usage"] == {
                "models": scraper.ocr_service.usage_stats,
                "totals": {
                    "requests": 2,
                    "successful_requests": 2,
                    "failed_requests": 0,
                    "input_tokens": 120,
                    "cached_tokens": 30,
                    "output_tokens": 60,
                    "reasoning_tokens": 4,
                },
                "human": (
                    "LLM total 2 reqs (2 ok, 0 failed), 120 input, 30 cached, 60 output, 4 reasoning | "
                    "gpt-4o: 2 reqs (2 ok, 0 failed), 120 input, 30 cached, 60 output, 4 reasoning"
                ),
            }

    @pytest.mark.asyncio
    async def test_save_summary_includes_empty_llm_usage(self):
        from src.database.saver import FileSaver
        from src.scraper.base.scraper import BaseScraper

        with tempfile.TemporaryDirectory() as tmp:
            scraper = BaseScraper.__new__(BaseScraper)
            scraper.saver = FileSaver(save_dir=Path(tmp), flush_interval=100)
            scraper.year_start = 2024
            scraper.year_end = 2025
            scraper.count = 7
            scraper.error_count = 1
            scraper._scrape_start = None
            scraper._types_summary = {"Lei": {"total": 7, "situations": {"Vigente": 7}}}
            scraper.ocr_service = None

            await scraper._save_summary()

            summary = json.loads(Path(tmp, "summary.json").read_text())
            assert summary["llm_usage"] == {
                "models": {},
                "totals": {
                    "requests": 0,
                    "successful_requests": 0,
                    "failed_requests": 0,
                    "input_tokens": 0,
                    "cached_tokens": 0,
                    "output_tokens": 0,
                    "reasoning_tokens": 0,
                },
                "human": "LLM total 0 reqs (0 ok, 0 failed), 0 input, 0 cached, 0 output, 0 reasoning",
            }


# =========================================================================
# LLM usage tracking (LLMOCRService, protocol, clients)
# =========================================================================


class TestLLMUsageTracking:
    def test_llm_usage_defaults(self):
        from src.services.ocr.protocol import LLMUsage

        u = LLMUsage()
        assert u.input_tokens == 0
        assert u.cached_tokens == 0
        assert u.output_tokens == 0
        assert u.reasoning_tokens == 0

    def test_accumulate_usage_single_model(self):
        from src.services.ocr.protocol import LLMUsage
        from src.services.ocr.llm import LLMOCRService

        svc = object.__new__(LLMOCRService)
        svc._usage = {}
        svc._record_attempt("gpt-4o")
        svc._accumulate_usage("gpt-4o", LLMUsage(input_tokens=100, output_tokens=50))
        svc._record_attempt("gpt-4o")
        svc._accumulate_usage(
            "gpt-4o", LLMUsage(input_tokens=200, cached_tokens=30, output_tokens=80)
        )
        stats = svc.usage_stats
        assert stats["gpt-4o"]["requests"] == 2
        assert stats["gpt-4o"]["successful_requests"] == 2
        assert stats["gpt-4o"]["failed_requests"] == 0
        assert stats["gpt-4o"]["input_tokens"] == 300
        assert stats["gpt-4o"]["cached_tokens"] == 30
        assert stats["gpt-4o"]["output_tokens"] == 130

    def test_accumulate_usage_multiple_models(self):
        from src.services.ocr.protocol import LLMUsage
        from src.services.ocr.llm import LLMOCRService

        svc = object.__new__(LLMOCRService)
        svc._usage = {}
        svc._record_attempt("model-a")
        svc._accumulate_usage("model-a", LLMUsage(input_tokens=100, output_tokens=50))
        svc._record_attempt("model-b")
        svc._record_failure("model-b")
        stats = svc.usage_stats
        assert "model-a" in stats and "model-b" in stats
        assert stats["model-a"]["requests"] == 1
        assert stats["model-a"]["successful_requests"] == 1
        assert stats["model-a"]["failed_requests"] == 0
        assert stats["model-b"]["requests"] == 1
        assert stats["model-b"]["successful_requests"] == 0
        assert stats["model-b"]["failed_requests"] == 1

    def test_openai_usage_extraction_non_stream(self):
        from src.services.ocr.clients.openai_client import _openai_usage
        from unittest.mock import MagicMock

        u = MagicMock()
        u.prompt_tokens = 150
        u.completion_tokens = 60
        u.prompt_tokens_details.cached_tokens = 40
        u.completion_tokens_details.reasoning_tokens = 10
        result = _openai_usage(u)
        assert result.input_tokens == 150
        assert result.output_tokens == 60
        assert result.cached_tokens == 40
        assert result.reasoning_tokens == 10

    def test_openai_usage_extraction_none(self):
        from src.services.ocr.clients.openai_client import _openai_usage
        from src.services.ocr.protocol import LLMUsage

        result = _openai_usage(None)
        assert result == LLMUsage()

    def test_openai_usage_infers_reasoning_from_total_tokens(self):
        from types import SimpleNamespace

        from src.services.ocr.clients.openai_client import _openai_usage

        usage = SimpleNamespace(
            prompt_tokens=150,
            completion_tokens=60,
            total_tokens=245,
            prompt_tokens_details=None,
            completion_tokens_details=None,
        )

        result = _openai_usage(usage)

        assert result.input_tokens == 150
        assert result.output_tokens == 60
        assert result.reasoning_tokens == 35

    def test_bedrock_usage_extraction(self):
        from src.services.ocr.protocol import LLMUsage

        raw = {
            "usage": {
                "inputTokens": 300,
                "outputTokens": 120,
                "cacheReadInputTokens": 50,
                "totalTokens": 420,
            }
        }
        u = raw.get("usage", {})
        usage = LLMUsage(
            input_tokens=u.get("inputTokens", 0),
            cached_tokens=u.get("cacheReadInputTokens", 0),
            output_tokens=u.get("outputTokens", 0),
        )
        assert usage.input_tokens == 300
        assert usage.output_tokens == 120
        assert usage.cached_tokens == 50
        assert usage.reasoning_tokens == 0

    def test_snowflake_returns_empty_usage(self):
        from src.services.ocr.protocol import LLMUsage

        usage = LLMUsage()
        assert usage.input_tokens == 0
        assert usage.output_tokens == 0

    @pytest.mark.asyncio
    async def test_call_with_retry_accumulates_usage(self):
        from src.services.ocr.protocol import LLMUsage
        from src.services.ocr.llm import LLMOCRService
        from src.services.ocr.config import LLMConfig
        from unittest.mock import AsyncMock, MagicMock

        mock_client = MagicMock()
        mock_client.generate = AsyncMock(
            return_value=("response text", LLMUsage(input_tokens=100, output_tokens=50))
        )
        config = LLMConfig(client=mock_client, model="test-model", rps=1000)
        svc = LLMOCRService(prompt="test", llm_config=config)

        result = await svc._call_with_retry([{"role": "user", "content": "hi"}])

        assert result == "response text"
        stats = svc.usage_stats
        assert "test-model" in stats
        assert stats["test-model"]["requests"] == 1
        assert stats["test-model"]["successful_requests"] == 1
        assert stats["test-model"]["failed_requests"] == 0
        assert stats["test-model"]["input_tokens"] == 100
        assert stats["test-model"]["output_tokens"] == 50

    @pytest.mark.asyncio
    async def test_call_with_retry_counts_failed_attempts_across_model_fallback(self):
        from src.services.ocr.protocol import LLMUsage
        from src.services.ocr.llm import LLMOCRService
        from src.services.ocr.config import LLMConfig
        from tenacity import (
            AsyncRetrying,
            retry_if_exception_type,
            stop_after_attempt,
            wait_fixed,
        )
        from unittest.mock import AsyncMock, MagicMock

        mock_client = MagicMock()
        mock_client.generate = AsyncMock(
            side_effect=[
                RuntimeError("flash failed"),
                ("response text", LLMUsage(input_tokens=100, output_tokens=50)),
            ]
        )
        config = LLMConfig(client=mock_client, model="model-a,model-b", rps=1000)
        svc = LLMOCRService(prompt="test", llm_config=config)
        svc._retry_strategy = AsyncRetrying(
            stop=stop_after_attempt(2),
            wait=wait_fixed(0),
            retry=retry_if_exception_type((RuntimeError,)),
        )

        result = await svc._call_with_retry([{"role": "user", "content": "hi"}])

        assert result == "response text"
        stats = svc.usage_stats
        assert stats["model-a"] == {
            "requests": 1,
            "successful_requests": 0,
            "failed_requests": 1,
            "input_tokens": 0,
            "cached_tokens": 0,
            "output_tokens": 0,
            "reasoning_tokens": 0,
        }
        assert stats["model-b"] == {
            "requests": 1,
            "successful_requests": 1,
            "failed_requests": 0,
            "input_tokens": 100,
            "cached_tokens": 0,
            "output_tokens": 50,
            "reasoning_tokens": 0,
        }

    @pytest.mark.asyncio
    async def test_call_with_retry_counts_exhausted_failures(self):
        from src.services.ocr.llm import LLMOCRService
        from src.services.ocr.config import LLMConfig
        from tenacity import (
            AsyncRetrying,
            retry_if_exception_type,
            stop_after_attempt,
            wait_fixed,
        )
        from unittest.mock import AsyncMock, MagicMock

        mock_client = MagicMock()
        mock_client.generate = AsyncMock(side_effect=RuntimeError("still failing"))
        config = LLMConfig(client=mock_client, model="test-model", rps=1000)
        svc = LLMOCRService(prompt="test", llm_config=config)
        svc._retry_strategy = AsyncRetrying(
            stop=stop_after_attempt(2),
            wait=wait_fixed(0),
            retry=retry_if_exception_type((RuntimeError,)),
        )

        result = await svc._call_with_retry([{"role": "user", "content": "hi"}])

        assert result == ""
        assert svc.usage_stats["test-model"] == {
            "requests": 2,
            "successful_requests": 0,
            "failed_requests": 2,
            "input_tokens": 0,
            "cached_tokens": 0,
            "output_tokens": 0,
            "reasoning_tokens": 0,
        }
