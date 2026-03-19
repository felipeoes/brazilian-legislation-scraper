"""Typed configuration for LLM-based OCR services."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.services.ocr.protocol import LLMClient
    from src.utils.concurrency import RateLimiter


@dataclass
class LLMConfig:
    """Typed configuration for LLM OCR services.

    Replaces the opaque ``dict`` that was passed around as ``llm_config``.

    Attributes:
        client: LLM backend implementing the ``LLMClient`` protocol.
        model: Comma-separated model identifier(s) for the LLM.
        rps: Requests per second limit for LLM API calls.
        rate_limiter: Shared rate limiter (created automatically if ``None``).
        batch_size: Number of pages/images to send per LLM request.
        raw: If ``True``, send raw PDF bytes instead of rendering to PNG first.
    """

    client: LLMClient
    model: str
    rps: float = 10
    rate_limiter: RateLimiter | None = field(default=None)
    batch_size: int = 5
    raw: bool = False

    def __post_init__(self) -> None:
        if self.rate_limiter is None:
            from src.utils.concurrency import RateLimiter

            self.rate_limiter = RateLimiter(self.rps)

    async def cleanup(self) -> None:
        """Close the underlying LLM client."""
        if self.client is not None:
            await self.client.close()

    @classmethod
    def _create_bedrock_client(
        cls, base_url: str, api_key: str, model: str
    ) -> "LLMConfig":
        from loguru import logger

        from src.services.ocr.clients import BedrockClient
        from src.services.request.service import RequestService

        client = BedrockClient(
            base_url=base_url,
            api_key=api_key,
            request_service=RequestService(rps=10, max_retries=6),
            inference_config={"maxTokens": 32768},
            performance_config={"latency": "standard"},
        )
        logger.info(f"Using Bedrock provider | Model: {model} | Base URL: {base_url}")
        return cls(client=client, model=model, rps=1, raw=True)

    @classmethod
    def _create_snowflake_client(
        cls,
        api_key: str,
        model: str,
        *,
        account: str = "",
        user: str = "",
        database: str = "",
        schema: str = "PUBLIC",
        stage: str = "",
    ) -> "LLMConfig":
        from loguru import logger

        from src.services.ocr.clients import SnowflakeClient

        client = SnowflakeClient(
            account=account,
            user=user,
            token=api_key,
            database=database,
            schema=schema,
            stage=stage,
        )
        logger.info(
            f"Using Snowflake provider | Model: {model} | Account: {client.account}"
        )
        return cls(client=client, model=model, rps=50)

    @classmethod
    def _create_openai_client(
        cls, api_key: str, base_url: str, model: str
    ) -> "LLMConfig":
        from loguru import logger
        from openai import AsyncOpenAI

        from src.services.ocr.clients import OpenAIClient

        raw_client = AsyncOpenAI(api_key=api_key, base_url=base_url, max_retries=1)
        logger.info(
            f"Using OpenAI provider | Model: {model} | Base URL: {raw_client.base_url}"
        )
        return cls(
            client=OpenAIClient(
                raw_client,
                max_completion_tokens=65536,
                reasoning_effort="high",
            ),
            model=model,
            rps=50,
        )

    @classmethod
    def from_env(cls) -> "LLMConfig | None":
        """Build an ``LLMConfig`` from environment variables.

        Returns ``None`` when no LLM credentials are configured.
        """
        from loguru import logger

        api_key = os.environ.get("LLM_API_KEY", "")
        base_url = os.environ.get("PROVIDER_BASE_URL", "")
        model = os.environ.get("LLM_MODEL", "")
        provider_env = os.environ.get("LLM_PROVIDER", "").strip().lower()

        # Read Snowflake vars once so they're available for both auto-detection
        # and client construction without a second os.environ.get() pass.
        snowflake_account = os.environ.get("SNOWFLAKE_ACCOUNT", "")
        snowflake_user = os.environ.get("SNOWFLAKE_USER", "")
        snowflake_database = os.environ.get("SNOWFLAKE_DATABASE", "")
        snowflake_schema = os.environ.get("SNOWFLAKE_SCHEMA", "PUBLIC")
        snowflake_stage = os.environ.get("SNOWFLAKE_STAGE", "")

        if not api_key and not base_url:
            logger.warning("No LLM_API_KEY or PROVIDER_BASE_URL set; LLM OCR disabled.")
            return None

        provider = provider_env
        if not provider:
            looks_like_snowflake = "snowflakecomputing.com" in base_url.lower()
            has_snowflake_config = all(
                [snowflake_account, snowflake_user, snowflake_database, snowflake_stage]
            )
            if looks_like_snowflake and has_snowflake_config:
                provider = "snowflake"
                logger.info(
                    "LLM_PROVIDER not set; inferred Snowflake provider from configuration."
                )
            else:
                provider = "openai"

        if provider == "bedrock":
            return cls._create_bedrock_client(base_url, api_key, model)
        if provider == "snowflake":
            return cls._create_snowflake_client(
                api_key,
                model,
                account=snowflake_account,
                user=snowflake_user,
                database=snowflake_database,
                schema=snowflake_schema,
                stage=snowflake_stage,
            )
        return cls._create_openai_client(api_key, base_url, model)
