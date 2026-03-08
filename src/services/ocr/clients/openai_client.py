"""OpenAI-compatible LLM client adapter.

Wraps ``AsyncOpenAI`` into the ``LLMClient`` protocol so that
``LLMOCRService`` can treat all backends uniformly.
"""

from __future__ import annotations

from typing import Any, cast

from openai import AsyncOpenAI

from ..protocol import LLMUsage


def _usage_value(obj, key: str):
    if obj is None:
        return None
    if isinstance(obj, dict):
        return obj.get(key)
    return getattr(obj, key, None)


def _usage_int(obj, key: str) -> int:
    value = _usage_value(obj, key)
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int | float):
        return int(value)
    return 0


def _openai_usage(u) -> LLMUsage:
    if not u:
        return LLMUsage()
    ptd = _usage_value(u, "prompt_tokens_details")
    ctd = _usage_value(u, "completion_tokens_details")

    input_tokens = _usage_int(u, "prompt_tokens")
    output_tokens = _usage_int(u, "completion_tokens")
    reasoning_tokens = _usage_int(ctd, "reasoning_tokens")
    total_tokens = _usage_int(u, "total_tokens")

    if reasoning_tokens == 0 and total_tokens > input_tokens + output_tokens:
        reasoning_tokens = total_tokens - input_tokens - output_tokens

    return LLMUsage(
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        cached_tokens=_usage_int(ptd, "cached_tokens"),
        reasoning_tokens=reasoning_tokens,
    )


class OpenAIClient:
    """Thin adapter from ``AsyncOpenAI`` to the ``LLMClient`` protocol."""

    def __init__(self, client: AsyncOpenAI, **kwargs) -> None:
        self._client = client
        self._kwargs = kwargs

    async def generate(
        self, messages: list[dict], model_id: str, timeout: int | None = None
    ) -> tuple[str, LLMUsage]:
        kwargs = self._kwargs.copy()
        kwargs.pop("stream", None)
        if timeout is not None:
            kwargs["timeout"] = timeout

        create = cast(Any, self._client.chat.completions.create)
        response = await create(
            model=model_id,
            messages=messages,
            **kwargs,
        )

        return response.choices[0].message.content, _openai_usage(response.usage)

    async def close(self) -> None:
        """Close the underlying OpenAI async client."""
        await self._client.close()
