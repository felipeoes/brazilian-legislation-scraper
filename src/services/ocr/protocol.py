"""Protocol for LLM clients used by LLMOCRService.

All LLM backends (OpenAI, Bedrock, Snowflake) must satisfy this
interface so that LLMOCRService can dispatch without isinstance checks.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, runtime_checkable

__all__ = ["LLMClient", "LLMUsage"]


@dataclass
class LLMUsage:
    """Token usage reported by a single LLM generate() call."""

    input_tokens: int = 0
    cached_tokens: int = 0
    output_tokens: int = 0
    reasoning_tokens: int = 0


@runtime_checkable
class LLMClient(Protocol):
    """Minimal interface every LLM backend must implement."""

    async def generate(
        self, messages: list[dict], model_id: str, timeout: int | None = None
    ) -> tuple[str, LLMUsage]: ...

    async def close(self) -> None: ...
