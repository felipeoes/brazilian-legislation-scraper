"""Protocol for LLM clients used by LLMOCRService.

All LLM backends (OpenAI, Bedrock, Snowflake) must satisfy this
interface so that LLMOCRService can dispatch without isinstance checks.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class LLMClient(Protocol):
    """Minimal interface every LLM backend must implement."""

    async def generate(
        self, messages: list[dict], model_id: str, timeout: int | None = None
    ) -> str: ...
