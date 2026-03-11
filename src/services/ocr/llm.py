"""LLM-based OCR service for converting PDFs and images to markdown."""

from __future__ import annotations

import asyncio
import base64

import aiohttp
import fitz
from src.services.ocr.config import LLMConfig
from src.services.ocr.protocol import LLMClient, LLMUsage
from src.utils.concurrency import RateLimiter
from src.utils import clean_md_tag
from loguru import logger
from openai import (
    RateLimitError,
    APITimeoutError,
    InternalServerError,
    APIConnectionError,
)
from tenacity import (
    AsyncRetrying,
    RetryError,
    stop_after_attempt,
    wait_random_exponential,
    wait_fixed,
    retry_if_exception_type,
)


class LLMOCRService:
    """Convert PDFs and images to markdown using an LLM vision model.

    All PDF inputs are rendered page-by-page to PNG images via PyMuPDF before
    being sent to the model, ensuring compatibility with providers that only
    support the Chat Completions API and do not accept raw PDF bytes.

    Args:
        prompt: Instruction prompt sent with every image.
        llm_config: Typed LLM configuration dataclass.
        verbose: Enable verbose logging.
    """

    def __init__(
        self,
        prompt: str,
        llm_config: LLMConfig | None = None,
        verbose: bool = False,
        timeout: int = 180,
    ) -> None:
        self.client: LLMClient | None = llm_config.client if llm_config else None
        raw_model = llm_config.model if llm_config else ""
        self.models = [m.strip() for m in raw_model.split(",") if m.strip()]

        self.prompt = prompt
        self.verbose = verbose
        self.timeout = timeout

        effective_rps = llm_config.rps if llm_config else 10
        self._rate_limiter = (
            llm_config.rate_limiter if llm_config else None
        ) or RateLimiter(effective_rps)
        self.batch_size = llm_config.batch_size if llm_config else 5
        self.raw = llm_config.raw if llm_config else False
        self._usage: dict[str, dict] = {}
        self._pdf_semaphore = asyncio.Semaphore(max(1, int(effective_rps)))
        self._max_retry_attempts = 10
        if verbose:
            logger.info(
                f"Initialized LLMOCRService with models: {self.models} | RPS: {effective_rps} | batch_size: {self.batch_size} | Timeout: {timeout}s | Raw: {self.raw}"
            )

    # ------------------------------------------------------------------
    # Usage tracking
    # ------------------------------------------------------------------

    @staticmethod
    def _default_usage_bucket() -> dict[str, int]:
        return {
            "requests": 0,
            "successful_requests": 0,
            "failed_requests": 0,
            "input_tokens": 0,
            "cached_tokens": 0,
            "output_tokens": 0,
            "reasoning_tokens": 0,
        }

    def _usage_bucket(self, model_id: str) -> dict[str, int]:
        return self._usage.setdefault(model_id, self._default_usage_bucket())

    def _record_attempt(self, model_id: str) -> None:
        self._usage_bucket(model_id)["requests"] += 1

    def _record_failure(self, model_id: str) -> None:
        self._usage_bucket(model_id)["failed_requests"] += 1

    def _accumulate_usage(self, model_id: str, usage: LLMUsage) -> None:
        b = self._usage_bucket(model_id)
        b["successful_requests"] += 1
        b["input_tokens"] += usage.input_tokens
        b["cached_tokens"] += usage.cached_tokens
        b["output_tokens"] += usage.output_tokens
        b["reasoning_tokens"] += usage.reasoning_tokens

    @property
    def usage_stats(self) -> dict[str, dict]:
        return {model_id: dict(usage) for model_id, usage in self._usage.items()}

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _new_retry(self) -> AsyncRetrying:
        """Create a fresh ``AsyncRetrying`` for each call.

        A single shared ``AsyncRetrying`` is NOT safe for concurrent use:
        ``__aiter__`` stores ``_retry_state`` on the instance, so concurrent
        ``async for`` loops corrupt each other's state.
        """
        return AsyncRetrying(
            stop=stop_after_attempt(self._max_retry_attempts),
            wait=wait_fixed(5) + wait_random_exponential(min=1, max=60, multiplier=2),
            retry=retry_if_exception_type(
                (
                    RateLimitError,
                    APITimeoutError,
                    InternalServerError,
                    APIConnectionError,
                    aiohttp.ClientError,
                    RuntimeError,
                    TimeoutError,
                )
            ),
        )

    def _pdf_bytes_to_images(self, content: bytes) -> list[bytes]:
        """Render each PDF page to a PNG at 50 DPI and return the raw bytes."""
        doc = fitz.open(stream=content, filetype="pdf")
        pages: list[bytes] = []
        for page_num in range(doc.page_count):
            pix = doc.load_page(page_num).get_pixmap(dpi=50)
            pages.append(pix.tobytes("png"))
        doc.close()
        return pages

    async def _call_with_retry(
        self,
        messages: list,
        desc: str = "LLM call",
        timeout: int | None = None,
    ) -> str:
        """Execute an LLM call with retry, rate limiting, and error handling."""
        if not self.client or not self.models:
            raise RuntimeError("LLM client or models are not initialized.")

        timeout = timeout or self.timeout
        response_text = ""
        try:
            async for attempt in self._new_retry():
                await self._rate_limiter.acquire()
                attempt_num = attempt.retry_state.attempt_number
                model = self.models[(attempt_num - 1) % len(self.models)]
                if self.verbose and attempt_num > 6:
                    logger.info(
                        f"Sending {desc} to LLM | Attempt {attempt_num} | Model: {model}"
                    )
                self._record_attempt(model)

                with attempt:
                    try:
                        response_text, usage = await self.client.generate(
                            messages=messages,
                            model_id=model,
                            timeout=timeout,
                        )
                    except Exception:
                        self._record_failure(model)
                        raise
                    self._accumulate_usage(model, usage)
                    return clean_md_tag(response_text or "")
        except RetryError as e:
            last_exc = e.last_attempt.exception() if e.last_attempt else e
            logger.error(f"{desc} failed after retries: {last_exc}")
            return ""
        except Exception as e:
            logger.error(f"{desc} failed: {e}")
            return ""

    async def images_to_markdown(
        self, images: list[bytes], timeout: int | None = None
    ) -> str:
        """Convert a batch of PNG images to markdown via the LLM in a single request."""
        if not images:
            return ""

        content_blocks: list[dict] = [{"type": "text", "text": self.prompt}]
        valid_count = 0
        for img_bytes in images:
            if not img_bytes:
                logger.debug("Skipping empty image bytes (blank/corrupt PDF page)")
                continue
            image_b64 = base64.standard_b64encode(img_bytes).decode()
            content_blocks.append(
                {
                    "type": "image_url",
                    "image_url": {"url": f"data:image/png;base64,{image_b64}"},
                }
            )
            valid_count += 1

        if valid_count == 0:
            return ""

        messages = [{"role": "user", "content": content_blocks}]
        return await self._call_with_retry(
            messages, desc=f"{valid_count} images", timeout=timeout
        )

    async def documents_to_markdown(
        self, docs: list[bytes], doc_format: str = "pdf", timeout: int | None = None
    ) -> str:
        """Convert a batch of documents to markdown via the LLM in a single request."""
        if not docs:
            return ""

        content_blocks: list[dict] = [{"type": "text", "text": self.prompt}]
        for i, doc_bytes in enumerate(docs):
            doc_b64 = base64.standard_b64encode(doc_bytes).decode()
            content_blocks.append(
                {
                    "type": "document",
                    "document": {
                        "name": f"document_{i}",
                        "format": doc_format,
                        "source": {"bytes": doc_b64},
                    },
                }
            )

        messages = [{"role": "user", "content": content_blocks}]
        return await self._call_with_retry(
            messages, desc=f"{len(docs)} documents", timeout=timeout
        )

    async def _sem_documents_to_markdown(
        self, docs: list[bytes], doc_format: str = "pdf", timeout: int | None = None
    ) -> str:
        async with self._pdf_semaphore:
            return await self.documents_to_markdown(
                docs, doc_format=doc_format, timeout=timeout
            )

    async def _sem_images_to_markdown(
        self, images: list[bytes], timeout: int | None = None
    ) -> str:
        async with self._pdf_semaphore:
            return await self.images_to_markdown(images, timeout=timeout)

    async def pdf_to_markdown(self, content: bytes, timeout: int | None = None) -> str:
        """Convert PDF bytes to markdown. If llm_raw=True, sends PDF bytes directly in batches."""
        if self.raw:
            try:
                doc = fitz.open(stream=content, filetype="pdf")
                total_pages = len(doc)

                pdf_chunks = []
                for i in range(0, total_pages, self.batch_size):
                    new_doc = fitz.open()
                    new_doc.insert_pdf(
                        doc,
                        from_page=i,
                        to_page=min(i + self.batch_size - 1, total_pages - 1),
                    )
                    pdf_chunks.append(new_doc.write())
                    new_doc.close()
                doc.close()
            except Exception as e:
                logger.error(f"Failed to chunk PDF: {e}")
                return ""

            results = await asyncio.gather(
                *[
                    self._sem_documents_to_markdown(
                        [chunk], doc_format="pdf", timeout=timeout
                    )
                    for chunk in pdf_chunks
                ]
            )

            markdown = "\n\n".join(r for r in results if r.strip())
            return markdown if markdown.strip() else ""

        try:
            pages_png = await asyncio.to_thread(self._pdf_bytes_to_images, content)
        except Exception as e:
            logger.error(f"PDF to images conversion failed: {e}")
            return ""

        batches = [
            pages_png[i : i + self.batch_size]
            for i in range(0, len(pages_png), self.batch_size)
        ]

        results = await asyncio.gather(
            *[self._sem_images_to_markdown(batch, timeout=timeout) for batch in batches]
        )

        markdown = "\n\n".join(r for r in results if r.strip())
        if not markdown.strip():
            return ""

        return markdown
