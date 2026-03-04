"""LLM-based OCR service for converting PDFs and images to markdown."""

from __future__ import annotations

import asyncio
import base64

import aiohttp
import fitz
from src.services.ocr.bedrock import BedrockClient
from src.services.request.service import RequestService
from src.scraper.base.concurrency import RateLimiter
from src.utils import clean_md_tag
from loguru import logger
from openai import (
    AsyncOpenAI,
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
        llm_config: Dictionary containing LLM configurations (llm_client, llm_model,
            llm_kwargs, and optionally llm_rps).
        prompt: Instruction prompt sent with every image.
        request_service: Request service instance to be used by the OCR service.
        verbose: Enable verbose logging.
    """

    def __init__(
        self,
        prompt: str,
        request_service: RequestService,
        llm_config: dict | None = None,
        verbose: bool = False,
        timeout: int = 180,
    ) -> None:
        self.llm_config = llm_config or {}
        self.client: AsyncOpenAI | BedrockClient | None = self.llm_config.get(
            "llm_client"
        )
        raw_model = self.llm_config.get("llm_model", "")
        if isinstance(raw_model, str):
            self.models = [m.strip() for m in raw_model.split(",") if m.strip()]
        else:
            self.models = raw_model if isinstance(raw_model, list) else []

        self.kwargs = self.llm_config.get("llm_kwargs", {})
        self.prompt = prompt
        self.request_service = request_service
        self.verbose = verbose
        self.timeout = timeout

        effective_rps = self.llm_config.get("llm_rps", 10)
        self._rate_limiter = RateLimiter(effective_rps)
        self.batch_size = self.llm_config.get("llm_batch_size", 5)
        self.raw = self.llm_config.get("llm_raw", False)
        if verbose:
            logger.info(
                f"Initialized LLMOCRService with models: {self.models} | RPS: {effective_rps} | batch_size: {self.batch_size} | Timeout: {timeout}s | Raw: {self.raw}"
            )

    # ------------------------------------------------------------------
    # Utilities
    # ------------------------------------------------------------------

    async def generate(
        self, messages: list, attempt_number: int = 1, timeout: int | None = None
    ) -> str:
        """Generate response from LLM.

        Dispatches to either the OpenAI Chat Completions API or the
        Amazon Bedrock Converse API depending on the configured client.
        """
        if not self.client or not self.models:
            raise RuntimeError("LLM client or models are not initialized.")

        model = self.models[(attempt_number - 1) % len(self.models)]

        if isinstance(self.client, BedrockClient):
            return await self.client.generate(messages, model_id=model, timeout=timeout)

        # OpenAI-compatible path
        is_stream = self.kwargs.get("stream", False)
        kwargs = self.kwargs.copy()
        if timeout is not None:
            kwargs["timeout"] = timeout
        response = await self.client.chat.completions.create(
            model=model, messages=messages, **kwargs
        )

        if is_stream:
            content = []
            async for chunk in response:
                if chunk.choices and chunk.choices[0].delta.content:
                    content.append(chunk.choices[0].delta.content)
            return "".join(content)

        return response.choices[0].message.content

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _create_retry_strategy(self, max_attempts: int = 10) -> AsyncRetrying:
        """Create a configured AsyncRetrying instance for LLM requests."""
        return AsyncRetrying(
            stop=stop_after_attempt(max_attempts),  # 10 attempts roughly 5 minutes
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

    def _clean_md_tag(self, md_content: str) -> str:
        """Strip markdown code block wrappers if present."""
        return clean_md_tag(md_content)

    def _pdf_bytes_to_images(self, content: bytes) -> list[bytes]:
        """Render each PDF page to a PNG at 50 DPI and return the raw bytes."""
        doc = fitz.open(stream=content, filetype="pdf")
        pages: list[bytes] = []
        for page_num in range(doc.page_count):
            pix = doc.load_page(page_num).get_pixmap(dpi=50)
            pages.append(pix.tobytes("png"))
        doc.close()
        return pages

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def text_to_markdown(
        self, text: str, prompt: str, timeout: int | None = None
    ) -> str:
        """Send plain text to the LLM and return its response as markdown.

        Useful for post-processing already-extracted text (e.g. extracting a
        specific norm from combined OCR pages that may contain multiple norms).

        Args:
            text: The text content to process.
            prompt: Instruction prompt sent alongside the text.
            timeout: Optional custom timeout in seconds.

        Returns:
            The LLM response string, or ``""`` on failure.
        """
        timeout = timeout or self.timeout
        try:
            async for attempt in self._create_retry_strategy():
                with attempt:
                    await self._rate_limiter.acquire()
                    if self.verbose and attempt.retry_state.attempt_number > 6:
                        model = self.models[
                            (attempt.retry_state.attempt_number - 1) % len(self.models)
                        ]
                        logger.info(
                            f"Sending text to LLM (size: {len(text)} chars) | Attempt {attempt.retry_state.attempt_number} | Model: {model}"
                        )
                    response_text = await self.generate(
                        messages=[
                            {
                                "role": "user",
                                "content": f"{text}\n\n{prompt}",
                            }
                        ],
                        attempt_number=attempt.retry_state.attempt_number,
                        timeout=timeout,
                    )
            return self._clean_md_tag(response_text or "")
        except RetryError:
            logger.error("LLM text conversion failed after 10 attempts.")
            return ""
        except Exception as e:
            logger.error(f"LLM text conversion failed: {e}")
            return ""

    async def images_to_markdown(
        self, images: list[bytes], timeout: int | None = None
    ) -> str:
        """Convert a batch of PNG images to markdown via the LLM in a single request."""
        if not images:
            return ""

        timeout = timeout or self.timeout
        try:
            async for attempt in self._create_retry_strategy():
                with attempt:
                    await self._rate_limiter.acquire()
                    if self.verbose and attempt.retry_state.attempt_number > 6:
                        model = self.models[
                            (attempt.retry_state.attempt_number - 1) % len(self.models)
                        ]
                        logger.info(
                            f"Sending {len(images)} images to LLM | Attempt {attempt.retry_state.attempt_number} | Model: {model}"
                        )

                    content_blocks = [{"type": "text", "text": self.prompt}]
                    for img_bytes in images:
                        image_b64 = base64.standard_b64encode(img_bytes).decode()
                        content_blocks.append(
                            {
                                "type": "image_url",
                                "image_url": {
                                    "url": f"data:image/png;base64,{image_b64}"
                                },
                            }
                        )

                    response_text = await self.generate(
                        messages=[
                            {
                                "role": "user",
                                "content": content_blocks,
                            }
                        ],
                        attempt_number=attempt.retry_state.attempt_number,
                        timeout=timeout,
                    )
                    return self._clean_md_tag(response_text or "")
        except RetryError as e:
            logger.error(
                f"LLM batch image conversion failed after 10 attempts: {e.last_attempt.exception()}"
            )
            return ""
        except Exception as e:
            logger.error(f"LLM batch image conversion failed: {e}")
            return ""

    async def documents_to_markdown(
        self, docs: list[bytes], format: str = "pdf", timeout: int | None = None
    ) -> str:
        """Convert a batch of documents to markdown via the LLM in a single request."""
        if not docs:
            return ""

        timeout = timeout or self.timeout
        try:
            async for attempt in self._create_retry_strategy():
                with attempt:
                    await self._rate_limiter.acquire()
                    if self.verbose and attempt.retry_state.attempt_number > 6:
                        model = self.models[
                            (attempt.retry_state.attempt_number - 1) % len(self.models)
                        ]
                        logger.info(
                            f"Sending {len(docs)} documents to LLM | Attempt {attempt.retry_state.attempt_number} | Model: {model}"
                        )

                    content_blocks = [{"type": "text", "text": self.prompt}]
                    for i, doc_bytes in enumerate(docs):
                        doc_b64 = base64.standard_b64encode(doc_bytes).decode()
                        content_blocks.append(
                            {
                                "type": "document",
                                "document": {
                                    "name": f"document_{i}",
                                    "format": format,
                                    "source": {"bytes": doc_b64},
                                },
                            }
                        )

                    response_text = await self.generate(
                        messages=[
                            {
                                "role": "user",
                                "content": content_blocks,
                            }
                        ],
                        attempt_number=attempt.retry_state.attempt_number,
                        timeout=timeout,
                    )
            return self._clean_md_tag(response_text or "")
        except RetryError:
            logger.error("LLM batch document conversion failed after 10 attempts.")
            return ""
        except Exception as e:
            logger.error(f"LLM batch document conversion failed: {e}")
            return ""

    async def pdf_to_markdown(self, content: bytes, timeout: int | None = None) -> str:
        """Convert PDF bytes to markdown. If llm_raw=True (from llm_config), sends PDF bytes directly in batches."""
        if self.raw:
            try:
                doc = fitz.open("pdf", content)
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
                    self.documents_to_markdown([chunk], format="pdf", timeout=timeout)
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
            *[self.images_to_markdown(batch, timeout=timeout) for batch in batches]
        )

        markdown = "\n\n".join(r for r in results if r.strip())
        if not markdown.strip():
            return ""

        return markdown
