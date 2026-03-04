"""Configuration options for Docling VLM (Vision Language Model) pipeline."""

from dataclasses import dataclass


@dataclass
class DoclingVlmConfig:
    """Configuration for Docling VLM pipeline.

    When provided, the scraper will use VLM pipeline instead of standard OCR.
    VLM provides better quality for complex documents but requires a remote inference service.

    Attributes:
        url: URL of the VLM inference service (e.g., vLLM, Ollama)
             Example: "http://localhost:8000/v1/chat/completions"
        concurrency: Number of concurrent requests to the VLM service
                    Higher values improve throughput on GPU-accelerated services
        prompt: The prompt to send to the VLM model for document conversion
               Default is optimized for document-to-markdown conversion
        response_format: Output format - "markdown", "html", or "doctags"
        batch_size: Number of pages to process in parallel (default: 64)
        temperature: Model temperature for generation (0.0 = deterministic)
    """

    url: str
    concurrency: int = 32
    prompt: str = "Convert this page to markdown. Do not miss any text and only output the bare markdown!"
    response_format: str = "markdown"  # "markdown", "html", or "doctags"
    batch_size: int = 64
    temperature: float = 0.0
    scale: float = 2.0
    timeout: float = 120.0
