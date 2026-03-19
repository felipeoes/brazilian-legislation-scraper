"""Tests for the PDF scan detection and pymupdf4llm conversion pipeline."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import fitz
import pytest

from src.scraper.base.converter import (
    MarkdownConverter,
    is_pdf_scanned,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_digital_pdf(
    text: str = "Art. 1º Este é um decreto digital com texto suficiente para validação.",
) -> bytes:
    """Create a minimal digital PDF with selectable text (no images)."""
    doc = fitz.open()
    page = doc.new_page(width=595, height=842)
    page.insert_text((72, 72), text, fontsize=12)
    content = doc.tobytes()
    doc.close()
    return content


def _make_scanned_pdf() -> bytes:
    """Create a PDF that looks scanned (full-page image, no text)."""
    doc = fitz.open()
    page = doc.new_page(width=595, height=842)
    # Insert a large image covering the full page to simulate a scan.
    # Use a minimal 2x2 white PNG.
    import struct
    import zlib

    def _minimal_png(w: int, h: int) -> bytes:
        raw = b""
        for _ in range(h):
            raw += b"\x00" + b"\xff" * (w * 3)
        compressed = zlib.compress(raw)

        def _chunk(ctype: bytes, data: bytes) -> bytes:
            c = ctype + data
            return (
                struct.pack(">I", len(data))
                + c
                + struct.pack(">I", zlib.crc32(c) & 0xFFFFFFFF)
            )

        ihdr_data = struct.pack(">IIBBBBB", w, h, 8, 2, 0, 0, 0)
        return (
            b"\x89PNG\r\n\x1a\n"
            + _chunk(b"IHDR", ihdr_data)
            + _chunk(b"IDAT", compressed)
            + _chunk(b"IEND", b"")
        )

    png = _minimal_png(595, 842)
    rect = fitz.Rect(0, 0, 595, 842)
    page.insert_image(rect, stream=png)
    content = doc.tobytes()
    doc.close()
    return content


def _make_converter(**overrides) -> MarkdownConverter:
    """Create a MarkdownConverter with a mock scraper."""
    scraper = MagicMock()
    scraper.ocr_service = overrides.pop("ocr_service", None)
    scraper.base_url = overrides.pop("base_url", "http://example.com")
    for k, v in overrides.items():
        setattr(scraper, k, v)
    return MarkdownConverter(scraper)


# ---------------------------------------------------------------------------
# is_pdf_scanned
# ---------------------------------------------------------------------------


class TestIsPdfScanned:
    def test_digital_pdf_detected(self):
        pdf = _make_digital_pdf("Art. 1º Ficam criadas as seguintes secretarias. " * 10)
        is_scanned, confidence = is_pdf_scanned(pdf)
        assert is_scanned is False
        assert confidence >= 0.7

    def test_scanned_pdf_detected(self):
        pdf = _make_scanned_pdf()
        is_scanned, confidence = is_pdf_scanned(pdf)
        assert is_scanned is True
        assert confidence > 0.5

    def test_empty_pdf(self):
        doc = fitz.open()
        doc.new_page()
        content = doc.tobytes()
        doc.close()
        # Empty page with no images and no text — blank page is excluded,
        # so we get no scores and the function returns (False, 1.0).
        is_scanned, confidence = is_pdf_scanned(content)
        assert is_scanned is False

    def test_invalid_bytes_raises(self):
        with pytest.raises(ValueError, match="Could not open PDF"):
            is_pdf_scanned(b"not a pdf")

    def test_zero_page_pdf(self):
        """A PDF with zero pages returns (False, 0.0)."""
        # fitz cannot serialize a zero-page PDF, so we construct one manually.
        raw = (
            b"%PDF-1.0\n"
            b"1 0 obj<</Type/Catalog/Pages 2 0 R>>endobj\n"
            b"2 0 obj<</Type/Pages/Kids[]/Count 0>>endobj\n"
            b"xref\n0 3\n"
            b"0000000000 65535 f \n"
            b"0000000009 00000 n \n"
            b"0000000052 00000 n \n"
            b"trailer<</Size 3/Root 1 0 R>>\n"
            b"startxref\n99\n%%EOF"
        )
        is_scanned, confidence = is_pdf_scanned(raw)
        assert is_scanned is False
        assert confidence == 0.0


# ---------------------------------------------------------------------------
# _pymupdf4llm_convert
# ---------------------------------------------------------------------------


class TestPymupdf4llmConvert:
    @pytest.mark.asyncio
    async def test_converts_digital_pdf(self):
        pdf = _make_digital_pdf(
            "Art. 1º Ficam criadas as seguintes secretarias de estado."
        )
        converter = _make_converter()
        result = await converter._pymupdf4llm_convert(pdf)
        assert "Art." in result
        assert len(result) > 20

    @pytest.mark.asyncio
    async def test_returns_empty_on_corrupt(self):
        converter = _make_converter()
        result = await converter._pymupdf4llm_convert(b"not a pdf")
        assert result == ""


# ---------------------------------------------------------------------------
# bytes_to_markdown routing
# ---------------------------------------------------------------------------


class TestBytesToMarkdownRouting:
    @pytest.mark.asyncio
    async def test_non_pdf_uses_html_to_markdown(self):
        """Non-PDF HTML content should go through html-to-markdown."""
        converter = _make_converter()
        html = b"<html><body><p>Hello world, this is a test document with enough text.</p></body></html>"

        with patch.object(
            converter, "_convert_html_with_images", new_callable=AsyncMock
        ) as mock_convert:
            mock_convert.return_value = (
                "Hello world, this is a test document with enough text."
            )
            result = await converter.bytes_to_markdown(
                html, filename="document.html", content_type="text/html"
            )
            mock_convert.assert_called_once()
            assert "Hello world" in result

    @pytest.mark.asyncio
    async def test_digital_pdf_uses_pymupdf4llm(self):
        """Digital PDFs should use pymupdf4llm, not html-to-markdown."""
        long_text = "Art. 1º Ficam criadas as seguintes secretarias. " * 20
        pdf = _make_digital_pdf(long_text)
        converter = _make_converter()

        with patch.object(
            converter, "_convert_html_with_images", new_callable=AsyncMock
        ) as mock_html:
            result = await converter.bytes_to_markdown(
                pdf, content_type="application/pdf"
            )
            # html-to-markdown should NOT be called for PDFs
            mock_html.assert_not_called()
            assert len(result) > 50

    @pytest.mark.asyncio
    async def test_scanned_pdf_uses_ocr(self):
        """Scanned PDFs should route to LLM OCR when available."""
        pdf = _make_scanned_pdf()
        ocr_mock = AsyncMock()
        ocr_mock.pdf_to_markdown = AsyncMock(return_value="# OCR Result\nArt. 1º ...")
        converter = _make_converter(ocr_service=ocr_mock)

        result = await converter.bytes_to_markdown(pdf, content_type="application/pdf")
        ocr_mock.pdf_to_markdown.assert_called_once_with(pdf)
        assert "OCR Result" in result

    @pytest.mark.asyncio
    async def test_low_confidence_digital_uses_ocr(self):
        """When is_pdf_scanned returns not-scanned but low confidence, use OCR."""
        pdf = _make_digital_pdf("short")
        ocr_mock = AsyncMock()
        ocr_mock.pdf_to_markdown = AsyncMock(return_value="# OCR low conf")
        converter = _make_converter(ocr_service=ocr_mock)

        with patch(
            "src.scraper.base.converter.is_pdf_scanned",
            return_value=(False, 0.5),
        ):
            result = await converter.bytes_to_markdown(
                pdf, content_type="application/pdf"
            )
            ocr_mock.pdf_to_markdown.assert_called_once()
            assert "OCR low conf" in result

    @pytest.mark.asyncio
    async def test_digital_pdf_falls_back_to_ocr_on_validation_failure(self):
        """If pymupdf4llm output fails validation, fall back to OCR."""
        pdf = _make_digital_pdf("x")  # very short text → validation will fail
        ocr_mock = AsyncMock()
        ocr_mock.pdf_to_markdown = AsyncMock(return_value="# OCR fallback content here")
        converter = _make_converter(ocr_service=ocr_mock)

        with patch(
            "src.scraper.base.converter.is_pdf_scanned",
            return_value=(False, 0.9),
        ):
            result = await converter.bytes_to_markdown(
                pdf, content_type="application/pdf"
            )
            ocr_mock.pdf_to_markdown.assert_called_once()
            assert "OCR fallback" in result

    @pytest.mark.asyncio
    async def test_scanned_pdf_no_ocr_tries_pymupdf4llm(self):
        """Scanned PDF without OCR service falls back to pymupdf4llm."""
        pdf = _make_scanned_pdf()
        converter = _make_converter(ocr_service=None)

        # Should not raise — returns empty or whatever pymupdf4llm produces
        result = await converter.bytes_to_markdown(pdf, content_type="application/pdf")
        assert isinstance(result, str)

    @pytest.mark.asyncio
    async def test_scan_detection_failure_assumes_scanned(self):
        """If scan detection raises, assume scanned and use OCR."""
        pdf = _make_digital_pdf("test content")
        ocr_mock = AsyncMock()
        ocr_mock.pdf_to_markdown = AsyncMock(return_value="# OCR after detection error")
        converter = _make_converter(ocr_service=ocr_mock)

        with patch(
            "src.scraper.base.converter.is_pdf_scanned",
            side_effect=RuntimeError("detection failed"),
        ):
            result = await converter.bytes_to_markdown(
                pdf, content_type="application/pdf"
            )
            ocr_mock.pdf_to_markdown.assert_called_once()
            assert "OCR after detection error" in result
