"""Tests for the PDF scan detection and pymupdf4llm conversion pipeline."""

import fitz
import pytest

from src.core.convert import is_pdf_scanned


def _make_digital_pdf(
    text: str = "Art. 1º Este é um decreto digital com texto suficiente para validação.",
) -> bytes:
    """Create a minimal digital PDF with selectable text (no images)."""
    doc = fitz.open()
    page = doc.new_page(width=595, height=842)
    page.insert_textbox(fitz.Rect(72, 72, 520, 770), text, fontsize=12)
    content = doc.tobytes()
    doc.close()
    return content


def _make_scanned_pdf() -> bytes:
    """Create a PDF that looks scanned (full-page image, no text)."""
    doc = fitz.open()
    page = doc.new_page(width=595, height=842)
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
                + struct.pack(">I", zlib.crc32(c) & 4294967295)
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


def _make_png_bytes() -> bytes:
    """Return bytes with a PNG signature for image inlining tests."""
    return b"\x89PNG\r\n\x1a\nfake-png"


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
        is_scanned, confidence = is_pdf_scanned(content)
        assert is_scanned is False

    def test_invalid_bytes_raises(self):
        with pytest.raises(ValueError, match="Could not open PDF"):
            is_pdf_scanned(b"not a pdf")

    def test_zero_page_pdf(self):
        """A PDF with zero pages returns (False, 0.0)."""
        raw = b"%PDF-1.0\n1 0 obj<</Type/Catalog/Pages 2 0 R>>endobj\n2 0 obj<</Type/Pages/Kids[]/Count 0>>endobj\nxref\n0 3\n0000000000 65535 f \n0000000009 00000 n \n0000000052 00000 n \ntrailer<</Size 3/Root 1 0 R>>\nstartxref\n99\n%%EOF"
        is_scanned, confidence = is_pdf_scanned(raw)
        assert is_scanned is False
        assert confidence == 0.0


async def test_digital_pdf_conversion_and_plain_fallback(monkeypatch):
    from src.core import convert

    pdf = _make_digital_pdf("Art. 1 - " + "Texto legal para a administracao. " * 8)
    result = await convert.pdf_to_markdown(pdf)
    assert "Art." in result
    monkeypatch.setattr(
        convert, "_digital_pdf", lambda *a: "![page](data:image/png;base64,AAAA)"
    )
    assert "Art." in await convert.pdf_to_markdown(pdf)


async def test_scan_requires_ocr_and_failure_propagates():
    from unittest.mock import AsyncMock

    from src.core.convert import pdf_to_markdown
    from src.core.ocr import OCRError

    pdf = _make_scanned_pdf()
    with pytest.raises(ValueError, match="needs OCR"):
        await pdf_to_markdown(pdf)
    ocr = AsyncMock()
    ocr.pdf.side_effect = OCRError("failed batch")
    with pytest.raises(OCRError):
        await pdf_to_markdown(pdf, ocr)


@pytest.mark.parametrize(
    "tag",
    [
        '<img src="/image.png">',
        '<img data-src="/image.png">',
        '<img srcset="/image.png 1x, /large.png 2x">',
        '<picture><source srcset="/image.png 1x"><img></picture>',
    ],
)
async def test_html_preserves_images_including_empty_containers(tag):
    from unittest.mock import AsyncMock

    from bs4 import BeautifulSoup

    from src.core.convert import clean_norm_soup, html_to_markdown

    html = str(
        clean_norm_soup(BeautifulSoup(f'<div><a href="other">{tag}</a></div>', "lxml"))
    )
    fetch = AsyncMock(return_value=_make_png_bytes())
    result = await html_to_markdown(html, "https://example.org/doc", fetch)
    fetch.assert_awaited_once_with("https://example.org/image.png")
    assert "data:image/png;base64," in result
