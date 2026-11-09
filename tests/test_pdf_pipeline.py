"""Tests for the PDF scan detection and pymupdf4llm conversion pipeline."""

import fitz
import pytest

from src.core.convert import _in_process as REAL_IN_PROCESS
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
    return b"\x89PNG\r\n\x1a\n" + b"fake-png" * 20


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


# A real Paraíba scan whose embedded OCR layer is noise.
GARBAGE_LAYER = (
    "{.lIni r.1;tC:'V tl.l C;I· ... ' :; :.,. : 11., t j' ,\"' (: I. '.' .Jo\"l t "
    'LEI N" 9 .!l1 7 ,DF. 0 (; IJI~ .I UI.IIO IlI ~ 20 J 2 AUTO IUA: DEPUTADO '
    "Certiíico. p:: r('! os dr'!vh~ o ~ fint; , (:1 1;' I.\"~:' L E I f o i I "
)
# Readable but short on function words, like decrees listing appointees.
NAME_LIST = (
    "NOME CARGO CÓDIGO AYRES ALVES DA LUZ ASSISTENTE DE GABINETE ASGRP "
    "CHRISTIANE VERENA LORENZUTTI SUPERVISOR GERAL DE GABINETE SGGRP "
)


@pytest.mark.parametrize(
    ("text", "ocr_called"),
    [(GARBAGE_LAYER * 4, True), (NAME_LIST * 4, False)],
    ids=["garbage", "name-list"],
)
async def test_garbage_text_layer_goes_to_ocr(text, ocr_called):
    from unittest.mock import AsyncMock

    from src.core.convert import pdf_to_markdown

    ocr = AsyncMock()
    ocr.pdf.return_value = "Art. 1º Texto reconhecido pelo modelo. " * 5
    result = await pdf_to_markdown(_make_digital_pdf(text), ocr)
    assert ocr.pdf.await_count == int(ocr_called)
    assert ("reconhecido" in result) is ocr_called


async def test_garbage_text_layer_is_kept_without_ocr():
    from src.core.convert import pdf_to_markdown

    assert "DEPUTADO" in await pdf_to_markdown(_make_digital_pdf(GARBAGE_LAYER * 4))


def _make_docx(paragraph: str) -> bytes:
    import io
    import zipfile

    files = {
        "[Content_Types].xml": '<?xml version="1.0"?><Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/><Override PartName="/word/document.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml"/></Types>',
        "_rels/.rels": '<?xml version="1.0"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="word/document.xml"/></Relationships>',
        "word/document.xml": f'<?xml version="1.0"?><w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main"><w:body><w:p><w:r><w:t>{paragraph}</w:t></w:r></w:p></w:body></w:document>',
    }
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        for name, content in files.items():
            archive.writestr(name, content)
    return buffer.getvalue()


@pytest.mark.parametrize(
    ("content", "ext"),
    [
        (b"%PDF-1.7 ...", ".pdf"),
        (_make_docx("x"), ".docx"),
        (b"\n<!DOCTYPE html><html><body>x</body></html>", ".html"),
        (b"<HTML><BODY>x", ".html"),
        (b"PK\x03\x04 not a word file", None),
        (b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1\x00\x00", ".doc"),
        (b"plain bytes", None),
    ],
    ids=[
        "pdf",
        "docx",
        "html-doctype",
        "html-upper",
        "zip-not-docx",
        "word97",
        "unknown",
    ],
)
def test_sniff_extension_trusts_content_over_names(content, ext):
    from src.core.convert import sniff_extension

    assert sniff_extension(content) == ext


async def test_docx_converts_through_html():
    from src.core.convert import bytes_to_markdown

    text = "Art. 1º Fica instituído o programa estadual de incentivo à leitura."
    result = await bytes_to_markdown(_make_docx(text), "", "https://x.org", None)
    assert text in result


async def test_legacy_word_doc_fails_with_clear_error():
    """RS serves some norms as Word 97 files; html-to-markdown only reports NUL bytes."""
    from src.core.convert import bytes_to_markdown

    data = b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1" + b"\x00" * 512
    with pytest.raises(ValueError, match="Legacy Word"):
        await bytes_to_markdown(data, "application/msword", "https://x.org/a.doc", None)


async def test_html_conversion_has_no_metadata_front_matter():
    from unittest.mock import AsyncMock

    from src.core.convert import html_to_markdown

    html = "<html><head><title>Portal</title></head><body><p>Art. 1º Texto.</p></body></html>"
    result = await html_to_markdown(html, "https://x.org", AsyncMock())
    assert result == "Art. 1º Texto."


async def test_pymupdf4llm_placeholder_headers_are_blanked(monkeypatch):
    from src.core import convert

    table = "|Col1|TABELA DE SUBSÍDIOS|Col3|\n|---|---|---|\n|CARGOS|A|B|\n"
    monkeypatch.setattr(convert, "_digital_pdf", lambda *a: table + "Texto. " * 30)
    pdf = _make_digital_pdf("Art. 1 - " + "Texto legal para a administracao. " * 8)
    result = await convert.pdf_to_markdown(pdf)
    assert "||TABELA DE SUBSÍDIOS||\n|---|---|---|\n|CARGOS|A|B|" in result


async def test_spacer_images_are_dropped_not_inlined():
    from unittest.mock import AsyncMock

    from src.core.images import inline_images_in_html

    spacer = b"GIF89a\x01\x00\x01\x00\x80\x00\x00\xff\xff\xff\x00\x00\x00!\xf9\x04\x01\x00\x00\x00\x00,\x00\x00\x00\x00\x01\x00\x01\x00\x00\x02\x02D\x01\x00;"
    fetch = AsyncMock(side_effect=[spacer, _make_png_bytes()])
    html = '<p><img src="/spacer.gif">Art. 1º<img src="/brasao.png"></p>'
    result = await inline_images_in_html(html, "https://x.org", fetch, max_concurrent=1)
    assert "spacer" not in result
    assert result.count("<img") == 1
    assert "data:image/png;base64," in result


def _crash_worker() -> None:
    import os

    os._exit(1)


async def test_pdf_work_runs_in_processes_and_survives_a_crashed_worker():
    from concurrent.futures.process import BrokenProcessPool

    from src.core import convert

    pdf = _make_digital_pdf("Art. 1 - " + "Texto legal para a administracao. " * 8)
    assert "Art." in await REAL_IN_PROCESS(convert._pdf_text_layer, pdf, 0.1, True)
    with pytest.raises(BrokenProcessPool):
        await REAL_IN_PROCESS(_crash_worker)
    assert "Art." in await REAL_IN_PROCESS(convert._pdf_text_layer, pdf, 0.1, True)


async def test_line_breaks_between_inline_elements_survive():
    from unittest.mock import AsyncMock

    from src.core.convert import html_to_markdown

    html = "<p><span>Art. 1º Fica criado;</span><br><span>Art. 2º Revoga-se.</span></p>"
    result = await html_to_markdown(html, "https://x.org", AsyncMock())
    assert "Fica criado;" in result and "\nArt. 2º" in result


@pytest.mark.parametrize("wrapper", ["form", "noscript", 'div style="display:none"'])
async def test_law_text_inside_forms_and_hidden_blocks_is_kept(wrapper):
    from unittest.mock import AsyncMock

    from src.core.convert import html_to_markdown

    tag = wrapper.split()[0]
    html = f"<html><body><{wrapper}><div><p>Art. 1º Texto da lei.</p></div></{tag}></body></html>"
    assert "Art. 1º Texto da lei." in await html_to_markdown(
        html, "https://x.org", AsyncMock()
    )


def test_c1_control_characters_become_their_windows_1252_meaning():
    # Goiás' API serves cp1252 punctuation decoded as Latin-1.
    from src.core.convert import clean_markdown

    assert clean_markdown("XII \x96 elaborar \x93Regimento\x94\x81") == (
        "XII – elaborar “Regimento”"
    )
