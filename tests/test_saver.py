"""Tests for FileSaver data operations."""

import json
import tempfile
from pathlib import Path

import pytest

from src.database.saver import FileSaver


class TestFileSaver:
    """Test FileSaver save/resume, validation, raw content, MHTML capture, and error logging."""

    @pytest.mark.asyncio
    async def test_save_and_flush_round_trip(self):
        with tempfile.TemporaryDirectory() as tmp:
            saver = FileSaver(
                save_dir=Path(tmp),
                error_log_dir=str(Path(tmp) / "errors"),
                flush_interval=100,
            )
            doc = {
                "year": 2024,
                "document_url": "http://example.com/doc1",
                "title": "Test Doc",
                "text_markdown": "# Hello World",
                "type": "Lei",
                "situation": "Vigente",
            }
            result = await saver.save_document(doc)
            assert result is not None
            assert result["title"] == "Test Doc"

            await saver.flush(2024)

            # Verify data.json was written
            data_file = Path(tmp) / "2024" / "data.json"
            assert data_file.exists()
            data = json.loads(data_file.read_text())
            assert "summary" in data
            assert "documents" in data
            assert data["summary"]["count"] == 1
            assert "Lei" in data["summary"]["types_summary"]
            assert data["summary"]["types_summary"]["Lei"]["total"] == 1
            assert data["summary"]["types_summary"]["Lei"]["situations"]["Vigente"] == 1
            assert "last_updated" in data["summary"]
            assert len(data["documents"]) == 1
            assert data["documents"][0]["title"] == "Test Doc"

    @pytest.mark.asyncio
    async def test_get_scraped_keys(self):
        with tempfile.TemporaryDirectory() as tmp:
            saver = FileSaver(save_dir=Path(tmp), flush_interval=100)
            doc = {
                "year": 2024,
                "document_url": "http://example.com/doc1",
                "title": "Test Doc",
                "text_markdown": "content",
                "type": "Lei",
                "situation": "Vigente",
            }
            await saver.save_document(doc)
            await saver.flush(2024)

            keys = await saver.get_scraped_keys(2024)
            assert ("http://example.com/doc1", "Test Doc") in keys

    @pytest.mark.asyncio
    async def test_validate_data_rejects_incomplete(self):
        with tempfile.TemporaryDirectory() as tmp:
            saver = FileSaver(save_dir=Path(tmp), flush_interval=100)
            # Missing required 'year' field
            doc = {"document_url": "http://example.com", "title": "Test"}
            result = await saver.save_document(doc)
            assert result is None

    @pytest.mark.asyncio
    async def test_save_with_raw_content(self):
        with tempfile.TemporaryDirectory() as tmp:
            saver = FileSaver(save_dir=Path(tmp), flush_interval=100)
            doc = {
                "year": 2024,
                "document_url": "http://example.com/doc1.pdf",
                "title": "Test PDF",
                "text_markdown": "content",
                "type": "Lei",
                "situation": "Vigente",
            }
            result = await saver.save_document(
                doc, raw_content=b"%PDF-1.4...", content_extension=".pdf"
            )
            assert result is not None
            assert "file_path" in result

    @pytest.mark.asyncio
    async def test_save_document_prefers_mhtml_capture_url(self):
        captured_urls = []

        async def fake_capture(url: str) -> bytes:
            captured_urls.append(url)
            return (
                b"From: saved\n"
                b"Snapshot-Content-Location: https://example.com/wrapped\n"
                b"Content-Type: multipart/related; boundary=foo\n\n"
                b"--foo\n"
                b"Content-Type: text/html\n"
                b"Content-Location: https://example.com/wrapped\n\n"
                b"<html><body>wrapped</body></html>"
            )

        with tempfile.TemporaryDirectory() as tmp:
            saver = FileSaver(
                save_dir=Path(tmp),
                flush_interval=100,
                mhtml_capture_fn=fake_capture,
            )
            doc = {
                "year": 2025,
                "document_url": "http://example.com/canonical",
                "_mhtml_url": "https://example.com/wrapped",
                "title": "Test HTML",
                "text_markdown": "content",
                "type": "Lei",
                "situation": "Vigente",
            }

            result = await saver.save_document(
                doc,
                raw_content=b"<html><body>fallback</body></html>",
                content_extension=".html",
            )

            assert result is not None
            assert captured_urls == ["https://example.com/wrapped"]
            assert result["file_path"].endswith(".mhtml")

            saved_file = Path(tmp) / result["file_path"]
            saved_text = saved_file.read_text()
            assert (
                "Snapshot-Content-Location: http://example.com/canonical" in saved_text
            )
            assert "Content-Location: http://example.com/canonical" in saved_text

    @pytest.mark.asyncio
    async def test_sanitize_filename(self):
        saver = FileSaver()
        assert saver._sanitize_filename("Test / Doc (2024)") == "Test__Doc_2024"
        assert saver._sanitize_filename("") == "document"

    @pytest.mark.asyncio
    async def test_save_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            saver = FileSaver(
                save_dir=Path(tmp),
                error_log_dir=str(Path(tmp) / "errors"),
            )
            error_data = {
                "title": "Bad Doc",
                "year": "2024",
                "situation": "Vigente",
                "type": "Lei",
                "html_link": "http://example.com/bad",
            }
            await saver.save_error(error_data, error_message="Parse failed")

            # Verify error file was created
            error_files = list(Path(tmp, "errors").rglob("*.json"))
            assert len(error_files) == 1
