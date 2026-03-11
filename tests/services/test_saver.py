"""Tests for FileSaver data operations."""

import json
import tempfile
from pathlib import Path

import pytest

from src.database.saver import FileSaver, aggregate_types_summary


class TestFileSaver:
    """Test FileSaver save/resume, validation, raw content, MHTML capture, and error logging."""

    @pytest.mark.asyncio
    async def test_save_and_flush_round_trip(self):
        with tempfile.TemporaryDirectory() as tmp:
            saver = FileSaver(
                save_dir=Path(tmp),
                log_dir=str(Path(tmp) / "logs"),
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
    async def test_save_document_mhtml_content(self):
        """Saver stores .mhtml content directly (MHTML capture is done at scraper level)."""
        mhtml_bytes = (
            b"From: saved\n"
            b"Snapshot-Content-Location: http://example.com/canonical\n"
            b"Content-Type: multipart/related; boundary=foo\n\n"
            b"--foo\n"
            b"Content-Type: text/html\n"
            b"Content-Location: http://example.com/canonical\n\n"
            b"<html><body>captured</body></html>"
        )

        with tempfile.TemporaryDirectory() as tmp:
            saver = FileSaver(
                save_dir=Path(tmp),
                flush_interval=100,
            )
            doc = {
                "year": 2025,
                "document_url": "http://example.com/canonical",
                "title": "Test MHTML",
                "text_markdown": "content",
                "type": "Lei",
                "situation": "Vigente",
            }

            result = await saver.save_document(
                doc,
                raw_content=mhtml_bytes,
                content_extension=".mhtml",
            )

            assert result is not None
            assert result["file_path"].endswith(".mhtml")

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
                log_dir=str(Path(tmp) / "logs"),
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
            error_files = list(Path(tmp, "logs").rglob("*.json"))
            assert len(error_files) == 1


def test_aggregate_types_summary_normalizes_placeholder_values():
    summary = aggregate_types_summary(
        [
            {"type": "", "situation": ""},
            {"type": "all", "situation": "NA"},
            {"type": "Lei", "situation": "Vigente"},
        ]
    )

    assert summary["Unknown"]["total"] == 2
    assert summary["Unknown"]["situations"]["Unknown"] == 2
    assert summary["Lei"]["situations"]["Vigente"] == 1
