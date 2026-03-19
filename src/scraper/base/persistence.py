"""Resume logic, document saving, and error logging.

Extracted from BaseScraper; access via ``self._persister`` on any BaseScraper subclass.
"""

from __future__ import annotations


from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.scraper.base.scraper import BaseScraper


def _normalize_year(value) -> int | None:
    """Coerce a year value to ``int``, returning ``None`` on failure."""
    if value is None:
        return None
    if isinstance(value, str) and not value.strip():
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


class PersistenceManager:
    """Resume logic, document saving, and error logging.

    Uses a back-reference to the owning scraper so that ``saver``,
    ``overwrite``, ``_scraped_keys``, and ``error_count`` are read/written
    on the scraper itself — compatible with ``object.__new__()`` test
    instantiation.
    """

    def __init__(self, scraper: BaseScraper):
        self._scraper = scraper

    def is_already_scraped(self, document_url: str, title: str = "") -> bool:
        """Return True if this (url, title) pair was already scraped."""
        if self._scraper.overwrite:
            return False
        return (document_url, title) in self._scraper._scraped_keys

    async def save_doc_result(self, doc_result) -> dict | None:
        """Persist a document result via FileSaver."""
        saver = self._scraper.saver
        if not saver:
            return None

        # Handle both dict and ScrapedDocument during refactor
        from src.scraper.base.schemas import ScrapedDocument

        if isinstance(doc_result, ScrapedDocument):
            doc_data = doc_result.model_dump(
                exclude={"raw_content", "content_extension"}
            )
            raw_content = doc_result.raw_content
            content_ext = doc_result.content_extension
        else:
            doc_data = dict(doc_result)
            raw_content = doc_data.pop("_raw_content", None)
            content_ext = doc_data.pop("_content_extension", None)

        return await saver.save_document(
            doc_data=doc_data,
            raw_content=raw_content,
            content_extension=content_ext,
        )

    async def load_scraped_keys(self, year: int) -> None:
        """Load already-scraped (url, title) keys for resume logic."""
        if self._scraper.overwrite:
            saver = self._scraper.saver
            reset_years = getattr(self._scraper, "_overwrite_reset_years", None)
            if reset_years is None:
                reset_years = set()
                self._scraper._overwrite_reset_years = reset_years
            if saver and year not in reset_years:
                await saver.reset_year(year)
                reset_years.add(year)
            self._scraper._scraped_keys = set()
            return
        saver = self._scraper.saver
        if saver:
            self._scraper._scraped_keys = await saver.get_scraped_keys(year)
        else:
            self._scraper._scraped_keys = set()

    async def save_doc_error(
        self,
        *,
        title: str,
        year: str | int = "",
        situation: str = "",
        norm_type: str = "",
        html_link: str = "",
        error_message: str = "Document processing failed",
        **extra,
    ) -> None:
        """Record a document-level failure and increment error_count."""
        self._scraper.error_count += 1
        saver = self._scraper.saver
        if not saver:
            return
        error_data = {
            "title": title,
            "year": year,
            "situation": situation,
            "type": norm_type,
            "html_link": html_link,
            **extra,
        }
        await saver.save_error(error_data, error_message=error_message)
