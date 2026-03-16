from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class ScrapedDocument(BaseModel):
    """Returned by _get_doc_data in scrapers.

    Required fields (must be non-empty after stripping whitespace):
        year, title, type, situation, text_markdown, document_url

    Optional fields:
        summary  — allowed to be empty string (not every source provides one)

    Extra fields are allowed so scraper-specific keys (e.g. ``date``,
    ``norm_number``) pass through unchanged.
    """

    model_config = ConfigDict(extra="allow", arbitrary_types_allowed=True)

    year: int
    title: str = Field(min_length=1)
    type: str = Field(min_length=1)
    situation: str = Field(min_length=1)
    summary: str = ""
    text_markdown: str = Field(min_length=1)
    document_url: str = Field(min_length=1)

    # Optional raw content for PDF/MHTML saving — excluded from JSON output.
    raw_content: bytes | None = Field(default=None, exclude=True)
    content_extension: str | None = Field(default=None, exclude=True)

    @model_validator(mode="before")
    @classmethod
    def _coerce_year(cls, values: Any) -> Any:
        """Coerce ``year`` to ``int`` so string years like ``'2024'`` work."""
        if isinstance(values, dict):
            raw_year = values.get("year")
            if raw_year is not None and not isinstance(raw_year, int):
                try:
                    values = dict(values)
                    values["year"] = int(str(raw_year).strip())
                except (ValueError, TypeError):
                    pass  # let pydantic report the type error
        return values

    @field_validator(
        "title", "type", "situation", "text_markdown", "document_url", mode="before"
    )
    @classmethod
    def _strip_and_require_nonempty(cls, v: Any, info) -> Any:  # noqa: ANN001
        """Strip whitespace and reject blank values for required string fields."""
        if isinstance(v, str):
            stripped = v.strip()
            if not stripped:
                raise ValueError(
                    f"Field '{info.field_name}' must not be empty or whitespace-only"
                )
            return stripped
        return v

    # ------------------------------------------------------------------
    # Dict-like access helpers
    # These allow code and tests that treat the scraped result as a plain
    # dict (e.g. ``result["title"]``, ``result.keys()``) to keep working
    # without modification, while the object is still a validated model.
    # ------------------------------------------------------------------

    def _as_dict(self) -> dict[str, Any]:
        """Return all fields (including extras and excluded ones) as a dict."""
        data = self.model_dump()
        # Re-add excluded fields (raw_content, content_extension) so that
        # code / tests that rely on ``result["_raw_content"]`` etc. still work.
        if self.raw_content is not None:
            data["raw_content"] = self.raw_content
            data["_raw_content"] = self.raw_content
        if self.content_extension is not None:
            data["content_extension"] = self.content_extension
            data["_content_extension"] = self.content_extension
        return data

    def __getitem__(self, item: str) -> Any:
        return self._as_dict()[item]

    def __contains__(self, item: object) -> bool:
        return item in self._as_dict()

    def keys(self) -> list[str]:  # type: ignore[override]
        return list(self._as_dict().keys())

    def get(self, item: str, default: Any = None) -> Any:
        return self._as_dict().get(item, default)


class SavedDocument(ScrapedDocument):
    """Validated in FileSaver before appending to data.json.

    Extends ``ScrapedDocument`` with the ``file_path`` field that is set by
    ``FileSaver.save_document`` after writing the raw file to disk.

    ``file_path`` is ``None`` when no raw file was saved (e.g. documents whose
    content is stored only in ``text_markdown``). When it is set it must be a
    non-empty string.
    """

    file_path: str | None = None

    @field_validator("file_path", mode="before")
    @classmethod
    def _validate_file_path(cls, v: Any) -> Any:
        """Allow None; reject empty strings."""
        if v is None:
            return None
        if isinstance(v, str) and not v.strip():
            raise ValueError(
                "file_path must not be empty or whitespace-only when provided"
            )
        return v
