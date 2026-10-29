"""Validated output shared by all scrapers."""

from typing import Annotated, Any

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, field_validator

NonEmptyStr = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]
type DocRef = dict[str, Any]


class Document(BaseModel):
    model_config = ConfigDict(extra="allow", validate_assignment=True)
    year: int
    title: NonEmptyStr
    type: NonEmptyStr
    situation: NonEmptyStr
    document_url: NonEmptyStr
    text_markdown: NonEmptyStr
    summary: str = ""
    raw: bytes | None = Field(default=None, exclude=True)
    raw_ext: str | None = Field(default=None, exclude=True)

    @field_validator("text_markdown")
    @classmethod
    def validate_markdown(cls, value: str) -> str:
        from src.core.convert import valid_markdown

        valid, reason = valid_markdown(value)
        if not valid:
            raise ValueError(reason)
        return value
