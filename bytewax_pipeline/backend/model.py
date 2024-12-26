from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field, field_validator
from uuid import uuid4

# Constants for formatting
CURRENT_TIMESTAMP = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")


class HackerNewsModel(BaseModel):
    by: Optional[str] = None
    id: int
    parent: Optional[int] = None
    text: Optional[str] = None
    time: Optional[int] = None
    type: str
    title: Optional[str] = None
    url: Optional[str] = None
    score: Optional[int] = None
    descendants: Optional[int] = None

    def to_common(self) -> "CommonDocument":
        """
        Convert HackerNewsModel to CommonDocument with the correct field mapping.
        """
        return CommonDocument(
            article_id=str(self.id),
            title=self.title or "N/A",
            url=self.url or "N/A",
            published_at=datetime.utcfromtimestamp(self.time).strftime("%Y-%m-%d %H:%M:%S") if self.time else CURRENT_TIMESTAMP,
            source_name="HackerNews",
            content=self.text or "N/A",
            author=self.by or None,
        )


class CommonDocument(BaseModel):
    """
    A generalized document structure that can represent articles
    from various news sources in a uniform manner.
    """
    article_id: str = Field(default_factory=lambda: str(uuid4()))
    title: str = "N/A"
    url: Optional[str] = "N/A"
    published_at: str = CURRENT_TIMESTAMP
    source_name: str = "Unknown"
    content: Optional[str] = None
    author: Optional[str] = None

    @field_validator("title", "content")
    def clean_text_fields(cls, v):
        return v or "N/A"

    @field_validator("url")
    def clean_url_fields(cls, v):
        return v or "N/A"

    @field_validator("published_at")
    def clean_date_field(cls, v):
        return v or CURRENT_TIMESTAMP
