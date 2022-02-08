from datetime import datetime
from typing import Optional, List
from uuid import UUID

from beanie import Document, Indexed
from pydantic import BaseModel

from app.model.platform import Platform
from app.model.media_download_status import MediaDownloadStatus


class Labels(BaseModel):
    topics: Optional[List[str]]
    persons: Optional[List[str]]
    organizations: Optional[List[str]]


class Scores(BaseModel):
    likes: Optional[int]
    views: Optional[int]
    engagement: Optional[int]
    shares: Optional[int]
    sad: Optional[int]
    wow: Optional[int]
    love: Optional[int]
    angry: Optional[int]


class Transcript(BaseModel):
    time: datetime
    text: str


class Post(Document):
    title: str
    text: str
    created_at: datetime
    platform: Platform
    platform_id: str
    data_source_id: Optional[UUID]
    author_platform_id: Optional[str]
    hate_speech: Optional[float]
    sentiment: Optional[float]
    has_video: Optional[bool]
    api_dump: dict
    url:Optional[str]
    media_download_status: Optional[MediaDownloadStatus]
    monitor_id: Optional[UUID]

    labels: Optional[Labels]
    scores: Optional[Scores]
    transcripts: Optional[List[Transcript]]

    class Config:
        use_enum_values = True
        validate_assignment = True

    class Collection:
        name = 'posts'
