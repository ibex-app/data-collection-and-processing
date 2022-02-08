from pydantic import BaseModel
from app.model.platform import Platform
from app.model.post import Post
from uuid import UUID
from typing import Optional

class DownloadTask(BaseModel):
    post_id: UUID
    post: Optional[Post]
    platform: Platform
    url: str