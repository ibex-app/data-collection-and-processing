from pydantic import BaseModel
from app.model.platform import Platform
from app.model.post_class import PostClass
from uuid import UUID
from typing import Optional

class DownloadTask(BaseModel):
    post_id: UUID
    post: Optional[PostClass]
    platform: Platform
    url: str