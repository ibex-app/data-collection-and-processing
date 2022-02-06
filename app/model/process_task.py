from pydantic import BaseModel
from app.model.processor import Processor
from uuid import UUID

class ProcessTask(BaseModel):
    post_id: UUID
    processor: Processor