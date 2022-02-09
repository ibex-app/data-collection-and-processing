from pydantic import BaseModel
from typing import Optional, List
from app.model.datasource import DataSource
from app.model.platform import Platform
from app.model.search_term import SearchTerm
from datetime import datetime
from uuid import UUID
# from __future__ import annotations


class CollectTask(BaseModel):
    # executor: str
    date_from: datetime
    date_to: datetime
    use_batch: bool
    curated: bool
    platform: Optional[Platform]
    data_sources: Optional[List[DataSource]]
    search_terms: Optional[List[SearchTerm]]
    monitor_id: UUID
