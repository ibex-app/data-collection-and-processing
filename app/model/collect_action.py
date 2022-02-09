# from __future__ import annotations
from typing import Optional, List
from beanie import Document
from app.model.platform import Platform
from pydantic import Field
from uuid import UUID, uuid4
from datetime import datetime


class CollectAction(Document):
    """
    Class to represent a CollectAction, single collect action might refer to 
        multiple collect actions, depending on curated and use_batch parameter,


    id: UUID - unique id
    curated: bool - if True 
    platform: Platform
    use_batch: bool - if True
    tags: List[str] = [] - the tags for collect actions
    parallel: bool if paralel is True, the tasks would be executed in paralel, otherwise in chain
    search_terms_tags: List[str] = [] tags to select search terms, if no search terms are passed, all the data from passed data soruces would be collected
    data_source_tag: List[str] = [] tags to select data sources, if no data sources are passed, all the data from for passed search terms would be collected
    last_collection_date: Optional[datetime]

    """

    id: UUID = Field(default_factory=uuid4, alias='_id')
    curated: bool
    platform: Platform
    use_batch: bool
    tags: List[str] = []
    parallel: bool
    search_terms_tags: List[str] = []
    data_source_tag: List[str] = []
    last_collection_date: Optional[datetime]
    monitor_id: UUID
    
    class Config:  
        use_enum_values = True
        validate_assignment = True

    class Collection:
        name = "collect_actions"
