from typing import List

from beanie import Document
# from __future__ import annotations
from pydantic import Field
from uuid import UUID, uuid4


class SearchTerm(Document):
    id: UUID = Field(default_factory=uuid4, alias='_id')
    tags: List[str]
    term: str
    
    # class Config:  
    #     use_enum_values = True
    #     validate_assignment = True

    class Collection:
        name = "search_terms"
