from typing import Optional, List
from beanie import Document
from app.model.platform import Platform
# from __future__ import annotations
from pydantic import Field
from uuid import UUID, uuid4
from datetime import datetime


class DataSource(Document):  # This is the model
    """
        These are pages/accounts that have already been identified as important. Data collection either proceeds via sources or keywords
        [indexed by name, platform]
        
        id - uuid for data source
        platform_id - the identifier provided by social media platform, page id for facebook, channel id for youtube and profile id for twitter
        title - the title of the data source, page/Chanel/profile name
        tags - tags for the data source, might be merged with already defined categories tag
        platform - facebook | twitter | youtube ...
        url - url for the data source
        img_url - profile image for data source
        
    """
    id: Optional[UUID] = Field(default_factory=uuid4, alias='_id')
    title: str
    platform: Platform
    platform_id: str
    program_title: Optional[str]
    url: str
    img: Optional[str]
    tags: List[str] = []
    broadcasting_start_time: Optional[datetime]
    broadcasting_end_time: Optional[datetime]

    class Config:  
        use_enum_values = True
        validate_assignment = True

    # @validator('name')
    # def set_name(cls, name):
    #     return name or 'foo'

    class Collection:
        name = "data_sources"
