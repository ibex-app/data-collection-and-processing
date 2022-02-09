from typing import Optional, List

from app.model.collect_action import CollectAction
from beanie.odm.operators.find.comparison import In

async def get_collect_actions(tags: Optional[List[str]] = None):
    if tags is None or not len(tags):
        return await CollectAction.find().to_list()
    else:
        return await CollectAction.find(In(CollectAction.tags, tags)).to_list()
