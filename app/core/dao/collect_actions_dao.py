from typing import Optional, List

from ibex_models import CollectAction
from beanie.odm.operators.find.comparison import In
import uuid

async def get_collect_actions(monitor_id: str):
    await CollectAction.find(CollectAction.monitor_id == uuid.UUID(monitor_id)).to_list()


async def get_collect_actions_by_tag(tags: Optional[List[str]] = None):
    if tags is None or not len(tags):
        return await CollectAction.find().to_list()
    else:
        return await CollectAction.find(In(CollectAction.tags, tags)).to_list()
