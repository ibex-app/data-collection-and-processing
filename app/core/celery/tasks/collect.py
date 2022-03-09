from __future__ import annotations
import asyncio

from typing import List

from app.config.logging_config import log
from app.util.model_utils import deserialize_from_base64

from app.core.datasources import collector_classes
from app.core.celery.worker import celery
from app.core.dao.collect_actions_dao import get_collect_actions
from app.core.dao.post_dao import remove_duplicates_from_db

from ibex_models import CollectTask, Post


@celery.task(name='app.core.celery.tasks.collect')
def collect(collect_task: str):
    """
    Collects the data from passed platform.
    :param task: base64 encoded CollectTask instance.
    :return:
    """

    collect_task: CollectTask = deserialize_from_base64(collect_task)
    if collect_task.platform not in collector_classes.keys():
        log.info(f"No implementation for platform [{collect_task.platform}] found! skipping..")
        return
    collector_class = collector_classes[collect_task.platform]()
    
    asyncio.run(collect_and_save_items_in_mongo(collector_class.collect, collect_task))


async def collect_and_save_items_in_mongo(collector_method, collect_task: CollectTask):
    """
    Initialize beanie & using the collector method:
        1. collect items
        2. remove duplicates
        3. save them to db.
    :param collector_method:
    :param collector_args:
    :return:
    """
    from app.config.mongo_config import init_mongo
    await init_mongo()

    # execute collect action
    collected_items: List[Post] = await collector_method(collect_task)

    # remove duplicates
    # TODO add monitor_id to post if in two or more results
    collected_items = await remove_duplicates_from_db(collected_items)

    if len(collected_items):
        await Post.insert_many(collected_items)

    
