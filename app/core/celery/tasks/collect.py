from __future__ import annotations
import asyncio
from typing import List

from app.config.logging_config import log
from app.util.model_utils import deserialize_from_base64

from app.core.datasources import collector_classes
from app.core.celery.worker import celery
from app.core.dao.collect_actions_dao import get_collect_actions
from app.core.dao.post_dao import insert_posts

from ibex_models import CollectTask, Post, CollectTaskStatus
from app.config.mongo_config import init_mongo
from time import sleep
from dotenv import load_dotenv

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

    load_dotenv(f'/home/.{collect_task.env.lower()}.env')

    asyncio.run(set_task_status(collect_task, CollectTaskStatus.is_running))
    collector_class = collector_classes[collect_task.platform]()
    
    if collect_task.get_hits_count:
        asyncio.run(collect_and_save_hits_count_in_mongo(collector_class.get_hits_count, collect_task))
    else:
        asyncio.run(collect_and_save_items_in_mongo(collector_class.collect, collect_task))


async def set_task_status(collect_task: CollectTask, collect_task_status: CollectTaskStatus):
    await init_mongo()
    collect_task_ = await CollectTask.get(collect_task.id)

    if not collect_task_: 
        raise KeyError(f'collect_task was not found for {collect_task.platform}, {collect_task.id}')

    collect_task_.status = collect_task_status
    await collect_task_.save()


async def collect_and_save_hits_count_in_mongo(collector_method, collect_task: CollectTask):
    await init_mongo()
    collect_task_ = await CollectTask.get(collect_task.id)
    
    try:
        hits_count: int = await collector_method(collect_task)
        collect_task_.hits_count = hits_count
        collect_task_.status = CollectTaskStatus.finalized
    except Exception as e: 
        print(f'Failed to collect hits count:', e, collect_task )
        log.error(f'Failed to collect hits count:', e)
        collect_task_.status = CollectTaskStatus.failed
        # collect_task_.hits_count = -2

    if not collect_task_: 
        raise KeyError(f'collect_task was not found for {collect_task.platform}, {collect_task.id}')

    
    await collect_task_.save()


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
    await init_mongo()


    collect_task_ = await CollectTask.get(collect_task.id)
    try:
        collected_posts: List[Post] = await collector_method(collect_task)
        
        for post in collected_posts:
            post.monitor_ids = [collect_task.monitor_id]

        count_inserts, count_updates, count_existed = await insert_posts(collected_posts, collect_task)
        collect_task_.status = CollectTaskStatus.finalized
        print(f'total posts: {len(collected_posts)}, new posts: {count_inserts}, existed in db: {count_updates}, existed in monitor: {count_existed}')
    except Exception as e: 
        log.error(f'Failed to collect posts:', e)
        print(f'Failed to collect posts:',e , collect_task )
        collect_task_.status = CollectTaskStatus.failed

    await collect_task_.save()
