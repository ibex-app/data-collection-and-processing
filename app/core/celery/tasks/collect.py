from __future__ import annotations
import asyncio

from typing import List

from app.config.logging_config import log
from app.util.model_utils import deserialize_from_base64

from app.core.datasources import collector_classes
from app.core.celery.worker import celery
from app.core.dao.collect_actions_dao import get_collect_actions
from app.core.dao.post_dao import remove_duplicates_from_db

from app.model import CollectTask, Post


@celery.task(name='app.core.celery.tasks.collect')
def collect(task: str):
    """
    Collects the data from passed platform.
    :param task: base64 encoded CollectTask instance.
    :return:
    """

    task: CollectTask = deserialize_from_base64(task)
    if task.platform not in collector_classes.keys():
        log.info(f"No implementation for platform [{task.platform}] found! skipping..")
        return
    collector_method = get_collector_method_and_args(task)

    asyncio.run(collect_and_save_items_in_mongo(collector_method, task))


def get_collector_method_and_args(task: CollectTask):
    """
    Get the correct collector method & its corresponding args from CollectTask.
    :param task:
    :return:
    """
    collector = collector_classes[task.platform]()

    if task.curated and task.use_batch:
        collector_method = collector.collect_curated_batch
    elif task.curated and not task.use_batch:
        collector_method = collector.collect_curated_single
    else:
        collector_method = collector.collect_firehose

    return collector_method


async def collect_and_save_items_in_mongo(collector_method, task: CollectTask):
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
    collected_items: List[Post] = collector_method(task)

    # remove duplicates
    # TODO add monitor_id to post if in two or more results
    collected_items = await remove_duplicates_from_db(collected_items)

    if len(collected_items):
        await Post.insert_many(collected_items)
