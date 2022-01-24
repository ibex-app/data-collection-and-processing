from __future__ import annotations
import asyncio
from datetime import datetime, timedelta

from typing import List

from beanie.odm.operators.find.comparison import In
from celery import chain, group, signature

from app.config.logging_config import log
from app.util.model_utils import serialize_to_base64, deserialize_from_base64

from app.core.datasources import collector_classes
from app.core.celery.worker import celery
from app.core.dao.collect_actions_dao import get_collect_actions
from app.core.dao.post_class_dao import remove_duplicates_from_db

from app.model import CollectAction, CollectTask, DataSource, PostClass, SearchTerm


async def get_collector_tasks() -> List[chain or group]:
    collect_actions: List[CollectAction] = await get_collect_actions()
    tasks_group: List[chain or group] = await to_tasks_group(collect_actions)
    return tasks_group


async def to_tasks_group(collect_actions: List[CollectAction]) -> List[CollectTask]:
    """
        if the collection process is curated and batch collection is
        possible collect all data sources at once,
        else run collection for each data source separately
    """
    task_group: List[signature] = []
    for collect_action in collect_actions:
        # create collect_task from collect_action
        collect_tasks: List[CollectTask] = []
        collect_task: CollectTask = CollectTask(
            platform=collect_action.platform,
            use_batch=collect_action.use_batch,
            curated=collect_action.curated,
            date_from=(datetime.now() - timedelta(hours=18)),
            date_to=datetime.now(),
        )

        # is curated?
        if collect_action.curated:
            collect_tasks += await to_collector_data_curated(
                collect_task, collect_action)
        else:
            collect_task.search_terms = await SearchTerm \
                .find(In(SearchTerm.tags, collect_action.search_terms_tags)) \
                .to_list()
            collect_task.platform = collect_action.platform
            collect_tasks.append(collect_task)

        # should celery run this task in parallel?
        if collect_action.parallel:
            task_group.append(group([collect.s(serialize_to_base64(task)) for task in collect_tasks]))
        else:
            task_group.append(collect.map([serialize_to_base64(task) for task in collect_tasks]))

    return task_group


async def to_collector_data_curated(collect_args: CollectTask, collect_action: CollectAction):
    collector_data_list = []
    data_sources = await DataSource.find_many(DataSource.platform == collect_action.platform).to_list()

    if collect_action.use_batch:
        collect_args.data_sources = data_sources
        collector_data_list.append(collect_args)
    else:
        for data_source in data_sources:
            collect_args_ = collect_args.copy()
            collect_args_.data_source = data_source
            collector_data_list.append(collect_args_)

    return collector_data_list


@celery.task(name='app.core.celery.tasks.collect.collect')
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
    collector_method, collector_args = get_collector_method_and_args(task)
    asyncio.run(collect_and_save_items_in_mongo(collector_method, collector_args))


def get_collector_method_and_args(task: CollectTask):
    """
    Get the correct collector method & its corresponding args from CollectTask.
    :param task:
    :return:
    """
    collector = collector_classes[task.platform]()

    collector_args = dict(
        date_from=task.date_from,
        date_to=task.date_to,
    )
    if task.curated and task.use_batch:
        collector_method = collector.collect_curated_batch
        collector_args["data_sources"] = task.data_sources
    elif task.curated and not task.use_batch:
        collector_method = collector.collect_curated_single
        collector_args["data_source"] = task.data_source
    else:
        collector_method = collector.collect_firehose
        collector_args["search_terms"] = task.search_terms

    return collector_method, collector_args


async def collect_and_save_items_in_mongo(collector_method, collector_args):
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
    collected_items: List[PostClass] = collector_method(**collector_args)

    # remove duplicates
    collected_items = await remove_duplicates_from_db(collected_items)

    if len(collected_items):
        await PostClass.insert_many(collected_items)
