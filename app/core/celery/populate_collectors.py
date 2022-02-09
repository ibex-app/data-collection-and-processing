from __future__ import annotations
from datetime import datetime, timedelta
from pkgutil import get_data

from typing import List

from beanie.odm.operators.find.comparison import In
from celery import chain, group, signature

from app.config.logging_config import log
from app.util.model_utils import serialize_to_base64

from app.core.datasources import collector_classes
from app.core.celery.worker import celery
from app.core.dao.collect_actions_dao import get_collect_actions

from app.model import CollectAction, CollectTask, DataSource, SearchTerm

from app.core.celery.tasks.collect import collect 


async def get_collector_tasks() -> List[chain or group]:
    collect_actions: List[CollectAction] = await get_collect_actions(['activated'])
    tasks_group: List[chain or group] = await to_tasks_group(collect_actions)
    return tasks_group


async def get_data_sources(collect_action: CollectAction) -> List[DataSource]:
    if collect_action.data_source_tag is not None and '*' not in collect_action.data_source_tag:
        return await DataSource.find(DataSource.platform == collect_action.platform, In(DataSource.tags, collect_action.data_source_tag)).to_list()

    return await DataSource.find(DataSource.platform == collect_action.platform).to_list()


async def get_search_terms(collect_action: CollectAction) -> List[SearchTerm]:
    if collect_action.search_terms_tags is not None and '*' not in collect_action.search_terms_tags:
        return await DataSource.find(In(DataSource.tags, collect_action.search_terms_tags)).to_list()

    return await DataSource.find().to_list()


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
            date_from=(datetime.now() - timedelta(hours=12)),
            date_to=datetime.now(),
        )

        # is curated?
        if collect_action.curated:
            collect_tasks += await to_collector_data_curated(collect_task, collect_action)
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
    data_sources = await get_data_sources(collect_action)

    if collect_action.use_batch:
        collect_args.data_sources = data_sources
        collector_data_list.append(collect_args)
    else:
        for data_source in data_sources:
            collect_args_ = collect_args.copy()
            collect_args_.data_source = data_source
            collector_data_list.append(collect_args_)

    return collector_data_list
