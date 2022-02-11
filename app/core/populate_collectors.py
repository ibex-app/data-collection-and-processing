from __future__ import annotations
from datetime import datetime, timedelta
from pkgutil import get_data
from telnetlib import SE
from bson import json_util
import json

from typing import List

from beanie.odm.operators.find.comparison import In
from celery import chain, group, signature, xmap

from app.config.logging_config import log
from app.util.model_utils import serialize_to_base64

from app.core.datasources import collector_classes
from app.core.celery.worker import celery
from app.core.dao.collect_actions_dao import get_collect_actions

from app.model import CollectAction, CollectTask, DataSource, SearchTerm, Platform

from app.core.celery.tasks.collect import collect 
from app.core.split import split_queries, split_sources


async def get_collector_tasks() -> List[chain or group]:
    collect_actions: List[CollectAction] = await get_collect_actions(['grass'])
    tasks_group: List[chain or group] = await to_tasks_group(collect_actions)
    return tasks_group


async def get_data_sources(collect_action: CollectAction) -> List[DataSource]:
    if collect_action.data_source_tags is None or len(collect_action.data_source_tags) == 0:
        return []

    if '*' not in collect_action.data_source_tags:
        return await DataSource.find(DataSource.platform == collect_action.platform, In(DataSource.tags, collect_action.data_source_tags)).to_list()
    
    return await DataSource.find(DataSource.platform == collect_action.platform).to_list()


async def get_search_terms(collect_action: CollectAction) -> List[SearchTerm]:
    if collect_action.search_term_tags is None or len(collect_action.search_term_tags) == 0:
        return []

    if '*' not in collect_action.search_term_tags:
        return await SearchTerm.find(In(SearchTerm.tags, collect_action.search_term_tags)).to_list()
    
    return await SearchTerm.find().to_list()


async def to_tasks_group(collect_actions: List[CollectAction]) -> List[CollectTask]:
    """
        if the collection process is curated and batch collection is
        possible collect all data sources at once,
        else run collection for each data source separately
    """
    task_group: List[signature] = []
    for collect_action in collect_actions:
        
        data_source: List[DataSource] = await get_data_sources(collect_action)
        search_terms: List[SearchTerm] = await get_search_terms(collect_action)

        collect_tasks: List[CollectTask] = split_to_tasks(data_source, search_terms, collect_action)

        collect_action.last_collection_date = datetime.now() - timedelta(hours=5)
        await collect_action.save()
    

    for platform in Platform:
        collect_tasks_group = [collect_task for collect_task in collect_tasks if collect_task.platform == platform]
        
        # should celery run this task in parallel?
        # for now all platform tasks are run in chain and different platform collections run in parallel
        if False:
            task_group.append(group([collect.s(json_util.dumps(task) + '_[SEP]_' + serialize_to_base64(task)) for task in collect_tasks_group]))
        else:
            task_group.append(collect.map([json_util.dumps(task) + '_[SEP]_' + serialize_to_base64(task) for task in collect_tasks_group]))
            

    return task_group


def split_to_tasks(data_sources: List[DataSource], search_terms: List[SearchTerm], collect_action: CollectAction) -> List[CollectTask]:
    sub_queries = split_queries(search_terms, collect_action, data_sources)[:5]
    sub_data_sources = split_sources(data_sources, collect_action)
    
    collect_tasks:List[CollectTask] = []
    
    if len(sub_data_sources) > 0 and len(sub_queries) > 0:
        for sub_data_source in sub_data_sources:
            for sub_query in sub_queries:
                collect_task: CollectTask = CollectTask(
                    platform=collect_action.platform,
                    date_from=get_last_collection_date(collect_action),
                    date_to=datetime.now()  - timedelta(hours=5),
                    monitor_id=collect_action.monitor_id,
                    data_sources=sub_data_source,
                    query=sub_query
                )
                collect_tasks.append(collect_task)
    elif len(sub_data_sources) > 0:
        for sub_data_source in sub_data_sources:
            collect_task: CollectTask = CollectTask(
                platform=collect_action.platform,
                date_from=get_last_collection_date(collect_action),
                date_to=datetime.now()  - timedelta(hours=5),
                monitor_id=collect_action.monitor_id,
                data_sources=sub_data_source
            )
            collect_tasks.append(collect_task)
    elif len(sub_queries) > 0:
         for sub_query in sub_queries:
            collect_task: CollectTask = CollectTask(
                platform=collect_action.platform,
                date_from=get_last_collection_date(collect_action),
                date_to=datetime.now()  - timedelta(hours=5),
                monitor_id=collect_action.monitor_id,
                query=sub_query
            )
            collect_tasks.append(collect_task)

    return collect_tasks

def get_last_collection_date(collect_action: CollectAction):
    if collect_action.last_collection_date is None:
        return datetime.now() - timedelta(days=1)
    if collect_action.last_collection_date < datetime.now() - timedelta(days=5):
        return datetime.now() - timedelta(days=5)
    
    return collect_action.last_collection_date 