from __future__ import annotations
from typing import List

from beanie.odm.operators.find.comparison import In
from celery import chain, group

from app.config.logging_config import log
from app.util.model_utils import serialize_to_base64
from app.core.dao.collect_actions_dao import get_collect_actions
from from ibex_models import CollectAction, CollectTask, DataSource, SearchTerm, Platform

from app.core.celery.tasks.collect import collect 
from app.core.split import get_time_intervals, split_to_tasks

async def get_collector_tasks(monitor_id: str = ['grass'], sample: bool = False) -> List[chain or group]:
    collect_actions: List[CollectAction] = await get_collect_actions(monitor_id)

    tasks_group: List[chain or group] = await to_tasks_group(collect_actions, sample)
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


async def to_tasks_group(collect_actions: List[CollectAction], sample:bool=False) -> List[CollectTask]:
    """
        if the collection process is curated and batch collection is
        possible collect all data sources at once,
        else run collection for each data source separately
    """
    task_group = []
    collect_tasks: List[CollectTask] = [] 

    for collect_action in collect_actions:
        
        data_source: List[DataSource] = await get_data_sources(collect_action)
        search_terms: List[SearchTerm] = await get_search_terms(collect_action)

        #Generating time intervals here, 
        # for actual data collection it would be from last collection date to now
        # for sample collection it would return 10 random intervals between start end end dates
        time_intervals = get_time_intervals(collect_action, sample)
        
        # [2021-01-01    -  2021-03-01]

        # [2021-01-07    -  2021-01-07,
        # 2021-01-21    -  2021-01-21,
        # 2021-01-01    -  2021-03-01,
        # 2021-01-01    -  2021-03-01,
        # .. ]

        for (date_from, date_to) in time_intervals:
            collect_tasks += split_to_tasks(data_source, search_terms, collect_action, date_from, date_to)
        
        ## Update last collection time 
        collect_action.last_collection_date = time_intervals[-1][1]
        await collect_action.save()
    
    for platform in Platform:
        collect_tasks_group = [collect_task for collect_task in collect_tasks if collect_task.platform == platform]
        if len(collect_tasks_group) == 0: continue

        if False: 
            task_group.append(group([collect.s(serialize_to_base64(task)) for task in collect_tasks_group]))
        else:
            task_group.append(collect.map([serialize_to_base64(task) for task in collect_tasks_group]))

    return task_group


