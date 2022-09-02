from __future__ import annotations
from datetime import datetime
from typing import List

from beanie.odm.operators.find.comparison import In
from celery import chain, group

from app.config.logging_config import log
from app.util.model_utils import serialize_to_base64
from app.core.dao.collect_actions_dao import get_collect_actions
from ibex_models import CollectAction, CollectTask, Account, SearchTerm, Platform, Monitor

from app.core.celery.tasks.collect import collect 
from app.core.split import get_time_intervals, split_to_tasks
from uuid import UUID

async def get_collector_tasks(monitor_id:UUID, sample: bool = False) -> List[chain or group]:
    if sample:
        await CollectTask.find(CollectTask.monitor_id == monitor_id).delete()
    
    monitor = await Monitor.find_one(Monitor.id == monitor_id)
    collect_actions: List[CollectAction] = await get_collect_actions(monitor_id)
    tasks_group: List[chain or group] = await to_tasks_group(collect_actions, monitor, sample)
    return tasks_group


async def get_accounts(collect_action: CollectAction) -> List[Account]:
    if collect_action.account_tags is None or len(collect_action.account_tags) == 0:
        return []

    if '*' not in collect_action.account_tags:
        return await Account.find(Account.platform == collect_action.platform, In(Account.tags, collect_action.account_tags)).to_list()
    
    return await Account.find(Account.platform == collect_action.platform).to_list()


async def get_search_terms(collect_action: CollectAction) -> List[SearchTerm]:
    if collect_action.search_term_tags is None or len(collect_action.search_term_tags) == 0:
        return []

    if '*' not in collect_action.search_term_tags:
        return await SearchTerm.find(In(SearchTerm.tags, collect_action.search_term_tags)).to_list()
    
    return await SearchTerm.find().to_list()


async def to_tasks_group(collect_actions: List[CollectAction], monitor: Monitor, sample:bool=False) -> List[CollectTask]:
    """
        if the collection process is curated and batch collection is
        possible collect all data sources at once,
        else run collection for each data source separately
    """
    task_group = []
    collect_tasks: List[CollectTask] = [] 

    for collect_action in collect_actions:
        
        account: List[Account] = await get_accounts(collect_action)
        search_terms: List[SearchTerm] = await get_search_terms(collect_action)

        print(f'Using {len(account)} account(s)')
        print(f'Using {len(search_terms)} search term(s)')

        # Generateing hits count task here
        if sample:
            # date_to = monitor.date_to or datetime.now()
            # TODO end data if not None
            date_to = datetime.now()
            
            # Generating hits count task for each search term
            hits_count_tasks = []
            for search_term in search_terms:
                hits_count_tasks_ = split_to_tasks(account, [search_term], collect_action, monitor.date_from, date_to, sample)
                
                for hits_count_task in hits_count_tasks_:
                    hits_count_task.get_hits_count = True
                    hits_count_task.search_terms = [search_term]

                hits_count_tasks += hits_count_tasks_

            # if len(hits_count_tasks):
            #     await CollectTask.insert_many(hits_count_tasks)

            collect_tasks += hits_count_tasks
            
            print(f'Generated {len(hits_count_tasks)} collect tasks for hits count')

        # Generating time intervals here, 
        # for actual data collection it would be from last collection date to now
        # for sample collection it would return 10 random intervals between start end end dates
        time_intervals = get_time_intervals(collect_action, monitor, 3, sample)
        # [2021-01-01    -  2021-03-01]

        # [2021-01-07    -  2021-01-07,
        # 2021-01-21    -  2021-01-21,
        # 2021-01-01    -  2021-03-01,
        # 2021-01-01    -  2021-03-01,
        # .. ]

        for (date_from, date_to) in time_intervals:
            collect_tasks += split_to_tasks(account, search_terms, collect_action, date_from, date_to, sample)
            print(f'{len(collect_tasks)} collect tasks for interval {date_from} {date_to} ')
        
                
        # TODO move this into point when data collection is finalized
        if not sample: 
            ## Update last collection time 
            collect_action.last_collection_date = time_intervals[-1][1]
            await collect_action.save()

    print(f'{len(collect_tasks)} collect tasks created...')
    if len(collect_tasks):
        print(f'saving {len(collect_tasks)} collect casts')
        print(f'from that {len(hits_count_tasks)} hits count collect casts')
        await CollectTask.insert_many(collect_tasks)

    # print(collect_tasks)
    # Create separate task groups for platforms, that groups can be executed in parallel 
    for platform in Platform:
        collect_tasks_group = [collect_task for collect_task in collect_tasks if collect_task.platform == platform]
        if len(collect_tasks_group) == 0: continue
        # TODO determin if tasks can be run in parallel
        if False:
            task_group.append(group([collect.s(serialize_to_base64(task)) for task in collect_tasks_group]))
        else:
            task_group.append(collect.map([serialize_to_base64(task) for task in collect_tasks_group]))

    return task_group


