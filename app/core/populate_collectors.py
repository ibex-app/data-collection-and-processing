from __future__ import annotations
from datetime import datetime, timedelta
from typing import List

from beanie.odm.operators.find.comparison import In
from beanie.odm.operators.find.element import Exists
from celery import chain, group

from app.config.logging_config import log
from app.util.model_utils import serialize_to_base64
from app.core.dao.collect_actions_dao import get_collect_actions
from ibex_models import CollectAction, CollectTask, Account, SearchTerm, Platform, Monitor, CollectTaskStatus, collect_task
from app.config.mongo_config import DBConstants 

from app.core.celery.tasks.collect import collect 
from app.core.split import get_time_intervals, split_to_tasks
from uuid import UUID

async def get_collector_tasks(monitor_id:UUID, sample: bool = False) -> List[chain or group]:
    if sample:
        # await CollectTask.find(CollectTask.monitor_id == monitor_id).delete()
        pass
    
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


def generate_hits_count_tasks(collect_action: CollectAction, 
                              monitor: Monitor,
                              accounts: List[Account],
                              search_terms: List[SearchTerm],
                              date_to: datetime,
                              sample: bool=False) -> List[CollectTask]:
    hits_count_tasks: List[CollectTask] = []

    if search_terms and len(search_terms) > 0:
        for search_term in search_terms:
            hits_count_tasks_: List[CollectTask] = split_to_tasks(accounts, [search_term], collect_action, monitor.date_from, date_to, sample)

            for hits_count_task in hits_count_tasks_:
                hits_count_task.get_hits_count = True
                hits_count_task.search_terms = [search_term]
                hits_count_task.search_term_ids = [search_term.id]

            hits_count_tasks += hits_count_tasks_
    elif accounts and len(accounts):
        hits_count_tasks_: List[CollectTask] = split_to_tasks(accounts, [], collect_action, monitor.date_from, date_to, sample)
        for hits_count_task in hits_count_tasks_:
            hits_count_task.get_hits_count = True
            
        hits_count_tasks += hits_count_tasks_
    return hits_count_tasks


async def to_tasks_group(collect_actions: List[CollectAction], monitor: Monitor, sample:bool=False) -> List[CollectTask]:
    """
        if the collection process is curated and batch collection is
        possible collect all data sources at once,
        else run collection for each data source separately
    """
    task_group = []
    collect_tasks: List[CollectTask] = [] 
    if sample:
        finalized_hits_count_ids = await get_collected_hits_count_ids(monitor)
        finalized_samplings_ids = await get_sampled_ids(monitor)
        print('finalized_hits_count_ids', finalized_hits_count_ids)
        print('finalized_samplings_ids', finalized_samplings_ids)
    for collect_action in collect_actions:
        
        accounts: List[Account] = await get_accounts(collect_action)
        search_terms: List[SearchTerm] = await get_search_terms(collect_action)

        print(f'Using {len(accounts)} account(s)')
        print(f'Using {len(search_terms)} search term(s)')

        # Generateing hits count task here
        date_to: datetime = monitor.date_to or datetime.now() - timedelta(hours=5)
        if sample:
            accounts_for_hits_count, search_terms_for_hits_count = remove_collected_samples(collect_action, accounts, search_terms, finalized_hits_count_ids)
            # Generating hits count task for each search term
            print(f'Using {len(accounts_for_hits_count)} new account(s) for hits count')
            print(f'Using {len(search_terms_for_hits_count)} new search term(s) for hits count')
            hits_count_tasks = generate_hits_count_tasks(collect_action,
                                                        monitor,
                                                        accounts_for_hits_count,
                                                        search_terms_for_hits_count,
                                                        date_to,
                                                        sample)

            # if len(hits_count_tasks):
            #     await CollectTask.insert_many(hits_count_tasks)
            print(f'Generated {len(hits_count_tasks)} collect tasks for hits count')
            collect_tasks += hits_count_tasks
        # Generating time intervals here,
        # for actual data collection it would be from last collection date to now
        # for sample collection it would return 10 random intervals between start end end dates
        time_intervals = get_time_intervals(collect_action, monitor, date_to, 3, sample)
        # [2021-01-01    -  2021-03-01]

        # [2021-01-07    -  2021-01-07,
        # 2021-01-21    -  2021-01-21,
        # 2021-01-01    -  2021-03-01,
        # 2021-01-01    -  2021-03-01,
        # .. ]
        print(f'{len(time_intervals)} time intervals created...', time_intervals)

        # TODO create time intervals for actual data collection depending on posts count
        for (date_from, date_to) in time_intervals:
            if sample:
                accounts, search_terms = remove_collected_samples(collect_action, accounts, search_terms, finalized_samplings_ids)
                print(f'Using {len(accounts)} new account(s) for sampling')
                print(f'Using {len(search_terms)} new search term(s) for sampling')
            collect_tasks += split_to_tasks(accounts, search_terms, collect_action, date_from, date_to, sample)
            print(f'{len(collect_tasks)} collect tasks for interval {date_from} {date_to} ')
        
                
        # TODO move this into point when data collection is finalized
        if not sample: 
            ## Update last collection time 
            collect_action.last_collection_date = time_intervals[-1][1]
            await collect_action.save()

    print(f'{len(collect_tasks)} collect tasks created...')

    if len(collect_tasks):
        if sample:
            print(f'from that {len([_ for _ in collect_tasks if _.get_hits_count])} hits count collect tasks')
        
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

def sampling_tasks_match(task_1: CollectTask, task_2: CollectTask) -> bool:
    if not task_1 or not task_2: return False
    if (not task_1.sample and not task_2.sample) and (not task_1.get_hits_count and not task_2.get_hits_count): return False
    
    # print('comparing', task_1, task_2)
    if task_1.accounts and task_2.accounts and len(task_1.accounts) and len(task_2.accounts) \
        and task_1.accounts[0].platform == task_2.accounts[0].platform \
        and task_1.accounts[0].platform_id == task_2.accounts[0].platform_id : return True
    if task_1.search_terms and task_2.search_terms and len(task_1.search_terms) and len(task_2.search_terms) \
        and task_1.search_terms[0].term == task_2.search_terms[0].term: return True
    
    return False

async def remove_collected_hits_counts(cllect_action:CollectAction, accounts:List[Account], search_terms:List[SearchTerm], sampled_ids):
    new_accounts_hits_count = [_ for _ in accounts if _.id not in sampled_ids[cllect_action.platform]['account_ids']]
    new_search_terms_hits_count = [_ for _ in search_terms if _.id not in sampled_ids[cllect_action.platform]['search_term_ids']]

    return new_accounts_hits_count, new_search_terms_hits_count

import pymongo
from uuid import UUID
from itertools import chain

async def get_sampled_ids(monitor:Monitor):
    client = pymongo.MongoClient(DBConstants.connection_string)
    mydb = client["ibex"]
    posts_collection = mydb["posts"]
    collect_tasks_for_accounts = await CollectTask.find(CollectTask.monitor_id == monitor.id,
                                                        Exists(CollectTask.accounts, True),
                                                        CollectTask.sample == True,
                                                        CollectTask.status == CollectTaskStatus.finalized).to_list()

    sampled_ids = {}
    for platform in monitor.platforms:
        sampled_ids[platform] = {}
        collect_tasks_for_accounts_ = chain.from_iterable([_.account_ids for _ in collect_tasks_for_accounts if _.platform == platform and _.account_ids])
        sampled_ids[platform]['account_ids'] = list(set([_.id for _ in collect_tasks_for_accounts_]))

        query = {
            'monitor_ids': {'$in': [monitor.id]},
            'platform': platform
        }
        search_terms_ids = posts_collection.find(query, { "search_terms_ids": 1, "_id": 0})
        sampled_ids[platform]['search_term_ids'] = list(set(chain.from_iterable([_['search_terms_ids'] for _ in search_terms_ids])))

    return sampled_ids

async def get_collected_hits_count_ids(monitor:Monitor):
    
    finalized_hits_count_tasks = await CollectTask.find(CollectTask.get_hits_count == True,
                                                CollectTask.monitor_id == monitor.id,
                                                CollectTask.status == CollectTaskStatus.finalized).to_list()
    sampled_ids = {}
    for platform in monitor.platforms:
        sampled_ids[platform] = {}

        sampled_ids[platform]['account_ids'] = list(set(chain.from_iterable([_.account_ids for _ in finalized_hits_count_tasks if _.platform == platform  and _.account_ids])))
        sampled_ids[platform]['search_term_ids'] = list(set(chain.from_iterable([_.search_term_ids for _ in finalized_hits_count_tasks if _.platform == platform and _.search_term_ids])))

    return sampled_ids
    # search_term_ids_already_collected = [_.search_terms[0].id for _ in finalized_hits_count_tasks if _.search_terms and len(_.search_terms)]
    # account_ids_already_collected = [_.accounts[0].id for _ in finalized_hits_count_tasks if _.accounts and len(_.accounts)]
    # pass

def remove_collected_samples(collect_action: CollectAction, accounts:List[Account], search_terms:List[SearchTerm], finalized_samplings):
    accounts = [_ for _ in accounts if _.id not in finalized_samplings[collect_action.platform]['account_ids']]
    search_terms = [_ for _ in search_terms if _.id not in finalized_samplings[collect_action.platform]['search_term_ids']]

    return accounts, search_terms
