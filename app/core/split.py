from app.core.declensions import get_declensions
from ibex_models import SearchTerm, Platform, CollectAction, Account, CollectTask, Monitor
from datetime import datetime, timedelta
from typing import List, Tuple
import langid
from app.core.datasources.facebook.helper import split_to_chunks
from copy import deepcopy

from random import randrange
from datetime import timedelta

import pytz
utc=pytz.UTC

def random_date_between(start, end):
    """
    This function will return a random datetime between two datetime 
    objects.
    """
    if not end.tzname():
        end = utc.localize(end)
    if not start.tzname():
        start = utc.localize(start)
    delta = end - start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = randrange(int_delta)
    return start + timedelta(seconds=random_second)
    
from faker import Faker
fake = Faker()

boolean_operators = dict()
boolean_operators[Platform.facebook] = dict(
    or_ = ' OR ',
    and_ = ' AND ',
    not_ = ' NOT ',
)
boolean_operators[Platform.twitter] = dict(
    or_ = ' OR ',
    and_ = ' ',
    not_ = ' -',
)
boolean_operators[Platform.youtube] = dict(
    or_ = '|',
    and_ = ' ',
    not_ = ' -',
)
default_boolean_operators = boolean_operators[Platform.facebook]
query_length_ = dict()
query_length_[Platform.facebook] = 908
# academic account
query_length_[Platform.twitter] = 1020
# Full Archive
query_length_[Platform.twitter] = 124
# 30-Day
query_length_[Platform.twitter] = 254

query_length_[Platform.youtube] = 1994

query_length_[Platform.telegram] = 99999

query_length_[Platform.vkontakte] = 99999

def get_query_length(collect_action: CollectAction, accounts: List[Account]) -> int:
    query_length = query_length_[collect_action.platform]

    if collect_action.platform == Platform.twitter and len(accounts) > 0:
        query_length -= 84
    
    return query_length


max_account_per_request = dict()
max_account_per_request[Platform.facebook] = 10
max_account_per_request[Platform.vkontakte] = 1
max_account_per_request[Platform.telegram] = 1
max_account_per_request[Platform.youtube] = 1
max_account_per_request[Platform.twitter] = 3

def split_accounts(accounts:List[Account], collect_action: CollectAction, sample: bool):
    global max_account_per_request
    max_account_per_request_ = 1 if sample else max_account_per_request[collect_action.platform]
    return list(split_to_chunks(accounts, max_account_per_request_))


def split_complex_query(keyword, operators = boolean_operators[Platform.facebook]):
    words = []
    statements = []
    and_splits = keyword.split(' AND ')
    for and_i, and_split in enumerate(and_splits):
        if and_i > 0: statements.append(operators["and_"])

        or_splits = and_split.split(' OR ')
        for or_i, or_split in enumerate(or_splits):
            if or_i > 0: statements.append(operators["or_"])

            not_splits = or_split.split(' NOT ')
            for not_i, not_split in enumerate(not_splits):
                if not_i > 0: statements.append(operators["not_"])
                words.append(not_split)

    return words, statements


def strip_and_append(all_queries, full_query, or_operator):
    full_query_striped = full_query.lstrip(f'{or_operator}')
    if full_query_striped != '':
        all_queries.append(full_query_striped)
    return all_queries


def split_queries(search_terms: List[SearchTerm], collect_action: CollectAction, accounts: List[Account]):
    terms:List[str] = [search_term.term for search_term in search_terms]

    all_queries = []
    full_query = ''
    operators = default_boolean_operators
    query_length_for_platform = get_query_length(collect_action, accounts)
    
    # generate queries that are less then max lenght each 
    # TODO DRY this duplicated part
    if collect_action.platform not in [Platform.facebook, Platform.twitter, Platform.youtube]:
        # for single_term in terms:
        #     if len(terms) == 1: return [terms[0]]
            
        #     full_query_ = f'{ full_query }{operators["or_"]}({single_term})'
        #     if len(full_query_) > query_length_for_platform and full_query:
        #         full_query_striped = full_query.lstrip(f'{operators["or_"]}')
        #         all_queries.append(full_query_striped)
        #         full_query = f'({single_term})'
        #     else:
        #         full_query = full_query_
        # all_queries = strip_and_append(all_queries, full_query, operators["or_"])
        
        # return all_queries
        return terms

    operators = boolean_operators[collect_action.platform]
    # print(f'terms --- {len(terms)}' )
    for keyword in terms:
        if ' OR ' not in keyword and ' AND ' not in keyword and ' NOT ' not in keyword:
            try:
                decls = get_declensions([keyword], langid.classify(keyword)[0])
            except:
                decls = [keyword]
            
            single_term = f'{operators["or_"]}'.join([f'"{word}"' for word in decls])
            # print(f'decls --- {decls}' )
            # print(f'single_term --- {single_term}' )
        else:
            words, statements = split_complex_query(keyword, operators)

            words_decls = []
            for word in words:
                try:
                    declensions = get_declensions([word], langid.classify(word)[0])
                    words_decls.append(declensions)
                except:
                    words_decls.append([word])

            # join all declancions into single query string separated by or statement
            # find longes set of declancions that fit into query length
            for i in range(17, 1, -1):
                new_words = [f'{operators["or_"]}'.join([f'"{word}"' for word in words_decl[:i]]) for words_decl in words_decls]
                single_term = ''.join([f'{statement}({new_word})' for new_word, statement in zip(new_words, [''] + statements)])
                if len(single_term) <= query_length_for_platform:
                    break 
        
        full_query_ = f'{ full_query }{operators["or_"]}({single_term})'
        if len(full_query_) > query_length_for_platform and full_query:
            full_query_striped = full_query.lstrip(f'{operators["or_"]}')
            all_queries.append(full_query_striped)
            full_query = f'({single_term})'
        else:
            full_query = full_query_

    all_queries = strip_and_append(all_queries, full_query, operators["or_"])

    return all_queries


def split_queries_youtube(search_terms, collect_action, accounts):
    terms = [search_term.term for search_term in search_terms]
    replace_operators = lambda term: '"' + term.replace(' OR ', f'"{boolean_operators[Platform.youtube]["or_"]}"').replace(' AND ', f'"{boolean_operators[Platform.youtube]["and_"]}"').replace(' NOT ', boolean_operators[Platform.youtube]["not_"]) + '"'
    
    return [replace_operators(term) for term in terms]


def split_to_tasks(accounts: List[Account], 
                   search_terms: List[SearchTerm], 
                   collect_action: CollectAction, 
                   date_from: datetime, 
                   date_to: datetime,
                   env: str, 
                   sample: bool=False) -> List[CollectTask]:

    if collect_action.platform == Platform.youtube:
        sub_queries = split_queries_youtube(search_terms, collect_action, accounts)
    else:
        sub_queries = split_queries(search_terms, collect_action, accounts)

    sub_accounts = split_accounts(accounts, collect_action, sample)
    print(f'{len(sub_queries)} sub quer(y/ies) created')
    print(f'{len(sub_accounts)} sub account(s) created')
    
    if not date_from.tzname():
        date_from = utc.localize(date_from)
    if not date_to.tzname():
        date_to = utc.localize(date_to)

    collect_tasks:List[CollectTask] = []
    
    collect_task_dict = dict(platform=collect_action.platform,
        date_from=date_from,
        date_to=date_to,
        monitor_id=collect_action.monitor_id,
        sample=sample,
        env=env
    )
    if len(sub_accounts) > 0 and len(sub_queries) > 0:
        for sub_accounts_chunk in sub_accounts:
            for sub_query in sub_queries:
                collect_task: CollectTask = CollectTask(**collect_task_dict, 
                                                        accounts = sub_accounts_chunk, 
                                                        account_ids = [_.id for _ in sub_accounts_chunk], 
                                                        query = sub_query)
                collect_tasks.append(collect_task)
    elif len(sub_accounts) > 0:
        for sub_accounts_chunk in sub_accounts:
            collect_task: CollectTask = CollectTask(**collect_task_dict, 
                                                    accounts = sub_accounts_chunk,
                                                    account_ids = [_.id for _ in sub_accounts_chunk] )
            collect_tasks.append(collect_task)
    elif len(sub_queries) > 0:
        for sub_query in sub_queries:
            collect_task: CollectTask = CollectTask(**collect_task_dict, query = sub_query)
            collect_tasks.append(collect_task)

    return collect_tasks




def get_time_intervals(collect_action: CollectAction, monitor: Monitor, date_to: datetime, number_of_intervals:int = 5, sample: bool = False) -> List[Tuple[datetime, datetime]]:
    date_from = monitor.date_from if collect_action.last_collection_date is None else collect_action.last_collection_date 
    date_from = utc.localize(date_from)
    if not date_to.tzname():
        date_to = utc.localize(date_from)
    intervals = []
    if sample:
        for i in range(number_of_intervals):
            rand_date = random_date_between(date_from, date_to)
            intervals.append((rand_date, date_to))
    else:
        intervals.append((date_from, date_to))

    return intervals