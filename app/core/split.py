from app.core.declensions import get_declensions
from ibex_models import SearchTerm, Platform, CollectAction, DataSource, CollectTask
from datetime import datetime, timedelta
from typing import List, Tuple
import langid
from app.core.datasources.facebook.helper import split_to_chunks
from copy import deepcopy

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

query_length_ = dict()
query_length_[Platform.facebook] = 910
# academic account
query_length_[Platform.twitter] = 1024
# Full Archive
query_length_[Platform.twitter] = 128
# 30-Day
query_length_[Platform.twitter] = 256

query_length_[Platform.youtube] = 2000

def get_query_length(collect_action: CollectAction, data_sources: List[DataSource]) -> int:
    query_length = query_length_[collect_action.platform]

    if collect_action.platform == Platform.twitter and len(data_sources) > 0:
        query_length -= 84
    
    return query_length

def split_complex_query(keyword, operators):
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

def split_queries(search_terms: List[SearchTerm], collect_action: CollectAction, data_sources: List[DataSource]):
    terms:List[str] = [search_term.term for search_term in search_terms]

    if collect_action.platform not in [Platform.facebook, Platform.twitter, Platform.youtube]:
        return terms

    all_queries = []
    full_query = ''
    operators = boolean_operators[collect_action.platform]
    query_length_for_platform = get_query_length(collect_action, data_sources)
    

    for keyword in terms:
        if ' OR ' not in keyword and ' AND ' not in keyword and ' NOT ' not in keyword:
            try:
                decls = get_declensions([keyword], langid.classify(keyword)[0])
            except:
                decls = [keyword]
            
            single_term = f'{operators["or_"]}'.join([f'"{word}"' for word in decls])

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
        
        if len(full_query_) > query_length_for_platform:
            all_queries.append(full_query.lstrip(f' {operators["or_"]} '))
            full_query = f'({single_term})'
        else:
            full_query = full_query_

    all_queries.append(full_query.lstrip(f' {operators["or_"]} '))

    return all_queries


def split_sources(data_sources:List[DataSource], collect_action: CollectAction):
    if collect_action.platform not in [Platform.facebook, Platform.twitter, Platform.youtube]:
        return data_sources

    chunk_sizes = dict() 
    chunk_sizes[Platform.facebook] = 10
    chunk_sizes[Platform.youtube] = 1
    chunk_sizes[Platform.twitter] = 3

    return list(split_to_chunks(data_sources, chunk_sizes[collect_action.platform]))



def split_to_tasks(data_sources: List[DataSource], search_terms: List[SearchTerm], collect_action: CollectAction, date_from: datetime, date_to: datetime) -> List[CollectTask]:
    sub_queries = split_queries(search_terms, collect_action, data_sources)[:5]
    sub_data_sources = split_sources(data_sources, collect_action)
    
    collect_tasks:List[CollectTask] = []
    
    collect_task: CollectTask = CollectTask(
        platform=collect_action.platform,
        date_from=date_from,
        date_to=date_to,
        monitor_id=collect_action.monitor_id,
    )
    
    if len(sub_data_sources) > 0 and len(sub_queries) > 0:
        for sub_data_source in sub_data_sources:
            for sub_query in sub_queries:
                collect_task_ = deepcopy(collect_task)
                collect_task_.data_sources = sub_data_source
                collect_task_.query = sub_query
                collect_tasks.append(collect_task_)
    elif len(sub_data_sources) > 0:
        for sub_data_source in sub_data_sources:
            collect_task_ = deepcopy(collect_task)
            collect_task_.data_sources = sub_data_source
            collect_tasks.append(collect_task_)
    elif len(sub_queries) > 0:
         for sub_query in sub_queries:
            collect_task_ = deepcopy(collect_task)
            collect_task_.query = sub_query
            collect_tasks.append(collect_task_)

    return collect_tasks


def get_last_collection_date(collect_action: CollectAction):
    if collect_action.last_collection_date is None:
        return datetime.now() - timedelta(days=1)
    if collect_action.last_collection_date < datetime.now() - timedelta(days=5):
        return datetime.now() - timedelta(days=5)
    
    return collect_action.last_collection_date 


def get_time_intervals(collect_action: CollectAction, sample: bool=False) -> List[Tuple[datetime, datetime]]:
    date_from = get_last_collection_date(collect_action)
    date_to = datetime.now()  - timedelta(hours=5)

    intervals = []
    if sample:
        for i in range(10):
            rand_date = fake.date_between(start_date=date_from, end_date=date_to - timedelta(hours=5))
            intervals.append((rand_date, rand_date + timedelta(hours=5)))
    else:
        intervals.append((date_from, date_to))

    return intervals