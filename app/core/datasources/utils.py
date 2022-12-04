from ibex_models import CollectTask, Post, SearchTerm, Monitor
from eldar import Query
from typing import List
from beanie.odm.operators.find.comparison import In
import langid
from app.core.split import split_complex_query
from app.core.declensions import get_declensions
from app.config.logging_config import log
from uuid import UUID
import langid

from ibex_models.platform import Platform

async def update_hits_count(collect_task: CollectTask, hits_count: int):
    collect_task_ = await CollectTask.find(CollectTask.id == collect_task.id)

    collect_task_.hits_count = hits_count
    collect_task_.sample = False
    await collect_task_.save()


def get_query_with_declancions(keyword):
    full_term = ''
    if ' OR ' not in keyword and ' AND ' not in keyword and ' NOT ' not in keyword:
        try:
            search_terms_with_declancions = get_declensions([keyword], langid.classify(keyword)[0])
            full_term =  '"' + '" OR "'.join(search_terms_with_declancions) + '"'
        except:
            full_term = f'"{keyword}"'
    else:
        single_keywords, statements = split_complex_query(keyword)
        words_decls = []
        for word in single_keywords:
            try:
                search_terms_with_declancions = get_declensions([word], langid.classify(word)[0])
                full_term_dec =  '"' + '" OR "'.join(search_terms_with_declancions) + '"'
                words_decls.append(' OR '.join(full_term_dec))
            except:
                words_decls.append(f'"{word}"')
        full_term = ''.join([f'{statement}({new_word})' for new_word, statement in zip(words_decls, [''] + statements)])

    full_term = replace_spaces_with_and(full_term)
    # print(f'[get_query_with_declancions] full_term for {keyword} : {full_term} ')
    return Query(full_term, ignore_accent=False)

def replace_spaces_with_and(query: str) -> str:

    assabmle1 = []
    assabmle2 = []
    for i, part in enumerate(query.split('"')):
        if i % 2 != 0:
            assabmle1.append(part.replace(' ', '" AND "'))
            assabmle2.append(part.replace(' ', ''))
        else:
            assabmle1.append(part)
            assabmle2.append(part)
    assabmled1 = '"'.join(assabmle1)    
    assabmled2 = '"'.join(assabmle2)
    
    if len(assabmle1) > 0:
        query_ = f'{assabmled1} OR {assabmled2}'
    else:
        query_ = query

    # log.info(f'[ValidatePostsByQuery] query {query}')
    return query_


def validate_posts_by_query(collect_task: CollectTask, posts: List[Post]) -> List[Post]:
    if not collect_task.query: return posts
    query = collect_task.query.replace('#', '').replace('@', '')
    if collect_task.platform in [Platform.vkontakte, Platform.telegram]:
        query = f'"{query}"'
    elif collect_task.platform == Platform.twitter:
        query = query.replace(') (', ') AND (').replace(') -(', ') NOT (')
    elif collect_task.platform == Platform.youtube:
        query = query.replace('" "', '" AND "').replace('"|"', '" OR "').replace('" -"', '" NOT "')

    # log.info(f'[ValidatePostsByQuery] query {query}')

    query = replace_spaces_with_and(query)

    # log.info(f'[ValidatePostsByQuery] replace_spaces_with_and {query}')

    eldar = Query(query, ignore_case=True, ignore_accent=False)
    posts_ = []
    for post in posts:
        text: str = str(post.api_dump)
        # log.info(f'[ValidatePostsByQuery] text {text}')
        if not text: continue
        query_search_result = eldar.filter([text])
        # log.info(f'[ValidatePostsByQuery] query_search_result {query_search_result}')
        # log.info(f'[ValidatePostsByQuery] query_search_result len {len(query_search_result)}')
        if len(query_search_result) == 0: continue
        posts_.append(post)
    return posts_


async def add_search_terms_to_posts(posts:List[Post], monitor_id: UUID = None) -> List[Post]:
    if monitor_id: 
        search_terms = await SearchTerm.find(In(SearchTerm.tags, [str(monitor_id)])).to_list()
    else:
        search_terms = await SearchTerm.find().to_list()

    # log.info(f'[AddSearchRermsToPosts] {len(search_terms)} total search terms in monitor')

    # TODO use subprocesses here
    for search_term in search_terms:
        term = search_term.term.replace('#', '').replace('@', '')

        eldar_query = get_query_with_declancions(term)
        # print('[AddSearchRermsToPosts] query', eldar_query)
        
        for post in posts:
            post.search_term_ids = post.search_term_ids or []
            text: str = str(post.api_dump)
            # print('[AddSearchRermsToPosts] text', ' '.join(text.splitlines()))
            if post.transcripts and len(post.transcripts):
                text += ' '.join([transcript.text for transcript in post.transcripts])
            if not text: continue
            quary_matches = eldar_query.filter([text])

            # print('[AddSearchRermsToPosts] match count ', len(quary_matches))
            if len(quary_matches) == 0: continue
            if search_term.id not in post.search_term_ids: post.search_term_ids.append(search_term.id)
    
    return posts
    
def set_account_id(post:Post, collect_task: CollectTask) -> Post:
    # log.info('[set_account_id] post', post)
    # log.info('[set_account_id] collect_task', collect_task)
    if not collect_task.accounts or len(collect_task.accounts) == 0:
        return post

    account_match = [_.id for _ in collect_task.accounts if _.platform_id == post.author_platform_id]
    # log.info('[set_account_id] account_match', account_match)

    if len(account_match) == 0:
        log.error(f'[set_account_id] no account found for {collect_task.platform} post: {post}')
        return post

    post.account_id = account_match[0]
    # log.info('[set_account_id] post account_id', post.account_id)
    return post


def set_total_engagement(post:Post) -> Post:
    # print('[set_total_engagement]', post)
    if not post.scores:
        # print('[set_total_engagement] no scores', post.scores)

        return post
    total = sum([_ for _ in post.scores.__dict__.values() if _ is not None])
    # print('[set_total_engagement] total', total)

    post.scores.total = sum([_ for _ in post.scores.__dict__.values() if _ is not None])
    # print('[set_total_engagement] post.scores.total', post.scores.total)

    return post
