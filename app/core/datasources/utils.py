from ibex_models import CollectTask, Post, SearchTerm, Monitor
from eldar import Query
from typing import List
from beanie.odm.operators.find.comparison import In
import langid
from app.core.split import split_complex_query
from app.core.declensions import get_declensions
from app.config.logging_config import log
from uuid import UUID

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
            full_term = ' OR '.join(search_terms_with_declancions)
        except:
            full_term = keyword
    else:
        single_keywords, statements = split_complex_query(keyword)
        words_decls = []
        for word in single_keywords:
            try:
                declensions = get_declensions([word], langid.classify(word)[0])
                words_decls.append(' OR '.join(declensions))
            except:
                words_decls.append(word)
        full_term = ''.join([f'{statement}({new_word})' for new_word, statement in zip(words_decls, [''] + statements)])

    # print(f'[DetectSearchTerm] full_term for {keyword} : {full_term} ')
    return Query(full_term)


def validate_posts_by_query(collect_task: CollectTask, posts: List[Post]) -> List[Post]:
    if not collect_task.search_terms: return posts
    if len(collect_task.search_terms) == 0: return posts
    eldar = Query(collect_task.query)
    posts_ = []
    for post in posts:
        text: str = f'{post.title} {post.text}'
        if not text: continue
        if len(eldar.filter([text])) == 0: continue
        posts_.append(post)
    return posts_


async def add_search_terms_to_posts(posts:List[Post], monitor_id: UUID = None) -> List[Post]:
    if monitor_id: 
        search_terms = await SearchTerm.find(In(SearchTerm.tags, [str(monitor_id)])).to_list()
    else:
        search_terms = await SearchTerm.find().to_list()

    log.info(f'[DetectSearchTerm] {len(search_terms)} total search terms in monitor')

    # TODO use subprocesses here
    for search_term in search_terms:
        eldar_query = get_query_with_declancions(search_term.term)
        
        for post in posts:
            post.search_terms_ids = post.search_terms_ids or []
            text: str = f'{post.title} {post.text}'
            if post.transcripts and len(post.transcripts):
                    text += ' '.join([transcript.text for transcript in post.transcripts])
            if not text: continue
            if len(eldar_query.filter([text])) == 0: continue
            if search_term.id not in post.search_terms_ids: post.search_terms_ids.append(search_term.id)
    
    return posts
    