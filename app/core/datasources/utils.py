from ibex_models import CollectTask, Post
from eldar import Query
from typing import List

async def update_hits_count(collect_task: CollectTask, hits_count: int):
    collect_task_ = await CollectTask.find(CollectTask.id == collect_task.id)

    collect_task_.hits_count = hits_count
    collect_task_.sample = False
    await collect_task_.save()

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

def add_search_terms_to_post(collect_task: CollectTask, post:Post) -> Post:
    print('collect_task.search_terms exists', bool(collect_task.search_terms))
    if not collect_task.search_terms: return post
    print('collect_task.search_terms len', len(collect_task.search_terms))
    if len(collect_task.search_terms) == 0: return post
    post.search_terms_ids = [search_term.id for search_term in collect_task.search_terms]
    return post
    
# async def validate_post_query(query, post: Post):
#     eldar = Query(collect_task.query)
#     text: str = f'{post.title} {post.text}' 
#     return text and len(eldar.filter([text])) > 0