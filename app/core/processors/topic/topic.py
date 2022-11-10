from cgitb import text
from sys import argv
from app.config.aop_config import slf, sleep_after
from ibex_models import ProcessTask, Post, Transcript, Tag
from typing import List 
from app.core.datasources.utils import get_query_with_declancions

@slf
class TopicProcessor:
    async def process(self, task:ProcessTask):
        # posts: List[Post] = await Post.find(In(Post.monitor_ids, [task.monitor_id]), {"$nin": {"processes", [task.processes]}} ).to_list()
        posts: List[Post] = await Post.find(In(Post.monitor_ids, [task.monitor_id]) ).to_list()
        tags: List[Tag] = await Tag.find({}).to_list()

        for post in posts:
            post_string = post.full_string()
            if not post_string:
                return
            for tag in tags: 
                search_string = f'"{tag.title}" OR {" OR ".join(tag.alias)}'
                eldar_query = get_query_with_declancions(search_string)
                if len(eldar_query.filter([text])) > 0:
                    post.tags.append(tag.id)
            if len(post.tags):
                await post.save()