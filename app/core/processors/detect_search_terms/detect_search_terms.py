from cgitb import text
import requests
import base64
from sys import argv
from app.config.aop_config import slf
from ibex_models import ProcessTask, Post, Transcript, Monitor, SearchTerm, CollectAction
from app.config.constants import media_directory
from beanie.odm.operators.find.comparison import In
import subprocess
from typing import List 
from eldar import Query
import langid
from app.core.split import split_complex_query
from app.core.declensions import get_declensions
from app.core.datasources.utils import get_query_with_declancions, add_search_terms_to_posts

@slf
class DetectSearchTerms:

    async def process(self, task:ProcessTask):
        # self.log.info(f'[DetectSearchTerm] {task.post.id} in process task')
        
        # TODO get only latest posts 
        # {sent_at: {$exists: false}} we need soms status here
        posts:List[Post] = await Post.find(In(Post.monitor_ids, [task.monitor_id]), ).to_list()
        posts = add_search_terms_to_posts(posts)

        for post in posts:
            await post.save()

        return True
