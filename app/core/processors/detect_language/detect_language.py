from cgitb import text
import requests
import base64
from sys import argv
from app.config.aop_config import slf
from ibex_models import ProcessTaskBatch, Post, Transcript, Monitor, SearchTerm, CollectAction
from app.config.constants import media_directory
from beanie.odm.operators.find.comparison import In
import subprocess
from typing import List 
from eldar import Query
import langid
from app.core.split import split_complex_query
from app.core.declensions import get_declensions
from app.core.datasources.utils import get_query_with_declancions, add_search_terms_to_posts
from ibex_models import Processor
from app.config.mongo_config import init_pymongo

@slf
class DetectLanguage:

    async def process(self, task:ProcessTaskBatch):
        self.log.info(f'[DetectLanguage] {task.monitor_id} in process task')

        posts_collection = init_pymongo('posts')
        posts = posts_collection.find({'monitor_id' : {'$in': [task.monitor_id]}, 'process_applied' : {'$nin': [Processor.detect_language]}})        

        for post in posts:
            post = self.set_lang(post)
            if not hasattr(post, 'process_applied'):
                post.process_applied = []
            post.process_applied.append(Processor.detect_language)
            await post.save()

        return True


    def set_lang(self, post:Post):
        post_text = post.text + ' ' + post.title
        if not post_text:
            return post
        
        detected_lang = langid.classify(post_text)[0]
        if len(detected_lang) == 0:
            return post

        post.language = detected_lang[0]

        return post