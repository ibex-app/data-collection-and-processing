from app.config.aop_config import slf
from ibex_models import ProcessTaskBatch, Post
import langid
from uuid import UUID
from ibex_models import Processor
from app.config.mongo_config import init_pymongo

@slf
class DetectLanguage:

    async def process(self, task:ProcessTaskBatch):
        self.log.info(f'[DetectLanguage] {task.monitor_id} in process task, type {type(task.monitor_id)}')

        posts_collection = init_pymongo('posts')
        query = {
            'monitor_ids' : {'$in': [task.monitor_id]}, 
            'process_applied' : {'$nin': [Processor.detect_language]}
        }
        self.log.info(f'[DetectLanguage] query - {query}')

        posts = posts_collection.find(query) 

        posts_ = list(posts)
        self.log.info(f'[DetectLanguage] {len(posts_)} posts to process')

        for i, post in enumerate(posts_):
            if i % 50 == 0: self.log.info(f'[DetectLanguage] {i} posts processed')
            post = self.set_lang(post)
            if 'process_applied' not in post:
                post['process_applied'] = []
            
            post['process_applied'].append(Processor.detect_language)
            posts_collection.replace_one({'_id': post['_id']}, post)
            # await post.save()

        self.log.info(f'[DetectLanguage] all posts processed')
        return True


    def set_lang(self, post:Post):
        post_text = post['text'] + ' ' + post['title']
        if not post_text:
            return post
        
        detected_lang = langid.classify(post_text)[0]
        if len(detected_lang) == 0:
            return post

        post['language'] = detected_lang[0]

        return post