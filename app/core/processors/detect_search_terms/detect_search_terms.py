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

@slf
class DetectSearchTerms:
    
    @staticmethod
    def get_query_with_declancions(keyword):
        if ' OR ' not in keyword and ' AND ' not in keyword and ' NOT ' not in keyword:
            try:
                search_terms_with_declancions = get_declensions([keyword], langid.classify(keyword)[0])
                eldar = Query(' OR '.join(search_terms_with_declancions))
            except:
                eldar = Query(keyword)
        else:
            single_keywords, statements = split_complex_query(keyword)
            words_decls = []
            for word in single_keywords:
                try:
                    declensions = get_declensions([word], langid.classify(word)[0])
                    words_decls.append(' OR '.join(declensions))
                except:
                    words_decls.append(declensions)
            full_term = ''.join([f'{statement}({new_word})' for new_word, statement in zip(words_decls, [''] + statements)])
            eldar = Query(full_term)
        return eldar


    async def process(self, task:ProcessTask):
        self.log.info(f'[DetectSearchTerm] {task.post.id} in process task')
        
        posts:List[Post] = await Post.find(In(Post.monitor_ids, [task.monitor_id])).to_list()
        monitor: Monitor = await Monitor.find_one(Monitor.id == task.monitor_id)
        
        # TODO test search terms for all / or singe monitor
        search_terms = await SearchTerm.find_all().to_list()
        search_terms_ = await SearchTerm.find(In(SearchTerm.tag, monitor.id)).to_list()

        self.log.info(f'[DetectSearchTerm] {len(search_terms)} total search terms')
        self.log.info(f'[DetectSearchTerm] {len(search_terms_)} total search terms in monitor')

        # TODO use subprocesses here
        for search_term in search_terms:
            eldar_query = self.get_query_with_declancions(search_term)
            
            for post in posts:
                text: str = f'{post.title} {post.text}'
                if post.transcripts and len(post.transcripts):
                     text += ' '.join([transcript.text for transcript in post.transcripts])
                if not text: continue
                if len(eldar_query.filter([text])) == 0: continue
                post.search_terms_ids += search_term.id
        
        await Post.save_many(posts)
        return True
