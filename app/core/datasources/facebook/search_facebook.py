from __future__ import annotations
from datetime import datetime
import requests
import pandas as pd
import os
from uuid import UUID

from typing import List, Dict

from ibex_models import Account, SearchTerm, Post, Scores, Platform, CollectTask
from app.config.aop_config import sleep_after, slf
from app.core.datasources.facebook.helper import split_to_chunks, needs_download
from app.core.datasources.utils import update_hits_count, validate_posts_by_query, add_search_terms_to_post

@slf
class FacebookCollector:
    def __init__(self, *args, **kwargs):
        self.token = os.getenv('CROWDTANGLE_TOKEN')
        self.app_id = os.getenv('FB_APP_ID')
        self.app_secret = os.getenv('FB_APP_SECRET')

        # TODO: double check the limit per post
        self.max_posts_per_call = 100
        self.max_requests = 20

        self.max_posts_per_call_sample = 50
        self.max_requests_sample = 1
        

    @staticmethod
    @sleep_after(tag='Facebook')
    def _collect_posts_by_param(params):
        res = requests.get("https://api.crowdtangle.com/posts/search", params=params).json()
        return res

    def generate_request_params(self, collect_task: CollectTask):
        self.max_requests_ = self.max_requests_sample if collect_task.sample else self.max_requests
        self.max_posts_per_call_ = self.max_posts_per_call_sample if collect_task.sample else self.max_posts_per_call

        params = dict(
            token=self.token,
            startDate=collect_task.date_from.isoformat(),
            endDate=collect_task.date_to.isoformat(),
            count=self.max_posts_per_call_
        )
        
        if collect_task.query is not None and len(collect_task.query) > 0:
            params['searchTerm'] = collect_task.query
        if collect_task.accounts is not None and len(collect_task.accounts) > 0:
            params['accounts'] = ','.join([account.platform_id for account in collect_task.accounts])
        
        # self.log.success(f'[Facebook] requests params has been generated: {params}.')
        return params


    async def collect(self, collect_task: CollectTask) -> List[Post]:
        params  = self.generate_request_params(collect_task)

        results: List[any] = self._collect(params)
        posts = self._map_to_posts(results, collect_task)

        valid_posts = validate_posts_by_query(collect_task, posts)
        self.log.success(f'[Facebook] {len(valid_posts)} valid posts collected')
    
        return valid_posts
        

    def _collect(self, params) -> List[Dict]:
        results = []
        res = {"result": {"pagination": {"nextPage": None}}}
        offset = 0

        while 'nextPage' in res["result"]["pagination"]:
            params["offset"] = self.max_posts_per_call_ * offset
            offset += 1
            # self.log.info(f'[Facebook] params {params}')
            res = self._collect_posts_by_param(params)
            # self.log.info(f'[Facebook] res {res}')

            if "result" not in res or "posts" not in res["result"]:
                self.log.warn(f'[Facebook] result not present in api response, breaking loop..')
                break

            results += res["result"]["posts"]

            if offset >= self.max_requests_:
                self.log.success(f'[Facebook] limit of {self.max_requests}')
                break

        if not len(results):
            self.log.warn('[Facebook] No data collected')
            return results

        self.log.success(f'[Facebook] {len(results)} posts collected')

        return results

    
    async def get_hits_count(self, collect_task: CollectTask) -> int:
        params  = self.generate_request_params(collect_task)
        params['count'] = 0
        
        responce = requests.get("https://api.crowdtangle.com/posts/search", params=params).json()
        print('fb ------ get_hits_count', responce)
        hits_count = responce["result"]["hitCount"]
        self.log.info(f'[Facebook] Hits count - {hits_count}')

        return hits_count

    @staticmethod
    def map_to_post(api_post: Dict, collect_task: CollectTask) -> Post:
        # create scores class
        scores = None
        if 'statistics' in api_post and 'actual' in api_post['statistics']:
            actual_statistics = api_post['statistics']['actual']
            likes = actual_statistics['likeCount'] if 'likeCount' in actual_statistics else None
            shares = actual_statistics['shareCount'] if 'shareCount' in actual_statistics else None
            love_count = actual_statistics['loveCount'] if 'loveCount' in actual_statistics else None
            wow_count = actual_statistics['wowCount'] if 'wowCount' in actual_statistics else None
            sad_count = actual_statistics['sadCount'] if 'sadCount' in actual_statistics else None
            angry_count = actual_statistics['angryCount'] if 'angryCount' in actual_statistics else None
            engagement = actual_statistics['commentCount'] if 'commentCount' in actual_statistics else None
            scores = Scores(likes=likes,
                            shares=shares,
                            love=love_count,
                            wow=wow_count,
                            sad=sad_count,
                            angry=angry_count,
                            engagement=engagement)

        # create post class
        title = ""
        if 'title' in api_post:
            title = api_post['title']
        elif 'message' in api_post:
            title = api_post['message']

        url = api_post['postUrl'] if 'postUrl' in api_post.keys() else None
            
        post = Post(title=title,
                    text=api_post['description'] if 'description' in api_post else "",
                    created_at=api_post['date'] if 'date' in api_post else datetime.now(),
                    platform=Platform.facebook,
                    platform_id=api_post['platformId'],
                    author_platform_id=api_post['account']['id'] if 'account' in api_post else None,
                    scores=scores,
                    api_dump=api_post,
                #  monitor_id=collect_task.monitor_id,
                    url=url)
        post = add_search_terms_to_post(collect_task, post)
        return post



    def _map_to_posts(self, posts: List[Dict], collect_task: CollectTask):
        res: List[Post] = []
        for post in posts:
            try:
                post = self.map_to_post(post, collect_task)
                res.append(post)
            except ValueError as e:
                self.log.error(f'[{collect_task.platform}] {e}')
        return res


    async def get_accounts(self, query, limit: int = 5) -> List[Account]:
        self.log.info(f'[Facebook] searching for accounts with query: {query}')

        params = dict(
            client_id=self.app_id,
            client_secret=self.app_secret,
            grant_type='client_credentials'
        )

        access_token = requests.get("https://graph.facebook.com/oauth/access_token", params=params).json()['access_token']

        params = dict(
            fields='id,name,location,link',
            access_token=access_token,
            q=query
        )

        api_accounts = requests.get("https://graph.facebook.com/pages/search", params=params).json()
        
        accounts = self.map_to_accounts(api_accounts['data'])
        self.log.info(f'[Facebook] {len(accounts)} found')
        return accounts


    def map_to_accounts(self, accounts: List) -> List[Account]:
        result: List[Account] = []
        for account in accounts:
            try:
                account = self.map_to_acc(account)
                result.append(account)
            except ValueError as e:
                print('Facebook', e)
        return result

    def map_to_acc(self, acc: Account) -> Account:
        mapped_acc = Account(
            title=acc['name'],
            url=acc['link'],
            platform=Platform.facebook,
            platform_id=acc['id'],
        )
        return mapped_acc

# async def test():
#     ibex_models.platform import Platform
#     from app.config.mongo_config import init_mongo
#     await init_mongo()
#     date_from = datetime.now() - timedelta(days=5)
#     date_to = datetime.now() - timedelta(days=1)
#     accounts = await Account.find(Account.platform == Platform.facebook).to_list()
#     fb = FacebookCollector()
#     res = fb.collect_curated_batch(date_from=date_from.isoformat(),
#                                     date_to=date_to.isoformat(),
#                                     accounts=accounts)
#     print(res)
#
#
# if __name__ == "__main__":
#     import asyncio
#
#     asyncio.run(test())



# os('python3 sample.py monitor_id=128376-81723618-087186238712 sample=True')