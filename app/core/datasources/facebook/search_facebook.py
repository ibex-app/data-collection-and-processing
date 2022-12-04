from __future__ import annotations
from datetime import datetime, timedelta
import requests
import pandas as pd
import os
from uuid import UUID
from statistics import mean
from typing import List, Dict
from math import ceil

from ibex_models import Account, SearchTerm, Post, Scores, Platform, CollectTask, Processor
from app.config.aop_config import sleep_after, slf
from app.core.datasources.facebook.helper import split_to_chunks, needs_download
from app.core.datasources.utils import update_hits_count, validate_posts_by_query, add_search_terms_to_posts, set_account_id, set_total_engagement
from app.core.split import random_date_between

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
        if 'accounts' in params and 'searchTerm' not in params:
            url = "https://api.crowdtangle.com/posts"
        else:
            url = "https://api.crowdtangle.com/posts/search"

        res = requests.get(url, params=params).json()
        return res

    def generate_request_params(self, collect_task: CollectTask):
        self.max_requests_ = self.max_requests_sample if collect_task.sample else self.max_requests
        self.max_posts_per_call_ = self.max_posts_per_call_sample if collect_task.sample else self.max_posts_per_call

        params = dict(
            token=self.token,
            startDate=collect_task.date_from.isoformat(),
            endDate=collect_task.date_to.isoformat(),
            count=self.max_posts_per_call_,
            sortBy='overperforming' if collect_task.sample else 'date',
        )
        
        if collect_task.query and len(collect_task.query) > 0:
            params['searchTerm'] = collect_task.query
        if collect_task.accounts and len(collect_task.accounts) > 0:
            params['accounts'] = ','.join([account.platform_id for account in collect_task.accounts])
        else:
            params['platforms']='facebook'
        self.log.success(f'[Facebook] requests params has been generated: {params}.')
        return params


    async def collect(self, collect_task: CollectTask) -> List[Post]:
        params  = self.generate_request_params(collect_task)

        results: List[any] = self._collect(params)
        posts = self._map_to_posts(results, collect_task)

        valid_posts = validate_posts_by_query(collect_task, posts)
        valid_posts = await add_search_terms_to_posts(valid_posts, collect_task.monitor_id)
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

    # @sleep_after(tag='Facebook')
    async def get_hits_count(self, collect_task: CollectTask) -> int:
        if collect_task.accounts and len(collect_task.accounts):
            hits_count = await self.get_hits_count_for_account(collect_task)
        else:
            hits_count = self.get_hits_count_with_searchterm(collect_task)
        self.log.info(f'[Facebook] Hits count - {hits_count}')
        return hits_count


    async def get_hits_count_for_account(self, collect_task: CollectTask) -> int:
        posting_rates = []
        for _ in range(3):
            collect_task_ = collect_task.copy()
            rand_date = random_date_between(collect_task.date_from, collect_task.date_to)
            collect_task_.date_from = rand_date
            params  = self.generate_request_params(collect_task_)
            params['count'] = self.max_posts_per_call
            params['endDate'] = None
            self.log.info('[Facebook] Hits count params', params)
            responce = self._collect_posts_by_param(params)
            if not len(responce["result"]["posts"]):
                posting_rates.append(0)
                continue

            time_delta = datetime.fromisoformat(responce["result"]["posts"][0]['date']) - datetime.fromisoformat(responce["result"]["posts"][-1]['date'])
            posting_rates.append(0 if not time_delta.seconds else len(responce["result"]["posts"])/time_delta.seconds)
            self.log.info(f'[Facebook] Hits count dates -  {time_delta} {len(responce["result"]["posts"])}')
            


        return ceil(mean(posting_rates) * (collect_task.date_to - collect_task.date_from).seconds)
        

    def get_hits_count_with_searchterm(self, collect_task: CollectTask) -> int:
        params  = self.generate_request_params(collect_task)
        params['count'] = 0
        self.log.info('[Facebook] Hits count params', params)
        responce = self._collect_posts_by_param(params)
        self.log.info('[Facebook] Hits count responce', responce)
        if responce["status"] != 200:
            hits_count = -2
        elif 'hitCount' not in responce["result"]:
            hits_count = 0
        else:
            hits_count = responce["result"]["hitCount"]
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
                    created_at=datetime.fromisoformat(api_post['date']) if 'date' in api_post else datetime.now(),
                    platform=Platform.facebook,
                    platform_id=api_post['platformId'] if '_' not in api_post['platformId'] else api_post['platformId'].split('_')[1],
                    author_platform_id=api_post['account']['platformId'],
                    scores=scores,
                    api_dump=api_post,
                    url=url)
        
        if 'languageCode' in api_post and api_post['languageCode'] != 'und': 
            post.language = api_post['languageCode']
            post.process_applied = [Processor.detect_language]

        post = set_account_id(post, collect_task)
        post = set_total_engagement(post)
        # if post.scores.total > 0:
        #     print('[set_total_engagement] post.scores.total 111111', post.scores.total)
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


    async def get_accounts(self, query, env:str = None, limit: int = 5) -> List[Account]:
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

    def map_to_acc(self, acc) -> Account:
        mapped_acc = Account(
            title=acc['name'],
            url=acc['link'],
            platform=Platform.facebook,
            platform_id=acc['id'],
        )
        return mapped_acc
