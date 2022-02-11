from __future__ import annotations
from datetime import datetime
import requests
import pandas as pd
import os
from uuid import UUID

from typing import List, Dict

from app.model import DataSource, SearchTerm, Post, Scores, Platform, CollectTask
from app.config.aop_config import sleep_after, slf
from app.core.datasources.facebook.helper import split_to_chunks, needs_download


@slf
class FacebookCollector:
    def __init__(self, *args, **kwargs):
        self.token = os.getenv('CROWDTANGLE_TOKEN')
        self.max_requests = 100
        # TODO: double check the limit per post
        self.max_posts_per_call = kwargs['max_posts_per_call'] if 'max_posts_per_call' in kwargs else 10


    @staticmethod
    @sleep_after(tag='Facebook')
    def _collect_posts_by_param(params):
        res = requests.get("https://api.crowdtangle.com/posts", params=params).json()
        return res


    def collect(self, collect_task: CollectTask) -> List[Post]:
        params = dict(
            token=self.token,
            startDate=collect_task.date_from.isoformat(),
            endDate=collect_task.date_to.isoformat(),
            count=self.max_posts_per_call,
        )
        
        if collect_task.query is not None and len(collect_task.query) > 0:
            params['searchTerm'] = collect_task.query
        if collect_task.data_sources is not None and len(collect_task.data_sources) > 0:
            params['accounts'] = ','.join([data_source.platform_id for data_source in collect_task.data_sources])

        results = self._collect(params)
        posts = self._map_to_posts(results, params)
        return posts


    def _collect(self, params) -> List[Dict]:
        results = []
        # TODO accounts length needs to be ckecked here
        res = {"result": {"pagination": {"nextPage": None}}}
        offset = 0
        # print(data_sources)
        while 'nextPage' in res["result"]["pagination"]:
            params["offset"] = self.max_posts_per_call * offset
            offset += 1
            res = self._collect_posts_by_param(params)

            if "result" not in res or "posts" not in res["result"]:
                self.log.warn(f'[Facebook] result not present in api response, breaking loop..')
                break

            results += res["result"]["posts"]

            if offset >= self.max_requests:
                self.log.success(f'[Facebook] limit of {self.max_requests}')
                                #  f' requests has been reached for params: {params}.')
                break

        if not len(results):
            self.log.warn('[Facebook] No data collected')
            return results

        self.log.success(f'[Facebook] {len(results)} posts collected')

        return results


    @staticmethod
    def map_to_post(api_post: Dict, monitor_id: UUID) -> Post:
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
        post_doc = Post(title=api_post['message'] if 'message' in api_post else "",
                             text=api_post['description'] if 'description' in api_post else "",
                             created_at=api_post['date'] if 'date' in api_post else datetime.now(),
                             platform=Platform.facebook,
                             platform_id=api_post['platformId'],
                             author_platform_id=api_post['account']['id'] if 'account' in api_post else None,
                             scores=scores,
                             api_dump=api_post,
                             monitor_id=monitor_id)
        return post_doc


    def _map_to_posts(self, posts: List[Dict], monitor_id: UUID):
        res: List[Post] = []
        for post in posts:
            try:
                post = self.map_to_post(post, monitor_id)
                res.append(post)
            except ValueError as e:
                self.log.error(f'[Facebook] {e}')
        return res




# async def test():
#     from app.model.platform import Platform
#     from app.config.mongo_config import init_mongo
#     await init_mongo()
#     date_from = datetime.now() - timedelta(days=5)
#     date_to = datetime.now() - timedelta(days=1)
#     data_sources = await DataSource.find(DataSource.platform == Platform.facebook).to_list()
#     fb = FacebookCollector()
#     res = fb.collect_curated_batch(date_from=date_from.isoformat(),
#                                     date_to=date_to.isoformat(),
#                                     data_sources=data_sources)
#     print(res)
#
#
# if __name__ == "__main__":
#     import asyncio
#
#     asyncio.run(test())
