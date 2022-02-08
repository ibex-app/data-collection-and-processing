from __future__ import annotations
from datetime import datetime, timedelta
import logging
import requests
import pandas as pd
import os

from typing import List, Dict

from app.model import DataSource, SearchTerm, Post, Scores, Platform
from app.config.aop_config import sleep_after, slf
from app.core.datasources.facebook.helper import split_to_chunks, needs_download


@slf
class FacebookCollector:
    def __init__(self, *args, **kwargs):
        self.token = os.getenv('CROWDTANGLE_TOKEN')
        self.max_requests = kwargs['max_requests'] if 'max_requests' in kwargs else 1
        self.max_posts_per_call = kwargs['max_posts_per_call'] if 'max_posts_per_call' in kwargs else 10

    def collect_curated_batch(self,
                              date_from: datetime,
                              date_to: datetime,
                              data_sources: List[DataSource]):

        logging.info('fb collection started - more data in logs...')
        data_source_chunks: List[str] = FacebookCollector.split_data_sources(
            data_sources)
        res = []
        for data_source_chunk in data_source_chunks:
            params = dict(
                token=self.token,
                startDate=date_from.isoformat(),
                endDate=date_to.isoformat(),
                count=self.max_posts_per_call,
                accounts=','.join(data_source_chunk)
            )
            res.extend(self._collect_curated_batch(params, data_sources))
        return res

    def collect_curated_single(self,
                               date_from: datetime,
                               date_to: datetime,
                               data_source: DataSource):
        params = dict(
            token=self.token,
            startDate=date_from.isoformat(),
            endDate=date_to.isoformat(),
            count=self.max_posts_per_call,
            accounts=data_source.platform_id
        )
        df = self._collect_curated_single(params, data_source)
        return df

    def collect_firehose(self,
                         date_from: datetime,
                         date_to: datetime,
                         search_terms: List[SearchTerm]):
        queries = FacebookCollector.split_to_queries(search_terms)
        res = []
        for query in queries:
            params = dict(
                token=self.token,
                startDate=date_from.isoformat(),
                endDate=date_to.isoformat(),
                count=self.max_posts_per_call,
                searchTerm=query,
            )
            res.extend(self.collect(params))

        return res

    @staticmethod
    @sleep_after(tag='Facebook')
    def _collect_posts_by_param(params):
        res = requests.get(
            "https://api.crowdtangle.com/posts", params=params).json()
        return res

    def _collect_posts(self, params) -> List[Dict]:
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
                self.log.success(f'[Facebook] limit of {self.max_requests}'
                                 f' requests has been reached for params: {params}.')
                break

        if not len(results):
            self.log.warn('No Facebook data collected')
            return results

        return results

    @staticmethod
    def split_data_sources(data_sources: List[DataSource],
                           chunk_size: int = 10):
        platform_ids: List[str] = [data_source.platform_id
                                   for data_source in data_sources]
        return list(split_to_chunks(platform_ids, chunk_size))

    @staticmethod
    def split_to_queries(search_terms: List[SearchTerm], max_length=910):
        queries = []
        query = ''
        for search_term in search_terms:
            if len(query + search_term.term) > max_length:
                queries.append(query.rstrip(','))
                query = ''
            query += f'"{search_term.term}",'
        queries.append(query.rstrip(','))
        return queries

    # # TODO finalize this abbstraction
    # @staticmethod
    # def split_and_collect(arr: List[any],
    #                       self_: any,
    #                       date_from: datetime,
    #                       date_to: datetime,
    #                       max_length: int):
    #     chunks: List[str] = self_.split_to_queries(arr, max_length)
    #
    #     dfs = []
    #     for chunk in chunks:
    #         params = self_.build_params(chunk, date_from, date_to)
    #         dfs.append(self_.collect(params))
    #
    #     return pd.concat(dfs)

    @staticmethod
    def map_to_post(api_post: Dict) -> Post:
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
                             api_dump=api_post)
        return post_doc

    def _map_to_posts(self, posts: List[Dict]):
        res: List[Post] = []
        for post in posts:
            try:
                post_class = self.map_to_post(post)
                res.append(post_class)
            except ValueError as e:
                self.log.error(f'[Facebook] {e}')
        return res

    def collect(self, params) -> List[Post]:
        results = self._collect_posts(params)
        posts = self._map_to_posts(results)
        return posts

    def _collect_curated_single(self, params: Dict, data_source: DataSource):
        res = self.collect(params)
        for e in res:
            e.data_source_id = data_source.id
        return res

    def _collect_curated_batch(self, params: Dict, data_sources: List[DataSource]):
        # collect posts
        res = self.collect(params)

        # set datasource ids
        platform_id_to_ds_id = {}
        for ds in data_sources:
            platform_id_to_ds_id[ds.platform_id] = ds.id
        for e in res:
            if e.platform_id in platform_id_to_ds_id:
                e.data_source_id = platform_id_to_ds_id[e.platform_id]

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
