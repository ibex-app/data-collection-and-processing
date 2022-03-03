from __future__ import annotations
import logging
import os
from typing import List, Dict

import requests
import pandas as pd
from datetime import datetime, timedelta

from ibex_models import DataSource, SearchTerm, Post, Scores, Platform, CollectTask
from app.config.aop_config import slf, sleep_after
from app.core.datasources.youtube.helper import SimpleUTC


@slf
class YoutubeCollector:

    def __init__(self, *args, **kwargs):
        self.token = os.getenv('YOUTUBE_TOKEN')
        self.max_requests = 20
        self.max_results_per_call = kwargs['max_results_per_call'] if 'max_results_per_call' in kwargs else 50

    def collect(self, collect_task: CollectTask):
        if collect_task.data_sources is not None and len(collect_task.data_sources) > 1:
             self.log.error('[YouTube] Can not collect data from mode than one channel per call!')

        params = dict(
            part='snippet',
            maxResults=self.max_results_per_call,
            publishedAfter=f'{collect_task.date_from.isoformat()[:19]}Z',
            publishedBefore=f'{collect_task.date_to.isoformat()[:19]}Z',
            key=self.token,
            order='relevance',
            type='video'
        )
        
        if collect_task.query is not None and len(collect_task.query) > 0:
            params['q'] = collect_task.query
        
        if collect_task.data_sources is not None and len(collect_task.data_sources) == 1:
            params['channelId'] = collect_task.data_sources[0].platform_id
        
        dfs = self._collect(params)
        posts = self._df_to_posts(dfs)

        self.log.success(f'[YouTube] {len(posts)} posts collected')
        
        return posts 


    @staticmethod
    @sleep_after(tag='YouTube')
    def _youtube_search(params):
        res = requests.get(
            "https://youtube.googleapis.com/youtube/v3/search",
            params=params)
        return res

    def _collect(self, params):
        ids = []
        for i in range(self.max_requests):
            res = self._youtube_search(params)

            res_dict = res.json()
            if 'items' not in res_dict or not len(res_dict['items']):
                self.log.error('[YouTube] no items!')
                self.log.error(res_dict)
            else:
                for i in res_dict["items"]:
                    try:
                        ids.append(i["id"]["videoId"])
                    except Exception as ex:
                        self.log.error(f'[YouTube] {i} {str(ex)}')

            if "nextPageToken" not in res_dict:
                self.log.warn(f'[YouTube] nextPageToken not present in api response, breaking loop..')
                break

            params["pageToken"] = res_dict["nextPageToken"]

        df = self._get_video_details(ids)
        if df.shape[0] == 0:
            self.log.warn('[YouTube] No data collected.')
            return df

        df["status"] = "media_needs_to_be_downloaded"
        df["platform_id"] = df["id"]
        df["url"] = df.platform_id.apply(
            lambda id: f'https://www.youtube.com/watch?v={id}')

        return df

    @staticmethod
    @sleep_after(tag='YouTube')
    def _youtube_details(params):
        res = requests.get(
            "https://www.googleapis.com/youtube/v3/videos", params=params)
        return res

    def _get_video_details(self, ids):
        results = []
        ids_chunks = [ids[i:i + self.max_results_per_call-1] for i in range(0, len(ids), self.max_results_per_call-1)]

        for ids_chunk in ids_chunks:
            params = dict(
                part='contentDetails,id,liveStreamingDetails,localizations,\
                    recordingDetails,snippet,statistics,status,topicDetails',
                id=','.join(ids_chunk),
                key=self.token,
            )

            res = requests.get(
                "https://www.googleapis.com/youtube/v3/videos", params=params)
            # res_dict = res.json()
            results += res.json()["items"]

        df = pd.DataFrame(results)

        for i, row in df.iterrows():
            for col in ['snippet', 'contentDetails',
                        'status', 'statistics', 'topicDetails']:
                if col not in row:
                    continue
                if type(row[col]) != dict:
                    continue
                for key in row[col]:
                    if type(row[col][key]) == dict:
                        continue
                    if type(row[col][key]) == list:
                        df.at[i, f'{col}_{key}'] = ','.join(row[col][key])
                        continue
                    try:
                        df.at[i, f'{col}_{key}'] = row[col][key]
                    except Exception as ex:
                        self.log.info(f"[YouTube] No more pages to collect {ex}")

        # logging.info(df.columns)
        return df

    @staticmethod
    def map_to_post(api_post: pd.Series) -> Post:
        # create scores class
        scores = None
        if 'statistics' in api_post:
            stats = api_post['statistics']
            likes = stats['likeCount'] if 'likeCount' in stats else None
            views = stats['viewCount'] if 'viewCount' in stats else None
            love = stats['favoriteCount'] if 'favoriteCount' in stats else None
            engagement = stats['commentCount'] if 'commentCount' in stats else None
            scores = Scores(likes=likes,
                            views=views,
                            love=love,
                            engagement=engagement)

        # create post class
        snip = api_post['snippet']
        try:
            post_doc = Post(title=snip['title'],
                             text=snip['description'] if 'description' in snip else '',
                             created_at=snip['publishedAt'],
                             platform=Platform.youtube,
                             platform_id=api_post['id']['videoId'],
                             scores=scores,
                             api_dump=dict(**api_post))
        except:
            post_doc = Post(title=snip['title'],
                             text=snip['description'] if 'description' in snip else '',
                             created_at=snip['publishedAt'],
                             platform=Platform.youtube,
                             platform_id='___',
                             scores=scores,
                             api_dump=dict(**api_post))   
        return post_doc

    def _df_to_posts(self, df: pd.DataFrame) -> List[Post]:
        posts = []
        for obj in df.iterrows():
            try:
                o = obj[1]
                post = self.map_to_post(o)
                posts.append(post)
            except ValueError as e:
                self.log.error(f'[YouTube] {e}')
        return posts



# async def test():
#     ibex_models.platform import Platform
#     from app.config.mongo_config import init_mongo
#     await init_mongo()
#     date_from = datetime.now() - timedelta(days=5)
#     date_to = datetime.now() - timedelta(days=1)
#     data_sources = await DataSource.find(DataSource.platform == Platform.youtube).to_list()
#     yt = YoutubeCollector()
#     res = yt.collect_curated_single(date_from=date_from,
#                                     date_to=date_to,
#                                     data_source=data_sources[0])
#     print(res)
#
#
# if __name__ == "__main__":
#     import asyncio
#     asyncio.run(test())
