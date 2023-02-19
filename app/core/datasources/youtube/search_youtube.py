from __future__ import annotations
import logging
import os
from typing import List, Dict

import requests
import pandas as pd
from datetime import datetime, timedelta

from ibex_models import Account, SearchTerm, Post, Scores, Platform, CollectTask, MediaStatus, monitor
from app.config.aop_config import slf, sleep_after
from app.core.datasources.youtube.helper import SimpleUTC
from app.core.datasources.utils import update_hits_count, validate_posts_by_query, add_search_terms_to_posts, set_account_id, set_total_engagement


@slf
class YoutubeCollector:

    def __init__(self, *args, **kwargs):
        self.token = os.getenv('YOUTUBE_TOKEN')
        self.max_posts_per_call = 100 #TODO Exact max videos limit
        self.max_requests = 50

        self.max_posts_per_call_sample = 50
        self.max_requests_sample = 1


    def generate_request_params(self, collect_task: CollectTask):
        self.max_requests_ = self.max_requests_sample if collect_task.sample else self.max_requests
        self.max_posts_per_call_ = self.max_posts_per_call_sample if collect_task.sample else self.max_posts_per_call

        if collect_task.accounts is not None and len(collect_task.accounts) > 1:
            self.log.error('[YouTube] Can not collect data from mode than one channel per call!')

        params = dict(
            part='snippet',
            maxResults=self.max_posts_per_call_,
            publishedAfter=f'{collect_task.date_from.isoformat()[:19]}Z',
            publishedBefore=f'{collect_task.date_to.isoformat()[:19]}Z',
            key=self.token,
            order='relevance',
            type='video'
        )

        if collect_task.query is not None and len(collect_task.query) > 0:
            params['q'] = collect_task.query
        
        if collect_task.accounts is not None and len(collect_task.accounts) == 1:
            params['channelId'] = collect_task.accounts[0].platform_id

        return params


    async def collect(self, collect_task: CollectTask):
        params = self.generate_request_params(collect_task)
        # self.log.success(f'[YouTube-params] {params}')
        posts_from_api = self._collect(params)
        posts = self.map_to_posts(posts_from_api, collect_task)
        self.log.success(f'[YouTube] {len(posts)} posts collected')
        
        valid_posts = validate_posts_by_query(collect_task, posts)
        valid_posts = await add_search_terms_to_posts(valid_posts, collect_task.monitor_id)
        self.log.success(f'[YouTube] {len(valid_posts)} valid posts collected')
    
        return valid_posts


    async def get_hits_count(self, collect_task: CollectTask) -> int:
        params = self.generate_request_params(collect_task)
        # self.log.info(f'[YouTube] Hits search_terms: ', collect_task.search_terms)
        # self.log.info(f'[YouTube] Hits accounts: ', collect_task.accounts)
        # self.log.info(f'[YouTube] Hits params - {params}')
        res = self._youtube_search(params).json()
        hits_count = res['pageInfo']['totalResults']
        self.log.info(f'[YouTube] Hits count - {hits_count}')

        return hits_count


    def _collect(self, params):
        ids = self.collect_ids(params)
        self.log.info(f'[YouTube] {len(ids)} ids collected')
        collected_posts = self._get_video_details(ids)
        
        if len(collected_posts) == 0:
            self.log.warn('[YouTube] No data collected.')

        return collected_posts


    def collect_ids(self, params) -> List[str]:
        ids = []
        for i in range(self.max_requests_):
            res = self._youtube_search(params)
            res_dict = res.json()

            if 'items' not in res_dict or not len(res_dict['items']):
                self.log.error('[YouTube] no items!')
            else:
                for i in res_dict["items"]:
                    try:
                        ids.append(i["id"]["videoId"])
                    except Exception as ex:
                        self.log.error(f'[YouTube] {i} {str(ex)}')
            
            self.log.info(f'[YouTube] Collectiong posts... {len(ids)} ids collected')

            if "nextPageToken" not in res_dict:
                self.log.warn(f'[YouTube] nextPageToken not present in api response, breaking loop..')
                break

            params["pageToken"] = res_dict["nextPageToken"]

        return ids


    def _get_video_details(self, ids):
        results = []
        ids_chunks = [ids[i:i + self.max_posts_per_call_-1] for i in range(0, len(ids), self.max_posts_per_call_-1)]

        for ids_chunk in ids_chunks:
            params = dict(
                part='contentDetails,id,liveStreamingDetails,localizations,recordingDetails,snippet,statistics,status,topicDetails',
                id=','.join(ids_chunk),
                key=self.token,
            )
            self.log.info(f'[YouTube] collecting details: {params}')
            res = self._youtube_details(params)
            try:
                results += res.json()["items"]
            except Exception as e: 
                self.log.error(f'[Youtube]: no details for request {res.json()}', e)
            
        return results


    @staticmethod
    @sleep_after(tag='YouTube', pause_time=4)
    def _youtube_search(params):
        res = requests.get("https://youtube.googleapis.com/youtube/v3/search",
            params=params)
        return res


    @staticmethod
    @sleep_after(tag='YouTube', pause_time=4)
    def _youtube_details(params):
        res = requests.get("https://www.googleapis.com/youtube/v3/videos", params=params)
        return res


    @staticmethod
    def map_to_post(api_post: Dict, collect_task: CollectTask) -> Post:
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


        post = Post(    platform_id =       api_post['id'],
                            title =             api_post['snippet']['title'],
                            text =              api_post['snippet']['description'],
                            created_at =        api_post['snippet']['publishedAt'],
                            author_platform_id =api_post['snippet']['channelId'],
                            image_url =         api_post['snippet']['thumbnails']['default']['url'],
                            url =               f'https://www.youtube.com/watch?v={api_post["id"]}',
                            platform =          Platform.youtube,
                            # monitor_ids =       [collect_task.monitor_id],
                            api_dump =          dict(**api_post),
                            scores =            scores,
                            media_status =      MediaStatus.to_be_downloaded
                        )
        post = set_account_id(post, collect_task)
        post = set_total_engagement(post)
        return post


    def map_to_posts(self, posts: List[Dict], collect_task: CollectTask):
        res: List[Post] = []
        for post in posts:
            try:
                post = self.map_to_post(post, collect_task)
                res.append(post)
            except ValueError as e:
                self.log.error(f'[{collect_task.platform}] {e}')
        return res


    
    async def get_accounts(self, query:str, env:str = None, limit: int = 5)-> List[Account]:
        self.log.info(f'[Youtube] searching for accounts with query: {query}')
        params = dict(
            part='snippet',
            maxResults=limit,
            key=self.token,
            order='relevance',
            type='channel',
            q=query
        )
        req_url = "https://www.googleapis.com/youtube/v3/search"

        res = requests.get(req_url, params)
        ids = [_['id']['channelId'] for _ in res.json()['items']]
        
        if len(ids) == 0:
            self.log.warn(f'[Youtube] no accounts for query: {query}')
            return []
        
        params = dict(
            part='snippet',
            key=self.token,
            id=ids
        )
        req_url = "https://www.googleapis.com/youtube/v3/channels"
        res = requests.get(req_url, params)
        
        accounts = self.map_to_accounts(res.json()['items'])
        self.log.info(f'[Youtube] {len(accounts)} found')
        return accounts

    def map_to_accounts(self, accounts: List) -> List[Account]:
        result: List[Account] = []
        for account in accounts:
            try:
                account = self.map_to_acc(account)
                result.append(account)
            except ValueError as e:
                print("Youtube", e)
        return result

    def map_to_acc(self, api_account) -> Account:
        # self.log.info(acc['snippet'])
        
        mapped_account = Account(
            title=api_account['snippet']['title'],
            img=api_account['snippet']['thumbnails']['default']['url'],
            url='https://youtube.com/channel/' + api_account['id'],
            platform=Platform.youtube,
            platform_id=api_account['id'],
            api_dump=api_account
        )

        return mapped_account
    
