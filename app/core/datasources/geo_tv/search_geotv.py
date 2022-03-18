from __future__ import annotations
from datetime import datetime, timedelta
import logging
from typing import List, Dict

import requests
import pandas as pd

from ibex_models import Account, SearchTerm, Post, Platform, CollectTask
from app.config.aop_config import sleep_after, slf


@slf
class TVGeorgiaCollector:
    headers_prog_token = {
        "accept": '*/*',
        "accept-encoding": 'gzip, deflate, br',
        "accept-language": 'en-US,en;q=0.9',
        "access-control-request-headers": 'x-myvideo-app,x-myvideo-app-model,x-myvideo-app-ota,x-myvideo-app-package,x-myvideo-app-version',
        "access-control-request-method": 'POST',
        "origin": 'https://tv.myvideo.ge',
        "referer": 'https://tv.myvideo.ge/',
        "sec-fetch-dest": 'empty',
        "sec-fetch-mode": 'cors',
        "sec-fetch-site": 'same-site',
        "user-agent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36',
    }

    headers_prog = {
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'ge',
        'Origin': 'https://tv.myvideo.ge',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Referer': 'https://tv.myvideo.ge/',
        'Pragma': 'no-cache',
        'Cache-Control': 'no-cache',
        'TE': 'Trailers',
        "user-agent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36'
    }

    def __init__(self, *args, **kwargs):
        pass

    def collect(self, collect_task: CollectTask):
        posts: List[Post] = []
        for account in collect_task.accounts:
            res = self._collect(account=account, date_from=collect_task.date_from, date_to=collect_task.date_to)
            posts.extend(res)
        return posts

    def _collect(self, account: Account, date_from: datetime, date_to: datetime):
        full_program = TVGeorgiaCollector._get_program(account.platform_id, date_from, date_to)
        res = self.map_to_posts(full_program["data"])
        for e in res:
            e.account_id = account.id
        return res

    @staticmethod
    @sleep_after(tag='TV Georgia')
    def _get_program(platform_id: str, start_date: datetime, end_date: datetime):
        res = requests.post('https://api.myvideo.ge/api/v1/auth/token',
                            data=dict(client_id=7, grant_type='client_implicit'),
                            headers=TVGeorgiaCollector.headers_prog_token)

        start_date = start_date.isoformat(sep=' ').split(' ')[0]
        end_date = end_date.isoformat(sep=' ').split(' ')[0]

        access_token = res.json()["access_token"]
        TVGeorgiaCollector.headers_prog["authorization"] = "Bearer " + access_token
        url = f'https://api.myvideo.ge/api/v1/programs?channelId={platform_id}' \
              f'&shift=enabled&thumbs=enabled&startDate={start_date}&endDate={end_date}'

        res = requests.get(url, headers=TVGeorgiaCollector.headers_prog)
        return res.json()

    @staticmethod
    def map_to_post(api_post: Dict) -> Post:
        attr = api_post['attributes']
        post_doc = Post(title=attr['name'] if 'name' in attr else "",
                             text="",
                             created_at=datetime.now(),
                             platform=Platform.geotv,
                             platform_id=attr['channelId'],
                             author_platform_id=attr['createdBy'] if 'createdBy' in attr else None,
                             api_dump=api_post)
        return post_doc

    def map_to_posts(self, posts: List[Dict]):
        res: List[Post] = []
        for post in posts:
            try:
                post = self.map_to_post(post)
                res.append(post)
            except ValueError as e:
                self.log.error(f'[TV Georgia] {e}')
        return res


# async def test():
#     ibex_models.platform import Platform
#     from app.config.mongo_config import init_mongo
#     await init_mongo()
#     date_from = datetime.now() - timedelta(days=5)
#     date_to = datetime.now() - timedelta(days=1)
#     accounts = await Account.find(Account.platform == Platform.geotv).to_list()
#     geotv = TVGeorgiaCollector()
#     res = geotv.collect_curated_single(date_from=date_from,
#                                         date_to=date_to,
#                                         account=accounts[0])
#     print(res)
#
#
# if __name__ == "__main__":
#     import asyncio
#     asyncio.run(test())
