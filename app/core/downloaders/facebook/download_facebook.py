from __future__ import annotations
import logging
import os
from typing import List, Dict

import requests
import pandas as pd
from datetime import datetime, timedelta

from app.model import DataSource, SearchTerm, Post, Scores, Platform, DownloadTask
from app.config.aop_config import slf, sleep_after
from app.core.datasources.youtube.helper import SimpleUTC

import logging
import os
import requests
from bs4 import BeautifulSoup as bs
from io import BytesIO
import shutil

video_dir = '/home/mik/Projects/mettamine/data-collection/vid/'


def get_fb_video(video_url, item_id):
    print(f"{video_dir}{item_id}.mp4")
    # video_url = "https://www.facebook.com/tvpirveli/videos/1450478415297070"

    headers = {
        'origin': 'https://www.getfvid.com',
        'referer': 'https://www.getfvid.com/',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'accept-encoding': 'gzip, deflate, br',
        'accept-language': 'en-US,en;q=0.9',
        'sec-ch-ua': '"Chromium";v="92", " Not A;Brand";v="99", "Google Chrome";v="92"',
        'sec-ch-ua-mobile': '?0',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'none',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36',
    }

    curSession = requests.Session()

    curSession.get('https://www.getfvid.com/downloader', headers=headers)

    headers = {
        'origin': 'https://www.getfvid.com',
        'referer': 'https://www.getfvid.com/',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'accept-encoding': 'gzip, deflate, br',
        'accept-language': 'en-US,en;q=0.9',
        'sec-ch-ua': '"Chromium";v="92", " Not A;Brand";v="99", "Google Chrome";v="92"',
        'sec-ch-ua-mobile': '?0',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'cache-control': 'max-age=0',
        'content-length': '74',
        'content-type': 'application/x-www-form-urlencoded',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36',
    }

    res = curSession.post("https://www.getfvid.com/downloader",
                          data={"url": video_url},
                          headers=headers)

    soup = bs(res.text)

    url = soup.select('.btn.btn-download')[1]["href"]

    with requests.get(url, stream=True, allow_redirects=True) as r:
        with open(f"{video_dir}{item_id}.mp4", 'wb') as f:
            shutil.copyfileobj(BytesIO(r.content), f)


class FacebookDownloader:
    def __init__(self):
        self.token = os.getenv('CROWDTANGLE_TOKEN')

    # def get_fb_video():

    def download(task: DownloadTask):
        get_fb_video('task.url', task.post_id)
        logging.info('fb collection started - more data in logs...')
        return True