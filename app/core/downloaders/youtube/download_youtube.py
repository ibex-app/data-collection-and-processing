from __future__ import annotations
import logging
import os
from typing import List, Dict

import requests
import pandas as pd
from datetime import datetime, timedelta
from pytube import YouTube

from ibex_models import DataSource, SearchTerm, Post, Scores, Platform, DownloadTask
from app.config.aop_config import slf, sleep_after
from app.core.datasources.youtube.helper import SimpleUTC
import os
from app.config.constants import media_directory

@slf
class YoutubeDownloader:
    def __init__(self, *args, **kwargs):

        self.token = os.getenv('YOUTUBE_TOKEN')

    def download(self, task: DownloadTask):
        try:
            self.log.info(f'[YouTube] Downloading media for {task.url}')
            video = YouTube(task.url)
            stream = video.streams.filter(progressive=True, file_extension='mp4').order_by('resolution').desc().last()
            stream.download(filename=f'{media_directory}{task.post_id}.mp4')
        except:
            self.log.error(f'[YouTube] Faild to download media for {task.url}')
