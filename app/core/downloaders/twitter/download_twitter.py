from __future__ import annotations
import logging
import os
from typing import List, Dict

import pandas as pd
from app.model import DownloadTask
import logging
import os

from pytube import YouTube

video_dir = '/home/mik/Projects/mettamine/data-collection/vid/'


class YoutubeDownloader:

    def __init__(self):
        self.token = os.getenv('YOUTUBE_TOKEN')

    def download(task: DownloadTask):
        try:
            logging.info(task.url)
            video = YouTube(task.url)
            stream = video.streams.filter(progressive=True,
                                          file_extension='mp4').order_by(
                'resolution').desc().last()
            stream.download(
                filename=f'{video_dir}{task.item_id}.mp4')

            return True

        except:
            print('err')
            return False