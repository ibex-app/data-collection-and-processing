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


class YoutubeDownloader:
    def download(self, task: DownloadTask):
        pass