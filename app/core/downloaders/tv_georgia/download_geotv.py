from __future__ import annotations
import logging
import os
from typing import List, Dict

import requests
import pandas as pd
from datetime import datetime, timedelta

from app.model import DataSource, SearchTerm, PostClass, Scores, Platform, DownloadTask
from app.config.aop_config import slf, sleep_after
from app.core.datasources.youtube.helper import SimpleUTC





from datetime import datetime
import logging
import pandas as pd
from time import sleep
import os
from pathlib import Path
import json


class TVGeorgiaDownloader:

    def __init__(self):
        pass

    def download(task: DownloadTask):
        channel_name = 'postv'

        vid_id = 'postv_15'
        start_time = datetime(2021, 10, 21, 15, 0)
        end_time = datetime(2021, 10, 21, 15, 1)
        videos_path = './vid'

        cookie_file_path_str = f"{videos_path}/cookie_{vid_id}.txt"
        cookie_file = Path(cookie_file_path_str)

        print(1)

        os.system(f"""curl -b {cookie_file_path_str} -c {cookie_file_path_str} 'https://www.myvideo.ge/tv/{channel_name}/{start_time.strftime("%d-%m-%Y/%T")}' \
        -H 'authority: www.myvideo.ge' \
        -H 'sec-ch-ua: "Chromium";v="94", "Google Chrome";v="94", ";Not A Brand";v="99"' \
        -H 'sec-ch-ua-mobile: ?0' \
        -H 'sec-ch-ua-platform: "Windows"' \
        -H 'upgrade-insecure-requests: 1' \
        -H 'user-agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36' \
        -H 'accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9' \
        -H 'sec-fetch-site: none' \
        -H 'sec-fetch-mode: navigate' \
        -H 'sec-fetch-user: ?1' \
        -H 'sec-fetch-dest: document' \
        -H 'accept-language: en-US,en;q=0.9' \
        --compressed --silent""")
        sleep(5)
        print(2)
        # CIA option
        os.system(f"""curl -b {cookie_file_path_str} -c {cookie_file_path_str}  'https://www.myvideo.ge/?CIA=1&ci_d=mobile&ci_c=livetv&ci_m=cutterFormData' \
        -X 'OPTIONS' \
        -H 'authority: www.myvideo.ge' \
        -H 'accept: */*' \
        -H 'access-control-request-method: GET' \
        -H 'access-control-request-headers: x-myvideo-app,x-myvideo-app-model,x-myvideo-app-ota,x-myvideo-app-package,x-myvideo-app-version' \
        -H 'origin: https://tv.myvideo.ge' \
        -H 'user-agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36' \
        -H 'sec-fetch-mode: cors' \
        -H 'sec-fetch-site: same-site' \
        -H 'sec-fetch-dest: empty' \
        -H 'referer: https://tv.myvideo.ge/' \
        -H 'accept-language: en-US,en;q=0.9' \
        --compressed --silent""")
        sleep(.5)
        print(3)
        # cia
        os.system(f"""curl -b {cookie_file_path_str} -c {cookie_file_path_str}  'https://www.myvideo.ge/?CIA=1&ci_d=mobile&ci_c=livetv&ci_m=cutterFormData' \
        -H 'authority: www.myvideo.ge' \
        -H 'sec-ch-ua: "Chromium";v="94", "Google Chrome";v="94", ";Not A Brand";v="99"' \
        -H 'x-myvideo-app-package: web' \
        -H 'x-myvideo-app-model: web' \
        -H 'sec-ch-ua-mobile: ?0' \
        -H 'x-myvideo-app-version: 1' \
        -H 'user-agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36' \
        -H 'x-myvideo-app-ota: web' \
        -H 'accept: application/json, text/plain, */*' \
        -H 'x-myvideo-app: web' \
        -H 'sec-ch-ua-platform: "Windows"' \
        -H 'origin: https://tv.myvideo.ge' \
        -H 'sec-fetch-site: same-site' \
        -H 'sec-fetch-mode: cors' \
        -H 'sec-fetch-dest: empty' \
        -H 'referer: https://tv.myvideo.ge/' \
        -H 'accept-language: en-US,en;q=0.9' \
        --compressed --silent""")
        print(4)
        #   options prep
        sleep(3)
        os.system(f"""curl -b {cookie_file_path_str} -c {cookie_file_path_str} 'https://www.myvideo.ge/dvr_cutter.php?mode=prepareDownload&chan={channel_name}' \
        -X 'OPTIONS' \
        -H 'authority: www.myvideo.ge' \
        -H 'accept: */*' \
        -H 'access-control-request-method: POST' \
        -H 'access-control-request-headers: x-myvideo-app,x-myvideo-app-model,x-myvideo-app-ota,x-myvideo-app-package,x-myvideo-app-version' \
        -H 'origin: https://tv.myvideo.ge' \
        -H 'user-agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36' \
        -H 'sec-fetch-mode: cors' \
        -H 'sec-fetch-site: same-site' \
        -H 'sec-fetch-dest: empty' \
        -H 'referer: https://tv.myvideo.ge/' \
        -H 'accept-language: en-US,en;q=0.9' \
        --compressed --silent""")
        print(5)
        # prep
        sleep(.5)
        file_id = os.popen(f"""curl -b {cookie_file_path_str} -c {cookie_file_path_str} 'https://www.myvideo.ge/dvr_cutter.php?mode=prepareDownload&chan={channel_name}' \
        -H 'authority: www.myvideo.ge' \
        -H 'sec-ch-ua: "Chromium";v="94", "Google Chrome";v="94", ";Not A Brand";v="99"' \
        -H 'x-myvideo-app-package: web' \
        -H 'x-myvideo-app-model: web' \
        -H 'sec-ch-ua-mobile: ?0' \
        -H 'x-myvideo-app-version: 1' \
        -H 'user-agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36' \
        -H 'x-myvideo-app-ota: web' \
        -H 'content-type: application/x-www-form-urlencoded' \
        -H 'accept: application/json, text/plain, */*' \
        -H 'x-myvideo-app: web' \
        -H 'sec-ch-ua-platform: "Windows"' \
        -H 'origin: https://tv.myvideo.ge' \
        -H 'sec-fetch-site: same-site' \
        -H 'sec-fetch-mode: cors' \
        -H 'sec-fetch-dest: empty' \
        -H 'referer: https://tv.myvideo.ge/' \
        -H 'accept-language: en-US,en;q=0.9' \
        --data-raw 'start={start_time.strftime("%d-%m-%Y+%T").replace(':', '%3A')}&end={end_time.strftime("%d-%m-%Y+%T").replace(':', '%3A')}' \
        --compressed """).read()
        print(6)

        download_id = json.loads(file_id)["download_id"]
        sleep(3)

        os.system(f"""curl -b {cookie_file_path_str} -c {cookie_file_path_str} 'https://www.myvideo.ge/dvr_cutter.php?mode=download&chan={channel_name}&id={download_id}' \
        -H 'authority: www.myvideo.ge' \
        -H 'sec-ch-ua: "Chromium";v="94", "Google Chrome";v="94", ";Not A Brand";v="99"' \
        -H 'sec-ch-ua-mobile: ?0' \
        -H 'sec-ch-ua-platform: "Windows"' \
        -H 'upgrade-insecure-requests: 1' \
        -H 'user-agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36' \
        -H 'accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9' \
        -H 'sec-fetch-site: same-site' \
        -H 'sec-fetch-mode: navigate' \
        -H 'sec-fetch-user: ?1' \
        -H 'sec-fetch-dest: iframe' \
        -H 'referer: https://tv.myvideo.ge/' \
        -H 'accept-language: en-US,en;q=0.9' \
        --compressed \
        --output {videos_path}/{vid_id}.mp4 --silent""")

        cookie_file.unlink()