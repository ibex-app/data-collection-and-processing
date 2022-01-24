from __future__ import annotations
from multiprocessing.queues import Queue
from storage import StorageClass
from core import DownloaderDataType
from downloaders import downloader_classes


def add_downloaders(queue: Queue, **kwags):
    # items: list[itemType]
    items_to_download = StorageClass.get_items_to_download()
    for item_to_download in items_to_download:
        queue.put(DownloaderDataType(
            executor='downloader',
            platform=item_to_download["platform"],
            platform_id=item_to_download["platform_id"],
            # mediaType=item_to_download["mediaType"],
            start_time=item_to_download["start_time"]\
            if "start_time" in item_to_download.keys() else None,
            end_time=item_to_download["end_time"]\
            if "end_time" in item_to_download.keys() else None,
            url=item_to_download["url"],
            item_id=item_to_download["id"]
        ))


def download(queue, **kwags):
    downloader = downloader_classes[kwags["platform"]].download
    # logging.info(kwags)
    # return
    status = downloader(kwags)

    if status:
        StorageClass.is_downloaded(kwags["item_id"])

