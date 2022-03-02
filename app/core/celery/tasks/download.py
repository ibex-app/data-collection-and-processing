from __future__ import annotations
import asyncio

from app.config.logging_config import log
from app.util.model_utils import deserialize_from_base64

from app.core.downloaders import downloader_classes
from app.core.celery.worker import celery

from ibex_models import Post, DownloadTask, MediaDownloadStatus

@celery.task(name='app.core.celery.tasks.download')
def download(task: str):

    task: DownloadTask = deserialize_from_base64(task)
    if task.platform not in downloader_classes.keys():
        log.info(f"No implementation for platform [{task.platform}] found! skipping..")
        return

    downloader_class = downloader_classes[task.platform]()
    downloader_method = downloader_class.download


    asyncio.run(download_and_update_mongo(downloader_method, task))
    
async def download_and_update_mongo(downloader_method, task: DownloadTask):

    from app.config.mongo_config import init_mongo
    await init_mongo()

    task.post = await Post.find(Post.id == task.post_id)

    download_status = downloader_method(task)

    if download_status:
        task.post.mdeia_download_status = MediaDownloadStatus.downloaded
        await Post.update()