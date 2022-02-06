from __future__ import annotations
import asyncio

from app.config.logging_config import log
from app.util.model_utils import deserialize_from_base64

from app.core.processors import processor_classes
from app.core.celery.worker import celery

from app.model import PostClass, ProcessTask, MediaDownloadStatus

@celery.task(name='app.core.celery.tasks.process.process')
def process(task: str):

    task: ProcessTask = deserialize_from_base64(task)
    if task.processor not in processor_classes.keys():
        log.info(f"No implementation for platform [{task.processor}] found! skipping..")
        return

    processor_class = processor_classes[task.processor]()
    processor_method = processor_class.process

    asyncio.run(download_and_update_mongo(processor_method, task))
    

async def download_and_update_mongo(processor_method, task: ProcessTask):

    from app.config.mongo_config import init_mongo
    await init_mongo()

    task.post = await PostClass.find(PostClass.id == task.post_id)

    process_status = processor_method(task)

    if process_status:
        pass
        # task.post.mdeia_download_status = MediaDownloadStatus.downloaded
        # await PostClass.update()