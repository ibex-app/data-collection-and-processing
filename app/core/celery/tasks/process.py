from __future__ import annotations
import asyncio
from dotenv import load_dotenv

from app.config.logging_config import log
from app.util.model_utils import deserialize_from_base64

from app.core.processors import processor_classes
from app.core.celery.worker import celery
from app.config.mongo_config import init_mongo
from ibex_models import Post, ProcessTask, MediaStatus, Processor

@celery.task(name='app.core.celery.tasks.process')
def process(task: str):
    task: ProcessTask = deserialize_from_base64(task)
    if task.processor not in processor_classes.keys():
        log.info(f"No implementation for platform [{task.processor}] found! skipping..")
        return

    load_dotenv(f'/home/.{task.env.lower()}.env')
    
    print(f'Loading environment variables from /home/.{task.env.lower()}.env', task)

    processor_class = processor_classes[task.processor]()
    processor_method = processor_class.process
    
    asyncio.run(process_and_update_mongo(processor_method, task))
    

async def process_and_update_mongo(processor_method, task: ProcessTask):
    await init_mongo()
    
    if task.processor == Processor.speech_to_text:
        task.post = await Post.get(task.post.id)

    process_status = await processor_method(task)

    if process_status and task.processor == Processor.speech_to_text:
        task.post.media_status = MediaStatus.processed
        await task.post.save()