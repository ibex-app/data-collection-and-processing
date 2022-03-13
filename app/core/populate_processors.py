from app.core.celery.tasks.process import process
from typing import List
from beanie.odm.operators.find.comparison import In
from celery import chain, group
from uuid import UUID
from ibex_models import Processor, Post, MediaStatus, ProcessTask
from app.util.model_utils import serialize_to_base64


async def get_processor_tasks(monitor_id:UUID):
    # batch_processors 
    # singular_processors
    posts_to_process = await Post.find(In(Post.monitor_ids, [monitor_id]), Post.media_status == MediaStatus.downloaded).to_list()
    
    if len(posts_to_process) == 0: return

    process_tasks: List[ProcessTask] = [ProcessTask(post=post_to_process, processor=Processor.speech_to_text) for post_to_process in posts_to_process]
    
    tasks_group = []
    tasks_group.append(process.map([serialize_to_base64(task) for task in process_tasks]))
    
    return tasks_group

