from app.core.celery.tasks.process import process
from typing import List
from beanie.odm.operators.find.comparison import In
from celery import chain, group
from uuid import UUID
from ibex_models import Processor, Post, MediaStatus, ProcessTask, ProcessTaskBatch
from app.util.model_utils import serialize_to_base64


async def get_processor_tasks(monitor_id:UUID, env:str):
    # batch_processors 
    # singular_processors
    process_tasks: List[ProcessTask] = []

    # posts:List[Post] = await Post.find(In(Post.monitor_ids, [monitor_id])).to_list()
    # posts_for_speech_to_text:List[Post] = [post for post in posts if post.media_status == MediaStatus.downloaded]
    
    # if len(posts_for_speech_to_text) == 0:
    #     tasks_for_speech_to_text: List[ProcessTask] = [ProcessTask(post=post_to_process, processor=Processor.speech_to_text, monitor_id=monitor_id) for post_to_process in posts_for_speech_to_text]
    #     process_tasks += tasks_for_speech_to_text

    #     print(f'{len(tasks_for_speech_to_text)} speech_to_text process tasks created...')

    # process_tasks.append(ProcessTask(processor=Processor.detect_search_terms, monitor_id=monitor_id))
    # print(f'single tasks_for_detect_search_terms process tasks created... for monitor {monitor_id}')

    # for post in posts:
    #     process_tasks.append(ProcessTask(post=post, processor=Processor.hate_speech, monitor_id=monitor_id))
    
    # print(f'single tasks_for_hate_speech process tasks created... for monitor {monitor_id}')
    
    # process_tasks.append(ProcessTaskBatch(processor=Processor.top_engagement, monitor_id=monitor_id))

    process_tasks.append(ProcessTaskBatch(processor=Processor.detect_language, monitor_id=monitor_id, env=env))
    # process_tasks.append(ProcessTaskBatch(processor=Processor.topic, monitor_id=monitor_id))

    print(f'single topic labeling process tasks created... for monitor {monitor_id}')

    tasks_group = []
    tasks_group.append(process.map([serialize_to_base64(task) for task in process_tasks]))
    
    return tasks_group

