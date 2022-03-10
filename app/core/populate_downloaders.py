from app.core.celery.tasks.download import download
from typing import List
from beanie.odm.operators.find.comparison import In
from celery import chain, group
from uuid import UUID
from ibex_models import DownloadTask, Platform, Post, MediaDownloadStatus
from app.util.model_utils import serialize_to_base64

def get_downloader_tasks(monitor_id:UUID):
    posts_to_download = Post.find(In(Post.monitor_ids, monitor_id), Post.status == MediaDownloadStatus.to_be_downloaded)

    download_tasks = [DownloadTask(post_id=post_to_download.id, post=post_to_download, platform=Post.platform) for post_to_download in posts_to_download]
    
    tasks_group: List[chain or group] = []

    for platform in Platform:
        download_tasks_group = [download_task for download_task in download_tasks if download_task.platform == platform]
        if len(download_tasks_group) == 0: continue

        if False: 
            task_group.append(group([collect.s(serialize_to_base64(task)) for task in download_tasks_group]))
        else:
            tasks_group.append(download.map([serialize_to_base64(task) for task in download_tasks_group]))

    return tasks_group
 