from app.core.celery.tasks.process import process
from app.core.celery.tasks.download import download
from typing import List
from beanie.odm.operators.find.comparison import In
from celery import chain, group
from uuid import UUID
from ibex_models import DownloadTask, Platform, Post, MediaStatus
from app.util.model_utils import serialize_to_base64


def get_processor_tasks(monitor_id:UUID):
    # batch_processors 
    # singular_processors
    posts_to_process = Post.find(In(Post.monitor_ids, monitor_id), Post.status == MediaStatus.downloaded)

