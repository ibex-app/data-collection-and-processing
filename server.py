import asyncio
from typing import List

from celery import group, xmap

from app.core.populate_collectors import get_collector_tasks
from app.core.populate_downloaders import get_downloader_tasks 
from app.core.populate_processors import get_processor_tasks

from app.config.mongo_config import init_mongo


async def run_collector_tasks():
    await init_mongo()
    collector_tasks: List[xmap or group] = await get_collector_tasks()
    g = group(collector_tasks)
    g.delay().get()


async def run_downloader_tasks():
    downloader_tasks: List[xmap or group] = await get_downloader_tasks()
    g = group(downloader_tasks)
    g.delay().get()


async def run_processor_tasks():
    processor_tasks: List[xmap or group] = await get_processor_tasks()
    g = group(processor_tasks)
    g.delay().get()


if __name__ == "__main__":
    asyncio.run(run_collector_tasks())
    # asyncio.run(run_downloader_tasks())
    # asyncio.run(run_processor_tasks())