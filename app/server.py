import asyncio
from typing import List

from celery import group, xmap

from app.core.celery.tasks.collect import get_collector_tasks


async def run_collector_tasks():
    from app.config.mongo_config import init_mongo
    await init_mongo()

    collector_tasks: List[xmap or group] = await get_collector_tasks()
    g = group(collector_tasks)
    g.delay().get()


if __name__ == "__main__":
    asyncio.run(run_collector_tasks())
