import asyncio
from typing import List
import argparse
from celery import group, xmap

from app.core.populate_collectors import get_collector_tasks
from app.core.populate_downloaders import get_downloader_tasks 
from app.core.populate_processors import get_processor_tasks

from app.config.mongo_config import init_mongo


async def run_collector_tasks(monitor_id:str, sample:bool):
    await init_mongo()
    collector_tasks: List[xmap or group] = await get_collector_tasks(monitor_id, sample)
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


if __name__ == '__main__':
    parser = argparse.ArgumentParser('Run data collection process for monitor')
    parser.add_argument('--monitor_id', type=str, help='Monitor id to run data collection for', required=True )
    parser.add_argument('--sample', help='If passed value is True, sample data would be collected', default=False)
    
    args = parser.parse_args()
    
    monitor_id = getattr(args, 'monitor_id')
    sample = getattr(args, 'sample')
    
    asyncio.run(run_collector_tasks(), monitor_id, sample)