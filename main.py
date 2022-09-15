import asyncio
from typing import List
import argparse
from celery import group, xmap
from uuid import UUID

from app.core.populate_collectors import get_collector_tasks
from app.core.populate_downloaders import get_downloader_tasks 
from app.core.populate_processors import get_processor_tasks

from app.config.mongo_config import init_mongo


async def run_collector_tasks(monitor_id:UUID, sample:bool):
    await init_mongo()
    collector_tasks: List[xmap or group] = await get_collector_tasks(monitor_id, sample)
    if not collector_tasks:
        return
    g = group(collector_tasks)
    g.delay().get()


async def run_downloader_tasks(monitor_id:UUID):
    await init_mongo()
    downloader_tasks: List[xmap or group] = await get_downloader_tasks(monitor_id)
    if not downloader_tasks:
        return
    g = group(downloader_tasks)
    g.delay().get()


async def run_processor_tasks(monitor_id:UUID):
    await init_mongo()
    processor_tasks: List[xmap or group] = await get_processor_tasks(monitor_id)
    print(f'{len(processor_tasks)} processor_tasks created')
    if not processor_tasks:
        return
    g = group(processor_tasks)
    g.delay().get()


if __name__ == '__main__':
    parser = argparse.ArgumentParser('Run data collection process for monitor')
    parser.add_argument('--monitor_id', type=str, help='Monitor id to run data collection for', required=True )
    parser.add_argument('--download_media', type=str, help='If True, video content is downloaded', required=False )
    parser.add_argument('--skip_collection', type=str, help='Skipping data collection, to apply processors on collected data', required=False )
    parser.add_argument('--sample', type=bool, help='If True, sample data is collected', required=False, default=False)
    
    args = parser.parse_args()
    
    monitor_id = UUID(getattr(args, 'monitor_id'))
    sample = getattr(args, 'sample')
    download_media = getattr(args, 'download_media')
    
    asyncio.run(run_collector_tasks(monitor_id, sample))
    if download_media and not sample:
        pass
        asyncio.run(run_downloader_tasks(monitor_id))

    if not sample:
        pass
        asyncio.run(run_processor_tasks(monitor_id))
        
    