import asyncio
from typing import List
import argparse
from celery import group, xmap
from uuid import UUID
import os
from dotenv import load_dotenv

from app.core.populate_collectors import get_collector_tasks
from app.core.populate_downloaders import get_downloader_tasks 
from app.core.populate_processors import get_processor_tasks

from app.config.mongo_config import init_mongo
from ibex_models.monitor import Monitor, MonitorStatus


async def populate_collector_tasks(monitor_id:UUID, sample:bool):
    await init_mongo()
    collector_tasks: List[xmap or group] = await get_collector_tasks(monitor_id, sample)
    if not collector_tasks:
        return
    g = group(collector_tasks)
    g.delay().get()

    monitor = await Monitor.get(monitor_id)
    monitor.status = MonitorStatus.sampled if sample else MonitorStatus.collected
    await monitor.save()
    print(f'collection complated, monitor_id, sample : {monitor_id}, {sample}')


async def populate_downloader_tasks(monitor_id:UUID):
    await init_mongo()
    downloader_tasks: List[xmap or group] = await get_downloader_tasks(monitor_id)
    if not downloader_tasks:
        return
    g = group(downloader_tasks)
    g.delay().get()


async def populate_processor_tasks(monitor_id:UUID):
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
    parser.add_argument('--env', type=str, help='Sub domain of celery instance', required=True )
    parser.add_argument('--skip_collection', type=str, help='Skipping data collection, to apply processors on collected data', required=False )
    parser.add_argument('--sample', action='store_true', help='If True, sample data is collected', required=False, default=False)


    args = parser.parse_args()
    
    monitor_id = UUID(getattr(args, 'monitor_id'))
    sample = getattr(args, 'sample')
    env = getattr(args, 'env')
    download_media = getattr(args, 'download_media')
    
    load_dotenv(f'/home/.{env.lower()}.env')

    print(f'Running data collection fo monitor_id {monitor_id}, download_media: {download_media}, sample: {sample} ')
    
    asyncio.run(populate_collector_tasks(monitor_id, sample))
    if download_media and not sample:
        pass
        asyncio.run(populate_downloader_tasks(monitor_id))

    if not sample:
        pass
        asyncio.run(populate_processor_tasks(monitor_id))
        
    