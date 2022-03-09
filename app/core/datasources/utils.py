from ibex_models import CollectTask

async def update_hits_count(collect_task: CollectTask, hits_count: int):
    collect_task_ = await CollectTask.find(CollectTask.id == collect_task.id)

    collect_task_.hits_count = hits_count
    collect_task_.sample = False
    await collect_task_.save()