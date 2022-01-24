from multiprocessing.queues import Queue
from core import ExtractorDataType, itemType
from storage import StorageClass
import logging
import time


def add_extractors(queue: Queue, **kwargs):
    extractor_models: list[dict] = StorageClass.get_extractor_models()
    items_to_extract: list[itemType] = StorageClass.get_items_to_extract()

    for item_to_extract in items_to_extract:
        for extractor_model in extractor_models:
            queue.put(ExtractorDataType(
                executor='extractor',
                mediaType=item_to_extract["mediaType"],
                path=item_to_extract["url"],
                item_id=item_to_extract["item_id"],
                extractor_model=extractor_model))


def extract(queue: Queue, **kwargs):
    logging.info("extracting")
    time.sleep(2)
