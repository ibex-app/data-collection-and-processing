import json

from beanie import init_beanie
from motor.motor_asyncio import AsyncIOMotorClient

from ibex_models import Account, SearchTerm, CollectAction, Post, Tag, CollectTask, Monitor

import asyncio
import os

class DBConstants:
    prefix = '../../'
    collect_actions = 'collect_actions'
    collect_actions_path = f'{prefix}resources/collect_actions.json'
    accounts = 'accounts'
    accounts_path = f'{prefix}resources/accounts.json'
    search_terms = 'search_terms'
    search_terms_path = f'{prefix}resources/search_terms.json'
    # TODO: Move to env
    connection_string = os.getenv('MONGO_CS_DEV')
    connection_string_local = "mongodb://127.0.0.1:27017/"    

DB = DBConstants


async def init_mongo():
    """
    Initialize a connection to MongoDB
    """
    client = AsyncIOMotorClient(DB.connection_string)
    await init_beanie(database=client.ibex, document_models=[CollectAction, Account, SearchTerm, Post, Tag, CollectTask, Monitor, CollectTask])

    # post_doc = Post(title='example', created_at=datetime.now(), platform_id='example',
    #                      author_platform_id='example', api_dump='example')
    #
    # await Post.find_one(Post.title == "example2").upsert(
    #     Set({Post.title: 'asdasdasd'}),
    #     on_insert=post_doc
    # )


async def fill_db():
    """
    Load JSON files & fill MongoDB with Datasource & CollectAction documents.
    """
    with open(DB.collect_actions_path, encoding='utf8') as f:
        collect_actions = json.load(f)[DB.collect_actions]
        await CollectAction.find_all().delete()
        await CollectAction.insert_many([CollectAction(**ca) for ca in collect_actions])

    with open(DB.accounts_path, encoding='utf8') as f:
        accounts = json.load(f)[DB.accounts]
        await Account.find_all().delete()
        await Account.insert_many([Account(**ds) for ds in accounts])

    with open(DB.search_terms_path, encoding='utf8') as f:
        search_terms = json.load(f)[DB.search_terms]
        await SearchTerm.find_all().delete()
        await SearchTerm.insert_many([SearchTerm(**st) for st in search_terms])


async def setup_mongo(fill=True):
    await init_mongo()
    if fill:
        await fill_db()


if __name__ == "__main__":
    asyncio.run(setup_mongo())
