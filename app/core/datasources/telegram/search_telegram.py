import os
from datetime import datetime
from telethon.sync import TelegramClient
from telethon import functions, types
from typing import List, Dict
from ibex_models import Post, Scores, CollectTask, Platform, Account
from app.core.datasources.datasource import Datasource

class TelegramCollector(Datasource):
    """The class for data collection from TelegramClient.

        All data collectors/data sources implement
        methods described below.
    """
    def __init__(self, *args, **kwargs):
        # hash parameter for authorization.
        self.hash = os.getenv('TELEGRAM_HASH')

        # id parameter for authorization.
        self.id = os.getenv('TELEGRAM_ID')

        # TODO: double check the limit per post
        # Variable for maximum number of posts per request
        self.max_posts_per_call = 10000

        # Variable for maximum number of requests
        self.max_requests = 20

    # def generate_req_params(self, collect_task: CollectTask):
    #     if collect_task.query is not None and len(collect_task.query) > 0:
    #         params['q'] = collect_task.query
    #     if collect_task.accounts is not None and len(collect_task.accounts) > 0:
    #         params['groups'] = ','.join([account.platform_id for account in collect_task.accounts])

    #     return params


    async def collect(self, collect_task: CollectTask) -> List[Post]:
        """The method is responsible for collecting posts
            from platforms.

        Args:
            collect_action(CollectTask): CollectTask object holds
                all the metadata needed for data collection.

        Returns:
            (List[Post]): List of collected posts.
        """

        # Dict variable for generated meta data.
        params = self.generate_req_params(collect_task)

        # Variable for TelegramClient instance
        client = TelegramClient('name', self.id, self.hash)
        await client.start()

        # List variable for all posts data.
        posts = []

        # Boolean variable for looping through pages.
        next_from = True

        dialog_name = collect_task.accounts[0].platform_id

        # Variables for searching through date range.
        pre_first_msg = await client.get_messages(dialog_name, offset_date=collect_task.date_from, limit=1)
        first_msg = await client.get_messages(dialog_name,  min_id=pre_first_msg[0].id, limit=1)
        last_msg = await client.get_messages(dialog_name, offset_date=collect_task.date_to, limit=1)
        
        offset = 0
        while next_from:
            messages = await client.get_messages(dialog_name, 
                                                    search=params['q'][:7],
                                                    min_id=last_msg[0].id, 
                                                    max_id=first_msg[0].id, 
                                                    add_offset=offset,
                                                    limit=self.max_posts_per_call
                                                )
            if len(messages) < params['count']:
                next_from = False

            offset += params['count']
            posts += messages
        
        maped_posts = self._map_to_posts(posts, collect_task)
        print(f'{len(maped_posts)} posts collected from dialog: {dialog_name}')
        return maped_posts


    async def get_hits_count(self, collect_task: CollectTask) -> int:
        """The method is responsible for collecting the number of posts,
            that satisfy all criterias in CollectTask object.

        Note:
            Do not collect actual posts here, this method is only
            applicable to platforms that provide this kind of information.

        Args:
            collect_action(CollectTask): CollectTask object holds
                all the metadata needed for data collection.

        Returns:
            (int): Number of posts existing on the platform.
        """

        # Dict variable for generated meta data.
        params = self.generate_req_params(collect_task)

        # Variable for TelegramClient instance
        client = TelegramClient('name', params['id'], params['hash'])
        await client.start()

        # Boolean variable for looping through pages.
        next_from = True

        # Variable for storing number of posts.
        hits_count = 0

        # Variable for getting all the channels or chats for user.
        dialogs = await client.get_dialogs()

        # Variables for searching through date range.
        pre_first_msg = await client.get_messages(dialogs[0].name, offset_date=params['start_time'], limit=1)
        first_msg = await client.get_messages(dialogs[0].name,  min_id=pre_first_msg[0].id, limit=1)
        last_msg = await client.get_messages(dialogs[0].name, offset_date=params['end_time'], limit=1)

        while next_from:
            messages = await client.get_messages(dialogs[0].name, search=params['q'][:7], min_id=last_msg[0].id, max_id=first_msg[0].id, add_offset=params['offset'], limit=params['count'])
            if len(messages) < params['count']:
                hits_count = params['offset'] + len(messages)
                next_from = False
            params['offset'] = params['offset'] + params['count']
        print(hits_count)
        return hits_count


    @staticmethod
    def map_to_post(api_post: Dict, collect_task: CollectTask) -> Post:
        """The method is responsible for mapping data redudned by plarform api
            into Post class.

        Args:
            api_post: responce from platform API.
            collect_action(CollectTask): the metadata used for data collection task.

        Returns:
            (Post): class derived from API data.
        """

        scores = Scores(
            likes=api_post.replies,
            shares=api_post.forwards,
        )
        post_doc = ''
        try:
            post_doc = Post(title="post" if 'post' in str(api_post) else "",
                            text=api_post.message if 'message' in str(api_post) else "",
                            created_at=api_post.date if 'date' in str(api_post) else datetime.now(),
                            platform=Platform.telegram,
                            platform_id=api_post.from_id if api_post.from_id is not None else '',
                            author_platform_id=api_post.peer_id.channel_id if 'channel_id' in str(api_post) else None,
                            scores=scores,
                            api_dump=dict({"dump": str(api_post)}),
                            monitor_id=api_post.id,
                            # url=url,
                            )
        except Exception as exc:
            print(exc)
        return post_doc


    def _map_to_posts(self, posts: List[Dict], collect_task: CollectTask):
        res: List[Post] = []
        for post in posts:
            try:
                post = self.map_to_post(post, collect_task)
                res.append(post)
            except ValueError as e:
                self.log.error(f'[{collect_task.platform}] {e}')
        return res

    def generate_params(self, query:str):
        params = dict(
            limit=5,
        )
        if query is not None and len(query) > 0:
            params['q'] = query

        return params

    async def get_accounts(self, query: str) -> List[Account]:
        # Dict variable for generated meta data.
        params = self.generate_params(query)

        # Variable for TelegramClient instance
        client = TelegramClient('username', self.id, self.hash)
        await client.start()


        dialogs = await client(functions.contacts.SearchRequest(
            q=params['q'],
            limit=params['limit'],
        ))

        # List variable for all accounts data.
        accounts = self.map_to_accounts(dialogs.chats)

        return accounts

    def map_to_accounts(self, accounts: List) -> List[Account]:
        """The method is responsible for mapping data redudned by plarform api
                   into Account class.
               Args:
                   accounts: responce from platform API.
                   collect_action(CollectTask): the metadata used for data collection task.
               Returns:
                   (Account): class derived from API data.
               """
        result: List[Account] = []
        for account in accounts:
            try:
                account = self.map_to_acc(account)
                result.append(account)
            except ValueError as e:
                print("Telegram", e)
        return result

    def map_to_acc(self, acc: Account) -> Account:
        mapped_account = Account(
            title=acc.title,
            url='t.me/'+acc.username,
            platform=Platform.telegram,
            platform_id=acc.id,
            broadcasting_start_time=acc.date
        )
        return mapped_account