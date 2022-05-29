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
        self.max_posts_per_call = 1000
        self.max_posts_per_call_sample = 20

        # Variable for maximum number of requests
        self.max_requests = 50
        self.max_requests_sample = 1


    async def get_first_and_last_message(self, dialog_name, collect_task):
        first_msg = await self.client.get_messages(dialog_name, offset_date=collect_task.date_from, limit=1)
        # first_msg = await self.client.get_messages(dialog_name, min_id=pre_first_msg[0].id, limit=1)
        last_msg = await self.client.get_messages(dialog_name, offset_date=collect_task.date_to, limit=1)

        return first_msg[0].id, last_msg[0].id


    async def connect(self):
         # Variable for TelegramClient instance
        self.client = TelegramClient('username', self.id, self.hash)
        try:
            await self.client.disconnect()
        except:
            pass

        await self.client.start()


    async def collect(self, collect_task: CollectTask) -> List[Post]:
        """The method is responsible for collecting posts
            from platforms.
        Args:
            collect_action(CollectTask): CollectTask object holds
                all the metadata needed for data collection.
        Returns:
            (List[Post]): List of collected posts.
        """
        self.max_requests_ = self.max_requests_sample if collect_task.sample else self.max_requests
        self.max_posts_per_call_ = self.max_posts_per_call_sample if collect_task.sample else self.max_posts_per_call

        await self.connect()
       

        dialog_name = ''
        if collect_task.accounts and collect_task.accounts[0]:
            dialog_name = collect_task.accounts[0].platform_id

        # List variable for all posts data.
        posts = []

        # Boolean variable for looping through pages.
        next_from = True
        
        # Variables for searching through date range.
        first_msg_id, last_msg_id = await self.get_first_and_last_message(dialog_name, collect_task)
        
        requests_count = 0
        while next_from:
            messages = await self.client.get_messages(dialog_name,
                                                 search=collect_task.query,
                                                 min_id=first_msg_id,
                                                 max_id=last_msg_id,
                                                 add_offset=requests_count * self.max_posts_per_call_,
                                                 limit=self.max_posts_per_call_
                                                )
            print('offset', requests_count * self.max_posts_per_call_)
            posts += messages
            requests_count += 1
            print(f'[Telegram] request # {requests_count} messages {len(messages)}')

            if not len(posts):
                print(f'[Telegram] No posts found for dialog: {dialog_name}')
                break
            
            if not len(messages):
                print(f'[Telegram] All posts collected: {dialog_name}')
                break

            if requests_count >= self.max_requests_:
                print(f'[Telegram] limit of {self.max_requests_} have been reached')
                break

            
        maped_posts = self.map_to_posts(posts, collect_task)
        print(f'[Telegram] {len(maped_posts)} posts collected from dialog: {dialog_name}')
        
        await self.client.disconnect()

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
        return -1


    def map_to_post(self, api_post: Dict, collect_task: CollectTask) -> Post:
        """The method is responsible for mapping data redudned by plarform api
            into Post class.
        Args:
            api_post: responce from platform API.
            collect_action(CollectTask): the metadata used for data collection task.
        Returns:
            (Post): class derived from API data.
        """

        scores = Scores(
            likes=api_post.replies, # Temporarily saved to likes.
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


    def map_to_posts(self, posts: List[Dict], collect_task: CollectTask):
        res: List[Post] = []
        for post in posts:
            try:
                post = self.map_to_post(post, collect_task)
                res.append(post)
            except ValueError as e:
                self.log.error(f'[{collect_task.platform}] {e}')
        return res


    async def get_accounts(self, query: str) -> List[Account]:
        # Variable for TelegramClient instance
        client = TelegramClient('username', self.id, self.hash)
        await client.start()


        dialogs = await client(functions.contacts.SearchRequest(
            q=query,
            limit=5,
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
