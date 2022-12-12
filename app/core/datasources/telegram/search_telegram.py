import os
import time
from math import floor
from datetime import datetime
from telethon.sync import TelegramClient
from telethon import functions, types
from telethon.tl.functions.messages import SearchGlobalRequest, SearchRequest
from telethon.tl.types import InputMessagesFilterEmpty, InputPeerEmpty, InputPeerChannel
from typing import List, Dict
from ibex_models import Post, Scores, CollectTask, Platform, Account
from app.core.datasources.datasource import Datasource
from app.config.aop_config import slf, sleep_after
from app.core.split import split_complex_query
from eldar import Query
from app.core.datasources.utils import add_search_terms_to_posts, set_account_id, set_total_engagement, validate_posts_by_query
import pytz


@slf
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
        self.max_posts_per_call_sample = 50

        # Variable for maximum number of requests
        self.max_requests = 50
        self.max_requests_sample = 1
        self.operators = dict(
            or_ = ' OR ',
            and_ = ' AND ',
            not_ = ' NOT ',
        )
        self.utc = pytz.UTC

    async def get_first_and_last_message(self, channel, collect_task, query:str = None):
        if query:
            first_msg = await self.client.get_messages(channel, offset_date=collect_task.date_from, limit=1, search=query)
            last_msg = await self.client.get_messages(channel, offset_date=collect_task.date_to, limit=1, search=query)
        else:
            first_msg = await self.client.get_messages(channel, offset_date=collect_task.date_from, limit=1)
            last_msg = await self.client.get_messages(channel, offset_date=collect_task.date_to, limit=1)

        return None if len(first_msg) == 0 else first_msg[0].id, None if len(last_msg) == 0 else last_msg[0].id


    async def connect(self, env: str):
         # Variable for TelegramClient instance
        self.log.info('[Telegram] trying to connect...')
        self.client = TelegramClient(f'/home/.{env}.telegram.session', self.id, self.hash)
        self.log.info('[Telegram] TelegramClient inited...')
        try:
            self.log.info('[Telegram] self.client.disconnect()...')
            await self.client.disconnect()
        except:
            self.log.info('[Telegram] failed self.client.disconnect()...')
            pass
        self.log.info('[Telegram] self.client.start()')
        
        await self.client.connect()


    async def get_keyword_with_least_posts(self, collect_task: CollectTask) -> str:
        # split_complex_query splits query into words and statements
        # keyword_1,           keyword_2,          keyword_3,      keyword_4
        #        statement_1,         statement_2,       statement_3
        keywords, statements = split_complex_query(collect_task.query, self.operators)

        tmp_query = collect_task.query
        keywords_with_hits_counts = []
        for i, keyword in enumerate(keywords):
            if statements[i - 1] == '_NOT': continue
            collect_task_ = collect_task.copy()
            collect_task_.query = keyword
            hits_count = await self.get_hits_count(collect_task_, True)
            keywords_with_hits_counts.append((keyword, hits_count))

        keywords_with_hits_counts.sort(key=lambda tup: tup[1])

        keyword_with_least_posts = keywords_with_hits_counts[0]
        collect_task.query = tmp_query
        return keyword_with_least_posts[0]


    async def get_query(self, collect_task):
        if collect_task.query and (' AND ' in collect_task.query or ' OR ' in collect_task.query):
            query = await self.get_keyword_with_least_posts(collect_task)
        else:
            query = collect_task.query
        return query or ''


    async def generate_search_params(self, collect_task: CollectTask):
        peer, channel = await self.get_channel(collect_task)
        query = await self.get_query(collect_task)
        if collect_task.accounts and len(collect_task.accounts):
            first_msg_id, last_msg_id = await self.get_first_and_last_message(channel, collect_task, query)
            params = dict(
                peer=peer,
                filter=InputMessagesFilterEmpty(),
                q=query,
                min_date=collect_task.date_from,
                max_date=collect_task.date_to,
                max_id=last_msg_id,
                limit=1 if collect_task.get_hits_count else self.max_posts_per_call_,
                from_id=InputPeerEmpty(),
                offset_id=0,
                min_id=first_msg_id,
                hash=channel.access_hash
            )
        else:
            params = dict(
                q=query,
                filter=InputMessagesFilterEmpty(),
                min_date=collect_task.date_from,
                max_date=collect_task.date_to,
                offset_peer=InputPeerEmpty(),
                offset_rate=0,
                offset_id=0,
                limit=1 if collect_task.get_hits_count else self.max_posts_per_call_,
                folder_id=None
            )
        
        # self.log.info('[Telegram] params ', params)
        return params


    async def get_channel(self, collect_task: CollectTask):
        if collect_task.accounts and collect_task.accounts[0]:
            channel = await self.client.get_entity(int(collect_task.accounts[0].platform_id))
            return InputPeerChannel(channel_id=int(collect_task.accounts[0].platform_id), access_hash=channel.access_hash), channel
        else:
            return InputPeerEmpty(), None


    def get_search_method(self,  collect_task: CollectTask):
        if collect_task.accounts and collect_task.accounts[0]:
            return SearchRequest
        else:
            return SearchGlobalRequest


    @sleep_after(tag='Telegram', pause_time=1, rang=1)
    async def api_call(self, search_method, params):
        return await self.client(search_method(**params))


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
        
        # Boolean variable for looping through pages.
        
        await self.connect(collect_task.env)

        requests_count = 0
        posts = []

        params = await self.generate_search_params(collect_task)
        search_method = self.get_search_method(collect_task)
        next_rate = 0
        while True:
            if collect_task.accounts and len(collect_task.accounts):
                params['add_offset'] = requests_count * self.max_posts_per_call_
                self.log.info('offset', params['add_offset'])
            else:
                params['offset_rate'] = next_rate

            api_result = await self.client(search_method(**params))
            posts += api_result.messages
            
            self.log.info(f'[Telegram] search_method: {search_method}, params: {params}')
            self.log.info(f'[Telegram] api_result {api_result}')
                
            requests_count += 1
            self.log.info(f'[Telegram] request # {requests_count}, messages: {len(api_result.messages)}, total: {len(posts)} ')

            if not len(posts):
                self.log.info(f'[Telegram] No posts found')
                break
            
            if requests_count >= self.max_requests_:
                self.log.info(f'[Telegram] limit of {self.max_requests_} have been reached')
                break

            if not len(api_result.messages):
                self.log.info(f'[Telegram] All posts collected')
                break
            
            last_message_date = api_result.messages[-1].date.replace(tzinfo=self.utc)
            date_from = collect_task.date_from.replace(tzinfo=self.utc)
            
            if last_message_date <= date_from:
                self.log.info(f'[Telegram] All posts collected')
                break

            if next_rate in api_result.__dict__:
                next_rate = api_result.next_rate

            time.sleep(1.5)
        await self.client.disconnect()

        maped_posts = self.map_to_posts(posts, collect_task)
        
        self.log.info(f'[Telegram] {len(maped_posts)} posts collected ')
        
        # valid_posts = validate_posts_by_query(collect_task, maped_posts)
        maped_posts = await add_search_terms_to_posts(maped_posts, collect_task.monitor_id)
        # self.log.success(f'[Telegram] {len(valid_posts)} valid posts collected')

        return maped_posts


    async def get_hits_count(self, collect_task: CollectTask, connected = False) -> int:
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
        if collect_task.query and (' AND ' in collect_task.query or ' OR ' in collect_task.query):
            return -1
        if not connected:
            await self.connect(collect_task.env)

        params = await self.generate_search_params(collect_task)
        
        if collect_task.accounts and len(collect_task.accounts):
            hits_count = floor(params['max_id'] - params['min_id'] / 4)
        else:
            api_result = await self.client(SearchGlobalRequest(**params))
            hits_count = 0 if 'count' not in api_result.__dict__ else api_result.count
        
        if not connected:
            await self.client.disconnect()

        self.log.info('[Telegram] hits count - ', hits_count)
        return hits_count


    def map_to_post(self, api_post: Dict, collect_task: CollectTask) -> Post:
        """The method is responsible for mapping data redudned by plarform api
            into Post class.
        Args:
            api_post: responce from platform API.
            collect_action(CollectTask): the metadata used for data collection task.
        Returns:
            (Post): class derived from API data.
        """
        # self.log.info('[Telegram] mapping', api_post.__dict__)
        # self.log.info('[Telegram] mapping', api_post.message)
        scores = self.get_scores(api_post)
        post = None
        try:
            
            post = Post(text="",
                            title=api_post.message,
                            created_at=api_post.date,
                            platform=Platform.telegram,
                            platform_id=api_post.id, #api_post.from_id if api_post.from_id is not None else '',
                            author_platform_id=api_post.peer_id.channel_id,
                            scores=scores,
                            # TODO dump full API responce as a dict
                            api_dump={'dump': str(api_post)},
                            url=f'https://t.me/c/{api_post.peer_id.channel_id}/{api_post.id}',
                    )
            post = set_account_id(post, collect_task)
            post = set_total_engagement(post)
        except Exception as exc:
            self.log.info('[Telegram] failed to map a post', api_post, exc)
        # self.log.info('[Telegram][set_account_id] post account_id', post.account_id)
        
        return post


    def get_scores(self, api_post):

        scores = Scores(
            comments=0 if not api_post.replies else api_post.replies.replies,
            shares=api_post.forwards,
            views=api_post.views
        )
        # self.log.info('[Telegram] scores', api_post.reactions)
        if not api_post.reactions:
            return scores

        other = 0
        for reaction in api_post.reactions.results:
            
            if reaction.reaction == '😢':
                scores.sad = reaction.count
            if reaction.reaction == '👍':
                scores.likes = reaction.count
            if reaction.reaction == '🤬':
                scores.angry = reaction.count
            if reaction.reaction == '👎':
                scores.dislikes = reaction.count
            if reaction.reaction == '❤':
                scores.love = reaction.count
            else:
                other += reaction.count

        scores.other = other

        return scores


    def map_to_posts(self, posts: List[Dict], collect_task: CollectTask):
        res: List[Post] = []
        for api_post in posts:
            try:
                post = self.map_to_post(api_post, collect_task)
                res.append(post)
            except ValueError as e:
                self.log.error(f'[{collect_task.platform}] {e}')
        return res


    async def get_accounts(self, query: str, env:str = None,  limit: int = 5) -> List[Account]:
        self.log.info(f'[Telegram] searching for accounts with query: {query}')
        await self.connect(env)

        dialogs = await self.client(functions.contacts.SearchRequest(
            q=query,
            limit=limit,
        ))

        # List variable for all accounts data.
        accounts = self.map_to_accounts(dialogs.chats)
        await self.client.disconnect()
 
        self.log.info(f'[Telegram] {len(accounts)} found')
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
                account = self.map_to_account(account)
                result.append(account)
            except ValueError as e:
                self.log.info("Telegram", e)
        return result


    def map_to_account(self, acc: Account) -> Account:
        mapped_account = Account(
            title=acc.title,
            url='https://t.me/'+acc.username,
            platform=Platform.telegram,
            platform_id=acc.id,
        )
        return mapped_account

    