import os
import requests
import math
from statistics import mean  
from typing import List, Dict
from ibex_models import Post, Scores, CollectTask, Platform, Account
from datetime import datetime
from app.core.datasources.datasource import Datasource
import vk_api
from app.config.aop_config import slf, sleep_after
from app.core.datasources.utils import update_hits_count, validate_posts_by_query, add_search_terms_to_posts, set_account_id, set_total_engagement

@slf
class VKCollector(Datasource):
    """The class for data collection from VKontakte.

        All data collectors/data sources implement
        methods described below.
    """

    def __init__(self, *args, **kwargs):
        self.token = os.getenv('VK_TOKEN')
        self.username = os.getenv('VK_USER')
        self.password = os.getenv('VK_PASS')
        # TODO: double check the limit per post

        # Variable for maximum number of posts per request
        self.max_posts_per_call = 50
        self.max_posts_per_call_sample = 20

        # Variable for maximum number of requests
        self.max_requests = 50
        self.max_requests_sample = 1

    
    def regenerate_token(self):
        self.log.info(f'[VKontakte] regeneration the token')
        vk_session = vk_api.VkApi(self.username, self.password)
        vk_session.auth()
        os.environ["VK_TOKEN"] = vk_session.token['access_token']
        self.token = os.getenv('VK_TOKEN')


    @sleep_after(tag='VKontakte')
    def call_api_sleep(self, url, params):
        return self.call_api(url, params)

    
    def call_api(self, url, params):
        req = requests.get(url, params)
        if 'response' not in req.json():
            self.regenerate_token()
            params['access_token'] = self.token
            req = requests.get(url, params)
        # self.log.info(f'[VKonakte] responce - {req.json()}')
        return req.json()['response']


    def get_posts_by_params(self, params: Dict):

        # Returned data for call of get_posts method.
        data = self.get_posts(params)

        # Variable for terminating while loop if posts are not left.
        has_next = True

        # List for posts to collect all posts data.
        posts = []

        # iterator for checking maximum call of requests.
        post_iterator = 0
        while has_next:
            posts = posts + data['items']
            post_iterator += 1

            if 'next_from' not in data.keys():
                self.log.info(f'[VKontakte] all end of a list been reached')
                break
            
            if post_iterator > self.max_requests_:
                self.log.info(f'[VKontakte] limit of {self.max_requests_} have been reached')
                break

            data = self.get_posts(params, data['next_from'])

        return posts


    def get_api_method(self, params: Dict):
        if 'owner_id' in params and 'query' in params:
            return "wall.search"
        elif 'owner_id' in params and 'query' not in params:
            return "wall.get"
        else:
            return "newsfeed.search"


    def get_posts(self, params: Dict,  next_from=None):
        """ The method is responsible for actual get of data
        Args:
            params - Generated dictionary with all needed metadata
            next_from - Parameter for checking next portion of posts existence
        """
        if next_from is None:
            params['start_from'] = None
        else:
            params['start_from'] = next_from
        
        posts =  self.call_api_sleep(f"https://api.vk.com/method/{self.get_api_method(params)}", params)        
        return posts


    def generate_req_params(self, collect_task: CollectTask):
        """ The method is responsible for generating params
        Args:
            collect_action(CollectTask): CollectTask object holds
                all the metadata.
        """
        params = dict(
            access_token=self.token,
            count=self.max_posts_per_call_,
            start_time=int(collect_task.date_from.strftime('%s')),
            end_time=int(collect_task.date_to.strftime('%s')),
            v=5.81,
        )
        if not collect_task.get_hits_count:
            params['fields'] = ['city', 'connections', 'counters', 'country', 'domain', 'exports', 'followers_count', 'has_photo', 'home_town', 'interests', 'is_no_index', 'first_name','last_name', 'deactivated', 'is_closed', 'military','nickname', 'personal', 'photo_50','relatives', 'schools','screen_name', 'sex', 'timezone', 'verified', 'wall_default', 'next_from']
        if collect_task.query is not None and len(collect_task.query) > 0:
            params['q'] = collect_task.query
        if collect_task.accounts is not None and len(collect_task.accounts) > 0:
            if len(collect_task.accounts) > 1: 
                self.log.error(f'[{collect_task.platform}] Can not collect from multiple pages at the same time')
            params['owner_id'] = int(collect_task.accounts[0].platform_id)
        return params


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
        
        # parameter for generated metadata
        params = self.generate_req_params(collect_task)
        # self.log.info(f'[VKonakte] request posts - {params}')
        # list of posts returned by method
        results: List[any] = self.get_posts_by_params(params)

        self.log.info(f'[VKonakte] posts collected unmapped - {len(results)}')

        # list of posts with type of Post for every element
        posts = self.map_to_posts(results, collect_task)
        self.log.info(f'[VKonakte] posts collected - {len(posts)}')

        valid_posts = validate_posts_by_query(collect_task, posts)
        valid_posts = await add_search_terms_to_posts(valid_posts, collect_task.monitor_id)
        self.log.success(f'[VKontakte] {len(valid_posts)} valid posts collected')
    
        return valid_posts


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
        self.max_posts_per_call_ = 1
        params = self.generate_req_params(collect_task)  # parameters for generated metadata
        self.log.info(f'[VKonakte] request hits count - {params}')
        responce = self.get_posts(params)
        if 'total_count' in responce:
            self.log.info(f'[VKonakte] total_count hits count - {responce["total_count"]}')

            return responce['total_count']

        offset_step_size = 1000 if responce['count'] > 5000 else math.ceil(responce['count']*.2)
        

        date_ = datetime.utcfromtimestamp(responce['items'][0]['date'])
        
        posting_rates = []
        for offset_step in range(1, 4):
            params['offset'] = offset_step_size * offset_step
            responce = self.get_posts(params)
            posting_rates.append((date_ - datetime.utcfromtimestamp(responce['items'][0]['date'])).total_seconds())
            date_ = datetime.utcfromtimestamp(responce['items'][0]['date'])

        mean_posting_rate = mean(posting_rates)
        # self.log.info(f'[VKonakte] mean_posting_rate hits count {mean_posting_rate}')
        avg_count = ((collect_task.date_to - collect_task.date_from).total_seconds() * offset_step_size)/mean_posting_rate
        # self.log.info(f'[VKonakte] avg_count hits count {avg_count}')
        return math.ceil(avg_count)
        

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
            likes= 0 if 'likes' not in api_post else api_post['likes']['count'],
            shares=0 if 'reposts' not in api_post else api_post['reposts']['count'],
            comments = 0 if 'comments' not in api_post else api_post['comments']['count'],
            views = 0 if 'views' not in api_post else api_post['views']['count']
        )
        post = Post(title=api_post['text'] if 'text' in api_post else "",
                        text="",
                        created_at=api_post['date'] if 'date' in api_post else datetime.now(),
                        platform=Platform.vkontakte,
                        platform_id=api_post['id'],
                        author_platform_id=api_post['owner_id'],
                        scores=scores,
                        api_dump=api_post,
                        monitor_id=api_post['id'],
                        url=f'https://vk.com/wall{api_post["owner_id"]}_{api_post["id"]}',
                    )
        post = set_account_id(post, collect_task)
        post = set_total_engagement(post)
        return post


    def map_to_posts(self, posts: List[Dict], collect_task: CollectTask):

        # Variable for collecting posts in one List with type of Post for each element.
        res: List[Post] = []
        for post in posts:
            try:
                # Variable of type Post for adding post to posts list.
                post = self.map_to_post(post, collect_task)
                res.append(post)
            except ValueError as e:
                self.log.error(f'[{collect_task.platform}] {e}')
        return res

    # @abstractmethod
    async def get_accounts(self, query, env:str = None, limit: int = 5) -> List[Account]:
        """The method is responsible for collecting Accounts
              from platforms.
          Args:
              collect_action(CollectTask): CollectTask object holds
                  all the metadata needed for data collection.
          Returns:
              (List[Account]): List of collected accounts.
          """
        self.log.info(f'[VKontakte] searching for accounts with query: {query}')
        # parameter for generated metadata
        params = dict(
            access_token=self.token,
            limit=limit*3,
            fields=[''],
            v=5.82,
            q=query,
            filters='publics,groups'
        )
        
        # results: List[any] = self.call_api("https://api.vk.com/method/search.getHints", params)
        # return []
        results: List[any] = self.call_api("https://api.vk.com/method/search.getHints", params)['items']
        results = [_ for _ in results if _['type'] != 'profile'][:limit]
        # list of accounts with type of Account for every element
        accounts = self.map_to_accounts(results)
        self.log.info(f'[VKontakte] {len(accounts)} found')

        return accounts


    def map_to_accounts(self, accounts: List[any]) -> List[Account]:
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
                self.log.error({Platform.vkontakte}, e)
        return result


    def map_to_acc(self, api_account: any) -> Account:
        group_id = ''
        group_name = ''
        group_photo = ''
        group_url = ''
        if  api_account['type'] == 'group':
            group_id = 0-api_account['group']['id']
            # group_id = api_account['group']['screen_name']
            group_photo = api_account['group']['photo_100']
            group_name = api_account['group']['name']
            group_url = api_account['group']['screen_name']
        if api_account['type'] == 'profile':
            group_id = api_account['profile']['id']
            group_photo = api_account['profile']['first_name']
            group_name = ''
            group_url = ''
        mapped_account = Account(
            title=group_name,
            url='https://vk.com/' + group_url,
            platform=Platform.vkontakte,
            platform_id=group_id,
            img=group_photo,
            api_dump=api_account
        )
        return mapped_account