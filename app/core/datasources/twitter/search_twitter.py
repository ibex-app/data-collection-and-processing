from __future__ import annotations

import os
from datetime import datetime, timedelta
from enum import Enum
from typing import List, Dict

from app.config.aop_config import slf, sleep_after
from searchtweets import (
    collect_results,
    gen_request_parameters,
    load_credentials
)
from app.core.datasources.utils import update_hits_count, validate_posts_by_query, add_search_terms_to_posts, set_account_id, set_total_engagement
import pandas as pd
import re
from itertools import chain
import json
from requests_oauthlib import OAuth1
import requests
from ibex_models import Account, CollectTask, Platform, Post, Scores, SearchTerm


class TweetFields(Enum):
    ATTACHMENTS = 'attachments'
    AUTHOR_ID = 'author_id'
    CONTEXT_ANNOTATIONS = 'context_annotations'
    CONVERSATION_ID = 'conversation_id'
    CREATED_AT = 'created_at'
    ENTITIES = 'entities'
    GEO = 'geo'
    ID = 'id'
    IN_REPLY_TO_USER_ID = 'in_reply_to_user_id'
    LANG = 'lang'
    POSSIBLY_SENSITIVE = 'possibly_sensitive'
    PUBLIC_METRICS = 'public_metrics'
    REFERENCED_TWEETS = 'referenced_tweets'
    REPLY_SETTINGS = 'reply_settings'
    SOURCE = 'source'
    TEXT = 'text'
    WITHHELD = 'withheld'


class PlaceFields(Enum):
    CONTAINED_WITHIN = 'contained_within'
    COUNTRY = 'country'
    COUNTRY_CODE = 'country_code'
    FULL_NAME = 'full_name'
    GEO = 'geo'
    ID = 'id'
    NAME = 'name'
    PLACE_TYPE = 'place_type'


class UserFields(Enum):
    CREATED_AT = 'created_at'
    DESCRIPTION = 'description'
    ENTITIES = 'entities'
    ID = 'id'
    LOCATION = 'location'
    NAME = 'name'
    PINNED_TWEET_ID = 'pinned_tweet_id'
    PROFILE_IMAGE_URL = 'profile_image_url'
    PROTECTED = 'protected'
    PUBLIC_METRICS = 'public_metrics'
    URL = 'url'
    USERNAME = 'username'
    VERIFIED = 'verified'
    WITHHELD = 'withheld'


@slf
class TwitterCollector:
    

    def __init__(self, *args, **kwargs):
        self.max_posts_per_call = 100 # 500 for premium/academic account?
        self.max_requests = 20

        self.max_posts_per_call_sample = 50
        self.max_requests_sample = 1

        self._set_fields(**kwargs)

    @staticmethod
    def _set_field(field, **kwargs):
        if field in kwargs:
            return ','.join([e.value for e in kwargs[field]])
        return None
    
    def load_credentials(self, collect_task:CollectTask):
        self.yaml_path = f"/home/.{collect_task.env}.twitter_keys.yaml"
        self.search_args = load_credentials(self.yaml_path, yaml_key="search_tweets_v2", env_overwrite=False)
        self.count_args = load_credentials(self.yaml_path, yaml_key="count_tweets_v2", env_overwrite=False)


    def _set_fields(self, *args, **kwargs):
        # TODO: remove these lines
        kwargs['tweet_fields'] = [e for e in TweetFields]
        kwargs['place_fields'] = [e for e in PlaceFields]
        kwargs['user_fields'] = [e for e in UserFields]

        self.tweet_fields = self._set_field('tweet_fields', **kwargs)
        self.place_fields = self._set_field('place_fields', **kwargs)
        self.user_fields = self._set_field('user_fields', **kwargs)

    def build_the_query(self, collect_task: CollectTask) -> str:
        query = ''
        if collect_task.query is not None and len(collect_task.query) > 0:
            query = collect_task.query

        if collect_task.accounts is not None and len(collect_task.accounts) > 0:
            accounts_query = ' OR '.join([f'from:{account.platform_id}' for account in collect_task.accounts])
            query = f'({accounts_query}) ' + query
        return query

    async def collect(self, collect_task: CollectTask) -> List[Post]:
        self.load_credentials(collect_task)
        self.max_requests_ = self.max_requests_sample if collect_task.sample else self.max_requests
        self.max_posts_per_call_ = self.max_posts_per_call_sample if collect_task.sample else self.max_posts_per_call

        params = gen_request_parameters(
            query = self.build_the_query(collect_task),
            granularity=False,
            results_per_call=self.max_posts_per_call_,
            start_time=collect_task.date_from.strftime("%Y-%m-%d %H:%M"),
            end_time=collect_task.date_to.strftime("%Y-%m-%d %H:%M"),
            tweet_fields=self.tweet_fields,
            place_fields=self.place_fields,
            user_fields=self.user_fields
        )
        
        tweets = self._collect(params)
        if len(tweets) == 0:
            self.log.success(f'[Twitter] 0 posts collected')
            return []     
            
        df = self._create_df(tweets)
        df = self._standardize(df)
        posts = self._map_to_posts(df, collect_task)
        self.log.success(f'[Twitter] {len(posts)} posts collected')
        
        valid_posts = validate_posts_by_query(collect_task, posts)
        valid_posts = await add_search_terms_to_posts(valid_posts, collect_task.monitor_id)
        self.log.success(f'[Twitter] {len(valid_posts)} valid posts collected')

        return valid_posts

    async def get_hits_count(self, collect_task: CollectTask) -> int:
        self.load_credentials(collect_task)
        params = gen_request_parameters(
            query = self.build_the_query(collect_task) + ' -is:retweet',
            granularity='hour',
            start_time=collect_task.date_from.strftime("%Y-%m-%d %H:%M"),
            end_time=collect_task.date_to.strftime("%Y-%m-%d %H:%M")
        )

        res = collect_results(params, result_stream_args=self.count_args)

        hits_count = 0
        for res_ in res:
            for count in res_['data']:
                hits_count += count['tweet_count']
        self.log.info(f'[Twitter] Hits count - {hits_count}')

        return hits_count


    @sleep_after(tag='Twitter')
    def _collect_tweets_by_rule(self, rule):
        return collect_results(rule, max_tweets=self.max_posts_per_call_, result_stream_args=self.search_args)


    @staticmethod
    def _upd_rule(tweets, rule):
        rule_json = json.loads(rule)
        rule_json['next_token'] = tweets[-1]["meta"]["next_token"]
        rule = json.dumps(rule_json)
        return rule


    def _collect(self, rule):
        tweets = []
        
        count_requests = 0
        while True:
            twts = self._collect_tweets_by_rule(rule)
            tweets += twts
            if len(twts) == 0 or len(tweets) == 0:
                self.log.warn(f'[Twitter] something went wrong - api response is empty, breaking loop..')
                break

            count_requests += 1
            if count_requests >= self.max_requests_:
                self.log.success(f'[Twitter] limit of {self.max_requests_} requests has been reached for query.')
                break

            if "next_token" not in tweets[-1]["meta"]:
                self.log.warn(f'[Twitter] next_token not present in api response, breaking loop..')
                break

            rule = self._upd_rule(tweets, rule)

        return tweets


    @staticmethod
    def _create_df(tweets):
        if len(tweets) == 0:
            return pd.DataFrame()

        all_tweets = list(chain.from_iterable([t["data"] for t in tweets]))

        key_groups = ["public_metrics", "entities"]
        for key_group in key_groups:

            tweets_with_entities = [
                tweet for tweet in all_tweets if key_group in tweet.keys()]
            if len(tweets_with_entities) == 0:
                continue

            tweet_with_entities = tweets_with_entities[0]

            for key in tweet_with_entities[key_group].keys():
                for tweet in all_tweets:
                    if (key_group not in tweet.keys() or
                            key not in tweet[key_group].keys()):
                        continue

                    tweet[f"{key_group}_{key}"] = tweet[key_group][key]

        df = pd.DataFrame(all_tweets)

        for key_group in key_groups:
            if key_group in df.columns:
                df = df.drop(key_group, 1)

        return df


    @staticmethod
    def _standardize(df: pd.DataFrame):
        
        df["platform_id"] = df["id"]
        df = df.drop("id", 1)

        return df


    @staticmethod
    def map_to_post(api_post: pd.Series, collect_task: CollectTask) -> Post:
        # create scores class
        likes = api_post['public_metrics_like_count'] if 'public_metrics_like_count' in api_post else None
        shares = api_post['public_metrics_retweet_count'] if 'public_metrics_retweet_count' in api_post else None
        reply_count = api_post['public_metrics_reply_count'] if 'public_metrics_reply_count' in api_post else 0
        quote_count = api_post['public_metrics_quote_count'] if 'public_metrics_quote_count' in api_post else 0
        
        engagement = reply_count + quote_count
        scores = Scores(likes=likes,
                        shares=shares,
                        engagement=engagement)
        # post_text = 
        # create post class
        # title = re.sub('Twitter Web App|SocialFlow|Twitter for iPhone|Twitter for Android|Twitter for iPad', '', api_post['text'])
        post = Post(title=api_post['text'],
                             text='', #api_post['source'],
                             created_at=api_post['created_at'],
                             platform=Platform.twitter,
                             platform_id=api_post['platform_id'],
                             author_platform_id=api_post['author_id'] if 'author_id' in api_post else None,
                             scores=scores,
                             url = f"https://twitter.com/{api_post['author_id']}/status/{api_post['platform_id']}",
                             api_dump=dict(**api_post))
        # print('[Twitter] setting  account_id')                              
        post = set_account_id(post, collect_task)
        post = set_total_engagement(post)
        return post


    def _map_to_posts(self, df: pd.DataFrame, collect_task: CollectTask) -> List[Post]:
        posts = []
        for obj in df.iterrows():
            try:
                o = obj[1]
                post = TwitterCollector.map_to_post(o, collect_task)
                posts.append(post)
            except ValueError as e:
                self.log.error(f'[Twitter] {e}')
        return posts

    async def get_accounts(self, query, env:str = None, limit: int = 5) -> List[Account]:
        """The method is responsible for collecting Accounts
              from platforms.
          Args:
              collect_action(CollectTask): CollectTask object holds
                  all the metadata needed for data collection.
          Returns:
              (List[Account]): List of collected accounts.
          """
        
        self.log.info(f'[Twitter] searching for accounts with query: {query}')
        # parameter for generated metadata
        API_KEY = os.getenv('TWITTER_API_KEY')
        API_SECRET = os.getenv('TWITTER_API_SECRET')
        ACCESS_TOKEN = os.getenv('TWITTER_ACCESS_TOKEN')
        ACCESS_TOKEN_SECRET = os.getenv('TWITTER_ACCESS_TOKEN_SECRET')
        url = 'https://api.twitter.com/1.1/account/verify_credentials.json'
        auth = OAuth1(API_KEY, API_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
        requests.get(url, auth=auth)
        params = dict(q=query)
        r = requests.get('https://api.twitter.com/1.1/users/search.json', params, auth=auth)
        
        # list of posts returned by method
        results: List[any] = r.json()

        # list of accounts with type of Account for every element
        accounts = self.map_to_accounts(results[:limit])
        self.log.info(f'[Twitter] {len(accounts)} found')
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
                print(e)
        return result

    def map_to_acc(self, acc: any) -> Account:
        mapped_account = Account(
            title=acc['screen_name'],
            url='https://twitter.com/'+acc['screen_name'],
            platform=Platform.twitter,
            platform_id=acc['id_str'],
            img=acc['profile_image_url_https']
        )
        return mapped_account

