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
import pandas as pd
# import os
from itertools import chain
import json

from app.model.datasource import DataSource
from app.model.platform import Platform
from app.model.post_class import Post, Scores
from app.model.search_term import SearchTerm


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
    # yaml_path = "app/core/datasources/twitter/.twitter_keys.yaml"
    yaml_path = ".twitter_keys.yaml"

    def __init__(self, *args, **kwargs):
        self.max_requests = kwargs['max_requests'] if 'max_requests' in kwargs else 1
        self.max_tweets_per_call = kwargs['max_tweets_per_call'] if 'max_tweets_per_call' in kwargs else 10
        self._set_fields(**kwargs)
        print(os.getcwd())
        print(self.yaml_path)
        self.search_args = load_credentials(self.yaml_path,
                                            yaml_key="search_tweets_v2",
                                            env_overwrite=False)

    @staticmethod
    def _set_field(field, **kwargs):
        if field in kwargs:
            return ','.join([e.value for e in kwargs[field]])
        return None

    def _set_fields(self, *args, **kwargs):
        # TODO: remove these lines
        kwargs['tweet_fields'] = [e for e in TweetFields]
        kwargs['place_fields'] = [e for e in PlaceFields]
        kwargs['user_fields'] = [e for e in UserFields]

        self.tweet_fields = self._set_field('tweet_fields', **kwargs)
        self.place_fields = self._set_field('place_fields', **kwargs)
        self.user_fields = self._set_field('user_fields', **kwargs)

    def collect_curated_batch(self,
                              date_from: datetime,
                              date_to: datetime,
                              data_sources: List[DataSource]):
        queries = []
        for ds in data_sources:
            queries.append(f"from:{ds.platform_id}")
        return self.collect(queries=queries, start_date=date_from, end_date=date_to)

    def collect_curated_single(self,
                               date_from: datetime,
                               date_to: datetime,
                               data_source: DataSource):
        query = f"from:{data_source.platform_id}"
        return self.collect(queries=[query], start_date=date_from, end_date=date_to)

    def collect_firehose(self,
                         date_from: datetime,
                         date_to: datetime,
                         search_terms: List[SearchTerm]):
        queries = []
        for search_term in search_terms:
            queries.append(search_term.term)
        return self.collect(queries=queries, start_date=date_from, end_date=date_to)

    def get_query_results_tw(self, queries, startdate, enddate):
        pass

    # @sleep_after(tag='Twitter')
    def _collect_tweets_by_rule(self, rule):
        return collect_results(rule, max_tweets=self.max_tweets_per_call, result_stream_args=self.search_args)

    @staticmethod
    def _upd_rule(tweets, rule):
        rule_json = json.loads(rule)
        rule_json['next_token'] = tweets[-1]["meta"]["next_token"]
        rule = json.dumps(rule_json)

        return rule

    def _collect_tweets(self, queries: List[str], start_date: datetime, end_date: datetime):
        tweets = []
        for query in queries:
            rule = gen_request_parameters(
                query=query,
                granularity=False,
                results_per_call=self.max_tweets_per_call,
                start_time=start_date.strftime("%Y-%m-%d %H:%M"),
                # end_time=end_date.strftime("%Y-%m-%d %H:%M"),
                tweet_fields=self.tweet_fields,
                place_fields=self.place_fields,
                user_fields=self.user_fields
            )

            count_requests = 0
            while True:
                twts = self._collect_tweets_by_rule(rule)
                tweets += twts
                if len(twts) == 0 or len(tweets) == 0:
                    self.log.warn(f'[Twitter] something went wrong - api response is empty, breaking loop..')
                    break

                count_requests += 1
                if count_requests >= self.max_requests:
                    self.log.success(f'[Twitter] limit of {self.max_requests}'
                                     f' requests has been reached for query: {query}.')
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
    def map_to_post(api_post: pd.Series) -> Post:
        # create scores class
        likes = api_post['public_metrics_like_count'] if 'public_metrics_like_count' in api_post else None
        shares = api_post['public_metrics_retweet_count'] if 'public_metrics_retweet_count' in api_post else None
        reply_count = api_post['public_metrics_reply_count'] if 'public_metrics_reply_count' in api_post else 0
        quote_count = api_post['public_metrics_quote_count'] if 'public_metrics_quote_count' in api_post else 0
        engagement = reply_count + quote_count
        scores = Scores(likes=likes,
                        shares=shares,
                        engagement=engagement)

        # create post class
        post_doc = Post(title=api_post['source'],
                             text=api_post['text'],
                             created_at=api_post['created_at'],
                             platform=Platform.twitter,
                             platform_id=api_post['platform_id'],
                             author_platform_id=api_post['author_id'] if 'author_id' in api_post else None,
                             scores=scores,
                             api_dump=dict(**api_post))
        return post_doc

    def _df_to_posts(self, df: pd.DataFrame) -> List[Post]:
        posts = []
        for obj in df.iterrows():
            try:
                o = obj[1]
                post_class = TwitterCollector.map_to_post(o)
                posts.append(post_class)
            except ValueError as e:
                self.log.error(f'[Twitter] {e}')
        return posts

    def collect(self, queries: List[str],
                start_date: datetime, end_date: datetime) -> List[Post]:
        tweets = self._collect_tweets(queries, start_date, end_date)
        df = self._create_df(tweets)
        df = self._standardize(df)
        return self._df_to_posts(df)


# async def test():
#     from app.config.mongo_config import init_mongo
#     await init_mongo()
#     twitter = TwitterCollector()
#     # data_source = await DataSource.find_one()
#     data_source = DataSource(title='', platform_id='katyperry', platform=Platform.twitter, url='')
#     data_source2 = DataSource(title='', platform_id='ddlovato', platform=Platform.twitter, url='')
#     res = twitter.collect_curated_batch(
#         data_sources=[data_source, data_source2],
#         date_from=datetime.today() - timedelta(days=7),
#         date_to=datetime.today() - timedelta(minutes=2))
#     print(res[0])
#
#
# if __name__ == "__main__":
#     # tweet_fields = [e for e in TweetFields]
#     # place_fields = [e for e in PlaceFields]
#     # user_fields = [e for e in UserFields]
#     import asyncio
#     asyncio.run(test())
#     # for obj in res.iterrows():
#     #     o = obj[1]
#     #     post_class = TwitterCollector.map_to_post(o)
