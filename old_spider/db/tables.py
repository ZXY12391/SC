from sqlalchemy import Table, Column, INTEGER, String, TEXT, TIMESTAMP
from sqlalchemy.dialects.mysql import DATETIME, JSON, LONGTEXT, LONGBLOB

from .basic import metadata

#######################################
# CERT tables                         #
#######################################
# 账号存放表
account_info_for_spider = Table('account_info_for_spider', metadata,
                                Column("id", String(150), primary_key=True),
                                Column("account", String(32)),
                                Column("password", String(32)),
                                Column("token", String(1024)),
                                Column("proxies", JSON),
                                Column("create_time", DATETIME),
                                Column("update_time", DATETIME),
                                Column("email", String(32)),
                                Column("email_password", String(32)),
                                Column("site", String(32)),
                                Column("task_number", INTEGER),
                                Column("alive", INTEGER, default=1, server_default='1'),
                                )

# 关键词相关表
search_keyword_tasks = Table('T_search_keyword_tasks', metadata,
                             Column("id", INTEGER, primary_key=True, autoincrement=True),
                             Column("keyword", String(128), unique=True),
                             Column("crawl_status", INTEGER, default=0),
                             Column("add_time", DATETIME),
                             Column("update_time", DATETIME),
                             )

post_info_search_results = Table('T_post_info_search_results', metadata,
                                 Column("tweet_url", String(128), primary_key=True),
                                 Column("tweet_id", String(64)),
                                 Column("author_url", String(128)),
                                 Column("img_url", String(256)),
                                 Column("img_binary", LONGBLOB),
                                 Column("screen_id", String(64)),
                                 Column("screen_name", String(64)),
                                 Column("author_name", String(128)),
                                 Column("content", LONGTEXT),
                                 Column("tweet_type", String(64)),
                                 Column("tweet_language", String(64)),
                                 Column("comment_count", INTEGER),
                                 Column("retweet_count", INTEGER),
                                 Column("quote_count", INTEGER),
                                 Column("praise_count", INTEGER),
                                 Column("publish_time", DATETIME),
                                 Column("fetch_time", DATETIME),
                                 Column("search_keyword", String(64)),
                                 )

user_info_search_results = Table('T_user_info_search_results', metadata,
                                 Column("user_url", String(128), primary_key=True),
                                 Column("screen_id", String(64)),
                                 Column("screen_name", String(64)),
                                 Column("user_name", String(64)),
                                 Column("img_url", String(256)),
                                 Column("img_binary", LONGBLOB),
                                 Column("image_bs64encode", LONGTEXT),
                                 Column("introduction", TEXT),
                                 Column("location", String(256)),
                                 Column("website", String(128)),
                                 Column("following_count", INTEGER),
                                 Column("followers_count", INTEGER),
                                 Column("tweets_count", INTEGER),
                                 Column("media_tweets_count", INTEGER),
                                 Column("favourites_tweets_count", INTEGER),
                                 Column("is_protected", INTEGER),
                                 Column("search_keyword", String(64)),
                                 Column("fetch_time", DATETIME),
                                 Column("register_time", DATETIME),
                                 )

# 采集用户信息任务表
user_tasks = Table('T_user_tasks', metadata,
                   Column("id", INTEGER, primary_key=True, autoincrement=True),
                   Column("user_url", String(128), unique=True),
                   Column("crawl_status", INTEGER, default=0),
                   Column("add_time", DATETIME),
                   Column("update_time", DATETIME),
                   )

# 用户档案信息表
user_info_results = Table('T_user_info', metadata,
                          Column("user_url", String(128), primary_key=True),
                          Column("screen_id", String(64)),
                          Column("screen_name", String(64)),
                          Column("user_name", String(64)),
                          Column("img_url", String(256)),
                          Column("img_binary", LONGBLOB),
                          Column("image_bs64encode", LONGTEXT),
                          Column("introduction", TEXT),
                          Column("location", String(256)),
                          Column("website", String(128)),
                          Column("following_count", INTEGER),
                          Column("followers_count", INTEGER),
                          Column("tweets_count", INTEGER),
                          Column("media_tweets_count", INTEGER),
                          Column("favourites_tweets_count", INTEGER),
                          Column("is_protected", INTEGER),
                          Column("fetch_time", DATETIME),
                          Column("register_time", DATETIME),
                          )

# 用户关系信息表
user_relationship_results = Table('T_user_relationship', metadata,
                                  Column("user_url", String(128), primary_key=True),
                                  Column("relationship_user_url", String(128), primary_key=True),
                                  Column("relationship_type", INTEGER, primary_key=True),
                                  Column("screen_id", String(64)),
                                  Column("screen_name", String(64)),
                                  Column("user_name", String(64)),
                                  Column("img_url", String(256)),
                                  Column("img_binary", LONGBLOB),
                                  Column("image_bs64encode", LONGTEXT),
                                  Column("introduction", TEXT),
                                  Column("location", String(256)),
                                  Column("website", String(128)),
                                  Column("following_count", INTEGER),
                                  Column("followers_count", INTEGER),
                                  Column("tweets_count", INTEGER),
                                  Column("media_tweets_count", INTEGER),
                                  Column("favourites_tweets_count", INTEGER),
                                  Column("is_protected", INTEGER),
                                  Column("fetch_time", DATETIME),
                                  Column("register_time", DATETIME),
                                  )

post_info_results = Table('T_post_info', metadata,
                          Column("tweet_url", String(128), primary_key=True),
                          Column("tweet_id", String(64)),
                          Column("author_url", String(128)),
                          Column("img_url", String(256)),
                          Column("img_binary", LONGBLOB),
                          Column("screen_id", String(64)),
                          Column("screen_name", String(64)),
                          Column("author_name", String(128)),
                          Column("content", LONGTEXT),
                          Column("tweet_type", String(64)),
                          Column("tweet_language", String(64)),
                          Column("comment_count", INTEGER),
                          Column("retweet_count", INTEGER),
                          Column("quote_count", INTEGER),
                          Column("praise_count", INTEGER),
                          Column("publish_time", DATETIME),
                          Column("fetch_time", DATETIME),
                          )

twitter_cookies = Table('twitter_cookies', metadata,
                          Column("account", String(128), primary_key=True),
                          Column("cookies", TEXT),
                          )

twitter_bot_info = Table('twitter_bot_info', metadata,
                         Column("id", INTEGER, primary_key=True, autoincrement=True),
                         Column("account", String(128)),
                          Column("uid", String(128)),
                         Column("ip", String(200)),
                         Column("password", String(128)),
                         Column('mobile_ua', TEXT),
                         Column('phone', String(128))
                          )