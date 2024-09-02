# coding:utf-8

#######################################
# mysql data attrs settings           #
#######################################
# 在以下属性字典中修改value部分为你的项目数据库表字段属性名（与key对应）
# 不需要的字段信息value值置为''或者删除该键即可（推荐置为''）
project_tables_dict = {'AccountInfoForSpider': {},
                       'UserTasks': {'id': 'id', 'user_url': 'user_url',
                                     'crawl_status': 'crawl_status',
                                     'add_time': 'add_time', 'update_time': 'update_time',
                                     },
                       'UserInfoResults': {'user_url': 'user_url', 'screen_id': 'screen_id',
                                           'screen_name': 'screen_name', 'image_bs64encode': 'image_bs64encode',
                                           'user_name': 'user_name', 'img_url': 'img_url',
                                           'register_time': 'register_time',
                                           'img_binary': 'img_binary', 'introduction': 'introduction',
                                           'location': 'location',
                                           'website': 'website', 'following_count': 'following_count',
                                           'followers_count': 'followers_count',
                                           'tweets_count': 'tweets_count',
                                           'media_tweets_count': 'media_tweets_count',
                                           'favourites_tweets_count': 'favourites_tweets_count',
                                           'is_protected': 'is_protected',
                                           'fetch_time': 'fetch_time',
                                           },
                       'PostInfoResults': {'tweet_url': 'tweet_url', 'tweet_id': 'tweet_id',
                                           'author_url': 'author_url', 'img_binary': 'img_binary',
                                           'screen_id': 'screen_id', 'screen_name': 'screen_name',
                                           'author_name': 'author_name', 'img_url': 'img_url',
                                           'content': 'content', 'tweet_type': 'tweet_type',
                                           'tweet_language': 'tweet_language',
                                           'comment_count': 'comment_count',
                                           'retweet_count': 'retweet_count',
                                           'quote_count': 'quote_count',
                                           'praise_count': 'praise_count',
                                           'publish_time': 'publish_time', 'fetch_time': 'fetch_time',
                                           },
                       'KeywordTasks': {'id': 'id', 'keyword': 'keyword',
                                        'crawl_status': 'crawl_status',
                                        'add_time': 'add_time', 'update_time': 'update_time',
                                        },
                       'UserInfoSearchResults': {'user_url': 'user_url', 'screen_id': 'screen_id',
                                                 'screen_name': 'screen_name',
                                                 'user_name': 'user_name', 'img_url': 'img_url',
                                                 'register_time': 'register_time',
                                                 'img_binary': 'img_binary', 'introduction': 'introduction',
                                                 'location': 'location', 'image_bs64encode': 'image_bs64encode',
                                                 'website': 'website', 'following_count': 'following_count',
                                                 'followers_count': 'followers_count',
                                                 'tweets_count': 'tweets_count',
                                                 'media_tweets_count': 'media_tweets_count',
                                                 'favourites_tweets_count': 'favourites_tweets_count',
                                                 'is_protected': 'is_protected',
                                                 'search_keyword': 'search_keyword', 'fetch_time': 'fetch_time',
                                                 },
                       'PostInfoSearchResults': {'tweet_url': 'tweet_url', 'tweet_id': 'tweet_id',
                                                 'author_url': 'author_url', 'img_binary': 'img_binary',
                                                 'screen_id': 'screen_id', 'screen_name': 'screen_name',
                                                 'author_name': 'author_name', 'img_url': 'img_url',
                                                 'content': 'content', 'tweet_type': 'tweet_type',
                                                 'tweet_language': 'tweet_language',
                                                 'comment_count': 'comment_count',
                                                 'retweet_count': 'retweet_count',
                                                 'quote_count': 'quote_count',
                                                 'praise_count': 'praise_count',
                                                 'publish_time': 'publish_time', 'fetch_time': 'fetch_time',
                                                 'search_keyword': 'search_keyword',
                                                 },
                       'UserRelationshipResults': {'user_url': 'user_url',
                                                   'relationship_user_url': 'relationship_user_url',
                                                   'screen_id': 'screen_id',
                                                   'screen_name': 'screen_name', 'image_bs64encode': 'image_bs64encode',
                                                   'user_name': 'user_name', 'img_url': 'img_url',
                                                   'register_time': 'register_time',
                                                   'img_binary': 'img_binary', 'introduction': 'introduction',
                                                   'location': 'location',
                                                   'website': 'website', 'following_count': 'following_count',
                                                   'followers_count': 'followers_count',
                                                   'tweets_count': 'tweets_count',
                                                   'media_tweets_count': 'media_tweets_count',
                                                   'favourites_tweets_count': 'favourites_tweets_count',
                                                   'is_protected': 'is_protected',
                                                   'fetch_time': 'fetch_time', 'relationship_type': 'relationship_type',
                                                   },
                       'praise_attrs_dict': {},
                       'retweet_attrs_dict': {},
                       'comment_attrs_dict': {},
                       'reply_attrs_dict': {},
                       'TwitterCookies': {"cookies": "cookies", "update_time": "update_time", "account": "account"}
                       }

######################################
# mysql settings                     #
######################################
# db_host = '10.0.12.2'
# db_port = '3306'
# db_user = 'vps'
# db_pass = 'VPS123db!'
# db_name = 'VPS'
# db_type = 'mysql'

# 战资数据库
db_host = '10.0.12.2'
db_user = 'zz'
db_pass = 'ZZ123db!'
db_name = 'WQCol1'
db_port = 3306
db_type = 'mysql'

######################################
# mongodb settings                   #
######################################
#
# mongodb数据库设置---正式使用
mongo_host = '10.0.12.2'
mongo_port = '27017'
mongo_user = 'fquser'
mongo_pass = 'VPS123db!'
mongo_name = 'Fqdl'
mongo_type = 'mongodb'
mongo_uri = '{}://{}:{}@{}:{}/{}'.format(mongo_type, mongo_user, mongo_pass, mongo_host, mongo_port, mongo_name)

# # mongodb数据库设置---测试可信环境
# mongo_host = '10.0.10.36'
# mongo_port = '27017'
# mongo_user = 'fqdluser'
# mongo_pass = 'db85408825'
# mongo_name = 'Fqdl'
# mongo_type = 'mongodb'
# mongo_uri = '{}://{}:{}@{}:{}/{}'.format(mongo_type, mongo_user, mongo_pass, mongo_host, mongo_port, mongo_name)

#######################################
# redis settings                      #
#######################################

# redis 链接信息--正式运行
REDIS_HOST = '10.0.10.36'
REDIS_PORT = '6379'
REDIS_PASS = ''
REDIS_DB = 10
# REDIS_HOST = '172.31.106.104'
# REDIS_PORT = 6379
# REDIS_DB = 10
# REDIS_PARAMS = {
#     "password": "zxcvbn",
# }

# # redis 链接信息--测试可信虚拟机
# REDIS_HOST = '10.0.10.36'
# # REDIS_HOST = '127.0.0.1'
# REDIS_PORT = '6379'
# REDIS_PASS = ''
# REDIS_DB = 4


PFT = 'twitter:proac'  # 列表类型：存储twitter账号的cookies信息和代理信息。元素为一个json字符串{"cookies":"", "proxies":""}
PROXY_POOL = 'twitter:proxy'
# TPT = 'twitter:picture'  # 要爬取的图片任务

# 任务队列
TASK_QUEUE = 'twitter:task'
# 布隆过滤器
BLOOM_FILTER_USER = 'RZ_bloom_filter_twitter_:task'  # 用户任务链接（存用户任务url）
BLOOM_FILTER_POST = 'RZ_bloom_filter_twitter:post'  # 用户推文过滤器（存推文url）
# BLOOM_FILTER_FOLLOWER = 'bloom_filter_twitter:follower'  # 用户follower过滤器（）
# BLOOM_FILTER_FOLLOWING = 'bloom_filter_twitter:following'
BLOOM_FILTER_FOLLOWER = 'RZ_bloom_filter_twitter:follower'
BLOOM_FILTER_FOLLOWING = 'RZ_bloom_filter_twitter:following'
# BLOOM_FILTER_KEYWORD_POST = 'bloom_filter_twitter:keyword_post'
BLOOM_FILTER_KEYWORD_POST = 'RZ_bloom_filter_twitter_:keyword_post'
# BLOOM_FILTER_KEYWORD_POST = 'bloom_filter_twitter:keyword_post_lqd'
BLOOM_FILTER_USER_POST='bloom_filter_twitter_:user_post'
# 分布式锁，锁名字前缀
LOCKER_PREFIX = 'lock:'

# cookies池
SPIDER_ACCOUNT_POOL = 'twitter:spider_account_pool'  # hash 存key（用户名）+value（cookies、proxy、header等信息--json）
SPIDER_ACCOUNT_USING = 'twitter:spider_account_using'  # zset 存用户名+时间戳（分数）
SPIDER_ACCOUNT_ERROR = 'twitter:spider_account_error'   # 存用户名（set）
#######################################
# logs settings                       #
#######################################
log_dir = 'logs'
log_name = 'spider.log'


#######################################
# spider account error codes          #
#######################################
SPIDER_ERROR_CODES = [32, 326, 130, 200]
PROXY_ERROR_CODE = 4

#######################################
# 采集任务delay设置                    #
#######################################
delay_person_all_info_tasks = 30
