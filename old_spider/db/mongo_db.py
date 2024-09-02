import pymongo
import time
import json

from contextlib import contextmanager

from config.settings import mongo_uri, mongo_name, PROXY_ERROR_CODE
from logger.log import db_logger

client = pymongo.MongoClient(mongo_uri)


class MongoCommonOper:

    @classmethod
    def query_data_by_condition(cls, collection, condition):
        try:
            data = client[mongo_name][collection].find_one(condition)
            # client.close()
            return data
        except Exception as e:
            db_logger.error('查询数据时发生异常,异常信息:{}'.format(e))

    @classmethod
    def query_datas_by_condition(cls, collection, condition=None):
        try:
            datas = client[mongo_name][collection].find(condition)
            # client.close()
            return list(datas)
        except Exception as e:
            db_logger.error('批量查询数据时发生异常,异常信息:{}'.format(e))
            return []

    @classmethod
    def query_datas_by_condition_limit(cls, collection, condition, nums):
        try:
            datas = client[mongo_name][collection].find(condition).limit(nums)
            # client.close()
            return list(datas)
        except Exception as e:
            db_logger.error('批量查询数据（限制条数）时发生异常,异常信息:{}'.format(e))
            return []

    @classmethod
    def insert_one_data(cls, collection, data):
        """
        插入数据
        :param collection:
        :param condition:
        :param data:
        :return:
        """
        try:
            client[mongo_name][collection].insert_one(data)
            # client.close()
        except Exception as e:
            db_logger.error('插入单条数据时发生异常,异常信息:{}'.format(e))

    @classmethod
    def insert_or_update_one_data(cls, collection, condition, data):
        """
        True表示条件查询数据不存在的时候，则插入该数据
        如果不加$set操作符，则会直接进行data替换，可能导致已存在的字段清空
        :param collection:
        :param condition:
        :param data:
        :return:
        """
        try:
            client[mongo_name][collection].update_one(condition, {'$set': data}, True)
            # client.close()
        except Exception as e:
            db_logger.error('插入或更新单条数据时发生异常,异常信息:{}'.format(e))

    @classmethod
    def insert_or_update_multi_array(cls, collection, condition, data, attrs):
        """
        该函数只会更新或插入符合条件的array
        :param collection: 集合
        :param condition: 字典，条件
        :param data: 字典类型
        :param attrs: 需要更新的array数组key
        :return:
        """
        try:
            # 需要更新的属性
            up_attr = {}
            for attr in attrs:
                up_attr[attr] = {'$each': data[attr]}
            # client = cls.get_mongodb_client()
            client[mongo_name][collection].update_one(
                condition,
                # $addToSet用于更新、插入array类型的数据---对于array中的数据进行更新，在之前的数据基础上增加数据，且自动去重
                {'$addToSet': up_attr}, True)
            # client.close()
        except Exception as e:
            db_logger.error('插入或更新列表数据时发生异常,异常信息:{}'.format(e))

    @classmethod
    def update_muti_data(cls, collection, condition, data):
        try:
            client[mongo_name][collection].update_many(condition, {'$set': data}, True)
        except Exception as e:
            db_logger.error('批量更新数据时发生异常,异常信息:{}'.format(e))

    @classmethod
    def delete_attr_by_condition(cls, collection, condition, attr):
        try:
            client[mongo_name][collection].update_many(condition, {"$unset": {attr: 1}})
        except Exception as e:
            db_logger.error('根据条件删除属性时发生异常,异常信息:{}'.format(e))

    @classmethod
    def insert_many_data(cls, collection, data_list):
        """
        批量插入数据（共n条，若其中x条插入失败，y条插入成功，则数据库里成功插入y条）
        :param collection:
        :param data_list:
        :return:
        """
        try:
            client[mongo_name][collection].insert_many(data_list, ordered=False)
        except Exception as e:
            db_logger.error('批量插入数据时发生异常,异常信息:{}'.format(e))

    @classmethod
    def create_index(cls, collection, attr):
        try:
            client[mongo_name][collection].create_index({attr: 1})
        except Exception as e:
            db_logger.error('创建索引时发生异常,异常信息:{}'.format(e))

    @classmethod
    def create_join_index(cls, collection, indexs):
        """
        创建联和索引----创建后该索引对应的键唯一 （unique=True加了才唯一）
        :param collection:
        :param indexs:
        :return:
        """
        try:
            client[mongo_name][collection].create_index(indexs, unique=True)
        except Exception as e:
            db_logger.error('创建索引时发生异常,异常信息:{}'.format(e))

    @classmethod
    def drop_join_index(cls, collection, indexs):
        """
        删除联和索引
        :param collection:
        :param indexs:
        :return:
        """
        try:
            client[mongo_name][collection].drop_index(indexs)
        except Exception as e:
            db_logger.error('创建索引时发生异常,异常信息:{}'.format(e))

    @classmethod
    def delete_one_data(cls, collection, condition):
        try:
            client[mongo_name][collection].delete_one(condition)
        except Exception as e:
            db_logger.error('删除一条数据时发生异常,异常信息:{}'.format(e))

    @classmethod
    def delete_many_data(cls, collection, condition):
        try:
            res = client[mongo_name][collection].delete_many(condition)
            return res.deleted_count
        except Exception as e:
            db_logger.error('删除多条数据时发生异常,异常信息:{}'.format(e))
            return 0

    @classmethod
    def drop_collection(cls, collection):
        try:
            return client[mongo_name][collection].drop()
        except Exception as e:
            db_logger.error('删除集合（数据库表）时发生异常,异常信息:{}'.format(e))

    @classmethod
    def inc_value(cls, collection, condition, attrs_maps):
        try:
            return client[mongo_name][collection].update_one(condition, {"$inc": attrs_maps})
        except Exception as e:
            db_logger.error('自增数据时发生异常,异常信息:{}'.format(e))

    @classmethod
    def get_collection_data_count(cls, collection):
        try:
            return client[mongo_name][collection].count()
        except Exception as e:
            db_logger.error('统计数据量时发生异常,异常信息:{}'.format(e))
            return 0

    @classmethod
    def get_data_count_by_condition(cls, collection, condition):
        try:
            return client[mongo_name][collection].find(condition).count()
        except Exception as e:
            db_logger.error('统计数据量时发生异常,异常信息:{}'.format(e))
            return 0

class MongoSpiderAccountOper(MongoCommonOper):
    colletion = 'account_info_for_spider'

    @classmethod
    def get_normal_proxies(cls, alive=1):
        condition = {'alive': alive, 'site': 'twitter'}
        proxies = cls.query_datas_by_condition(cls.colletion, condition)
        proxies = list(set([json.dumps(p.get('proxies')) for p in proxies]))
        return proxies

    @classmethod
    def get_spider_account(cls, condition):
        return cls.query_data_by_condition(cls.colletion, condition)

    @classmethod
    def get_spider_accounts(cls, alive, task_number):
        return cls.query_datas_by_condition(cls.colletion, {'site': 'twitter', 'alive': alive, 'task_number': task_number})

    @classmethod
    def get_spider_accounts_ignore_task_number(cls, alive_codes):
        return cls.query_datas_by_condition(cls.colletion, {'site': 'twitter', 'alive': {"$in": alive_codes}})

    @classmethod
    def get_error_proxies(cls):
        return cls.query_datas_by_condition(cls.colletion, {'site': 'twitter', 'alive': PROXY_ERROR_CODE})

    @classmethod
    def get_all_proxies(cls):
        return cls.query_datas_by_condition(cls.colletion, {'site': 'twitter', "proxies": {"$ne": None}})

    @classmethod
    def update_spider_account_status(cls, account_name, update_status):
        condition = {'account': account_name}
        update_status['update_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        cls.insert_or_update_one_data(cls.colletion, condition, update_status)

    @classmethod
    def update_spider_account_proxies(cls, account_name, proxies):
        condition = {'account': account_name}
        proxies['update_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        cls.insert_or_update_one_data(cls.colletion, condition, proxies)

    @classmethod
    def insert_or_update_spider_account(cls, spider_account):
        condition = {'account': spider_account.get('account')}
        cls.insert_or_update_one_data(cls.colletion, condition, spider_account)

    @classmethod
    def spider_account_existed(cls, account_name):
        condition = {'account': account_name}
        return cls.query_data_by_condition(cls.colletion, condition)


class MongoUserTaskOper(MongoCommonOper):
    collection = 'RZ_task'

    # 获取任务
    @classmethod
    def get_user_tasks(cls, important_user, nums):
        flag = 0
        task_conditions = {'$or': [{'follower_task': 1, 'follower_crawl_status': flag},
                                   {'following_task': 1, 'following_crawl_status': flag},
                                   {'post_task': 1, 'post_crawl_status': flag},
                                   {'profile_task': 1, 'profile_crawl_status': flag}
                                   ],
                           'important_user': important_user}
        return cls.query_datas_by_condition_limit(collection=cls.collection, condition=task_conditions, nums=nums)

    # 获取用户档案信息爬取任务
    @classmethod
    def get_user_profile_tasks(cls, important_user, nums):
        flag = 0
        task_conditions = {'profile_task': 1,
                           'profile_crawl_status': flag,
                           'important_user': important_user}
        return cls.query_datas_by_condition_limit(collection=cls.collection, condition=task_conditions, nums=nums)

    @classmethod
    def get_user_tasks_by_condition(cls, condition, nums):
        """
        根据条件获取任务
        :param condition:
        :param nums:
        :return:
        """
        return cls.query_datas_by_condition_limit(cls.collection, condition, nums)

    # 设置任务状态
    @classmethod
    def set_user_task_status(cls, user_url, update_status):
        client[mongo_name][cls.collection].update_one({'user_url': user_url}, {'$set': update_status})

    # 设置任务状态
    @classmethod
    def set_important_user(cls, user_url, important_user):
        client[mongo_name][cls.collection].update_one({'user_url': user_url}, {'$set': important_user})

    @classmethod
    def insert_user_task(cls, user_task):
        """
        可实现对已存在的数据不处理，未存在的数据插入
        :param user_task:
        :return:
        """
        if cls.query_data_by_condition(cls.collection, {'user_url': user_task.get('user_url')}):
            return
        cls.insert_one_data(cls.collection, user_task)

    @classmethod
    def is_exit_user_task(cls, user_url):
        if cls.query_data_by_condition(cls.collection, {'user_url': user_url}):
            return True
        return False


class MongoKeywordTaskOper(MongoCommonOper):
    collection = 'RZ_keyword_tasks'

    # 获取任务
    @classmethod
    def get_keyword_tasks(cls):
        """
        :return: 任务列表
        """
        return cls.query_datas_by_condition(collection=cls.collection, condition={'crawl_status': 2})

    # 设置任务状态
    @classmethod
    def set_keyword_task_update_time(cls, keyword, update_time):
        cls.insert_or_update_one_data(cls.collection, {'keyword': keyword}, {'update_time': update_time})


class MongoKeywordTweetOper(MongoCommonOper):
    # collection = 'T_keyword_post_info'
    collection = 'RZ_keyword_post_info'
    # collection = 'T_keyword_post_info_lqd'

    # 新增批量插入推文数据
    @classmethod
    def insert_many_keyword_tweet_data(cls, datas):
        return cls.insert_many_data(cls.collection, datas)

    @classmethod
    def query_tweet_is_existed(cls, tweet_url):
        condition = {'tweet_url': tweet_url}
        return cls.query_data_by_condition(cls.collection, condition)

    @classmethod
    def insert_or_update_keyword_tweet_info(cls, tweet_data):
        condition = {'tweet_url': tweet_data.get('tweet_url')}
        return cls.insert_or_update_one_data(cls.collection, condition, tweet_data)

    @classmethod
    def get_all_tweet(cls):
        return cls.query_datas_by_condition(cls.collection, {})


class MongoUserInfoOper(MongoCommonOper):
    collection = 'RZ_profile'#'RZ_profile'

    @classmethod
    def insert_or_update_user_basic_info(cls, user_data):
        condition = {'user_url': user_data.get('user_url')}
        return cls.insert_or_update_one_data(cls.collection, condition, user_data)

    @classmethod
    def insert_or_update_user_topic_info(cls, screen_id, topic_data):
        condition = {'screen_id': screen_id}
        attrs = ['topic']
        return cls.insert_or_update_multi_array(cls.collection, condition, topic_data, attrs)

    @classmethod
    def get_user_info_data(cls, user_url):
        condition = {'user_url': user_url}
        return cls.query_data_by_condition(cls.collection, condition)


class MongoUserTweetOper(MongoCommonOper):
    collection = 'RZ_post_for_vertfiy_temp'

    @classmethod
    def query_tweet_is_existed(cls, tweet_url):
        condition = {'tweet_url': tweet_url}
        return cls.query_data_by_condition(cls.collection, condition)

    @classmethod
    def get_tweet_image_tasks(cls, nums):
        condition = {"tweet_img_url": {"$ne": None}, "tweet_img_binary": None}
        return cls.query_datas_by_condition_limit(cls.collection, condition, nums)

    @classmethod
    def get_tweet_tourist_tasks(cls, nums):
        tourist_number = 3
        condition = {"tweet_type": {"$ne": ["转发", "引用"]},
                     "$or": [{"praise_count": {"$gte": tourist_number}}, {"comment_count": {"$gte": tourist_number}},
                             {"retweet_count": {"$gte": tourist_number}}],
                     "tourist_crawl_status": 0
                     }
        return cls.query_datas_by_condition_limit(cls.collection, condition, nums)

    @classmethod
    def insert_or_update_user_tweet_info(cls, tweet_data):
        condition = {'tweet_url': tweet_data.get('tweet_url')}
        return cls.insert_or_update_one_data(cls.collection, condition, tweet_data)

    @classmethod
    def insert_or_update_tweet_image(cls, tweet_url, image_data):
        condition = {'tweet_url': tweet_url}
        return cls.insert_or_update_one_data(cls.collection, condition, image_data)

    @classmethod
    def update_tourist_crawl_status(cls, tweet_url, update_status):
        condition = {'tweet_url': tweet_url}
        return cls.insert_or_update_one_data(cls.collection, condition, update_status)

    # 新增批量插入推文数据
    @classmethod
    def insert_many_tweet_data(cls, datas):
        return cls.insert_many_data(cls.collection, datas)


class MongoUserRelationshipOper(MongoCommonOper):
    collection = 'RZ_relationship'

    @classmethod
    def query_relationship_is_existed(cls, user_url, relationship_user_url, relationship_type):
        condition = {'user_url': user_url, 'relationship_user_url': relationship_user_url,
                     'relationship_type': relationship_type}
        return cls.query_data_by_condition(cls.collection, condition)

    @classmethod
    def insert_or_update_user_relationship_info(cls, tweet_data):
        condition = {'user_url': tweet_data.get('user_url'),
                     'relationship_user_url': tweet_data.get('relationship_user_url'),
                     'relationship_type': tweet_data.get('relationship_type')
                     }
        return cls.insert_or_update_one_data(cls.collection, condition, tweet_data)

    @classmethod
    def get_avatar_tasks(cls, nums):
        condition = {"avatar_url": {"$ne": [None, '']}, "avatar_binary": None}
        return cls.query_datas_by_condition_limit(cls.collection, condition, nums)

    @classmethod
    def insert_or_update_avatar(cls, relationship_user_url, avatar_data):
        condition = {'relationship_user_url': relationship_user_url}
        return cls.update_muti_data(cls.collection, condition, avatar_data)

    @classmethod
    def get_topic_tasks(cls, nums):
        condition = {"topic": None}
        return cls.query_datas_by_condition_limit(cls.collection, condition, nums)

    @classmethod
    def insert_or_update_topic(cls, screen_id, topic_data):
        condition = {'screen_id': screen_id}
        return cls.update_muti_data(cls.collection, condition, topic_data)

    @classmethod
    def insert_or_update_yz_bot_data(cls, data):
        condition = {'url': data.get('url')}
        collection = 'social_bot_task'
        return cls.insert_or_update_one_data(collection, condition, data)

    # 新增批量插入关系数据
    @classmethod
    def insert_many_relationship_data(cls, datas):
        return cls.insert_many_data(cls.collection, datas)


class MongoUserTweetTouristOper(MongoCommonOper):
    collection = 'RZ_tourist'

    # 新增批量插入点赞者数据
    @classmethod
    def insert_many_praise_data(cls, datas):
        return cls.insert_many_data(cls.collection, datas)

    # 新增批量插入转发者数据
    @classmethod
    def insert_many_retweet_data(cls, datas):
        return cls.insert_many_data(cls.collection, datas)

    # 新增批量插入评论者和回复者数据
    @classmethod
    def insert_many_comment_reply_data(cls, datas):
        return cls.insert_many_data(cls.collection, datas)

    @classmethod
    def insert_or_update_tweet_praise(cls, data):
        condition = {'tweet_url': data.get('tweet_url'), 'author_url': data.get('author_url'),
                     'user_url': data.get('user_url'), 'praise': data.get('praise')}
        return cls.insert_or_update_one_data(cls.collection, condition, data)

    @classmethod
    def insert_or_update_tweet_retweet(cls, data):
        condition = {'tweet_url': data.get('tweet_url'), 'author_url': data.get('author_url'),
                     'user_url': data.get('user_url'), 'retweet': data.get('retweet')}
        return cls.insert_or_update_one_data(cls.collection, condition, data)

    @classmethod
    def insert_or_update_tweet_comment(cls, data):
        condition = {'tweet_url': data.get('tweet_url'), 'author_url': data.get('author_url'),
                     'user_url': data.get('user_url'), 'comment': data.get('comment')}
        return cls.insert_or_update_one_data(cls.collection, condition, data)

    @classmethod
    def insert_or_update_tweet_reply(cls, data):
        condition = {'tweet_url': data.get('tweet_url'), 'author_url': data.get('author_url'),
                     'user_url': data.get('user_url'), 'reply': data.get('reply')}
        return cls.insert_or_update_one_data(cls.collection, condition, data)

    @classmethod
    def get_avatar_tasks(cls, nums):
        condition = {"avatar_url": {"$ne": [None, '']}, "avatar_binary": None}
        return cls.query_datas_by_condition_limit(cls.collection, condition, nums)

    @classmethod
    def insert_or_update_avatar(cls, user_url, avatar_data):
        condition = {'user_url': user_url}
        return cls.update_muti_data(cls.collection, condition, avatar_data)

    @classmethod
    def get_topic_tasks(cls, nums):
        condition = {"topic": None, "screen_id": {"$ne": None}}
        return cls.query_datas_by_condition_limit(cls.collection, condition, nums)

    @classmethod
    def insert_or_update_topic(cls, screen_id, topic_data):
        condition = {'screen_id': screen_id}
        return cls.update_muti_data(cls.collection, condition, topic_data)


class MongoCrawlerStatusOper(MongoCommonOper):
    collection = 'crawler_status'

    @classmethod
    def inc_crawler_status(cls, spider_name):
        condition = {'name': spider_name}
        attrs_maps = {'crawl_status': 1}
        cls.inc_value(cls.collection, condition, attrs_maps)

    @classmethod
    def sub_crawler_status(cls, spider_name):
        condition = {'name': spider_name}
        attrs_maps = {'crawl_status': -1}
        cls.inc_value(cls.collection, condition, attrs_maps)

    @classmethod
    def update_crawl_status_info(cls, spider_name, attrs_maps):
        condition = {'name': spider_name}
        cls.insert_or_update_one_data(cls.collection, condition, attrs_maps)


# ####################### 测试 ############################
# 插入任务
def insert_user_task_data():
    import time
    data = {
        'user_url': 'www.f.com',
        'user_name': 'f',
        'important_user': 0,
        'add_time': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
        'update_time': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),

        'follower_task': 1,
        'following_task': 1,
        'post_task': 1,
        'profile_task': 1,

        'post_praise_task': 1,
        'post_repost_task': 1,
        'post_comment_task': 1,
        'post_reply_task': 1,

        'post_crawl_status': 0,
        'following_crawl_status': 0,
        'follower_crawl_status': 0,
        'profile_crawl_status': 0,
        'user_tag': None,
    }
    MongoCommonOper.insert_or_update_one_data(collection='T_user_tasks',
                                              condition={'user_url': data.get('user_url')},
                                              data=data)


def insert_keyword_task_data(keyword):
    import time
    data = {
        'keyword': keyword,
        'crawl_status': 0,
        'add_time': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
        'update_time': None,
    }
    MongoCommonOper.insert_or_update_one_data(collection='keyword_tasks',
                                              condition={'keyword': data.get('keyword')},
                                              data=data)


def insert_user_profile_data():
    import time
    publish_time = time.strptime('Wed Jan 06 23:23:59 +0000 2021', "%a %b %d %H:%M:%S +0000 %Y")
    publish_time = time.strftime("%Y-%m-%d %H:%M:%S", publish_time)


def delete_noentity_important_task():
    """
    删除未采集到信息的重点用户
    :return:
    """
    tasks = MongoCommonOper.query_datas_by_condition('T_user_task', {'important_user': 1})
    print(len(tasks))
    for task in tasks:
        if not MongoCommonOper.query_data_by_condition('T_user_profile', {'user_url': task.get('user_url')}):
            MongoCommonOper.delete_one_data('T_user_task', {'user_url': task.get('user_url')})
            print(task.get('user_url'))


def create_bot_table():
    # T bot info
    data = {'account': 'test_account', 'uid': None, 'name': None, 'ip': None, 'port': None, 'description': None, 'place': None, 'enable': None, 'password': None, 'pc_ua': None,
            'inited': None, 'create_time': None, 'phone': None, 'email': None, 'person_url': None, 'first_login': None, 'birth_year': None, 'birth_month': None, 'birth_day': None,
            'target_user_url': None,
            'cookies': None, 'update_time': None}
    # F bot info
    data = {'account': 'test_account', 'appr_code': None, 'name': None, 'ip': None, 'port': None, 'bio': None, 'current_city': None, 'hometown': None,
            'enable': None, 'password': None, 'pc_ua': None,
            'inited': None, 'create_time': None, 'phone': None, 'email': None, 'first_login': None, 'birth_year': None, 'birth_month': None, 'birth_day': None,
            'target_user_url': None,
            'cookies': None, 'update_time': None}
    # T action info
    data = {'account': 'test_account', 'password': None, 'uid': None, 'ip': None, 'port': None, 'prepare_time': None,
            'act_time': None, 'is_executed': None, 'pc_ua': None, 'is_succeeded': None, 'first_login': None,
            'ip_last_act_time': None, 'detail': None, 'action_info': None, 'failed_action_info': None, 'inited': None,
            # '': None, '': None, '': None, '': None,
            # '': 'test_account', '': None, '': None, '': None, '': None, '': None, '': None, '': None, '': None,
            # '': None
            }
    # F action info
    data = {'account': 'test_account', 'password': None, 'appr_code': None, 'ip': None, 'port': None, 'prepare_time': None,
            'act_time': None, 'is_executed': None, 'pc_ua': None, 'is_succeeded': None, 'first_login': None,
            'ip_last_act_time': None, 'detail': None, 'action_info': None, 'failed_action_info': None, 'inited': None,

            }
    MongoCommonOper.insert_or_update_one_data('F_action_info', {'account': data['account']}, data)
# ####################### 测试 ############################


# 插入强智杯待采集用户
def insert_qzb_tasks():
    user_urls = ['https://twitter.com/Senem42586713', 'https://twitter.com/ze76247193', 'https://twitter.com/GenPrev', 'https://twitter.com/nHYtAiBFm0HmVQY', 'https://twitter.com/Sam00070683', 'https://twitter.com/Nrgzee', 'https://twitter.com/nigelsleftboot', 'https://twitter.com/nvanderklippe', 'https://twitter.com/YusufAhad1', 'https://twitter.com/iamnanatna', 'https://twitter.com/abduniyazi', 'https://twitter.com/tibetinitiative', 'https://twitter.com/Marswe4', 'https://twitter.com/alicanlier', 'https://twitter.com/show_awarenes', 'https://twitter.com/Adil0621', 'https://twitter.com/TLaptonHKer', 'https://twitter.com/kitabhumar101', 'https://twitter.com/PUyguhur', 'https://twitter.com/xinjiangreview', 'https://twitter.com/IsraelGervais', 'https://twitter.com/VoiceRohingya', 'https://twitter.com/eccolospacelab', 'https://twitter.com/757747544', 'https://twitter.com/honolulu_lululu', 'https://twitter.com/mewlude5', 'https://twitter.com/WILL10833273', 'https://twitter.com/Khalidraxidgma1', 'https://twitter.com/menevere', 'https://twitter.com/Altenaier', 'https://twitter.com/2rbsLYkunz7tLBK', 'https://twitter.com/David49645926', 'https://twitter.com/tursun_alimjan', 'https://twitter.com/birayim1', 'https://twitter.com/Chinar02602129', 'https://twitter.com/KawapPoloLehman', 'https://twitter.com/AlePulmocare', 'https://twitter.com/VOAChinese', 'https://twitter.com/abdulla58261760', 'https://twitter.com/MattCKnight', 'https://twitter.com/bjliu6', 'https://twitter.com/Adiljan70939416', 'https://twitter.com/murat40846248', 'https://twitter.com/UyghurAhmat', 'https://twitter.com/EmilyTwinings', 'https://twitter.com/HasanUyghur', 'https://twitter.com/joe_leo_', 'https://twitter.com/Uyguriye', 'https://twitter.com/Zeena_5', 'https://twitter.com/AbdulazizSabit1', 'https://twitter.com/Alaman58369814', 'https://twitter.com/IUyghurs', 'https://twitter.com/esin72589029', 'https://twitter.com/aiziheer', 'https://twitter.com/WZDkm3onCce4Uh1', 'https://twitter.com/Y__Kurum', 'https://twitter.com/UyghurQ', 'https://twitter.com/AAyqin', 'https://twitter.com/dweenzers', 'https://twitter.com/drusamahasan', 'https://twitter.com/NaNa45715776', 'https://twitter.com/Tom97500077', 'https://twitter.com/Muhamme17601728', 'https://twitter.com/Kary50981323', 'https://twitter.com/Je5786', 'https://twitter.com/AvKilinboz', 'https://twitter.com/fahribozgeyik', 'https://twitter.com/Abdulqa10305852', 'https://twitter.com/hk777pk', 'https://twitter.com/Winterstorm2021', 'https://twitter.com/uighur2019', 'https://twitter.com/Nmuhammed998', 'https://twitter.com/AAhed024', 'https://twitter.com/HKer_Fighting_', 'https://twitter.com/TheOneHK1', 'https://twitter.com/Amina_Lone', 'https://twitter.com/AlimBughra', 'https://twitter.com/charkenzie', 'https://twitter.com/Hasanen48178678', 'https://twitter.com/bIackobject', 'https://twitter.com/UguzBugrahan', 'https://twitter.com/riyazulkhaliq', 'https://twitter.com/Abdulra32695440', 'https://twitter.com/takemori888', 'https://twitter.com/xiami3271', 'https://twitter.com/TarimUyghur1', 'https://twitter.com/tengritagdoliti', 'https://twitter.com/KenRoth', 'https://twitter.com/manaiyosri1', 'https://twitter.com/ihlasmanvip', 'https://twitter.com/matthew_petti', 'https://twitter.com/SenRubioPress', 'https://twitter.com/Perhat34001377', 'https://twitter.com/sabriuyghur', 'https://twitter.com/GhubarimE', 'https://twitter.com/Ana12323213', 'https://twitter.com/UmmuMeryem10', 'https://twitter.com/hk13974688', 'https://twitter.com/BaburIlchi', 'https://twitter.com/uyghur_man', 'https://twitter.com/frostychatteNZ', 'https://twitter.com/UlughBeg2', 'https://twitter.com/muslima520', 'https://twitter.com/Bharatniti', 'https://twitter.com/turanuygur1', 'https://twitter.com/Aziz51502129', 'https://twitter.com/YTohti', 'https://twitter.com/officeuy', 'https://twitter.com/SOLOMONHIGHEND', 'https://twitter.com/AYigiti', 'https://twitter.com/mmmmmuee', 'https://twitter.com/bds_china', 'https://twitter.com/GuvenlikKino', 'https://twitter.com/kahin22400761', 'https://twitter.com/malohaaloha', 'https://twitter.com/stoop_k', 'https://twitter.com/MKayranci', 'https://twitter.com/SabetSidik', 'https://twitter.com/uyghurrr', 'https://twitter.com/Bunyamin_Atmaca', 'https://twitter.com/HappyEn00321420', 'https://twitter.com/wilfredchan', 'https://twitter.com/Kchung71685436', 'https://twitter.com/imtiyaz33721842', 'https://twitter.com/abdusal81969007', 'https://twitter.com/mantrk1', 'https://twitter.com/bore0626', 'https://twitter.com/zorlyar', 'https://twitter.com/Freedom26124892', 'https://twitter.com/insan105', 'https://twitter.com/firelight61047', 'https://twitter.com/MMamteli', 'https://twitter.com/IlxatC', 'https://twitter.com/HashimjanKashg4', 'https://twitter.com/FonsecaVintage', 'https://twitter.com/Marcofung14', 'https://twitter.com/ayshabkhan', 'https://twitter.com/hpeaks']
    # user_urls = []
    user_urls = ['https://twitter.com/aliceysu','https://twitter.com/jotted','https://twitter.com/cinaforum']
    datas = []
    for url in user_urls:
        # 存入用户任务表
        user_task = {
            'user_url': url,
            'user_name': 'qzb_malicious_user',
            'important_user': 0,
            'add_time': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
            'update_time': None,

            'follower_task': 1,
            'following_task': 1,
            'post_task': 1,
            'profile_task': 1,

            'post_praise_task': 1,
            'post_repost_task': 1,
            'post_comment_task': 1,
            'post_reply_task': 1,

            'post_crawl_status': 0,
            'following_crawl_status': 0,
            'follower_crawl_status': 0,
            'profile_crawl_status': 0,
            'tourist_crawl_status': 0,

            'user_tag': None,
        }
        datas.append(user_task)
    MongoUserTaskOper.insert_many_data('qzb_T_user_task', datas)


# 2022-08-15 临时立场数据采集———张中尧找的用户
def insert_stance_user_tasks_zzy():
    import pandas as pd
    user_tasks = pd.read_excel('../../twitter.xlsx', index_col=False)['用户url'].to_list()  # 147个
    user_urls = list(map(lambda x: x.strip(), user_tasks))
    datas = []
    for url in user_urls:
        # 存入用户任务表
        user_task = {
            'user_url': url,
            'user_name': '',
            'important_user': 0,
            'add_time': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
            'update_time': None,

            'follower_task': 1,
            'following_task': 1,
            'post_task': 1,
            'profile_task': 1,

            'post_praise_task': 1,
            'post_repost_task': 1,
            'post_comment_task': 1,
            'post_reply_task': 1,

            'post_crawl_status': 0,
            'following_crawl_status': 0,
            'follower_crawl_status': 0,
            'profile_crawl_status': 0,
            'tourist_crawl_status': 0,

            'user_tag': None,
        }
        datas.append(user_task)
    MongoUserTaskOper.insert_many_data('negative_stance_user_tasks', datas)


# 2022-08-15 临时立场数据采集——sc数据库中已有的用户
def insert_stance_user_tasks_exited():
    import pandas as pd
    user_tasks = pd.read_csv('../../sc_open_source_person_task.csv', index_col=False)  # 33个，其中有一个和zzy找的147个重复
    user_tasks = user_tasks[user_tasks['twitter_url'].notnull()]['twitter_url'].to_list()

    user_urls = list(map(lambda x: x.strip(), user_tasks))
    datas = []
    for url in user_urls:
        # 存入用户任务表
        user_task = {
            'user_url': url,
            'user_name': 'sc_open_source_person_task',
            'important_user': 0,
            'add_time': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
            'update_time': None,

            'follower_task': 1,
            'following_task': 1,
            'post_task': 1,
            'profile_task': 1,

            'post_praise_task': 1,
            'post_repost_task': 1,
            'post_comment_task': 1,
            'post_reply_task': 1,

            'post_crawl_status': 0,
            'following_crawl_status': 0,
            'follower_crawl_status': 0,
            'profile_crawl_status': 0,
            'tourist_crawl_status': 0,

            'user_tag': None,
        }
        if MongoCommonOper.query_data_by_condition('negative_stance_user_tasks', {'user_url': url}):
            print(url, '已存在')
            continue
        MongoUserTaskOper.insert_one_data('negative_stance_user_tasks', user_task)
        # datas.append(user_task)
    # MongoUserTaskOper.insert_many_data('negative_stance_user_tasks', datas)


# 获取强智杯恶意用户档案信息
def get_malicious_profile():
    user_urls = ['https://twitter.com/Senem42586713', 'https://twitter.com/ze76247193', 'https://twitter.com/GenPrev',
                 'https://twitter.com/nHYtAiBFm0HmVQY', 'https://twitter.com/Sam00070683', 'https://twitter.com/Nrgzee',
                 'https://twitter.com/nigelsleftboot', 'https://twitter.com/nvanderklippe',
                 'https://twitter.com/YusufAhad1', 'https://twitter.com/iamnanatna', 'https://twitter.com/abduniyazi',
                 'https://twitter.com/tibetinitiative', 'https://twitter.com/Marswe4', 'https://twitter.com/alicanlier',
                 'https://twitter.com/show_awarenes', 'https://twitter.com/Adil0621', 'https://twitter.com/TLaptonHKer',
                 'https://twitter.com/kitabhumar101', 'https://twitter.com/PUyguhur',
                 'https://twitter.com/xinjiangreview', 'https://twitter.com/IsraelGervais',
                 'https://twitter.com/VoiceRohingya', 'https://twitter.com/eccolospacelab',
                 'https://twitter.com/757747544', 'https://twitter.com/honolulu_lululu', 'https://twitter.com/mewlude5',
                 'https://twitter.com/WILL10833273', 'https://twitter.com/Khalidraxidgma1',
                 'https://twitter.com/menevere', 'https://twitter.com/Altenaier', 'https://twitter.com/2rbsLYkunz7tLBK',
                 'https://twitter.com/David49645926', 'https://twitter.com/tursun_alimjan',
                 'https://twitter.com/birayim1', 'https://twitter.com/Chinar02602129',
                 'https://twitter.com/KawapPoloLehman', 'https://twitter.com/AlePulmocare',
                 'https://twitter.com/VOAChinese', 'https://twitter.com/abdulla58261760',
                 'https://twitter.com/MattCKnight', 'https://twitter.com/bjliu6', 'https://twitter.com/Adiljan70939416',
                 'https://twitter.com/murat40846248', 'https://twitter.com/UyghurAhmat',
                 'https://twitter.com/EmilyTwinings', 'https://twitter.com/HasanUyghur', 'https://twitter.com/joe_leo_',
                 'https://twitter.com/Uyguriye', 'https://twitter.com/Zeena_5', 'https://twitter.com/AbdulazizSabit1',
                 'https://twitter.com/Alaman58369814', 'https://twitter.com/IUyghurs',
                 'https://twitter.com/esin72589029', 'https://twitter.com/aiziheer',
                 'https://twitter.com/WZDkm3onCce4Uh1', 'https://twitter.com/Y__Kurum', 'https://twitter.com/UyghurQ',
                 'https://twitter.com/AAyqin', 'https://twitter.com/dweenzers', 'https://twitter.com/drusamahasan',
                 'https://twitter.com/NaNa45715776', 'https://twitter.com/Tom97500077',
                 'https://twitter.com/Muhamme17601728', 'https://twitter.com/Kary50981323',
                 'https://twitter.com/Je5786', 'https://twitter.com/AvKilinboz', 'https://twitter.com/fahribozgeyik',
                 'https://twitter.com/Abdulqa10305852', 'https://twitter.com/hk777pk',
                 'https://twitter.com/Winterstorm2021', 'https://twitter.com/uighur2019',
                 'https://twitter.com/Nmuhammed998', 'https://twitter.com/AAhed024',
                 'https://twitter.com/HKer_Fighting_', 'https://twitter.com/TheOneHK1',
                 'https://twitter.com/Amina_Lone', 'https://twitter.com/AlimBughra', 'https://twitter.com/charkenzie',
                 'https://twitter.com/Hasanen48178678', 'https://twitter.com/bIackobject',
                 'https://twitter.com/UguzBugrahan', 'https://twitter.com/riyazulkhaliq',
                 'https://twitter.com/Abdulra32695440', 'https://twitter.com/takemori888',
                 'https://twitter.com/xiami3271', 'https://twitter.com/TarimUyghur1',
                 'https://twitter.com/tengritagdoliti', 'https://twitter.com/KenRoth',
                 'https://twitter.com/manaiyosri1', 'https://twitter.com/ihlasmanvip',
                 'https://twitter.com/matthew_petti', 'https://twitter.com/SenRubioPress',
                 'https://twitter.com/Perhat34001377', 'https://twitter.com/sabriuyghur',
                 'https://twitter.com/GhubarimE', 'https://twitter.com/Ana12323213', 'https://twitter.com/UmmuMeryem10',
                 'https://twitter.com/hk13974688', 'https://twitter.com/BaburIlchi', 'https://twitter.com/uyghur_man',
                 'https://twitter.com/frostychatteNZ', 'https://twitter.com/UlughBeg2',
                 'https://twitter.com/muslima520', 'https://twitter.com/Bharatniti', 'https://twitter.com/turanuygur1',
                 'https://twitter.com/Aziz51502129', 'https://twitter.com/YTohti', 'https://twitter.com/officeuy',
                 'https://twitter.com/SOLOMONHIGHEND', 'https://twitter.com/AYigiti', 'https://twitter.com/mmmmmuee',
                 'https://twitter.com/bds_china', 'https://twitter.com/GuvenlikKino',
                 'https://twitter.com/kahin22400761', 'https://twitter.com/malohaaloha', 'https://twitter.com/stoop_k',
                 'https://twitter.com/MKayranci', 'https://twitter.com/SabetSidik', 'https://twitter.com/uyghurrr',
                 'https://twitter.com/Bunyamin_Atmaca', 'https://twitter.com/HappyEn00321420',
                 'https://twitter.com/wilfredchan', 'https://twitter.com/Kchung71685436',
                 'https://twitter.com/imtiyaz33721842', 'https://twitter.com/abdusal81969007',
                 'https://twitter.com/mantrk1', 'https://twitter.com/bore0626', 'https://twitter.com/zorlyar',
                 'https://twitter.com/Freedom26124892', 'https://twitter.com/insan105',
                 'https://twitter.com/firelight61047', 'https://twitter.com/MMamteli', 'https://twitter.com/IlxatC',
                 'https://twitter.com/HashimjanKashg4', 'https://twitter.com/FonsecaVintage',
                 'https://twitter.com/Marcofung14', 'https://twitter.com/ayshabkhan', 'https://twitter.com/hpeaks']
    datas = []
    for url in user_urls:
        res = MongoCommonOper.query_data_by_condition('T_user_profile', {'user_url': url})
        if res:
            datas.append(res)

    import pandas as pd
    datas = pd.DataFrame(datas)
    datas.to_csv('../qzb_data/crawl_qzb_malicious_user_profile.csv', index=False)
    print(datas.columns)


if __name__ == '__main__':
    # topic_data = [{"name": "Economics", "description": "Field of study"}, {"name": "World news", "description": "News"}]
    # topic_data = {"topic": topic_data}
    # MongoUserInfoOper.insert_or_update_user_topic_info('24709718', topic_data)

    insert_qzb_tasks()
    # get_malicious_profile()
    assert ()
    # insert_stance_user_tasks_zzy()
    insert_stance_user_tasks_exited()
    assert ()
    # insert_tasks()
    # assert ()
    # create_bot_table()
    # MongoUserTweetOper.update_muti_data('T_user_post', {'tourist_crawl_status': 0}, {'tourist_crawl_status': 1})
    # res = MongoCommonOper.query_datas_by_condition('T_user_task', {'tourist_crawl_status': 0})
    # print(len(res))
    # delete_noentity_important_task()
    # res = MongoCommonOper.query_datas_by_condition('F_user_profile', {'is_bot': None})
    # print(len(res))
    # import random
    # random.shuffle(res)
    # for r in res[:223]:
    #     print(r.get('url'))
    #     MongoCommonOper.insert_or_update_one_data('F_user_profile', {'url': r.get('url')}, {'is_bot': 0})
    # print(len(res))
    # data = [{'a': 'fengsong', 'b': 1, 'c': 'fss', 'd': 'asasfaf'}, {'a': 'fs'}, {'a': None, 'b': 1, 'c': 'fss', 'd': 'asasfaf'}]
    #
    # MongoCommonOper.insert_many_data('index_test', data)
    # MongoCommonOper.insert_one_data('index_test', data)
    # MongoCommonOper.delete_attr_by_condition('crawler_status', {}, 'mail')
    # data = {'comment': ['chartdata', '1296167551869702144', 'ARIANA RUN POP n6er4ZIG0U'], 'post_text': ['Biggest Spotify debuts of All Time1. I Dont Care, Ed Sheeran & JB - 10,98M2. Nonstop, Drake - 9,29M3. Survival, Drake - 8,61M4. 7rings, Ariana G. - 8,55M5. HITR, Travis S. - 8M6. ME!, Taylor S. ft. Brendon - 7,94M7. LWYMMD, Taylor S. - 7,9M8. #Dynamite, @BTS_twt - 7,78M']}
    # MongoCommonOper.insert_or_update_one_data('T_action_info', {"account": "test_account2"}, {"action_info": data, "detail": "comment&post"})
    # MongoSpiderAccountOper.update_muti_data('account_info_for_spider', {'alive': PROXY_ERROR_CODE}, {'alive': 1})
    # insert_user_task_data()
    # keywords = ['vpn', '翻墙', '科学上网', 'GFW', 'Shadowsocks', 'v2ray', '自由门', '无界浏览', '法轮功', 'SS vpn', '机场 vpn']
    # for keyword in keywords:
    #     insert_keyword_task_data(keyword)
    # distribute_tasks()

    # # 更新多条数据,新增字段
    # data = {"video_urls": []}
    # condition1 = {"user_url": {"$ne": ["https://twitter.com/RinhoaSakura", "https://twitter.com/realCarlosEC", "https://twitter.com/Nalanda_crux", "https://twitter.com/WWBSYDSN"]}}
    # condition2 = {"avatar_binary": None}
    # conditions = {'post_url':'https://m.facebook.com/story.php?story_fbid=361037682259773&id=100050606321191&m_entstream_source=timeline'}
    # # MongoCommonOper.insert_one_data('T_user_post_test', {'fs':None, 'video_urls': 'fs'})
    # keywords = ['CyberGhost VPN', 'F-SecureFREEDOMEVPN', 'algovpn', 'hideipvpn', 'octovpn', 'xeovo vpn',
    #             'azirevpn', 'tuxlervpn', 'accesovpn', 'vpn.ac', 'bestevpn', 'wevpn', '金刚翻墙梯子']
    # datas = []
    # for k in keywords:
    #     data = {'keyword': k, 'add_time': None, 'update_time': None, 'crawl_status': 0}
    #     datas.append(data)
    # MongoCommonOper.insert_many_data('keyword_tasks', datas)
    # res = MongoCommonOper.query_datas_by_condition_limit('account_info_for_spider', {'alive': 1}, 5)
    # print(res)
    # datas = [{'_id': ('60ddb85074fe9f128386d653'), 'keyword': 'vpn', 'add_time': '2021-07-01 20:44:38', 'update_time': '2022-03-03 01:04:45', 'F_crawl_status': 0, 'crawl_status': 0}]
    # accounts = []
    # for d in datas:
    #     d.pop('_id')
    #     accounts.append(d)
    # MongoCommonOper.insert_many_data('keyword_tasks', accounts)
    # datas = MongoCommonOper.query_datas_by_condition('T_user_task', {'profile_crawl_status': 0})
    # print(len(datas))
    # for d in datas:
    #     d.pop('_id')
    # with open('task_data.json', 'w', encoding='utf-8') as f:
    #     f.write(json.dumps(datas))

    # with open('task_data.json', 'r', encoding='utf-8') as f:
    #     datas = f.read()
    #     datas = json.loads(datas)
    #     print(len(datas))
    #     MongoCommonOper.insert_many_data('T_user_task', datas)

    # data = MongoCommonOper.query_datas_by_condition('T_user_task', {})
    # data = [d.get('user_url') for d in data]
    # from collections import Counter
    # data = Counter(data)
    # print(len(data))
    # for k, v in data.items():
    #     if v > 1:
    #         print(k, v)
    #         MongoCommonOper.delete_many_data('T_user_task', {'user_url': k})

    # MongoCommonOper.update_muti_data('T_user_task', {'profile_crawl_status': 2}, {'profile_crawl_status': 0})

    # res = MongoCommonOper.query_datas_by_condition('account_info_for_spider', {'proxies': {"http": "http://10.0.12.1:10916", "https": "http://10.0.12.1:10916"}})
    # print(res)
    # conditions = {"keyword": data.get("keyword"), "source": data.get("source")}
    # MongoCommonOper.insert_or_update_one_data(collection='keyword_tasks', condition=conditions, data=data)

    # res = MongoCommonOper.query_datas_by_condition('T_user_task', {'video_urls': {"$ne": []}})
    # print(res)
    # pass
    # 删除字段
    # data = {'a': 11, 'b': 11}
    # MongoCommonOper.insert_or_update_one_data('test_fs', {'a': data.get('a')}, data)
    # MongoCommonOper.delete_attr_by_condition('test_fs', {}, 'b')
    # MongoUserTweetOper.delete_attr_by_condition('T_user_tourist', {}, 'avatar_crawl_status')

    # # # 查询数据
    # # res = MongoUserInfoOper.query_data_by_condition(collection='T_user_profile', condition={'user_url': 'https://twitter.com/Nalanda_crux'})
    # data = {}
    # crawl_type = 'retweet'
    # if crawl_type == 'praise':
    #     data['praise'] = 1
    #     data['retweet'] = 0
    # else:
    #     data['retweet'] = 1
    #     data['praise'] = 0
    # data['comment'] = 0
    # # data['comment_content'] = None
    # # data['comment_time'] = None
    # data['reply'] = 0
    # data['tweet_url'] = 'tweet_url'
    # data['author_url'] = 'fs'
    # data['user_url'] = 'person_url'  # 转发者或者点赞者
    # data['screen_name'] = 'screen_name'
    # data['user_name'] = 'name'
    # data['img_url'] = 'person_img_url'
    # data['register_time'] = 'register_time'
    # # data['img_binary'] = b''
    # # data['image_bs64encode'] = ''
    # data['introduction'] = 'description'
    # data['location'] = 'location'
    # data['website'] = 'website'
    # data['following_count'] = 'friends_count'
    # data['followers_count'] = 'followers_count'
    # data['tweets_count'] = 'statuses_count'
    # data['media_tweets_count'] = ['media_count']
    # data['favourites_tweets_count'] = ['favourites_count']
    # data['is_protected'] = 1
    # import time
    # data['fetch_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    # # 新增，mysql没有
    # data['banner_url'] = 'fsbannn'  # 没有背景图片的用户无该字段,get没有就返回None
    # data['banner_binary'] = None  # 没有背景图片的用户无该字段
    # data['is_verified'] = 1  # 是否认证，类似于机构认证那种
    # # data['topic'] = None
    # data['is_default_profile'] = 1
    # data['is_default_profile_image'] = 1
    # # data['user_tag'] = user_tag
    # if crawl_type == 'praise':
    #     MongoUserTweetTouristOper.insert_or_update_tweet_praise(data)
    # else:
    #     MongoUserTweetTouristOper.insert_or_update_tweet_retweet(data)

    # res = MongoCommonOper.delete_many_data('T_user_post', {'author_url': 'https://twitter.com/917Yangzhou'})
    # print(res)

    # from collections import Counter
    #
    # # collection = 'social_bot_task'
    # # condition_query = {'source': 'twitter'}
    # # key1 = 'url'
    #
    # collection = 'T_user_relationship'
    # condition_query = {'user_url': 'https://twitter.com/917Yangzhou'}
    # key1 = 'relationship_user_url'
    #
    # res = MongoCommonOper.query_datas_by_condition(collection, condition_query)
    # temp = [r.get(key1) for r in res]
    # print(len(temp))
    # c_dict = dict(Counter(temp))
    # for url, count in c_dict.items():
    #     if count > 1:
    #         print(url)
    #         condition2 = {key1: url}
    #         # MongoCommonOper.delete_one_data(collection, condition2)
    # pass
    # pass
