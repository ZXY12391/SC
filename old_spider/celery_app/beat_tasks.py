import time
import random
import json

from celery_app import app
from celery_app.task_workers import (user_post_worker, user_following_worker, user_follower_worker, keyword_worker,
                                     user_profile_worker, tweet_image_worker, avatar_image_worker, user_topic_worker,
                                     distribute_task_worker, keyword_worker_sc, user_post_worker_stance)
from celery_app.tourist_worker import tourist_worker
from utils.task_utils import get_task_from_redis
from config.settings import TASK_QUEUE, SPIDER_ACCOUNT_ERROR, SPIDER_ACCOUNT_POOL, PROXY_ERROR_CODE
from db.mongo_db import (MongoKeywordTaskOper, MongoUserTweetOper, MongoUserRelationshipOper, MongoUserTweetTouristOper,
                         MongoUserTaskOper, MongoSpiderAccountOper)
from db.redis_db import RedisCommonOper, SpiderAccountOper
#keyword_worker 关键字 MongoKeywordTaskOper MongoUserTweetOper
#user_post_worker  MongoUserTaskOper MongoUserTweetOper
#user_following_worker MongoUserTaskOper MongoUserInfoOper
#user_follower_worker  MongoUserTaskOper MongoUserInfoOper
#tourist_worker MongoUserTweetOper MongoUserTweetTouristOper
#user_topic_worker MongoUserTweetTouristOper
#user_profile_worker MongoUserInfoOper
#具体任务来的时候一次性搞定

@app.task(ignore_result=True)
def keyword_task():
    """
    关键词任务---可设为定时任务
    :return:
    """
    print("关键词相关推文和用户信息采集开始时间：" + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))

    # keyword_tasks = MongoKeywordTaskOper.get_keyword_tasks()
    keyword_tasks=['侵略台灣','入侵台灣','武力攻臺','武統']
    for keyword_task in keyword_tasks:
        # todo 爬取关键词任务
        # from celery import group
        # group(keyword_worker.delay(keyword_task.get('keyword'), 'user'),
        #       keyword_worker.delay(keyword_task.get('keyword'), 'top'),
        #       keyword_worker.delay(keyword_task.get('keyword'), 'latest')
        #       )
        # keyword_worker.apply_async(args=[keyword_task.get('keyword'), 'user'], countdown=random.randint(1, 10))
        keyword_worker.apply_async(args=[keyword_task.get('keyword'), 'top'], countdown=random.randint(1, 10))
        keyword_worker.apply_async(args=[keyword_task.get('keyword'), 'latest'], countdown=random.randint(1, 10))

    print("关键词相关推文和用户信息采集结束时间：" + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
#keyword 搜推文和用户的任务

@app.task(ignore_result=True)
def keyword_task_sc():
    """
    审查关键词任务-
    :return:
    """
    print("关键词相关推文和用户信息采集开始时间：" + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))

    keyword_tasks = MongoKeywordTaskOper.get_keyword_tasks()
    for keyword_task in keyword_tasks:
        # todo 爬取关键词任务
        # from celery import group
        # group(keyword_worker.delay(keyword_task.get('keyword'), 'user'),
        #       keyword_worker.delay(keyword_task.get('keyword'), 'top'),
        #       keyword_worker.delay(keyword_task.get('keyword'), 'latest')
        #       )
        # keyword_worker.apply_async(args=[keyword_task.get('keyword'), 'user'], countdown=random.randint(1, 10))
        keyword_worker_sc.apply_async(args=[keyword_task.get('keyword'), 'top'], countdown=random.randint(1, 10))
        keyword_worker_sc.apply_async(args=[keyword_task.get('keyword'), 'latest'], countdown=random.randint(1, 10))

    print("关键词相关推文和用户信息采集结束时间：" + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))


@app.task(ignore_result=True)
def distribute_task_mongo2redis_queue(nums):
    """
    30分钟执行一次
    进行任务分配，当twitter:task队列中没有任务时，进行任务分配，即将mongodb中的任务放入redis任务队列twitter:task中
    :param nums:
    :return:
    """
    if RedisCommonOper.LGET_LEN(TASK_QUEUE) != 0:
        print('twitter:task任务队列中还有任务未分发，故停止本次任务分发.')
        return
    user_tasks = MongoUserTaskOper.get_user_tasks(important_user=0, nums=nums)  # 这里执行需要几秒的时间
    # 将任务分配到redis队列中
    distribute_task_worker(user_tasks)


@app.task(ignore_result=True)
def distribute_task_redis2worker_queue():
    """
    40分钟执行一次
    执行爬取档案信息、言论信息、关系信息任务
    :return:
    """
    while True:
        user_task = get_task_from_redis(TASK_QUEUE)
        if not user_task:
            print('任务队列twitter:task中的所有任务已全部发送到celery任务队列中.')
            return
        # if user_task.get('profile_task') and not user_task.get('profile_crawl_status'):
        #     # print(user_task.get('user_url') + '需要爬取档案信息')
        #     user_profile_worker.delay(user_task)
        #     time.sleep(0.2)
        flag = 0
        if user_task.get('following_task') and user_task.get('following_crawl_status') == flag:
            print(user_task.get('user_url') + '需要爬取following关系')
            user_following_worker.apply_async(args=[user_task])

        if user_task.get('follower_task') and user_task.get('follower_crawl_status') == flag:
            print(user_task.get('user_url') + '需要爬取follower关系')
            user_follower_worker.apply_async(args=[user_task])

        if user_task.get('post_task') and user_task.get('post_crawl_status') == flag:
            print(user_task.get('user_url') + '需要爬取言论')
            user_post_worker.apply_async(args=[user_task])

        time.sleep(5)


@app.task(ignore_result=True)
def important_user_task(nums):
    """
    定时爬取重点用户的信息
    :param nums:
    :return:
    """
    condition = {'$or': [{'follower_task': 1}, {'following_task': 1}, {'post_task': 1}, {'profile_task': 1}],
                 'important_user': 1
                 }
    # # 添加qzb任务
    # condition = {'user_name': 'qzb_malicious_user'}

    user_tasks = MongoUserTaskOper.get_user_tasks_by_condition(condition=condition, nums=nums)  # 这里执行需要几秒的时间
    random.shuffle(user_tasks)
    print('开始写入{}条重点用户任务...'.format(len(user_tasks)))
    for user_task in user_tasks:
        user_task.pop('_id')
        updated_task_status = {}

        if user_task.get('profile_task'):
            user_profile_worker.apply_async(args=[user_task])
            updated_task_status['profile_crawl_status'] = 1

        if user_task.get('post_task'):
            user_post_worker.apply_async(args=[user_task])
            updated_task_status['post_crawl_status'] = 1

        if user_task.get('following_task'):
            user_following_worker.apply_async(args=[user_task])
            updated_task_status['following_crawl_status'] = 1

        if user_task.get('follower_task'):
            user_follower_worker.apply_async(args=[user_task])
            updated_task_status['follower_crawl_status'] = 1

        MongoUserTaskOper.set_user_task_status(user_task.get('user_url'), updated_task_status)
    print('{}条重点用户爬取任务写入worker队列成功!'.format(len(user_tasks)))


@app.task(ignore_result=True)
def user_profile_task(nums):
    """
    10分钟执行一次
    对mongodb中的用户进行档案信息定时爬取
    :param nums:
    :return:
    """
    if RedisCommonOper.LGET_LEN('celery_twitter_profile_queue_RZ') != 0:
        print('profile worker队列中还有任务未执行，故停止本次用户档案信息爬取任务.')
        return
    user_profile_tasks = MongoUserTaskOper.get_user_profile_tasks(important_user=0, nums=nums)  # 这里执行需要几秒的时间
    # 将档案信息任务分配到爬取档案信息的redis队列中
    print('开始写入{}条档案信息任务...'.format(len(user_profile_tasks)))
    for user_task in user_profile_tasks:
        user_task.pop('_id')
        updated_task_status = {}
        if user_task.get('profile_task') and user_task.get('profile_crawl_status') == 0:
            # print(user_task.get('user_url') + '需要爬取档案信息')
            user_profile_worker.apply_async(args=[user_task])
            updated_task_status['profile_crawl_status'] = 1
        MongoUserTaskOper.set_user_task_status(user_task.get('user_url'), updated_task_status)
    print('{}条档案信息爬取任务写入profile worker队列成功!'.format(nums))


@app.task(ignore_result=True)
def user_follower_task(nums):
    """
    10分钟执行一次
    对mongodb中的用户follower信息定时爬取
    :param nums:
    :return:
    """
    if RedisCommonOper.LGET_LEN('celery_twitter_follower_queue_RZ') != 0:
        print('follower worker队列中还有任务未执行，故停止本次用户follower信息爬取任务.')
        return
    condition = {'follower_task': 1, 'follower_crawl_status': 0, 'important_user': 0}

    user_follower_tasks = MongoUserTaskOper.get_user_tasks_by_condition(condition=condition, nums=nums)  # 这里执行需要几秒的时间
    # 将关系follower信息任务分配到爬取follower信息的redis worker队列中
    print('开始写入{}条follower信息任务...'.format(len(user_follower_tasks)))
    for user_task in user_follower_tasks:
        user_task.pop('_id')
        updated_task_status = {}
        if user_task.get('follower_task') and user_task.get('follower_crawl_status') == 0:
            # print(user_task.get('user_url') + '需要爬取follower信息')
            user_follower_worker.apply_async(args=[user_task])
            updated_task_status['follower_crawl_status'] = 1
        MongoUserTaskOper.set_user_task_status(user_task.get('user_url'), updated_task_status)
    print('{}条follower信息爬取任务写入follower worker队列成功!'.format(nums))


@app.task(ignore_result=True)
def user_following_task(nums):
    """
    10分钟执行一次
    对mongodb中的用户following信息定时爬取
    :param nums:
    :return:
    """
    if RedisCommonOper.LGET_LEN('celery_twitter_following_queue_RZ') != 0:
        print('following worker队列中还有任务未执行，故停止本次用户following信息爬取任务.')
        return
    condition = {'following_task': 1, 'following_crawl_status': 0, 'important_user': 0}

    user_following_tasks = MongoUserTaskOper.get_user_tasks_by_condition(condition=condition, nums=nums)  # 这里执行需要几秒的时间
    # 将关系following信息任务分配到爬取following信息的redis worker队列中
    print('开始写入{}条following信息任务...'.format(len(user_following_tasks)))
    for user_task in user_following_tasks:
        user_task.pop('_id')
        updated_task_status = {}
        if user_task.get('following_task') and user_task.get('following_crawl_status') == 0:
            # print(user_task.get('user_url') + '需要爬取following信息')
            user_following_worker.apply_async(args=[user_task])
            updated_task_status['following_crawl_status'] = 1
        MongoUserTaskOper.set_user_task_status(user_task.get('user_url'), updated_task_status)
    print('{}条following信息爬取任务写入following worker队列成功!'.format(nums))


@app.task(ignore_result=True)
def user_post_task(nums):
    """
    10分钟执行一次
    对mongodb中的用户post信息定时爬取
    :param nums:
    :return:
    """
    if RedisCommonOper.LGET_LEN('celery_twitter_post_queue_RZ') != 0:
        print('post worker队列中还有任务未执行，故停止本次用户post信息爬取任务.')
        return
    condition = {'post_task': 1, 'post_crawl_status': 0, 'important_user': 0}

    user_post_tasks = MongoUserTaskOper.get_user_tasks_by_condition(condition=condition, nums=nums)  # 这里执行需要几秒的时间
    # 将关系post信息任务分配到爬取post信息的redis worker队列中
    print('开始写入{}条post信息任务...'.format(len(user_post_tasks)))
    for user_task in user_post_tasks:
        user_task.pop('_id')
        updated_task_status = {}
        if user_task.get('post_task') and user_task.get('post_crawl_status') == 0:
            # print(user_task.get('user_url') + '需要爬取post信息')
            user_post_worker.apply_async(args=[user_task])
            updated_task_status['post_crawl_status'] = 1
        MongoUserTaskOper.set_user_task_status(user_task.get('user_url'), updated_task_status)
    print('{}条post信息爬取任务写入post worker队列成功!'.format(nums))


@app.task(ignore_result=True)
def stance_post_task(nums):
    """
    10分钟执行一次
    对mongodb中的用户post信息定时爬取
    :param nums:
    :return:
    """
    # if RedisCommonOper.LGET_LEN('celery_twitter_post_queue_stance') != 0:
    #     print('stance post worker队列中还有任务未执行，故停止本次用户post信息爬取任务.')
    #     return
    condition = {'post_task': 1, 'post_crawl_status': 2, 'important_user': 0}

    user_post_tasks = MongoUserTaskOper.get_user_tasks_by_condition(condition=condition, nums=nums)  # 这里执行需要几秒的时间

    # 将关系post信息任务分配到爬取post信息的redis worker队列中
    print('开始写入{}条post信息任务...'.format(len(user_post_tasks)))
    for user_task in user_post_tasks:
        user_task.pop('_id')
        updated_task_status = {}
        if user_task.get('post_task') and user_task.get('post_crawl_status') == 2:
            # print(user_task.get('user_url') + '需要爬取post信息')
            user_post_worker_stance.apply_async(args=[user_task])
            updated_task_status['post_crawl_status'] = 1
        MongoUserTaskOper.set_user_task_status(user_task.get('user_url'), updated_task_status)
    print('{}条post信息爬取任务写入post worker队列成功!'.format(nums))


# 当前tourist爬取不设为定时任务--
@app.task(ignore_result=True)
def tweet_tourist_task(nums):
    """
    不设为定时任务
    爬取推文的点赞者、评论者、转发者、回复者
    :param nums:
    :return:
    """
    if RedisCommonOper.LGET_LEN('celery_twitter_post_tourist_queue_RZ') != 0:
        print('post worker队列中还有任务未执行，故停止本次用户post信息爬取任务.')
        return
    tweets = MongoUserTweetOper.get_tweet_tourist_tasks(nums=nums)
    task = {'post_praise_task': 1, 'post_repost_task': 1, 'post_comment_task': 1}
    print('开始爬取{}条推文的tourist...'.format(len(tweets)))
    for tweet in tweets:
        tweet.pop('_id')
        tweet.pop('tweet_img_binary')  # 不能传二进制
        tourist_worker.apply_async(args=[tweet])

    print('{}条推文tourist爬取结束'.format(len(tweets)))


@app.task(ignore_result=True)
def tweet_image_task(nums):
    """
    5分钟执行一次
    推文图片任务---从mongodb中读取
    :return:
    """
    tasks = MongoUserTweetOper.get_tweet_image_tasks(nums=nums)
    print('开始爬取{}条推文的图片...'.format(len(tasks)))
    for task in tasks:
        # tweet_image_worker.delay(task.get('tweet_url'), task.get('tweet_img_url'))
        tweet_image_worker.apply_async(args=[task.get('tweet_url'), task.get('tweet_img_url')], countdown=random.randint(1, 10))

    print('{}条推文图片爬取结束'.format(len(tasks)))


@app.task(ignore_result=True)
def relationship_avatar_task(nums):
    """
    5分钟执行一次
    抓取关系表的头像
    :return:
    """
    tasks = MongoUserRelationshipOper.get_avatar_tasks(nums=nums)
    print('开始爬取{}条relationship用户的头像...'.format(len(tasks)))
    for task in tasks:
        # avatar_image_worker.delay(task.get('relationship_user_url'), task.get('avatar_url'), 'relationship')
        avatar_image_worker.apply_async(args=[task.get('relationship_user_url'), task.get('avatar_url'), 'relationship'], countdown=random.randint(1, 10))
    print('{}条用户头像爬取结束'.format(len(tasks)))


@app.task(ignore_result=True)
def tourist_avatar_task(nums):
    """
    5分钟执行一次
    抓取tourist表的头像
    :return:
    """
    tasks = MongoUserTweetTouristOper.get_avatar_tasks(nums=nums)
    print('开始爬取{}条tourist用户的头像...'.format(len(tasks)))
    for task in tasks:
        # avatar_image_worker.delay(task.get('user_url'), task.get('avatar_url'), 'tourist')
        avatar_image_worker.apply_async(args=[task.get('user_url'), task.get('avatar_url'), 'tourist'], countdown=random.randint(1, 10))
    print('{}条用户头像爬取结束'.format(len(tasks)))


@app.task(ignore_result=True)
def relationship_topic_task(nums):
    """
    5分钟执行一次
    抓取关系表的话题信息
    :return:
    """
    tasks = MongoUserRelationshipOper.get_topic_tasks(nums=nums)
    print('开始爬取{}条relationship用户的话题信息...'.format(len(tasks)))
    for task in tasks:
        # user_topic_worker.delay(task.get('screen_id'), 'relationship')
        user_topic_worker.apply_async(args=[task.get('screen_id'), 'relationship'], countdown=random.randint(1, 10))
    print('{}条用户话题信息爬取结束'.format(len(tasks)))


@app.task(ignore_result=True)
def tourist_topic_task(nums):
    """
    5分钟执行一次
    抓取tourist表的话题信息
    :return:
    """
    tasks = MongoUserTweetTouristOper.get_topic_tasks(nums=nums)
    print('开始爬取{}条tourist用户的话题信息...'.format(len(tasks)))
    for task in tasks:
        # user_topic_worker.delay(task.get('screen_id'), 'tourist')
        user_topic_worker.apply_async(args=[task.get('screen_id'), 'tourist'], countdown=random.randint(1, 10))
    print('{}条用户话题信息爬取结束'.format(len(tasks)))


@app.task(ignore_result=True)
def del_expire_using_spider_worker():
    """
    删除expire_time小时之前的账号---就算删除了当前正在爬取数据的账号也不会出问题，因为程序正常结束或账号异常时释放该账号时对于不存在的键会正常执行
    :return:
    """
    print('开始删除正在使用队列中超过0.5小时的账号...')
    res = SpiderAccountOper.delete_expire_using_account(expire_time=0.5)
    print('删除正在使用队列中过期账号 {} 个成功!'.format(res))


@app.task(ignore_result=True)
def del_error_account_task():
    """
    删除错误队列里的爬虫账号，并将mongodb中alive为 PROXY_ERROR_CODE 的账号放入正常的爬虫账号队列中，同时将mongodb中alive设为1
    :return:
    """
    print('开始删除错误队列...')
    res = SpiderAccountOper.DEL_KEY(SPIDER_ACCOUNT_ERROR)
    if res == 1:
        print('删除错误队列成功，将alive为{}（代理有问题的账号）和alive为1（正常账号）的账号放入正常的爬虫账号队列并将mongodb中该字段置为1'.format(PROXY_ERROR_CODE, res))
        accounts = MongoSpiderAccountOper.get_spider_accounts_ignore_task_number(alive_codes=[PROXY_ERROR_CODE, 1])
        for account in accounts:
            # mongodb中规定task_number=1的账号为购买的账号，task_number!=1的账号为培育的账号
            # mongodb中规定task_number=2的账号为001长期账号培育的账号，3表示cert短期培育的账号--更新cookies后所有的账号cookies格式
            # 都为培育的格式，即为一个列表
            cookies = {cookie['name']: cookie['value'] for cookie in account.get('token')}
            account_info = {'proxies': account.get('proxies'),
                            'cookies': cookies,
                            }
            SpiderAccountOper.HSET(SPIDER_ACCOUNT_POOL, account.get('account'), json.dumps(account_info))
            MongoSpiderAccountOper.update_spider_account_status(account.get('account'), {'alive': 1})
        print('{}个账号操作成功!'.format(len(accounts)))
    else:
        print('删除错误队列失败，结束本次操作'.format(res))


@app.task(ignore_result=True)
def paper_user_task(nums):
    """
    :param nums:
    :return:
    """
    # 添加论文任务
    condition = {'$or': [{'follower_crawl_status': 0}, {'following_crawl_status': 0}, {'profile_crawl_status': 0}],
                 'important_user': 0
                 }

    user_tasks = MongoUserTaskOper.get_user_tasks_by_condition(condition=condition, nums=nums)  # 这里执行需要几秒的时间
    random.shuffle(user_tasks)
    print('开始写入{}条用户任务...'.format(len(user_tasks)))
    for user_task in user_tasks:
        user_task.pop('_id')
        updated_task_status = {}

        if user_task.get('profile_task'):
            user_profile_worker.apply_async(args=[user_task])
            updated_task_status['profile_crawl_status'] = 1

        if user_task.get('following_task'):
            user_following_worker.apply_async(args=[user_task])
            updated_task_status['following_crawl_status'] = 1

        if user_task.get('follower_task'):
            user_follower_worker.apply_async(args=[user_task])
            updated_task_status['follower_crawl_status'] = 1

        MongoUserTaskOper.set_user_task_status(user_task.get('user_url'), updated_task_status)
    print('{}条重点用户爬取任务写入worker队列成功!'.format(len(user_tasks)))


@app.task(ignore_result=True)
def paper_user_post_task(nums):
    """
    :param nums:
    :return:
    """
    # 添加论文任务
    condition = {'important_user': 0,
                 'post_crawl_status': 1
                 }

    user_tasks = MongoUserTaskOper.get_user_tasks_by_condition(condition=condition, nums=nums)  # 这里执行需要几秒的时间
    random.shuffle(user_tasks)
    print('开始写入{}条用户任务...'.format(len(user_tasks)))
    for user_task in user_tasks:
        user_task.pop('_id')
        updated_task_status = {}

        if user_task.get('post_task'):
            user_post_worker.apply_async(args=[user_task])
            updated_task_status['post_crawl_status'] = 1

        MongoUserTaskOper.set_user_task_status(user_task.get('user_url'), updated_task_status)
    print('{}条重点用户爬取任务写入worker队列成功!'.format(len(user_tasks)))


if __name__ == '__main__':
    keyword_tasks = ['侵略台灣', '入侵台灣', '武力攻臺', '武統']
    for keyword_task in keyword_tasks:
        # todo 爬取关键词任务
        # from celery import group
        # group(keyword_worker.delay(keyword_task.get('keyword'), 'user'),
        #       keyword_worker.delay(keyword_task.get('keyword'), 'top'),
        #       keyword_worker.delay(keyword_task.get('keyword'), 'latest')
        #       )
        # keyword_worker.apply_async(args=[keyword_task.get('keyword'), 'user'], countdown=random.randint(1, 10))
        keyword_worker.apply_async(args=[keyword_task, 'top'], countdown=random.randint(1, 10))
        keyword_worker.apply_async(args=[keyword_task, 'latest'], countdown=random.randint(1, 10))
    #del_error_account_task()
    # paper_user_task(20000)
    # paper_user_post_task(100000)
    # assert ()
    # tweet_tourist_task(nums=100000)
    # del_error_account_task()
    # user_profile_task(200000)
    # user_profile_task(1)
    # del_error_account_task()
    # user_follower_task(20)
    # # 关键词
    # keyword_task()
    # keyword_task_sc()
    # important_user_task(500)
    # celery -A celery_app worker -l info -P gevent -c 10 -Q celery_twitter_keyword_queue_sc

    # important_user_task(600)
    # # 推文图片爬取
    # tweet_image_task()
    # del_error_account_task()

    # # 头像爬取
    # tourist_avatar_task()
    # relationship_avatar_task()
    #
    # # 话题爬取
    # tourist_topic_task()
    # relationship_topic_task()

    # # 推文点赞者评论者爬取
    # tweet_tourist_task(nums=300000)
    # print(RedisCommonOper.LGET_LEN(TASK_QUEUE))
    #
    # # 分配任务
    # distribute_task_mongo2redis_queue(nums=5)
    # # # # print(RedisCommonOper.LGET_LEN(TASK_QUEUE))
    # # # #
    # # 执行任务
    # distribute_task_redis2worker_queue()

    # while True:
    #     # 推文图片爬取
    #     tweet_image_task()
    #
    #     # 头像爬取
    #     tourist_avatar_task()
    #     relationship_avatar_task()
    #
    #     # # 话题爬取
    #     # tourist_topic_task()
    #     # relationship_topic_task()
    #     time.sleep(0.1 * 60 * 60)

# 异常Modifiers operate on fields but we found type array instead. For example: {$mod: {<field>: ...}} not {$set: []}, full error: {'index': 0
# , 'code': 9, 'errmsg': 'Modifiers operate on fields but we found type array instead. For example: {$mod: {<field>: ...}} not {$set: []}'},返回的数据为:b'{"data":{"user":{"result":{"__ty
# pename":"UserUnavailable"}}}}'
#找到五个基本任务
#keyword，人，推文
#tweet,获取一个人的推文
#relation 获取一个人的关系（tourist and follow fan）
#人 个人信息采集