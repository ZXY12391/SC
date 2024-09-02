import time
import random
import threading

from celery_app import app
from db.mongo_db import MongoCommonOper, MongoUserTaskOper, MongoKeywordTaskOper, MongoUserTweetOper
from tasks.process_task import (user_info_process, user_tweet_process, user_relationship_process, keyword_user_process,
                                keyword_tweet_process, user_topic_process)
from tasks.process_tourist import praise_retweet_comment_process
from config.settings import TASK_QUEUE
from db.redis_db import get_redis_conn, acquire_lock, release_lock, SpiderAccountOper
from bloom_filter.bloom_filter import bloom_filter_follower, bloom_filter_following, bloom_filter_user
from utils.task_utils import push_task_to_redis, get_task_from_redis
from utils.image_utils import crawl_tweet_image, crawl_avatar_image
from utils.other_utils import get_timestamp
from exceptions.my_exceptions import NoSpiderAccountException, ProxyOrNetworkException
import json

conn = get_redis_conn()


@app.task(ignore_result=True)
def distribute_task_worker(user_tasks):
    """
    从mongodb中获取任务分发到redis任务队列里--可设置为定时任务
    :return:
    """
    print('开始从mongodb分发任务到redis任务队列...')
    if not user_tasks:
        print('mongodb中所有任务已分发，任务分发完毕.')
        return
    for user_task in user_tasks:
        # _id object类型的对象无法放入redis
        user_task.pop('_id')

        # # 如果当前任务已加入，则不再处理（可防止用户重复配置相同的目标用户）----该语句也可以注释
        # if bloom_filter_user.is_exist(user_task.get('user_url')):
        #     print(user_task.get('user_url') + ' 任务已加入任务队列')
        #     continue

        push_task_to_redis(TASK_QUEUE, user_task)

        updated_task_status = {}
        if user_task.get('profile_task') and user_task.get('profile_crawl_status') == 0:
            updated_task_status['profile_crawl_status'] = 1

        if user_task.get('following_task') and user_task.get('following_crawl_status') == 0:
            updated_task_status['following_crawl_status'] = 1

        if user_task.get('follower_task') and user_task.get('follower_crawl_status') == 0:
            updated_task_status['follower_crawl_status'] = 1

        if user_task.get('post_task') and user_task.get('post_crawl_status') == 0:
            updated_task_status['post_crawl_status'] = 1

        MongoUserTaskOper.set_user_task_status(user_task.get('user_url'), updated_task_status)

        # 插入布隆过滤器（用户在mongodb中添加相同的url时，不重复爬取）
        bloom_filter_user.save(user_task.get('user_url'))
    print('{}条任务分发完毕.'.format(len(user_tasks)))


@app.task(ignore_result=True, autoretry_for=(NoSpiderAccountException, ),
          retry_kwargs={'max_retries': 3, 'countdown': 5, 'default_retry_delay ': 1 * 60 * 30})
def keyword_worker(keyword, crawl_type):
    """
    关键词任务---可设为定时任务
    :return:
    """
    if crawl_type == 'user':
        keyword_user_process(keyword)
    else:
        keyword_tweet_process(keyword, crawl_type)


@app.task(ignore_result=True, autoretry_for=(NoSpiderAccountException, ),
          retry_kwargs={'max_retries': 3, 'countdown': 5, 'default_retry_delay ': 1 * 60 * 30})
def keyword_worker_sc(keyword, crawl_type):
    """
    审查任务
    :return:
    """
    if crawl_type == 'user':
        keyword_user_process(keyword)
    else:
        keyword_tweet_process(keyword, crawl_type)


@app.task(ignore_result=True)
def tweet_image_worker(tweet_url, tweet_img_urls):
    flag = crawl_tweet_image(tweet_url, tweet_img_urls)
    # 返回1表示不需要重新爬取（即认为爬取成功），返回0则爬取失败需要重新爬取
    if flag:
        print('{} 推文图片抓取成功'.format(tweet_url))
    else:
        # tweet_image_worker.delay(tweet_url, tweet_img_urls)
        tweet_image_worker.apply_async(args=[tweet_url, tweet_img_urls])
        print('爬取 {} 图片信息失败，将该任务重新放回celery任务队列!'.format(tweet_url))


@app.task(ignore_result=True)
def avatar_image_worker(user_url, avatar_url, crawl_type):
    flag = crawl_avatar_image(user_url, avatar_url, crawl_type)
    # 返回1表示不需要重新爬取（即认为爬取成功），返回0则爬取失败需要重新爬取
    if flag:
        print('{} 头像抓取成功'.format(user_url))
    else:
        # tweet_image_worker.delay(user_url, avatar_url, crawl_type)
        tweet_image_worker.apply_async(args=[user_url, avatar_url, crawl_type])
        print('爬取 {} 头像失败，将该任务重新放回celery任务队列!'.format(user_url))


@app.task(ignore_result=True, autoretry_for=(NoSpiderAccountException, ),
          retry_kwargs={'max_retries': 3, 'countdown': 5, 'default_retry_delay ': 1 * 60 * 30})
def user_topic_worker(screen_id, crawl_type):
    flag = user_topic_process(screen_id, crawl_type)
    # 返回1表示不需要重新爬取（即认为爬取成功），返回0则爬取失败需要重新爬取
    if flag:
        print('{} 话题信息抓取成功'.format(screen_id))
    else:
        # user_topic_worker.delay(screen_id, crawl_type)
        user_topic_worker.apply_async(args=[screen_id, crawl_type])
        print('爬取 {} 话题信息失败，将该任务重新放回celery任务队列!'.format(screen_id))


@app.task(ignore_result=True, autoretry_for=(NoSpiderAccountException, ),
          retry_kwargs={'max_retries': 3, 'countdown': 5, 'default_retry_delay ': 1 * 60 * 30})
def user_profile_worker(task):
    if not task:
        return
    flag, _, _, _ = user_info_process(task)  # 这里不用改回去
    # 返回1表示不需要重新爬取（即认为爬取成功），返回0则爬取失败需要重新爬取
    if flag:
        # 在user_info_process函数中已更新爬取成功后的状态，故这里不用再更新了
        print('爬取 {} 档案信息成功，更新该任务状态成功!'.format(task.get('user_url')))

    else:
        # user_profile_worker.delay(task)
        user_profile_worker.apply_async(args=[task])
        print('爬取 {} 档案信息失败，将该任务重新放回celery任务队列!'.format(task.get('user_url')))


@app.task(ignore_result=True, autoretry_for=(NoSpiderAccountException, ),
          retry_kwargs={'max_retries': 3, 'countdown': 5, 'default_retry_delay ': 1 * 60 * 30})
def user_post_worker(task):
    if not task:
        return
    flag = user_tweet_process(task)
    # 返回1表示不需要重新爬取（即认为爬取成功），返回0则爬取失败需要重新爬取
    if flag:
        update_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        MongoUserTaskOper.set_user_task_status(task.get('user_url'),
                                               {'post_crawl_status': 2, 'update_time': update_time})
        print('爬取 {} 言论信息成功，更新该任务状态成功!'.format(task.get('user_url')))
    else:
        # user_post_worker.delay(task)
        user_post_worker.apply_async(args=[task])
        print('爬取 {} 言论信息失败，将该任务重新放回celery任务队列!'.format(task.get('user_url')))


@app.task(ignore_result=True, autoretry_for=(NoSpiderAccountException, ),
          retry_kwargs={'max_retries': 3, 'countdown': 5, 'default_retry_delay ': 1 * 60 * 30})
def user_post_worker_stance(task):
    if not task:
        return
    flag = user_tweet_process(task)
    # 返回1表示不需要重新爬取（即认为爬取成功），返回0则爬取失败需要重新爬取
    if flag:
        update_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        MongoUserTaskOper.set_user_task_status(task.get('user_url'),
                                               {'post_crawl_status': 2, 'update_time': update_time})
        print('爬取 {} 言论信息成功，更新该任务状态成功!'.format(task.get('user_url')))
    else:
        # user_post_worker.delay(task)
        user_post_worker_stance.apply_async(args=[task])
        print('爬取 {} 言论信息失败，将该任务重新放回celery任务队列!'.format(task.get('user_url')))


@app.task(ignore_result=True, autoretry_for=(NoSpiderAccountException, ),
          retry_kwargs={'max_retries': 3, 'countdown': 5, 'default_retry_delay ': 1 * 60 * 30})
def user_following_worker(task):
    if not task:
        return
    flag = user_relationship_process(task, 'following')
    # 返回1表示不需要重新爬取（即认为爬取成功），返回0则爬取失败需要重新爬取
    if flag:
        update_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        MongoUserTaskOper.set_user_task_status(task.get('user_url'),
                                               {'following_crawl_status': 2, 'update_time': update_time})
        print('爬取 {} following信息成功，更新该任务状态成功!'.format(task.get('user_url')))
    else:
        # user_following_worker.delay(task)
        user_following_worker.apply_async(args=[task])
        print('爬取 {} following信息失败，将该任务重新放回celery任务队列!'.format(task.get('user_url')))


@app.task(ignore_result=True, autoretry_for=(NoSpiderAccountException, ),
          retry_kwargs={'max_retries': 3, 'countdown': 5, 'default_retry_delay ': 1 * 60 * 30})
def user_follower_worker(task):
    if not task:
        return
    flag = user_relationship_process(task, 'follower')
    # 返回1表示不需要重新爬取（即认为爬取成功），返回0则爬取失败需要重新爬取
    if flag:
        # 如果爬取成功，则将mongodb任务表状态字段置为2
        update_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        MongoUserTaskOper.set_user_task_status(task.get('user_url'),
                                               {'follower_crawl_status': 2, 'update_time': update_time})
        print('爬取 {} follower信息成功，更新该任务状态成功!'.format(task.get('user_url')))
    else:
        # user_follower_worker.delay(task)
        user_follower_worker.apply_async(args=[task])
        print('爬取 {} follower信息失败，将该任务重新放回celery任务队列!'.format(task.get('user_url')))


if __name__ == '__main__':
    # tweet_image_worker()
    # res = get_task_from_redis(FOLLOWER_KEY)
    # print(res)
    # keyword_worker()
    # tweet_image_worker()
    # tweet_image_worker()
    # del_expire_using_spider_worker()
    user_profile_worker()
    pass
