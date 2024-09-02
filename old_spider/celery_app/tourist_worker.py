import random

from celery_app import app
from db.mongo_db import MongoUserTweetOper
from tasks.process_tourist import praise_retweet_comment_process
from exceptions.my_exceptions import NoSpiderAccountException


# tourist表中的数据修改为celery形式
@app.task(ignore_result=True, autoretry_for=(NoSpiderAccountException,),
          retry_kwargs={'max_retries': 3, 'countdown': 5, 'default_retry_delay ': 1 * 60 * 30})
def tourist_worker(tweet):
    flag = praise_retweet_comment_process(tweet)
    # 返回1表示不需要重新爬取（即认为爬取成功），返回0则爬取失败需要重新爬取
    if flag:
        # # 这个地方非常耗时
        MongoUserTweetOper.update_tourist_crawl_status(tweet.get('tweet_url'), {'tourist_crawl_status': 1})
        print('{} 的tourist信息抓取成功'.format(tweet.get('tweet_url')))
    else:
        # tourist_worker.delay(task, tweet)
        tourist_worker.apply_async(args=[tweet], countdown=random.randint(1, 10))
        print('爬取 {} tourist信息失败，将该任务重新放回celery任务队列!'.format(tweet.get('tweet_url')))

    # tourist_number = 10
    # if tweet.get('praise_count') >= tourist_number or tweet.get('retweet_count') >= tourist_number or tweet.get('comment_count') >= tourist_number:
    #     tourist_worker.apply_async(args=[task, tweet], countdown=random.randint(1, 10))
    #     print("重新放入:{} {} {}".format(tweet.get('praise_count'), tweet.get('retweet_count'), tweet.get('comment_count')))
    #
    #     return
    # print("不需要爬取:{} {} {}".format(tweet.get('praise_count'), tweet.get('retweet_count'), tweet.get('comment_count')))
