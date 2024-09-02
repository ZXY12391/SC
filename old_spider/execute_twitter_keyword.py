import time
import threading
import random
import schedule

from tasks.process_task import keyword_tweet_process, keyword_user_process
from db.mongo_db import MongoKeywordTaskOper


def crawl_keyword_tweet_and_user(crawl_type):
    '''
    根据关键词爬取相关推文信息（top、latest）
    :return:
    '''
    print("关键词相关推文和用户信息采集开始时间：" + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
    seeds = MongoKeywordTaskOper.get_keyword_tasks()

    tasks = []
    for seed in seeds:
        # 默认爬取top下的相关推文
        task = threading.Thread(target=keyword_tweet_process, args=(seed.get('keyword'), crawl_type, ))
        tasks.append(task)
    for task in tasks:
        task.start()
        time.sleep(random.randint(1, 3))

    for task in tasks:
        task.join()

    print("关键词相关推文和用户信息采集结束时间：" + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))


def crawl_keyword_user():
    """
    搜索与名字相关的用户信息----2021-7-5号晚上20点已把所有关键词爬取了一遍
    :return:
    """
    print("相关用户信息采集开始时间：" + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
    seeds = MongoKeywordTaskOper.get_keyword_tasks()
    print('采集关键词数为：%d' % (len(seeds)))
    tasks = []
    for seed in seeds:
        task = threading.Thread(target=keyword_user_process, args=(seed.get('keyword'),))
        tasks.append(task)
    for task in tasks:
        task.start()
        time.sleep(random.randint(1, 3))

    for task in tasks:
        task.join()

    update_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    print("关键词相关用户信息采集结束时间：" + update_time)


if __name__ == '__main__':
    # 初始化数据库、表
    # create_all()
    # crawl_keyword_user()
    
    crawl_keyword_tweet_and_user(crawl_type='latest')
    time.sleep(5)
    crawl_keyword_tweet_and_user(crawl_type='top')
    time.sleep(5)
    crawl_keyword_user()

    # schedule.every(24).hours.do(crawl_keyword_tweet_and_user)
    # while True:
    #     schedule.run_pending()
    #     time.sleep(1)
"""
该代码测试完毕---2021-05-26 

"""
