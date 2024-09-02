import time
import json
import random
import schedule
import threading

from db.dao import TaskTablesOper
from db.redis_db import get_redis_conn
from tasks.process_task import user_relationship_process as relationship_process, keyword_tweet_process, user_info_process, user_tweet_process
from utils.account_utils import select_normal_account


# 开源采集
def execute_task(seed, crawl_type):
    # todo 选择正常的账号和代理
    cookies, proxies, account = select_normal_account(get_redis_conn())
    if cookies is None:
        print('账号选择次数达到限制，爬取{}失败'.format(seed.user_url))
        return
    cookies = json.loads(cookies)

    # # ######################我添加的代码（为了使我的cookies能用）######################
    # temp_cookies = json.loads(cookies)
    # cookies = {}
    # for cookie in temp_cookies:
    #     cookies[cookie['name']] = cookie['value']
    # # ############################################################################

    # # #####################################################################################################
    # def get_account(account):
    #     from db.basic import get_db_session
    #     from db.models import AccountInfoForSpider
    #
    #     with get_db_session() as db_session:
    #         rs = db_session.query(AccountInfoForSpider).filter_by(account=account).first()
    #     return rs.token, rs.proxies, rs.account
    #
    # account = '664540966Mi'
    # cookies, proxies, account = get_account(account)
    # temp_cookies = json.loads(cookies)
    # cookies = {}
    # for cookie in temp_cookies:
    #     cookies[cookie['name']] = cookie['value']
    # # # ############################################################################

    # _flag表示该用户是否需要重新爬取---0表示需要重新爬取，1表示不需要(账号不存在)重新爬取
    _flag, user_id, username, image_url = user_info_process(seed.user_url, proxies)
    if user_id == 'failed':
        if _flag == 1:
            TaskTablesOper.update_user_task_status(seed.user_url, 1)
        return

    flag = 1
    if crawl_type == 'userinfo':
        flag = user_info_process(seed.user_url, proxies)
    elif crawl_type == 'following':
        flag = relationship_process(seed.user_url, 'following')
    elif crawl_type == 'follower':
        flag = relationship_process(seed.user_url, 'follower')
    elif crawl_type == 'tweet':
        flag = user_tweet_process(seed.user_url, user_id, username, image_url, cookies, proxies)
    else:
        flag = keyword_tweet_process(seed.keyword)

    if flag == 1:
        TaskTablesOper.update_user_task_status(seed.user_url, 1)


def schedule_center(crawl_type):
    print("{}信息采集开始时间：".format(crawl_type) + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
    if crawl_type == 'tweet':
        seeds = TaskTablesOper.get_user_tweet_tasks()
    elif crawl_type == 'following' or crawl_type == 'follower':
        seeds = TaskTablesOper.get_relationship_tasks()
    else:
        seeds = TaskTablesOper.get_keyword_tasks()
    seeds = seeds[:10]
    print('采集用户数为：%d' % (len(seeds)))
    tasks = []
    for seed in seeds:
        task = threading.Thread(target=execute_task, args=(seed, crawl_type, ))
        tasks.append(task)
    for task in tasks:
        task.start()
        time.sleep(random.randint(1, 3))

    for task in tasks:
        task.join()

    print("{}信息采集结束时间：".format(crawl_type) + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
