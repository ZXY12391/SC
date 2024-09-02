import json
import jsonpath
import random
import time
from json import JSONDecodeError
from urllib.parse import quote
from db.mongo_db import MongoKeywordTaskOper, MongoUserInfoOper, MongoUserTaskOper, MongoUserTweetOper, MongoSpiderAccountOper
from keyword_new_simple.cookies_list import d,d3,dx
from logger.log import download_logger, other_logger, parser_logger, spider_logger
from crawlers.downloader import download_with_cookies, download_without_cookies,download_with_cookies_keyword
from parsers.parser_data import (parser_keyword_tweet, parser_user_info, parser_relationship, parser_user_tweet,
                                 parser_user_topic, parser_keyword_user, parser_praise_or_retweet, parser_comment_reply)

def get_cookie_dict(x):
    # print(x)
    xx = x.split(';')
    c_d = {}
    for xxx in xx:
        z = xxx.strip()
        # print(z)
        key = z.split('=')
        c_d[key[0]] = key[1]
    return c_d

def user_info_process(task, proxies=None,cookies=None):
    """
    爬取用户档案信息
    :param task:
    :param cookies:
    :param proxies:
    :return:
    """
    user_url, user_tag, important_user = task.get('twitter_url'), task.get('user_tag'), task.get('important_user')
    proxies = proxies
    # print(proxies)
    # proxies = {"http": "http://10.0.12.1:10916", "https": "http://10.0.12.1:10916"}
    print('抓取用户个人主页URL：{} 的基本信息'.format(user_url))
    # print('使用Torsocks代理爬取数据...')
    screen_name = user_url[20:]  # 获取用户的screen_name
    # 该链接不需要登录就可爬取链接
    url = 'https://api.twitter.com/graphql/-xfUfZsnR_zqjFd-IfrN5A/UserByScreenName?variables=%7B%22screen_name%22%3A%22{}%22%2C%22withHighlightedLabel%22%3Atrue%7D'.format(quote(screen_name))
    # # 该链接需要登录才能爬取信息
    # url = 'https://twitter.com/i/api/graphql/Vf8si2dfZ1zmah8ePYPjDQ/UserByScreenNameWithoutResults?variables=%7B%22screen_name%22%3A%22{}%22%2C%22withHighlightedLabel%22%3Atrue%7D'.format(quote(screen_name))
    resp = download_with_cookies(url, cookies, proxies)
    # resp = download_without_cookies(url,proxies)
    if resp is None:
        download_logger.error('代理或网络异常，url {} 基本信息抓取失败'.format(user_url))
        return 0, 'failed', 'failed', 'failed'  # 0表示需要重新爬取，1表示不需要重新爬取
    try:
        content = json.loads(resp.content)
        # # 存储json文件
        # store_json(content, './filter_data/json_data/info_json.json')
        # print('存储成功')
        # time.sleep(10000)

        # # 对信息进行过滤---基本信息可正常过滤
        # t1 = time.time()
        # try:
        #     FilterSecurity.traverseJson(content)
        # except Exception:
        #     pass
        # t2 = time.time()
        # print('过滤用时：{}秒'.format(t2 - t1))

        # 判断该用户url是否存在
        user_status_code = content.get("errors")[0].get('code') if content.get("errors") else 1
        if user_status_code == 50:
            parser_logger.error('url {} 不存在,采集基本信息失败'.format(user_url))
            # 更新用户档案信息爬取状态为2已完成
            update_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            # MongoUserTaskOper.set_user_task_status(task.get('twitter_url'),
            #                                        {'profile_crawl_status': 2, 'update_time': update_time})
            return 0, 'failed', 'failed', 'failed'  # 0表示需要重新爬取，1表示不需要(账号不存在)重新爬取

        if user_status_code == 63:
            parser_logger.error('url {} 异常（ User has been suspended）,采集基本信息失败'.format(user_url))
            # 更新用户档案信息爬取状态为2已完成
            update_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            # MongoUserTaskOper.set_user_task_status(task.get('twitter_url'),
            #                                        {'profile_crawl_status': 2, 'update_time': update_time})
            return 1, 'failed', 'failed', 'failed'
        # 解析页面
        screen_id, username, image_url = parser_user_info(user_url, user_tag, important_user, content, proxies)

        if screen_id == 0:
            parser_logger.error('url {} 异常,采集基本信息失败'.format(user_url))
            # 更新用户档案信息爬取状态为2已完成
            update_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            # MongoUserTaskOper.set_user_task_status(task.get('twitter_url'),
            #                                        {'profile_crawl_status': 2, 'update_time': update_time})
            return 1, 'failed', 'failed', 'failed'

        # 爬取话题
        # flag = user_topic_process(screen_id, 'profile')
        # if flag:
        #     pass
        # else:
        #     other_logger.error('用户 {} topic信息需要重新抓取'.format(screen_id))

        print(' {} 基本信息抓取成功'.format(user_url))

        # 更新用户档案信息爬取状态为2已完成
        update_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        # MongoUserTaskOper.set_user_task_status(task.get('twitter_url'),
        #                                        {'profile_crawl_status': 2, 'update_time': update_time})
        return 1, screen_id, username, image_url
    except JSONDecodeError as e:
        download_logger.error('JSON异常{}，可能返回的不是json数据，返回的数据为:{}'.format(e, resp.content))
        download_logger.error('url {} 基本信息抓取失败'.format(user_url))
    except Exception as e:
        download_logger.error('异常{},返回的数据为:{}'.format(e, resp.content))
        download_logger.error('url {} 基本信息抓取失败'.format(user_url))
    return 0, 'failed', 'failed', 'failed'


def task_one(user_task):
        # print(user_task)
        # user_task.pop('_id')
        d2 = random.choice(d)
        print(d2)
        cookies = get_cookie_dict(d2['cookie'])
        proxies = d2['proxy']
        # for user_task in user_profile_tasks:
        updated_task_status = {}
        if user_task.get('profile_task') and user_task.get('profile_crawl_status') == 0:
            print(user_task.get('user_url') + '需要爬取档案信息')
            try:
                flag,_,_,_=user_info_process(user_task,proxies=proxies,cookies=cookies)
            except Exception as e:
                flag=0
                pass
            if flag:
                updated_task_status['profile_crawl_status'] = 1
                MongoUserTaskOper.set_user_task_status(user_task.get('user_url'), updated_task_status)


def task_one_new(user_task):
    d2 = random.choice(dx)
    print(d2)
    cookies = get_cookie_dict(d2['cookie'])
    proxies = d2['proxy']
    # for user_task in user_profile_tasks:
    print(user_task.get('twitter_url') + '需要爬取档案信息')
    try:
        flag, _, _, _ = user_info_process(user_task, proxies=proxies, cookies=cookies)
    except Exception as e:
        flag = 0
        pass
    if flag:
        #cbd['RZ_multi_user_tasks'].update_one({'twitter_url':user_task['twitter_url']},{'$set':{'profile': 1}})
        pass

if __name__=='__main__':
    # for i in range(10):
        from config.settings import mongo_uri, mongo_name, PROXY_ERROR_CODE
        from logger.log import db_logger
        import pymongo
        client = pymongo.MongoClient(mongo_uri)
        cbd=client['Fqdl']
        # user_profile_tasks =list(cbd['RZ_multi_user_tasks'].find({'profile':0}))
        # p=r'E:\RZ项目\川大\川大\123.xlsx'
        # import pandas as pd
        # data = pd.read_excel(io=p)
        # data = data.loc[:, ["new_url"]]
        # data = data.to_dict(orient='records')
        # data=[_['new_url'] for _ in data]
        # user_profile_tasks_=[]
        # for u in user_profile_tasks:
        #     if u['facebook_url'] in data:
        #         user_profile_tasks_.append(u)
        # print(len(user_profile_tasks_))
        # task_one_new(user_profile_tasks[0])
        # cbd['RZ_multi_user_tasks'].update_one({'twitter_url': user_task['twitter_url']}, {'$set': {'profile': 1}})
        # random.shuffle(user_profile_tasks)
        # p = r'E:\RZ项目\RZ\测试数据.xlsx'
        # import pandas as pd
        # data = pd.read_excel(io=p)
        # data = data.loc[:, ["end node"]]
        # data = data.to_dict(orient='records')
        data=[{'twitter_url':_['end node']} for _ in data]
        from multiprocessing.dummy import Pool
        pool = Pool(3)
        res = pool.map(task_one_new,data)
    # u=['https://twitter.com/book686']
    # for uu in u:
    #     ux={
    #         'user_url':uu,
    #         'user_tag':0,
    #         'important_user':0,
    #         'profile_task':1,
    #         'profile_crawl_status':0,
    #     }
    #     task_one(ux)
    # for i in range(500):
    #     nums = 1000
    #     user_profile_tasks = MongoUserTaskOper.get_user_profile_tasks(important_user=0, nums=nums)  # 这里执行需要几秒的时间
    #     # 将档案信息任务分配到爬取档案信息的redis队列中
    #     print('开始写入{}条档案信息任务...'.format(len(user_profile_tasks)))
    #     from multiprocessing.dummy import Pool
    #     pool = Pool(10)
    #     res = pool.map(task_one,user_profile_tasks)
    #     time.sleep(60)





















