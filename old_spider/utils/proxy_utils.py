import requests
import time
import random
import urllib3
import json
import jsonpath

from requests.exceptions import ProxyError

from db.dao import AccountInfoOper
from db.redis_db import get_redis_conn
from config.settings import PROXY_POOL
from config.headers import verify_headers
from logger.log import other_logger

# conn = get_redis_conn()


def get_one_proxy(conn):
    pickled_proxy = conn.rpop(PROXY_POOL)
    if not pickled_proxy:
        print('代理池为空')
        return None
    conn.lpush(PROXY_POOL, pickled_proxy)
    return json.loads(pickled_proxy)


# mysql
def push_proxies(conn):
    # 从数据库account_info_for_spider中选择twitter平台的proxy存入redis
    conn.ltrim(PROXY_POOL, 1, 0)  # 删除索引号是0-1之外的元素，只保留索引号是0-1的元素---这里即删除建PROXY_POOL
    proxies = set([json.dumps(p.proxies) for p in AccountInfoOper.get_all_proxy(alive=1)])
    for proxy in proxies:
        conn.lpush(PROXY_POOL, proxy)


# 获取错误代理
def get_error_proxies():
    from db.mongo_db import MongoSpiderAccountOper
    error_proxies = MongoSpiderAccountOper.get_error_proxies()
    print(error_proxies)
    error_proxies = list(set([p.get('proxies').get('http') for p in error_proxies]))
    error_proxies.sort()
    print(error_proxies)
    print(len(error_proxies))


# mongodb获取所有代理---
def get_all_proxies():
    from db.mongo_db import MongoSpiderAccountOper
    all_proxies = MongoSpiderAccountOper.get_all_proxies()
    all_proxies = list(set([int(p.get('proxies').get('http').split(':')[-1]) for p in all_proxies]))
    all_proxies.sort()
    print(all_proxies)
    for i in range(10802, 10980):
        p = 'http://10.0.12.1:' + str(i)
        if i not in all_proxies:
            print(i)

    return all_proxies


def statistic_proxy_account_count():
    """
    统计每个代理绑定的账号数量
    :return:
    """
    from pymongo import MongoClient  # 需要pip安装
    from db.mongo_db import client, mongo_name
    import pandas as pd

    collection = client[mongo_name]["account_info_for_spider"]
    data = collection.find({'alive': {"$in": [1, 4, 130]}, 'site': 'twitter'})
    data = list(data)  # 在转换成列表时，可以根据情况只过滤出需要的数据。(for遍历过滤)

    df = pd.DataFrame(data)  # 读取整张表 (DataFrame)
    df['proxies'] = df['proxies'].astype(str)
    df = df[['_id', 'proxies']]
    data = df.groupby('proxies').count().reset_index()
    data.columns = ['proxies', 'account_count']

    res = data[data['account_count'] == 2]
    res = list(res['proxies'])
    res = [eval(r).get('http').split(':')[-1] for r in res]
    print(len(res))
    # p = json.loads(random.choice(res).replace("'", '"'))
    # return json.dumps(p.get('http').split(':')[-1])
    return res


def push_proxies_from_mongo2redis(conn):
    """
    从数据库account_info_for_spider中选择twitter平台的proxy存入redis
    :param conn:
    :return:
    """
    from db.mongo_db import MongoSpiderAccountOper
    # 从数据库account_info_for_spider中选择twitter平台的proxy存入redis
    conn.ltrim(PROXY_POOL, 1, 0)  # 删除索引号是0-1之外的元素，只保留索引号是0-1的元素---这里即删除建PROXY_POOL
    proxies = MongoSpiderAccountOper.get_normal_proxies(alive=1)
    for proxy in proxies:
        conn.lpush(PROXY_POOL, proxy)


if __name__ == '__main__':
    # res = conn.lrange(PROXY_POOL, 0, 1)
    # print(res)
    # res = conn.zadd('zset_name1', {str({'a': 10}): 10})
    # # push_proxies(conn)
    # pro = get_one_proxy(conn)
    # print(type(pro))
    # get_all_proxies()
    conn = get_redis_conn()
    # get_error_proxies()
    push_proxies_from_mongo2redis(conn)
    # get_all_proxies()
    # res = statistic_proxy_account_count()
    # print(res)
    # push_proxies_from_mongo2redis(get_redis_conn())

