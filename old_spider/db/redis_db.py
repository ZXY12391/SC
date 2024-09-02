import redis
import json
import time
import uuid
import random

from utils.other_utils import get_timestamp
from utils.account_utils import validate_cookies
from config.settings import REDIS_HOST, REDIS_PORT, REDIS_DB, REDIS_PASS, LOCKER_PREFIX, SPIDER_ACCOUNT_ERROR, SPIDER_ACCOUNT_POOL, \
    SPIDER_ACCOUNT_USING
from logger.log import spider_logger
from account_manager.account_class import SpiderAccount
from db.mongo_db import MongoSpiderAccountOper

Pool = redis.ConnectionPool(host=REDIS_HOST, port=REDIS_PORT, max_connections=10, db=REDIS_DB)


def get_redis_conn(**kwargs):
    host = kwargs.get('host', REDIS_HOST)
    port = kwargs.get('port', REDIS_PORT)
    db = kwargs.get('db', REDIS_DB)
    password = kwargs.get('password', REDIS_PASS)
    return redis.StrictRedis(connection_pool=Pool)


def acquire_lock(conn, lock_name, acquire_timeout=10, lock_timeout=10):
    """inspired by book 'redis in action' """
    identifier = str(uuid.uuid4())
    lock_name = LOCKER_PREFIX + lock_name
    end = time.time() + acquire_timeout

    while time.time() < end:
        if conn.set(lock_name, identifier, lock_timeout, nx=True):
            return identifier
        elif not conn.ttl(lock_name) or conn.ttl(lock_name) == -1:
            conn.expire(lock_name, lock_timeout)
        time.sleep(0.2)

    return False


def release_lock(conn, lock_name, identifier):
    pipe = conn.pipeline(True)
    lock_name = LOCKER_PREFIX + lock_name
    while True:
        try:
            pipe.watch(lock_name)
            identifier_origin = pipe.get(lock_name).decode()
            if identifier_origin == identifier:
                pipe.multi()
                pipe.delete(lock_name)
                pipe.execute()
                return True
            pipe.unwatch()
            break

        except redis.exceptions.WatchError:
            pass

    return False


conn = get_redis_conn()

# # 新增代码---账号管理
# def get_pipe():
#     conn = get_redis_conn()
#     pipe = conn.pipeline(True)
#     return pipe, conn
#
#
# pipe, conn = get_pipe()


# class RedisCommonOper:
#     # 哈希用来存可用的爬虫账号（包括正在使用的）
#     @classmethod
#     def HSET(cls, key, name, value):
#         pipe.hset(key, name, value)
#         pipe.execute()
#
#     @classmethod
#     def HGET(cls, key, name):
#         pipe.hget(key, name)
#         res = pipe.execute()
#         return res[0] if len(res) > 0 else None
#
#     @classmethod
#     def HGET_KEYS(cls, key):
#         """
#         获得hash表中的所有键
#         :param key:
#         :return: 列表
#         """
#         pipe.hkeys(key)
#         res = pipe.execute()[0]
#         return res
#
#     @classmethod
#     def IS_EXIST(cls, key, name):
#         pipe.hexists(key, name)
#         res = pipe.execute()
#         return res
#
#     @classmethod
#     def HCOUNT(cls, key):
#         pipe.hlen(key)
#         res = pipe.execute()
#         return res
#
#     @classmethod
#     def HDEL(cls, key, name):
#         pipe.hdel(key, name)
#         pipe.execute()
#
#     # 有序集合用来存正在使用的爬虫账号
#     @classmethod
#     def ZADD(cls, key, name, value):
#         pipe.zadd(key, {name: value})
#         pipe.execute()
#
#     @classmethod
#     def ZDEL(cls, key, name):
#         pipe.zrem(key, name)
#         pipe.execute()
#
#     @classmethod
#     def ZGET_MEMBERS(cls, key):
#         """
#         返回有序集合的所有元素（不返回分数）
#         :param key:
#         :return:
#         """
#         pipe.zrangebylex(key, '-', '+')
#         res = pipe.execute()[0]
#         return res
#
#     @classmethod
#     def DEL_KEY(cls, key):
#         pipe.delete(key)
#         pipe.execute()
#
#
# class SpiderAccountOper(RedisCommonOper):
#
#     @classmethod
#     def push_account_from_mongodb_to_redis(cls, alive=1):
#         """
#         将爬虫账号放入redis中
#         :return:
#         """
#         accounts = MongoSpiderAccountOper.get_spider_accounts(alive=alive)
#         for account in accounts:
#             # mongodb中规定task_number=2的账号为账号培育的账号
#             cookies = {cookie['name']: cookie['value'] for cookie in account.get('token')} if account.get('task_number') == 2 else account.get('token')
#             account_info = {'proxies': account.get('proxies'),
#                             'cookies': cookies,
#                             }
#             SpiderAccountOper.HSET(SPIDER_ACCOUNT_POOL, account.get('account'), json.dumps(account_info))
#
#     @classmethod
#     def get_using_spider_accounts(cls):
#         return cls.ZGET_MEMBERS(SPIDER_ACCOUNT_USING)
#
#     @classmethod
#     def get_spider_account_info(cls, account_name):
#         """
#         从可用账号hash表中获取账号，并将该账号放入正在使用的队列中
#         :param account_name:
#         :return:
#         """
#         pipe.hget(SPIDER_ACCOUNT_POOL, account_name)
#         pipe.zadd(SPIDER_ACCOUNT_USING, {account_name: get_timestamp()})
#         res = pipe.execute()[0]
#         return res
#
#     @classmethod
#     def get_spider_account_object(cls):
#         """
#         随机获取一个可用的爬虫账号
#         :return:
#         """
#         # 加锁
#         uid = acquire_lock(conn, 'get_spider_account')
#         spider_accounts = cls.HGET_KEYS(SPIDER_ACCOUNT_POOL)
#         using_spider_account = cls.get_using_spider_accounts()
#         accounts = [a for a in spider_accounts if a not in using_spider_account]
#         random.shuffle(accounts)  # accounts为[]也不会报错
#         if not accounts:
#             spider_logger.error('当前没有可用的爬虫账号')
#             # 解锁
#             release_lock(conn, 'get_spider_account', uid)
#             return None
#         else:
#             res = cls.get_spider_account_info(accounts[0])
#             res = json.loads(res)
#             # 解锁
#             release_lock(conn, 'get_spider_account', uid)
#             return SpiderAccount(accounts[0].decode(), **res)
#
#     @classmethod
#     def select_normal_account_from_redis(cls, select_time=1 * 60):
#         """
#         新增选择正常账号---从redis中选---尝试1分钟
#         :param select_time:
#         :return:
#         """
#         end_time = time.time() + select_time
#         while time.time() < end_time:
#             res = cls.get_spider_account_object()
#             if res:
#                 status = validate_cookies(res.cookies, res.proxies, res.account_name)
#                 # 更新mongodb中该账号的alive
#                 MongoSpiderAccountOper.update_spider_account_status(res.account_name, {'alive': status})
#                 if status == 1:
#                     return res
#                 else:
#                     # 将该账号移动到error队列
#                     cls.push_banned_account_to_error_queue(res.account_name)
#             time.sleep(random.randint(30, 60))
#         return None
#
#     @classmethod
#     def push_banned_account_to_error_queue(cls, account_name):
#         """
#         如果用户被封，直接移动到无效队列,从有效队列与正在使用队列中删除
#         :param name:
#         :return:
#         """
#         res = cls.HGET(SPIDER_ACCOUNT_POOL, account_name)
#         if res is None:
#             return False
#         else:
#             pipe.hdel(SPIDER_ACCOUNT_POOL, account_name)  # 删除可用队列（hash）的键account_name键值
#             pipe.zrem(SPIDER_ACCOUNT_USING, account_name)  # 删除正在使用队列（zset）的account_name值
#             pipe.hset(SPIDER_ACCOUNT_ERROR, account_name, res)  # 将account_name账号的信息放入错误队列（hash）
#             pipe.execute()
#
#     @classmethod
#     def free_using_account(cls, account_name):
#         """
#         账号爬取完数据后从正在使用队列中删除（当该队列中没有该账号时也可正常运行，不会报错）
#         :param account_name:
#         :return:
#         """
#         cls.ZDEL(SPIDER_ACCOUNT_USING, account_name)
#
#     @classmethod
#     def free_banned_account(cls, account_name):
#         """
#         异常账号处理之后变正常，将错误队列中的号删除，并加入正常队列中
#         :param account_name:
#         :return:
#         """
#         res = cls.HGET(SPIDER_ACCOUNT_ERROR, account_name)
#         if res is None:
#             return False
#         else:
#             pipe.hdel(SPIDER_ACCOUNT_ERROR, account_name)  # 删除错误队列（hash）的键account_name键值
#             pipe.hset(SPIDER_ACCOUNT_POOL, account_name, res)  # 将account_name账号的信息放入可用队列（hash）
#             pipe.execute()
#
#     @classmethod
#     def delete_using_account_key(cls, key):
#         """
#         当程序异常停止时，正在使用队列中还有一些账号，需要手动删除
#         :return:
#         """
#         cls.DEL_KEY(key)

class RedisCommonOper:
    # 哈希用来存可用的爬虫账号（包括正在使用的）
    @classmethod
    def HSET(cls, key, name, value):
        conn.hset(key, name, value)

    @classmethod
    def HGET(cls, key, name):
        """
        存在则返回值，不存在则返回None
        :param key:
        :param name:
        :return:
        """
        res = conn.hget(key, name)
        return res

    @classmethod
    def HGET_KEYS(cls, key):
        """
        获得hash表中的所有键
        :param key:
        :return: 列表
        """
        res = conn.hkeys(key)
        return res

    @classmethod
    def HIS_EXIST(cls, key, name):
        """
        hash类型，返回布尔类型，存在返回True，不存在返回False
        :param key:
        :param name:
        :return:
        """
        res = conn.hexists(key, name)
        print(res)
        return res

    @classmethod
    def HCOUNT(cls, key):
        """
        得到hash表的长度
        :param key:
        :return:
        """
        res = conn.hlen(key)
        return res

    @classmethod
    def HDEL(cls, key, name):
        """
        删除hash中的某个键值
        :param key:
        :param name:
        :return:
        """
        conn.hdel(key, name)

    # 有序集合用来存正在使用的爬虫账号
    @classmethod
    def ZADD(cls, key, name, value):
        conn.zadd(key, {name: value})

    @classmethod
    def ZDEL(cls, key, name):
        conn.zrem(key, name)

    @classmethod
    def ZGET_MEMBERS(cls, key):
        """
        返回有序集合的所有元素（不返回分数）
        :param key:
        :return:
        """
        res = conn.zrangebylex(key, '-', '+')
        return res

    @classmethod
    def ZDELS_BY_SCORES(cls, key, score1, score2):
        """
        删除区间score1-score2分数的成员信息（闭区间）
        :param key:
        :param score1:
        :param score2:
        :return:
        """
        res = conn.zremrangebyscore(key, score1, score2)
        return res

    @classmethod
    def LGET_LEN(cls, key):
        return conn.llen(key)

    @classmethod
    def DEL_KEY(cls, key):
        """
        删除任务类型的键
        存在一个问题：当错误队列的键不存在时，也会返回0---已解决
        :param key:
        :return: 删除失败返回0，删除成功返回1
        """
        # if conn.llen(key) == 0:
        #     return 1
        if conn.exists(key) == 0:  # 返回0表示该键不存在，1表示存在
            return 1
        return conn.delete(key)


class SpiderAccountOper(RedisCommonOper):

    @classmethod
    def push_account_from_mongodb_to_redis(cls, alive, task_number):
        """
        将爬虫账号放入redis中
        :return:
        """
        accounts = MongoSpiderAccountOper.get_spider_accounts(alive=alive, task_number=task_number)
        for account in accounts:
            # mongodb中规定task_number=2的账号为账号培育的账号
            cookies = {cookie['name']: cookie['value'] for cookie in account.get('token')} if account.get('task_number') == 2 else account.get('token')
            account_info = {'proxies': account.get('proxies'),
                            'cookies': cookies,
                            }
            SpiderAccountOper.HSET(SPIDER_ACCOUNT_POOL, account.get('account'), json.dumps(account_info))

    @classmethod
    def get_using_spider_accounts(cls):
        """
        获取有序集合中的成员，不返回分数
        :return:
        """
        return cls.ZGET_MEMBERS(SPIDER_ACCOUNT_USING)

    @classmethod
    def get_spider_account_info(cls, account_name):
        """
        从可用账号hash表中获取账号，并将该账号放入正在使用的队列中
        :param account_name:
        :return:
        """
        # 加锁
        uid = acquire_lock(conn, 'get_spider_account_info')
        res = conn.hget(SPIDER_ACCOUNT_POOL, account_name)
        conn.zadd(SPIDER_ACCOUNT_USING, {account_name: get_timestamp()})
        # 解锁
        release_lock(conn, 'get_spider_account_info', uid)
        return res

    @classmethod
    def get_spider_account_object(cls):
        """
        随机获取一个可用的爬虫账号
        :return:
        """
        # 加锁
        uid = acquire_lock(conn, 'get_spider_account')
        spider_accounts = cls.HGET_KEYS(SPIDER_ACCOUNT_POOL)
        using_spider_account = cls.get_using_spider_accounts()
        accounts = [a for a in spider_accounts if a not in using_spider_account]
        random.shuffle(accounts)  # accounts为[]也不会报错
        if not accounts:
            spider_logger.error('当前没有可用的爬虫账号')
            # 解锁
            release_lock(conn, 'get_spider_account', uid)
            return None
        else:
            res = cls.get_spider_account_info(accounts[0])
            res = json.loads(res)
            # 解锁
            release_lock(conn, 'get_spider_account', uid)
            return SpiderAccount(accounts[0].decode(), **res)

    @classmethod
    def select_normal_account_from_redis(cls, select_time=5 * 60):
        """
        新增选择正常账号---从redis中选---尝试1分钟
        :param select_time:
        :return:
        """
        end_time = time.time() + select_time
        while time.time() < end_time:
            res = cls.get_spider_account_object()
            if res:
                # status = validate_cookies(res.cookies, res.proxies, res.account_name)
                # # 更新mongodb中该账号的alive
                # MongoSpiderAccountOper.update_spider_account_status(res.account_name, {'alive': status})
                # if status == 1:
                #     return res
                # else:
                #     # 将该账号移动到error队列
                #     cls.push_banned_account_to_error_queue(res.account_name)
                return res
            time.sleep(random.randint(30, 60))
        return None

    @classmethod
    def push_banned_account_to_error_queue(cls, account_name):
        """
        如果用户被封，直接移动到无效队列,从有效队列与正在使用队列中删除
        :param name:
        :return:
        """
        res = cls.HGET(SPIDER_ACCOUNT_POOL, account_name)
        if res is None:
            return False
        else:
            # 加锁
            uid = acquire_lock(conn, 'push_banned_account')
            conn.hdel(SPIDER_ACCOUNT_POOL, account_name)  # 删除可用队列（hash）的键account_name键值
            conn.zrem(SPIDER_ACCOUNT_USING, account_name)  # 删除正在使用队列（zset）的account_name值
            conn.hset(SPIDER_ACCOUNT_ERROR, account_name, res)  # 将account_name账号的信息放入错误队列（hash）
            # 解锁
            release_lock(conn, 'push_banned_account', uid)

    @classmethod
    def free_using_account(cls, account_name):
        """
        账号爬取完数据后从正在使用队列中删除（当该队列中没有该账号时也可正常运行，不会报错）
        :param account_name:
        :return:
        """
        cls.ZDEL(SPIDER_ACCOUNT_USING, account_name)

    @classmethod
    def free_banned_account(cls, account_name):
        """
        异常账号处理之后变正常，将错误队列中的号删除，并加入正常队列中（对于不存在的键也不会报错）
        :param account_name:
        :return:
        """
        res = cls.HGET(SPIDER_ACCOUNT_ERROR, account_name)
        if res is None:
            return False
        else:
            # 加锁
            uid = acquire_lock(conn, 'free_banned_account')
            conn.hdel(SPIDER_ACCOUNT_ERROR, account_name)  # 删除错误队列（hash）的键account_name键值
            conn.hset(SPIDER_ACCOUNT_POOL, account_name, res)  # 将account_name账号的信息放入可用队列（hash）
            # 解锁
            release_lock(conn, 'free_banned_account', uid)

    @classmethod
    def delete_using_account_key(cls, key):
        """
        当程序异常停止时，正在使用队列中还有一些账号，需要手动删除--目前采用设一个定时任务（对于不存在的键也不会报错）
        :return:
        """
        cls.DEL_KEY(key)

    @classmethod
    def delete_expire_using_account(cls, expire_time):
        """
        删除超过过期时间的账号
        :param expire_time:
        :return:
        """
        return cls.ZDELS_BY_SCORES(SPIDER_ACCOUNT_USING, 0, get_timestamp() - expire_time * 60 * 60 * 1000)


if __name__ == '__main__':
    #38个账号可用，其他由于代理问题无法判断
    #twitter
#DB操作 OK
    spider_account = SpiderAccountOper.HGET_KEYS(SPIDER_ACCOUNT_POOL)
    print(len(spider_account))
    # print(spider_account.account_name)
    # print(spider_account.cookies)
    # print(spider_account.proxies)
    # print(spider_account.alive)
    # if spider_account is None:
    #     raise Exception
#     a={'aa':['xxx','zzz'],'b':['yyyy','kkkk'],'t':2}
# #cookies = {cookie['name']: cookie['value'] for cookie in account.get('token')} if account.get('task_number') == 2 else account.get('token')
#     cookies =a.get('aa') if a.get('t') == 2 else a.get('b')
#     print(cookies)
#     SpiderAccountOper.push_account_from_mongodb_to_redis(alive=1, task_number=2)
#     conn.zadd(SPIDER_ACCOUNT_USING, {'test_1': get_timestamp()})
#     a=SpiderAccountOper.get_using_spider_accounts()
#     print(a)
#     SpiderAccountOper.delete_using_account_key(SPIDER_ACCOUNT_USING)
    # SpiderAccountOper.push_banned_account_to_error_queue('fs')
    # SpiderAccountOper.ZADD('FS', 'fs1', 100)
    # SpiderAccountOper.ZADD('FS', 'fs2', 200)
    # SpiderAccountOper.ZADD('FS', 'fs3', 300)
    # SpiderAccountOper.ZADD('FS', 'fs4', 400)
    # SpiderAccountOper.ZADD('FS', 'fs5', 500)
    # SpiderAccountOper.ZADD('FS', 'fs6', 600)

    # SpiderAccountOper.ZDELS_BY_SCORES('FS', 0, 700 - 300)

    # spider_account = SpiderAccountOper.get_spider_account_object()
    # SpiderAccountOper.free_banned_account('664540966Mi')

    # import threading
    # tasks = []
    # for seed in range(20):
    #     task = threading.Thread(target=SpiderAccountOper.get_spider_account_object)
    #     tasks.append(task)
    # for task in tasks:
    #     task.start()
    #     time.sleep(random.randint(0, 1))
    #
    # for task in tasks:
    #     task.join()
    # print('结束')
    # SpiderAccountOper.delete_using_account_key(SPIDER_ACCOUNT_POOL)
    # 多线程模拟爬虫任务
    # import threading
    #
    # def fs_test_account():
    #     res = SpiderAccountOper.select_normal_account_from_redis()
    #     if res is None:
    #         raise Exception
    #     print('{}开始任务。。'.format(res.account_name))
    #     time.sleep(20)
    #     if random.randint(1, 10) < 3:
    #         print('账号异常')
    #         SpiderAccountOper.push_banned_account_to_error_queue(res.account_name)
    #     print('完成任务')
    #     time.sleep(20)
    #     SpiderAccountOper.free_using_account(res.account_name)
    #
    # tasks = []
    # for seed in range(10):
    #     # 默认爬取top下的相关推文
    #     task = threading.Thread(target=fs_test_account)
    #     tasks.append(task)
    # for task in tasks:
    #     task.start()
    #     time.sleep(random.randint(1, 2))
    #
    # for task in tasks:
    #     task.join()
    # print('结束')


