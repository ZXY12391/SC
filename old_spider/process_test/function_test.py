# from db.models import UserInfoSearchResults
# from db.dao import KeywordsOper
# data = UserInfoSearchResults()
# data.user_url = 'fs'
# KeywordsOper.insert_one_entity(data)
import pandas as pd

df = pd.DataFrame({'a': [5, 1,2,10,8,9,100,78]})
df.sort_values('a', ascending=False, inplace=True)
print(df)
# keyword_user_attrs = ['user_url', 'screen_id', 'screen_name', 'user_name', 'img_url', 'register_time', 'img_binary',
#                       'introduction', 'location', 'website', 'following_count', 'followers_count', 'tweets_count',
#                       'media_tweets_count', 'favourites_tweets_count', 'is_protected', 'search_keyword', 'fetch_time',
#
#                       ]
# keyword_tweet_attrs = ['tweet_url', 'author_url', 'author_name', 'content', 'tweet_type', 'tweet_language',
#                        'comment_count', 'retweet_count', 'quote_count', 'praise_count', 'publish_time', 'fetch_time',
#                        'search_keyword',
#                        ]
# keyword_tweet_attrs_dict = {}
# for key in keyword_tweet_attrs:
#     if key == 'tweet_language':
#         keyword_tweet_attrs_dict[key] = ''
#         continue
#     keyword_tweet_attrs_dict[key] = key
#
# print(keyword_tweet_attrs_dict)
# keyword_tweet_attrs_temp = [(key, value) for key, value in keyword_tweet_attrs_dict.items() if value]
# print(keyword_tweet_attrs)
# print(keyword_tweet_attrs_temp)
# keyword_tweet_attrs_dict = {'tweet_url': 'url', 'author_url': 'user_id', 'author_name': 'screenname',
#                             'content': 'content', 'tweet_type': '', 'tweet_language': '',
#                             'comment_count': '', 'retweet_count': 'retweet_count',
#                             'quote_count': '', 'praise_count': 'favorite_count',
#                             'publish_time': 'publish_time', 'fetch_time': '',
#                             'search_keyword': '',
#                             }
#
# keyword_tweet_attrs_temp = [(key, value) for key, value in keyword_tweet_attrs_dict.items() if value]
# for key1, key2 in keyword_tweet_attrs_temp:
#     print(key1, key2)
# import time
# current_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
# print(type(current_time))

# dice = {'b': 12}
# if dice.get('a'):
#     print(dice)
# else:
#     dice.get('a')
# if dice.get('a') == None:
#     print('aaaaa')
# from celery_app.workers import get_task_from_redis
# from config.settings import POST_KEY, FOLLOWING_KEY, FOLLOWER_KEY
# tasks = {key: [get_task_from_redis(key) for i in range(5)]} for key in [POST_KEY, FOLLOWER_KEY, FOLLOWING_KEY]
# tasks = [{'twitter:post': [{'user_url': 'www.f.com', 'add_time': '2021-07-01 16:09:50', 'follower_crawl_status': 0, 'follower_task': 1, 'following_crawl_status': 0, 'following_task': 1, 'important_user': 0, 'post_commenter_task': 1, 'post_crawl_status': 0, 'post_praiser_task': 1, 'post_reposter_task': 1, 'post_task': 1, 'user_name': 'f', 'user_tag': '甲方用户2'}, None, None, None, None]}, {'twitter:follower': [None, None, None, None, None]}, {'twitter:following': [{'user_url': 'www.ws.com', 'add_time': '2021-07-01 16:09:35', 'follower_crawl_status': 0, 'follower_task': 1, 'following_crawl_status': 0, 'following_task': 1, 'important_user': 0, 'post_commenter_task': 1, 'post_crawl_status': 0, 'post_praiser_task': 1, 'post_reposter_task': 1, 'post_task': 0, 'user_name': 'ws', 'user_tag': '甲方用户2'}, {'user_url': 'www.fs.com', 'add_time': '2021-07-01 16:09:43', 'follower_crawl_status': 0, 'follower_task': 0, 'following_crawl_status': 0, 'following_task': 1, 'important_user': 0, 'post_commenter_task': 1, 'post_crawl_status': 0, 'post_praiser_task': 1, 'post_reposter_task': 1, 'post_task': 0, 'user_name': 'fs', 'user_tag': '甲方用户2'}, {'user_url': 'www.f.com', 'add_time': '2021-07-01 16:09:50', 'follower_crawl_status': 0, 'follower_task': 1, 'following_crawl_status': 0, 'following_task': 1, 'important_user': 0, 'post_commenter_task': 1, 'post_crawl_status': 0, 'post_praiser_task': 1, 'post_reposter_task': 1, 'post_task': 1, 'user_name': 'f', 'user_tag': '甲方用户2'}, None, None]}]
# for task in tasks:
#     for t in task.get()
# print(tasks)
# import datetime
# import time
# publish_time = time.strptime('Wed Jan 06 23:23:59 +0000 2021', "%a %b %d %H:%M:%S +0000 %Y")
# publish_time = time.strftime("%Y-%m-%d %H:%M:%S", publish_time)
# print(type(publish_time))

# def fs():
#     from db.mongo_db import MongoUserInfoOper
#     person_url = 'https://twitter.com/realEmperorPoo'
#     task_profile = MongoUserInfoOper.get_user_info_data(person_url)
#     if task_profile:
#         screen_id, username, image_url = task_profile.get('screen_id'), task_profile.get('user_name'), task_profile.get(
#             'img_url'),
#     else:
#         screen_id, username, image_url = 'fs', 'fs', 'fs'
#     print(screen_id, username, image_url)

# fs()
# def crawl_screen_id_2():
#     import json
#     from db.basic import get_db_session
#     from db.models import AccountInfoForSpider
#     def get_account(account):
#
#
#         with get_db_session() as db_session:
#             rs = db_session.query(AccountInfoForSpider).filter_by(account=account).first()
#         return rs.token, rs.proxies, rs.account
#
#     account = '664540966Mi'
#     cookies, proxies, account = get_account(account)
#     cookies = {cookie['name']: cookie['value'] for cookie in json.loads(cookies)}
#     from utils.proxy_oper import get_one_proxy
#     from db.mongo_db import MongoUserTweetTouristOper
#     from db.redis_db import get_redis_conn
#     import json
#     import jsonpath
#     import random
#     import time
#     from json import JSONDecodeError
#     from urllib.parse import quote
#     from db.dao import AccountInfoOper
#     from db.mongo_db import MongoKeywordTaskOper, MongoUserInfoOper, MongoUserTaskOper
#     from utils.image_oper import crawl_avatar_image
#     import base64
#
#     from db.redis_db import get_redis_conn
#     from utils.account_oper import select_normal_account
#     from utils.proxy_oper import get_one_proxy
#     from logger.log import download_logger, other_logger, parser_logger
#     from crawlers.downloader import download_with_cookies, download_without_cookies
#     from parsers.parser_data import (parser_keyword_tweet, parser_user_info, parser_relationship, parser_user_tweet,
#                                      parser_user_topic, parser_keyword_user, parser_praise_or_retweet,
#                                      parser_comment_reply)
#     user_list = MongoUserTweetTouristOper.query_datas_by_condition(collection='T_user_tourist', condition={'screen_id': {'$exists': False}})
#     user_urls = set([tweet.get('user_url') for tweet in user_list])
#     print(len(user_urls))
#     conn = get_redis_conn()
#     for index, user_url in enumerate(user_urls):
#         proxies = get_one_proxy(conn)
#         print('{} 抓取用户个人主页URL：{} 的基本信息'.format(index, user_url))
#         screen_name = user_url[20:]  # 获取用户的screen_name
#         # 该链接不需要登录就可爬取链接
#         url = 'https://api.twitter.com/graphql/-xfUfZsnR_zqjFd-IfrN5A/UserByScreenName?variables=%7B%22screen_name%22%3A%22{}%22%2C%22withHighlightedLabel%22%3Atrue%7D'.format(
#             quote(screen_name))
#         # # 该链接需要登录才能爬取信息
#         # url = 'https://twitter.com/i/api/graphql/Vf8si2dfZ1zmah8ePYPjDQ/UserByScreenNameWithoutResults?variables=%7B%22screen_name%22%3A%22{}%22%2C%22withHighlightedLabel%22%3Atrue%7D'.format(quote(screen_name))
#         resp = download_without_cookies(url, proxies)
#         if resp is None:
#             download_logger.error('url {} 基本信息抓取失败'.format(user_url))
#             continue  # 0表示需要重新爬取，1表示不需要重新爬取
#         try:
#             content = json.loads(resp.content)
#             # 判断该用户url是否存在
#             user_status_code = content.get("errors")[0].get('code') if content.get("errors") else 1
#             if user_status_code == 50:
#                 parser_logger.error('url {} 不存在,采集基本信息失败'.format(user_url))
#                 MongoUserTweetTouristOper.update_muti_data(collection='T_user_tourist',
#                                                            condition={'user_url': user_url}, data={'screen_id': None})
#
#                 continue  # 0表示需要重新爬取，1表示不需要(账号不存在)重新爬取
#             if user_status_code == 63:
#                 parser_logger.error('url {} 异常（ User has been suspended）,采集基本信息失败'.format(user_url))
#                 MongoUserTweetTouristOper.update_muti_data(collection='T_user_tourist',
#                                                            condition={'user_url': user_url}, data={'screen_id': None})
#                 continue
#
#             try:
#                 user = content['data']['user']['legacy']
#             except Exception as e:
#                 continue
#             screen_id = content['data']['user']['rest_id']
#
#             user_data = {}
#             user_data['user_url'] = user_url
#             user_data['screen_id'] = screen_id
#             MongoUserTweetTouristOper.update_muti_data(collection='T_user_tourist', condition={'user_url': user_url}, data=user_data)
#             print('更新成功')
#         except Exception as e:
#             print('异常')
#
# def crawl_screen_id():
#     def get_account(account):
#         from db.basic import get_db_session
#         from db.models import AccountInfoForSpider
#
#         with get_db_session() as db_session:
#             rs = db_session.query(AccountInfoForSpider).filter_by(account=account).first()
#         return rs.token, rs.proxies, rs.account
#
#     account = '664540966Mi'
#     cookies, proxies, account = get_account(account)
#     cookies = {cookie['name']: cookie['value'] for cookie in json.loads(cookies)}
#
#     from db.mongo_db import MongoUserTweetTouristOper
#     tweet_list = MongoUserTweetTouristOper.query_datas_by_condition(collection='T_user_tourist', condition={'screen_id': {'$exists': False}})
#     tweet_urls = set([tweet.get('tweet_url') for tweet in tweet_list])
#     print(len(tweet_urls))
#     for index, tweet_url in enumerate(tweet_urls):
#         tweet = MongoUserTweetTouristOper.query_data_by_condition(collection='T_user_tourist', condition={'tweet_url': tweet_url})
#         print(index+1, ' author:{}'.format(tweet.get('author_url')))
#         praise_retweet_process(tweet.get('author_url'), tweet.get('tweet_url'), 'retweet', cookies, proxies)
#         praise_retweet_process(tweet.get('author_url'), tweet.get('tweet_url'), 'praise', cookies, proxies)
#
#     # for tweet in tweet_list:
#     #     if tweet.get('praise'):
#     #         praise_retweet_process(tweet.get('author_url'), tweet.get('tweet_url'), 'praise', cookies, proxies)
#     #     if tweet.get('retweet'):
#     #         praise_retweet_process(tweet.get('author_url'), tweet.get('tweet_url'), 'retweet', cookies, proxies)
#     #     if tweet.get('comment'):
#     #         continue
#     #         # comment_process(tweet.get('author_url'), tweet.get('tweet_url'), tweet.get('tweet_url').split('/')[-3], cookies, proxies)
#
# crawl_screen_id_2()
#
# import time
# from celery_app.execute_task import exec_all_task


# 测试抓取基本信息
def user_info_test():
    import time
    from tasks.process_task import user_info_process
    task = {
        'user_url': 'https://twitter.com/664540966Mi',
        'user_name': 'fs',
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
        'tourist_crawl_status': 0,
        'user_tag': None,
    }
    # todo 解析点赞者、评论者的代码需要修改回去，现在正在爬取tourist表中的screen_id

    # user_tweet_process(task)
    # user_relationship_process(task, 'follower')
    user_info_process(task)


def run_keyword():
    from execute_twitter_keyword import crawl_keyword_user
    crawl_keyword_user()


def get_important_user():
    from db.mongo_db import MongoKeywordTweetOper
    all_important_user = set()

    all_tweet = MongoKeywordTweetOper.get_all_tweet()
    key_words = ['vpn', '科学上网', 'GFW', 'Shadowsocks', 'v2ray', '自由门', '无界浏览', 'SS vpn', '机场 vpn']
    for tweet in all_tweet:
        for k in key_words:
            if k in tweet.get('author_name'):
                all_important_user.add(tweet.get('author_url'))
                break
    print(all_important_user)
    print(len(all_important_user))


def get_error_proxies():
    """
    获取错误代理---83个
    :return:
    """
    from db.mongo_db import MongoSpiderAccountOper

    all_prosies = set()
    for proxy in [p.get('proxies') for p in MongoSpiderAccountOper.get_error_proxies()]:
        all_prosies.add(proxy.get('http'))
    print(len(all_prosies))


def get_important_users():
    from db.mongo_db import MongoKeywordTweetOper, MongoUserTaskOper
    important_users = MongoKeywordTweetOper.query_datas_by_condition('T_keyword_post_info', {'important_user': '1'})
    important_user_urls = set([u.get('author_url') for u in important_users])
    print(len(important_user_urls))
    for user_url in important_user_urls:
        MongoUserTaskOper.set_important_user(user_url, {'important_user': 1})
        print(user_url)


def proxies_data():
    """
    把cert这边正在使用的代理存入数据库
    :return:
    """
    import time
    import pymysql

    from db.mongo_db import MongoCommonOper
    from utils.proxy_utils import get_all_proxies

    # 链接数据库
    def conn_db():
        # 战资数据库--port默认为3306
        db_host_intranet = '10.0.12.2'
        db_user_intranet = 'zz'
        db_password_intranet = 'ZZ123db!'
        db_name_intranet = 'WQCol1'
        # 连接数据库，创建连接对象connection
        # 连接对象作用是：连接数据库、发送数据库信息、处理回滚操作（查询中断时，数据库回到最初状态）、创建新的光标对象
        connection = pymysql.connect(host=db_host_intranet,  # host属性
                                     user=db_user_intranet,  # 用户名
                                     password=db_password_intranet,  # 此处填登录数据库的密码
                                     db=db_name_intranet,  # 数据库名
                                     charset='utf8mb4'
                                     )
        # 使用 cursor() 方法创建一个游标对象 cursor
        cursor = connection.cursor()
        return connection, cursor

    def get_isp(port):
        connection, cursor = conn_db()
        sql = "select isp from proxy_data where port={}".format(port)
        cursor.execute(sql)
        return cursor.fetchone()[0]


    F_proxies = [10803, 10820, 10821, 10824, 10825, 10826, 10828, 10830, 10831, 10833, 10836, 10843, 10844, 10845,
                 10846, 10851, 10852, 10854, 10857, 10858, 10859]
    T_proxies = get_all_proxies()
    all_proxies = list(set(F_proxies + T_proxies))
    for p in all_proxies:
        d = {}
        d['port'] = p
        d['isp'] = get_isp(p)
        status = 'F' if p in F_proxies else ''
        status += 'T' if p in T_proxies else ''
        d['status'] = status
        d['host'] = None
        d['update_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        d['create_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        print(d)
        MongoCommonOper.insert_or_update_one_data('proxy_data', {'port': p}, d)


def process_test():
    import time
    from tasks.process_task import user_tweet_process, user_relationship_process, user_info_process
    task = {
        'user_url': 'https://twitter.com/917Yangzhou',
        'user_name': '',
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
        'tourist_crawl_status': 0,
        'user_tag': 'yz',
    }
    # todo 解析点赞者、评论者的代码需要修改回去，现在正在爬取tourist表中的screen_id

    # user_tweet_process(task)
    user_relationship_process(task, 'follower')
    # user_info_process(task)


def zpw_timezone1202():
    import pandas as pd
    import json
    data = pd.read_csv('./countryname.csv', encoding='GBK', delimiter="\t")

    keys = ['Afghanistan'] + data['Afghanistan'].tolist()
    name_cns = ['阿富汗'] + data['阿富汗'].tolist()
    en_zh_map = {k.strip(): v.strip() for k, v in zip(keys, name_cns)}
    print(len(en_zh_map))
    with open('./timezone.json', encoding='utf-8') as f:
        results = json.load(f)
    data = {}
    # zh_data = {}
    for res in results:
        data[res.get('name').strip()] = res.get('gmt').strip()
        if res.get('name') in en_zh_map.keys():
            data[en_zh_map[res.get('name').strip()]] = res.get('gmt').strip()
        else:
            print(res.get('name'))

    with open('en_zh_timezone.json', 'w', encoding='utf-8') as f:
        f.write(json.dumps(data, ensure_ascii=False))


if __name__ == '__main__':
    # process_test()
    # from utils.other_utils import get_data_nums
    # get_data_nums()
    # from utils.other_utils import get_data_nums
    # from celery_app.beat_tasks import user_post_task
    # user_post_task(10)
    import time
    task = {
        'user_url': 'https://twitter.com/664540966Mi',
        'user_name': '',
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
        'tourist_crawl_status': 0,
        'user_tag': None,
    }
    # urls = ['https://twitter.com/Aryan6079', 'https://twitter.com/kendalljepit__', 'https://twitter.com/nudeaesth',
    #         'https://twitter.com/AlbertanFranco', 'https://twitter.com/shwwetaaa_', 'https://twitter.com/NeverTymus']
    # seeds = []
    # for url in urls:
    #     task = {
    #         'user_url': '',
    #         'user_name': 'Netinho_91',
    #         'important_user': 0,
    #         'add_time': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
    #         'update_time': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
    #
    #         'follower_task': 1,
    #         'following_task': 1,
    #         'post_task': 1,
    #         'profile_task': 1,
    #
    #         'post_praise_task': 1,
    #         'post_repost_task': 1,
    #         'post_comment_task': 1,
    #         'post_reply_task': 1,
    #
    #         'post_crawl_status': 0,
    #         'following_crawl_status': 0,
    #         'follower_crawl_status': 0,
    #         'profile_crawl_status': 0,
    #         'tourist_crawl_status': 0,
    #         'user_tag': None,
    #     }
    #     task['user_url'] = url
    #     seeds.append(task)
    # import threading
    #
    # tasks = []
    # for seed in seeds:
    #     task = threading.Thread(target=user_relationship_process, args=(seed, 'follower'))
    #     tasks.append(task)
    #
    # for task in tasks:
    #     task.start()
    #     time.sleep(random.randint(1, 3))
    #
    # for task in tasks:
    #     task.join()

    # todo 解析点赞者、评论者的代码需要修改回去，现在正在爬取tourist表中的screen_id
    # todo 为什么爬取推文信息的时候在服务器上跑，爬完一页需要20几分钟，但是在本地却只要30秒左右？？ 但是关系的爬取很快
    # todo 难道是因为celery携程开得太多了？---已验证，将携程数改为20 还是很慢
    # todo 或者是将推文插入celery worker队列的操作太耗时？（因为采关系的吴该操作，故有可能有该原因---已验证不是该原因，该程序在服务器上与celery同时跑，速度很快）
    # todo 还有一个原因是不是因为堆积的任务太多造成celery出问题？？
    # user_tweet_process(task)
    # user_relationship_process(task, 'follower')
    # from tasks.process_task import user_info_process
    # user_info_process(task)