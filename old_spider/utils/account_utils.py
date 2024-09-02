import requests
import time
import random
import urllib3
import json
import jsonpath

from requests import ConnectTimeout
from urllib3.exceptions import NewConnectionError
from requests.exceptions import ProxyError, SSLError, ReadTimeout

from db.dao import AccountInfoOper
from db.mongo_db import MongoCommonOper, MongoSpiderAccountOper
from config.settings import PFT, SPIDER_ACCOUNT_POOL, PROXY_ERROR_CODE
from config.headers import verify_headers
from logger.log import other_logger, download_logger

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# conn = get_redis_conn()


def get_unpickled(conn):
    pickled_proxy_cookie = conn.rpop(PFT)
    # print(pickled_proxy_cookie)
    # if not pickled_proxy_cookie:
    #     print('无账号可用')
    #     return 0, 0
    # # print(pickled_proxy_cookie)
    conn.lpush(PFT, pickled_proxy_cookie)
    unpickled_pc = json.loads(pickled_proxy_cookie)
    return unpickled_pc['cookies'], unpickled_pc['proxies'], unpickled_pc['account']


# 从数据库account_info_for_spider中选择twitter平台alive为1的账号存入redis-----mysql-2-redis
def push_proxy_account(conn, alive):
    conn.ltrim(PFT, 1, 0)  # 删除索引号是0-1之外的元素，只保留索引号是0-1的元素---这里即删除建PFT
    proxy_cookies = AccountInfoOper.get_proxy_cookies(alive)
    random.shuffle(proxy_cookies)
    for proxy_cookie in proxy_cookies:
        # pickled_proxy_cookie = json.dumps({'cookies': proxy_cookie.token, 'proxies': proxy_cookie.proxies})
        pickled_proxy_cookie = json.dumps(
            {'cookies': proxy_cookie.token, 'proxies': proxy_cookie.proxies, 'account': proxy_cookie.account})
        conn.lpush(PFT, pickled_proxy_cookie)


# 从数据库account_info_for_spider中选择twitter平台alive为1,task_number=1的账号存入redis-----mongodb-2-redis  001用
def push_proxy_account_001(conn, alive):
    conn.ltrim(PFT, 1, 0)  # 删除索引号是0-1之外的元素，只保留索引号是0-1的元素---这里即删除建PFT
    proxy_cookies = MongoSpiderAccountOper.get_spider_accounts(alive=alive, task_number=2)
    random.shuffle(proxy_cookies)
    for proxy_cookie in proxy_cookies:
        cookies = json.dumps({cookie['name']: cookie['value'] for cookie in proxy_cookie.get('token')})
        pickled_proxy_cookie = json.dumps(
            {'cookies': cookies, 'proxies': proxy_cookie.get('proxies'), 'account': proxy_cookie.get('account')})
        conn.lpush(PFT, pickled_proxy_cookie)


# 验证账号是否异常
def validate_cookies(cookies, proxies, account):
    print('正在验证 {} 账号是否正常...'.format(account))
    verify_url = "https://twitter.com/i/api/2/notifications/all.json?include_profile_interstitial_type=1&include_blocking=1&include_blocked_by=1&include_followed_by=1&include_want_retweets=1&include_mute_edge=1&include_can_dm=1&include_can_media_tag=1&skip_status=1&cards_platform=Web-12&include_cards=1&include_ext_alt_text=true&include_quote_count=true&include_reply_count=1&tweet_mode=extended&include_entities=true&include_user_entities=true&include_ext_media_color=true&include_ext_media_availability=true&send_error_codes=true&simple_quoted_tweet=true&count=40&cursor=DAABDAABCgABDaoQ3b9UMAEIAAIAAAABCAADSQ3bEQgABM0TNtoACwACAAAAC0FYYk5FWGY0aURnAAA&ext=mediaStats%2ChighlightedLabel"
    verify_headers['x-csrf-token'] = cookies.get('ct0')
    verify_headers['referer'] = 'https://twitter.com/{}'.format(account)
    try:
        resp = requests.get(verify_url, headers=verify_headers, verify=False, proxies=proxies,
                            cookies=cookies, timeout=10)
        res = resp.json()
        errors = jsonpath.jsonpath(json.loads(resp.content), '$..errors[*]')

        if errors and not res.get('timeline'):
            error_code = errors[0]['code']
            other_logger.error('{}账号异常，异常代码：{}，异常信息：{}'.format(account, error_code, errors[0]['message']))
            return error_code
        print(' {} 账户正常'.format(account))
        return 1
    except ReadTimeout as e:
        download_logger.error('代理{}, 超时{}'.format(proxies, e))
    except (NewConnectionError, ConnectionError, TimeoutError, ConnectTimeout) as e:
        download_logger.error('代理{}, 异常{}'.format(proxies, e))
    except ProxyError as e:
        download_logger.error('代理{}, 代理出错{}'.format(proxies, e))
    except SSLError as e:
        download_logger.error('代理{}, ssl链接{}'.format(proxies, e))
    except Exception as e:
        download_logger.error('代理{}, 异常{}'.format(proxies, e))
    return PROXY_ERROR_CODE


# mysql版本
def select_normal_account(conn):
    count = 0
    while count < 4:
        cookies, proxies, account = get_unpickled(conn)
        print(account, proxies)
        status = validate_cookies(json.loads(cookies), proxies, account)
        time.sleep(random.randint(5, 10))
        if status == 1:
            AccountInfoOper.set_account_status(account, status)
            return cookies, proxies, account
        else:
            AccountInfoOper.set_account_status(account, status)
            count += 1
            continue
    return None, None, None


# 验证mysql中的所有账户是否可用
def varify_account():
    search_keyword = '自由门'
    tweet_nums = 200
    page_count = 1
    import json
    from urllib.parse import quote
    from db.redis_db import get_redis_conn
    from logger.log import download_logger, other_logger, parser_logger, db_logger
    from crawlers.downloader import download_with_cookies

    all_account = AccountInfoOper.get_all_account(alive=1)
    first_url = 'https://twitter.com/i/api/2/search/adaptive.json?include_profile_interstitial_type=1&include_blocking=1&include_blocked_by=1&include_followed_by=1&include_want_retweets=1&include_mute_edge=1&include_can_dm=1&include_can_media_tag=1&skip_status=1&cards_platform=Web-12&include_cards=1&include_ext_alt_text=true&include_quote_count=true&include_reply_count=1&tweet_mode=extended&include_entities=true&include_user_entities=true&include_ext_media_color=true&include_ext_media_availability=true&send_error_codes=true&simple_quoted_tweet=true&q={}&tweet_search_mode=live&count={}&query_source=typed_query&pc=1&spelling_corrections=1&ext=mediaStats%2ChighlightedLabel'.format(
        quote(search_keyword), tweet_nums)
    for account in all_account:
        cookies, proxies = json.loads(account.token), account.proxies

        url = first_url if page_count == 1 else 'next_url'
        print("抓取关键词 {} 第{}页推文信息（每页数据最多{}条推文）".format(search_keyword, page_count, tweet_nums))
        resp = download_with_cookies(url, cookies, proxies)
        if resp is None:
            print('代理、链接或网络异常')
            print('关键词 {} 第{}页信息抓取失败'.format(search_keyword, page_count))
            download_logger.error('代理、链接或网络异常: {} ; 关键词 {} 第{}页信息抓取失败'.format(proxies, search_keyword, page_count))
            AccountInfoOper.set_account_status(account.account, PROXY_ERROR_CODE)
            continue
        try:
            content = json.loads(resp.content)
            if content.get('errors'):
                print('账号异常:{}'.format(content))
                other_logger.error(' {} 账号异常,返回的内容为:{}'.format(account, content))
                status = content.get('errors')[0]['code']
                AccountInfoOper.set_account_status(account.account, status)
            else:
                print(account.account, '账号正常')
                AccountInfoOper.set_account_status(account.account, 1)
        except Exception as e:
            print(e)
            continue


# 一些辅助函数---将vps数据库中的账号放入mongodb
def push_spider_account_from_mysql_to_mongodb():
    import json
    all_account = AccountInfoOper.get_all_account(alive=3)
    for a in all_account:
        spider_account = {'account': a.account,
                          'alive': a.alive,
                          'create_time': str(a.create_time),
                          'email': a.email,
                          'email_password': a.email_password,
                          'password': a.password,
                          'proxies': a.proxies,
                          'site': a.site,
                          'task_number': a.task_number,
                          'token': json.loads(a.token) if a.token else None,
                          'update_time': str(a.update_time),
                          'phone_number': None,
                          }
        MongoSpiderAccountOper.insert_or_update_spider_account(spider_account)


# mongodb验证账户
def valid_one_account():
    import json
    from urllib.parse import quote
    from db.redis_db import get_redis_conn
    from logger.log import download_logger, other_logger, parser_logger, db_logger
    from crawlers.downloader import download_with_cookies

    spider_account = MongoSpiderAccountOper.get_spider_account({'account': 'kerortifs'})
    cookies, proxies, account = spider_account.get('token'), spider_account.get('proxies'), spider_account.get('account')

    search_keyword = '自由门'
    tweet_nums = 200
    page_count = 1

    first_url = 'https://twitter.com/i/api/2/search/adaptive.json?include_profile_interstitial_type=1&include_blocking=1&include_blocked_by=1&include_followed_by=1&include_want_retweets=1&include_mute_edge=1&include_can_dm=1&include_can_media_tag=1&skip_status=1&cards_platform=Web-12&include_cards=1&include_ext_alt_text=true&include_quote_count=true&include_reply_count=1&tweet_mode=extended&include_entities=true&include_user_entities=true&include_ext_media_color=true&include_ext_media_availability=true&send_error_codes=true&simple_quoted_tweet=true&q={}&tweet_search_mode=live&count={}&query_source=typed_query&pc=1&spelling_corrections=1&ext=mediaStats%2ChighlightedLabel'.format(
        quote(search_keyword), tweet_nums)

    url = first_url if page_count == 1 else 'next_url'
    print("抓取关键词 {} 第{}页推文信息（每页数据最多{}条推文）".format(search_keyword, page_count, tweet_nums))
    resp = download_with_cookies(url, cookies, proxies)
    if resp is None:

        download_logger.error('代理、链接或网络异常: {} ; 关键词 {} 第{}页信息抓取失败'.format(proxies, search_keyword, page_count))

    try:
        content = json.loads(resp.content)
        print(content)
        if content.get('errors'):
            other_logger.error(' {} 账号异常,返回的内容为:{}'.format(account, content))
            status = content.get('errors')[0]['code']
        else:
            print(account, '账号正常')
    except Exception as e:
        print(e)


# mongodb验证批量账户
def valid_many_account():
    from db.redis_db import SpiderAccountOper
    all_acount = MongoSpiderAccountOper.get_spider_accounts(alive=-2, task_number=2)
    print(len(all_acount))
    for index, spider_account in enumerate(all_acount):
        if not spider_account.get('token'):
            continue
        cookies, proxies, account = spider_account.get('token'), spider_account.get('proxies'), spider_account.get(
            'account')
        cookies = {cookie['name']: cookie['value'] for cookie in spider_account.get('token')} if spider_account.get('task_number') == 2 else spider_account.get('token')
        status = validate_cookies(cookies, proxies, account)
        # MongoSpiderAccountOper.update_spider_account_status(account, {'alive': status})
        time.sleep(random.randint(5, 10))
        if status == 1:
            print('{}. {} 正常'.format(index, account))
        else:
            print('{}. {} 异常，代码{}'.format(index, account, status))


# 更换代理
def update_proxies():
    from utils.proxy_utils import statistic_proxy_account_count
    all_acount = MongoSpiderAccountOper.query_datas_by_condition('account_info_for_spider', {'site': 'twitter', 'alive': 5, 'task_number': 2, 'token': None})
    for acc in all_acount:
        print(acc.get('account'))
        port = statistic_proxy_account_count()
        proxies = {"proxies": {"http": "http://10.0.12.1:{}".format(port), "https": "http://10.0.12.1:{}".format(port)},
                    "alive": 2}
        MongoSpiderAccountOper.update_spider_account_proxies(acc.get('account'), proxies)


# 将mongodb中的账号放入cookies池
def push_account_to_redis():
    accounts = MongoSpiderAccountOper.get_spider_accounts_ignore_task_number(alive=2)
    for account in accounts:
        # mongodb中规定task_number=2的账号为001长期账号培育的账号，3表示cert短期培育的账号--更新cookies后所有的账号cookies格式
        # 都为培育的格式，即为一个列表
        cookies = {cookie['name']: cookie['value'] for cookie in account.get('token')}
        account_info = {'proxies': account.get('proxies'),
                        'cookies': cookies,
                        }
        SpiderAccountOper.HSET(SPIDER_ACCOUNT_POOL, account.get('account'), json.dumps(account_info))
        MongoSpiderAccountOper.update_spider_account_status(account.get('account'), {'alive': 1})


if __name__ == '__main__':
    # 获取能用的账号
    # res = MongoSpiderAccountOper.query_datas_by_condition('account_info_for_spider', {'alive': 1})
    # print(len(res))
    # from db.redis_db import get_redis_conn
    # push_proxy_account_001(get_redis_conn(), 1)
    from db.redis_db import SpiderAccountOper, get_redis_conn
    # push_account_to_redis()
    push_proxy_account_001(get_redis_conn(), 1)
    # valid_many_account()
    #
    # valid_many_account()
    # update_proxies()
    # valid_many_account()
    # push_proxy_account(conn=conn, alive=1)
    # # 我的cookie
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
    # cookies = {cookie['name']: cookie['value'] for cookie in json.loads(cookies)}
    # print(cookies)
    # SpiderAccountOper.push_account_from_mongodb_to_redis(alive=1)
    # from exceptions.my_exceptions import NoSpiderAccountException
    # res = select_normal_account_from_redis()
    # if res is None:
    #     raise NoSpiderAccountException
    # SpiderAccountOper.push_account_from_mongodb_to_redis()
    # token = {"token": {"ct0": "c69bf4938bcfcbcc3fc03809fe918e0313dd63e4002f65a44fa68b7ffc512afb4d9f21fe1c08f5e6d497c4a4927438d028212821bd0c1b30b8e1c6fc216faeff483caec1ed69b943ba3be9c2bf94bffa", "auth_token": "028383b4d87ab3275e05af6619de4d8f8dadd725", "_twitter_sess": "BAh7CiIKZmxhc2hJQzonQWN0aW9uQ29udHJvbGxlcjo6Rmxhc2g6OkZsYXNo%250ASGFzaHsABjoKQHVzZWR7ADoPY3JlYXRlZF9hdGwrCA5YcOt6AToMY3NyZl9p%250AZCIlN2U4MDlmNzFjMmZmYmI0MmQwYjQyNjkxZTc2Zjk3OGM6B2lkIiU1ZDEy%250AZTY2MTM4YjRkZTdmOGUwYWMzNDFhZTRkMzc2NDoJdXNlcmkEA%252BalHA%253D%253D--7bdc3c276da77cc4f33918da5e43b6b22ddda32c", "ads_prefs": "\"HBESAAA=\"", "remember_checked_on": "1", "gt": "1420244481605996555", "_ga": "GA1.2.1548702113.1627447647", "dnt": "1", "kdt": "VSreWdzOCTsuVLpWCBoUoYNghIvWdl3Whr4lLEwB", "_gid": "GA1.2.1480356469.1627447647", "personalization_id": "\"v1_NiDgcuOjaZ2YVB2njmLlxQ==\"", "twid": "u%3D480634371", "guest_id": "v1%3A162744764284243684", "_sl": "1"},
    #          "proxies": {"http": "http://10.0.12.1:10803", "https": "http://10.0.12.1:10803"}}
    # MongoSpiderAccountOper.update_spider_account_status('RMezzogori', token)
