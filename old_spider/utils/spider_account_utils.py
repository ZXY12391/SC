import json
import time
from db.dao import TwitterCookiesOper, TwitterBotInfoOper
from db.mongo_db import MongoSpiderAccountOper, MongoCommonOper

"""
将001账号长期培育的账号和cert短期培育的账号放入cert Mongodb数据库
"""

# {'104.168.202.26': 10825, '104.168.198.218': 10826, '142.11.236.182': 10827, '142.11.237.70': 10828, '142.11.237.149': 10829, '104.168.142.71': 10833, '104.168.142.227': 10834, '104.168.147.39': 10835, '104.168.148.243': 10836, '104.168.151.138': 10837, '104.168.176.192': 10838, '104.168.213.51': 10839, '104.168.213.55': 10840, '23.254.211.121': 10846, '23.254.215.206': 10847, '23.254.217.205': 10848, '23.254.244.147': 10849, '23.254.253.75': 10850, '108.174.198.140': 10851, '142.11.199.245': 10852, '198.23.229.135': 10858, '108.174.60.20': 10860, '108.174.60.22': 10861, '198.23.229.138': 10862, '107.172.25.137': 10863, '198.23.229.139': 10864, '108.174.60.24': 10865, '107.172.25.138': 10866, '108.174.60.26': 10867, '108.174.60.27': 10868, '198.23.229.140': 10869, '10.0.12.1': 10903, '107.172.86.118': 10966, '23.95.96.4': 10968, '23.94.104.37': 10969, '23.95.215.105': 10971, '107.175.31.157': 10973, '198.46.233.34': 10974, '192.3.73.172': 10975}
ip_infos = [
    {
        "ip": "104.168.202.26",
        "source": "twitter",
        "port": 10825
    },
    {
        "ip": "104.168.198.218",
        "source": "twitter",
        "port": 10826
    },
    {
        "ip": "142.11.236.182",
        "source": "twitter",
        "port": 10827
    },
    {
        "ip": "142.11.237.70",
        "source": "twitter",
        "port": 10828
    },
    {
        "ip": "142.11.237.149",
        "source": "twitter",
        "port": 10829
    },
    {
        "ip": "104.168.142.71",
        "source": "twitter",
        "port": 10833
    },
    {
        "ip": "104.168.142.227",
        "source": "twitter",
        "port": 10834
    },
    {
        "ip": "104.168.147.39",
        "source": "twitter",
        "port": 10835
    },
    {
        "ip": "104.168.148.243",
        "source": "twitter",
        "port": 10836
    },
    {
        "ip": "104.168.151.138",
        "source": "twitter",
        "port": 10837
    },
    {
        "ip": "104.168.176.192",
        "source": "twitter",
        "port": 10838
    },
    {
        "ip": "104.168.213.51",
        "source": "twitter",
        "port": 10839
    },
    {
        "ip": "104.168.213.55",
        "source": "twitter",
        "port": 10840
    },
    {
        "ip": "23.254.211.121",
        "source": "twitter",
        "port": 10846
    },
    {
        "ip": "23.254.215.206",
        "source": "twitter",
        "port": 10847
    },
    {
        "ip": "23.254.217.205",
        "source": "twitter",
        "port": 10848
    },
    {
        "ip": "23.254.244.147",
        "source": "twitter",
        "port": 10849
    },
    {
        "ip": "23.254.253.75",
        "source": "twitter",
        "port": 10850
    },
    {
        "ip": "108.174.198.140",
        "source": "twitter",
        "port": 10851
    },
    {
        "ip": "142.11.199.245",
        "source": "twitter",
        "port": 10852
    },
    {
        "ip": "198.23.229.135",
        "source": "twitter",
        "port": 10858
    },
    {
        "ip": "108.174.60.20",
        "source": "twitter",
        "port": 10860
    },
    {
        "ip": "108.174.60.22",
        "source": "twitter",
        "port": 10861
    },
    {
        "ip": "198.23.229.138",
        "source": "twitter",
        "port": 10862
    },
    {
        "ip": "107.172.25.137",
        "source": "twitter",
        "port": 10863
    },
    {
        "ip": "198.23.229.139",
        "source": "twitter",
        "port": 10864
    },
    {
        "ip": "108.174.60.24",
        "source": "twitter",
        "port": 10865
    },
    {
        "ip": "107.172.25.138",
        "source": "twitter",
        "port": 10866
    },
    {
        "ip": "108.174.60.26",
        "source": "twitter",
        "port": 10867
    },
    {
        "ip": "108.174.60.27",
        "source": "twitter",
        "port": 10868
    },
    {
        "ip": "198.23.229.140",
        "source": "twitter",
        "port": 10869
    },
    {
        "ip": "10.0.12.1",
        "source": "twitter",
        "port": 10903
    },
    {
        "ip": "107.172.86.118",
        "source": "twitter",
        "port": 10966
    },
    {
        "ip": "23.95.96.4",
        "source": "twitter",
        "port": 10968
    },
    {
        "ip": "23.94.104.37",
        "source": "twitter",
        "port": 10969
    },
    {
        "ip": "23.95.215.105",
        "source": "twitter",
        "port": 10971
    },
    {
        "ip": "107.175.31.157",
        "source": "twitter",
        "port": 10973
    },
    {
        "ip": "198.46.233.34",
        "source": "twitter",
        "port": 10974
    },
    {
        "ip": "192.3.73.172",
        "source": "twitter",
        "port": 10975
    }
]


def get_spider_account_from_twitter_cookies_table():
    ip_map_port = {p['ip']: p['port'] for p in ip_infos}
    print(ip_map_port)
    rs = TwitterCookiesOper.get_all_cookies()

    for r in rs:
        data = {}
        info = TwitterBotInfoOper.get_bot_info(r.account)
        data['phone_number'] = info.phone
        data['account'] = info.uid
        data['password'] = info.password
        data['token'] = json.loads(r.cookies)
        data['proxies'] = {"http": "http://10.0.12.1:{}".format(ip_map_port.get(info.ip)),
                           "https": "http://10.0.12.1:{}".format(ip_map_port.get(info.ip))}
        data['site'] = 'twitter'
        data['alive'] = 2  # 将刚放入的账号alive设为2
        data['task_number'] = 2
        data['user_agent'] = info.mobile_ua
        data['create_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        data['update_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        MongoSpiderAccountOper.insert_or_update_spider_account(data)


def get_spider_account_from_twitter_bot_table():
    ip_map_port = {p['ip']: p['port'] for p in ip_infos}

    rs = TwitterBotInfoOper.get_all_bot_info()
    for info in rs:
        if MongoSpiderAccountOper.spider_account_existed(info.uid):
            continue
        data = {}
        data['phone_number'] = info.phone
        data['account'] = info.uid
        data['password'] = info.password
        data['token'] = None
        data['proxies'] = {"http": "http://10.0.12.1:{}".format(ip_map_port.get(info.ip)),
                           "https": "http://10.0.12.1:{}".format(ip_map_port.get(info.ip))}
        data['site'] = 'twitter'
        data['alive'] = 2  # 将刚放入的账号alive设为2
        data['task_number'] = 2
        data['user_agent'] = info.mobile_ua
        data['create_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        data['update_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        MongoSpiderAccountOper.insert_or_update_spider_account(data)


# 将账号培育表T_bot_info中短期培育的账号放入爬虫账号表account_info_for_spider
def get_spider_account_from_bot_info():
    """将账号培育表T_bot_info中培育的账号放入爬虫账号表account_info_for_spider"""
    spider_accounts = MongoCommonOper.query_datas_by_condition('T_bot_info', {'enable': 1, 'cultivation_type': 0})
    cnt = 0
    for account in spider_accounts:
        if not account.get('cookies'):
            continue
        if MongoSpiderAccountOper.spider_account_existed(account.get('uid')):
            continue
        data = {}
        data['phone_number'] = account.get('phone')
        data['account'] = account.get('uid')
        data['password'] = account.get('password')
        data['token'] = account.get('cookies')
        data['proxies'] = {"http": "http://10.0.12.1:{}".format(int(account.get('port'))),
                           "https": "http://10.0.12.1:{}".format(int(account.get('port')))}
        data['site'] = 'twitter'
        data['alive'] = 1  # 将刚放入的账号alive设为1
        data['task_number'] = 3
        data['user_agent'] = account.get('pc_ua')
        data['create_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        data['update_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        MongoSpiderAccountOper.insert_or_update_spider_account(data)
        cnt += 1
    print('共加入新账号：{}个'.format(cnt))


def get_bot_account_from_account_info_for_spider():
    """将account_info_for_spider表中的爬虫账号放入T_bot_info表"""
    spider_accounts = MongoCommonOper.query_datas_by_condition('account_info_for_spider', {'alive': 1, 'task_number': 2, 'site': 'twitter'})
    cnt = 200
    for account in spider_accounts:
        if not account.get('token'):
            continue
        if MongoCommonOper.query_data_by_condition('T_bot_info', {'account': account.get('account')}):
            continue
        data = {
            'account': account.get('account'),
            'avatar_binary': None,
            'avatar_bs64encode': None,
            'avatar_url': None,
            'birth_day': '',
            'birth_month': '',
            'birth_year': '',
            'cookies': account.get('token'),
            'create_time': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
            'cultivation_type': 0,
            'description': '',
            'email': '',
            'enable': 1,
            'first_login': 0,
            'inited': 0,
            'ip': '10.0.12.1',
            'name': '',
            'password': account.get('password'),
            'pc_ua': account.get('user_agent'),
            'person_url': '',
            'phone': account.get('phone_number'),
            'place': '',
            'port': int(account.get('proxies').get('http').split(':')[-1]),
            'target_user_url': None,
            'uid': account.get('account'),
            'update_time': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),

        }
        MongoCommonOper.insert_one_data('T_bot_info', data)
        cnt += 1
        if cnt >= 220:
            break
    print('共加入新账号：{}个'.format(cnt))


# 由于001中的cookies失效，将cert中的cookies更新到001数据库
def update_cookies_from_cert_to_001():
    from db.models import TwitterCookies

    ip_map_port = {p['ip']: p['port'] for p in ip_infos}
    bots_001 = TwitterBotInfoOper.get_bots_by_condition("enable=1")
    print(len(bots_001))
    for index, bot in enumerate(bots_001):
        res = MongoSpiderAccountOper.get_spider_account({"account": bot.uid})
        if not res:
            continue
        data = TwitterCookies()
        data.account = bot.account
        data.cookies = json.dumps(res.get("token"))
        data.update_time = res.get('update_time')
        TwitterCookiesOper.insert_one_entity(data)
        print(index, bot.uid, bot.account)


if __name__ == '__main__':
    # get_spider_account_from_twitter_bot_table()
    get_bot_account_from_account_info_for_spider()
    # update_cookies_from_cert_to_001()
