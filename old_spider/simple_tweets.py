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
from config.settings import SPIDER_ERROR_CODES

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

def user_tweet_process(task,proxies=None,cookies=None):
    """
    爬取用户推文信息
    :param task:
    :return:
    """
    person_url, user_tag = task.get('twitter_url'), task.get('user_tag')
    # 判断是否需要爬取用户信息
    task_profile = MongoUserInfoOper.get_user_info_data(person_url)
    if not task_profile:
        print('no profile',person_url)
        #cbd['RZ_multi_user_tasks'].update_one({'twitter_url': person_url},{'$set':{'profile':0}})
        return 0

    screen_id, username, image_url = task_profile.get('screen_id'), task_profile.get('user_name'), task_profile.get('img_url'),
    # 抓取推文信息
    cursor = ''
    page_count = 1
    tweet_number = 200  # 浏览器链接参数为20，但这里经测试可写大于20，一页获取的推文数量更多，这里写200则一页返回200条推文，这里也可以写成2000条，大概返回1600多条数据
    # first_url = 'https://twitter.com/i/api/2/timeline/profile/{}.json?include_profile_interstitial_type=1&include_blocking=1&include_blocked_by=1&include_followed_by=1&include_want_retweets=1&include_mute_edge=1&include_can_dm=1&include_can_media_tag=1&skip_status=1&cards_platform=Web-12&include_cards=1&include_ext_alt_text=true&include_quote_count=true&include_reply_count=1&tweet_mode=extended&include_entities=true&include_user_entities=true&include_ext_media_color=true&include_ext_media_availability=true&send_error_codes=true&simple_quoted_tweet=true&include_tweet_replies=true&count={}&userId={}&ext=mediaStats%2ChighlightedLabel'.format(screen_id, tweet_number, screen_id)
    first_url = 'https://twitter.com/i/api/graphql/CwLU7qTfeu0doqhSr6tW4A/UserTweetsAndReplies?variables=%7B%22userId%22%3A%22{}%22%2C%22count%22%3A{}%2C%22includePromotedContent%22%3Atrue%2C%22withCommunity%22%3Atrue%2C%22withSuperFollowsUserFields%22%3Atrue%2C%22withBirdwatchPivots%22%3Afalse%2C%22withDownvotePerspective%22%3Afalse%2C%22withReactionsMetadata%22%3Afalse%2C%22withReactionsPerspective%22%3Afalse%2C%22withSuperFollowsTweetFields%22%3Atrue%2C%22withVoice%22%3Atrue%2C%22withV2Timeline%22%3Afalse%2C%22__fs_interactive_text%22%3Afalse%2C%22__fs_dont_mention_me_view_api_enabled%22%3Afalse%7D'.format(screen_id, tweet_number)
    # 只爬5页
    while page_count < 5:
        # next_url = 'https://twitter.com/i/api/2/timeline/profile/{}.json?include_profile_interstitial_type=1&include_blocking=1&include_blocked_by=1&include_followed_by=1&include_want_retweets=1&include_mute_edge=1&include_can_dm=1&include_can_media_tag=1&skip_status=1&cards_platform=Web-12&include_cards=1&include_ext_alt_text=true&include_quote_count=true&include_reply_count=1&tweet_mode=extended&include_entities=true&include_user_entities=true&include_ext_media_color=true&include_ext_media_availability=true&send_error_codes=true&simple_quoted_tweet=true&include_tweet_replies=true&count={}&cursor={}&userId={}&ext=mediaStats%2ChighlightedLabel'.format(screen_id, tweet_number, quote(cursor), screen_id)
        next_url = 'https://twitter.com/i/api/graphql/CwLU7qTfeu0doqhSr6tW4A/UserTweetsAndReplies?variables=%7B%22userId%22%3A%22{}%22%2C%22count%22%3A{}%2C%22cursor%22%3A%22{}%22%2C%22includePromotedContent%22%3Atrue%2C%22withCommunity%22%3Atrue%2C%22withSuperFollowsUserFields%22%3Atrue%2C%22withBirdwatchPivots%22%3Afalse%2C%22withDownvotePerspective%22%3Afalse%2C%22withReactionsMetadata%22%3Afalse%2C%22withReactionsPerspective%22%3Afalse%2C%22withSuperFollowsTweetFields%22%3Atrue%2C%22withVoice%22%3Atrue%2C%22withV2Timeline%22%3Afalse%2C%22__fs_interactive_text%22%3Afalse%2C%22__fs_dont_mention_me_view_api_enabled%22%3Afalse%7D'.format(screen_id, tweet_number, quote(cursor))
        url = first_url if page_count == 1 else next_url
        print("抓取 {} tweet第{}页推文信息（每页数据最多{}条推文）".format(person_url, page_count, tweet_number))
        # import datetime
        # starttime = datetime.datetime.now()

        resp = download_with_cookies(url, cookies, proxies)

        #
        # endtime = datetime.datetime.now()
        # print('resquest消耗时间：{}'.format((endtime - starttime).seconds))
        if resp is None:
            print('NONE RETURN')
            download_logger.error('user {} 第{}页推文信息抓取失败 '.format(person_url, page_count))
            # SpiderAccountOper.push_banned_account_to_error_queue(account)
            # MongoSpiderAccountOper.update_spider_account_status(account, {'alive': PROXY_ERROR_CODE})
            return 0
        try:
            content = json.loads(resp.content)
            # # 存储json文件
            # store_json(content, './filter_data/json_data/tweet_json.json')
            # print('存储成功')
            # time.sleep(10000)

            # # 对信息进行过滤---推文信息过滤后踩不到数据
            # t1 = time.time()
            # try:
            #     FilterSecurity.traverseJson(content)
            # except Exception:
            #     pass
            # t2 = time.time()
            # print('过滤用时：{}秒'.format(t2 - t1))

            user_status_code = content.get("errors")[0].get('code') if content.get("errors") else 1
            if user_status_code == 22:
                other_logger.error('{}开启tweets隐私保护,第{}页推文信息抓取失败'.format(person_url, page_count))
                # SpiderAccountOper.free_using_account(account)
                return 1
            # 状态码为这些，表明爬虫账号出了问题，需要将该账号放入错误队列，并更新mongodb数据库中的alive字段
            if user_status_code in SPIDER_ERROR_CODES:
                print('SPIDER_ERROR_CODES')
                # SpiderAccountOper.push_banned_account_to_error_queue(account)
                # MongoSpiderAccountOper.update_spider_account_status(account, {'alive': user_status_code})
                return 0
            #
            # tweet_cnts = content['globalObjects']['tweets']
            # # 当只返回一条推文信息时，表示没有数据了用来解决pin tweet推文问题;如果用户没有pin tweet则tweet_cnts为空
            # if not tweet_cnts or len(tweet_cnts) == 1:
            #     print('user {} 第{}页无推文信息'.format(person_url, page_count))
            #     SpiderAccountOper.free_using_account(account)
            #     return 1
            # # 页面中有两个cursor，用于翻页的为第二个cursor
            # cursor = jsonpath.jsonpath(content, '$..cursor[value]')
            # if not cursor:
            #     SpiderAccountOper.free_using_account(account)
            #     return 1
            # cursor = cursor[1]

            entries = jsonpath.jsonpath(content, '$..entries[*]')
            # len(content) == 2表示无推文了，这两个元素为两个cursor，最后一个cursor用于翻页
            # 若继续下翻得到的cursor永远都一样了（目标用户有置顶推文时，当推文翻完后也为2）
            if len(entries) == 2:
                # SpiderAccountOper.free_using_account(account)
                print('user {} 所有推文爬取完毕'.format(person_url))
                return 1
            cursor = entries[-1]['content']['value']

            res = parser_user_tweet(content, person_url, screen_id, username, user_tag, image_url, task)
            # for tweet in tweet_list:
            #     if tweet.get('_id'):
            #         tweet.pop('_id')
            #     # tourist_worker.delay(task, tweet)
            #     tourist_worker.apply_async(args=[task, tweet])  # 修改为放入队列立即执行
            #     # print('{} 放入tourist任务队列'.format(tweet.get('tweet_url')))

            page_count += 1
            if res <= 1:
                print('当前页面未爬取推文仅{}条推文，故停止爬取下一页'.format(res))
                # SpiderAccountOper.free_using_account(account)
                return 1
        except JSONDecodeError as e:
            print(e,resp.content)
            if page_count==1:
                return 0
            other_logger.error('异常{},user {} 第{}页推文信息抓取失败,返回的不是json,返回的信息为：{}'.format(e, person_url, page_count, resp.content))
            # SpiderAccountOper.free_using_account(account)
            return 1
        except Exception as e:
            print(resp.content)
            print(e)
            if page_count == 1:
                return 0
            other_logger.error('异常：{},user {} 第{}页推文信息抓取失败,目标账号可能出现异常,返回的信息为：{}'.format(e, person_url, page_count, resp.content))
            # SpiderAccountOper.free_using_account(account)
            return 1
    return 1
    # SpiderAccountOper.free_using_account(account)


def task_one_new(task):
    d2 = random.choice(d)
    print(d2)
    cookies = get_cookie_dict(d2['cookie'])
    proxies = d2['proxy']
    flag = user_tweet_process(task,proxies,cookies)
    # 返回1表示不需要重新爬取（即认为爬取成功），返回0则爬取失败需要重新爬取
    if flag:
        update_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        #cbd['RZ_multi_user_tasks'].update_one({'twitter_url': task['twitter_url']}, {'$set':{'tweets': 1}})
        #cbd['RZ_profile_abstract'].update_one({'user_url': task['twitter_url']}, {'$set': {'is_tweets': 1}})
        print('爬取 {} 言论信息成功，更新该任务状态成功!'.format(task.get('twitter_url')))
    else:
        # user_post_worker.delay(task)
        print('爬取 {} 言论信息失败，将该任务重新放回celery任务队列!'.format(task.get('twitter_url')))

def get_tasks(task):
    # tasks = cbd['RZ_multi_user_tasks'].find_one({'twitter_url': task})
    # if not tasks:
    #     tasks=cbd['RZ_multi_public_tasks'].find_one({'twitter_url': task})
    #     if not tasks:
    #         return None
    return {'twitter_url':task}

if __name__=='__main__':
    # p = r'E:\RZ项目\RZ\测试数据.xlsx'
    # import pandas as pd
    # data = pd.read_excel(io=p)
    # data = data.loc[:, ["end node"]]
    # data = data.to_dict(orient='records')
    # data = [{'twitter_url': _['end node']} for _ in data]
    # from config.settings import mongo_uri, mongo_name, PROXY_ERROR_CODE
    # from logger.log import db_logger
    # import pymongo
    # client = pymongo.MongoClient(mongo_uri)
    # cbd=client['Fqdl']
    # user_list=list(cbd['RZ_profle_stage1_predict_test'].find())
    # urls=[_['user_url'] for _ in user_list]
    urls=['https://twitter.com/chingtelai']#'https://twitter.com/hsuchiaohsin',
    from multiprocessing.dummy import Pool
    pool = Pool(50)
    res = pool.map(get_tasks,urls)
    print(len(res))
    res=[_ for _ in res if _]
    # task_one_new(res[0])
    from multiprocessing.dummy import Pool
    pool = Pool(1)
    res = pool.map(task_one_new, res)
    # user_tweet_tasks =list(cbd['RZ_multi_user_tasks'].find({'tweets':0}))
    # user_tweet_tasks += list(cbd['RZ_multi_user_tasks'].find({'profile': 2, 'tweets': 0}))
    # user_tweet_tasks += list(cbd['RZ_multi_user_tasks'].find({'profile': 3, 'tweets': 0}))
    # p = r'E:\RZ项目\川大\川大\123.xlsx'
    # import pandas as pd
    # data = pd.read_excel(io=p)
    # data = data.loc[:, ["new_url"]]
    # data = data.to_dict(orient='records')
    # data = [_['new_url'] for _ in data]
    # user_profile_tasks_ = []
    # for u in user_tweet_tasks:
    #     if u['facebook_url'] in data:
    #         user_profile_tasks_.append(u)
    # print(len(user_profile_tasks_))
    # random.shuffle(user_profile_tasks_)
    # task_one_new(user_tweet_tasks[0])
    # cbd['RZ_multi_user_tasks'].update_one({'twitter_url': user_task['twitter_url']}, {'$set': {'profile': 1}})
    # p = './label.csv'
    # import pandas as pd
    # data = pd.read_csv(p,encoding="gbk")
    # data = data.loc[:, ["url"]]
    # data = data.to_dict(orient='records')
    # urls=[_['url'] for _ in data]
    # from multiprocessing.dummy import Pool
    # pool = Pool(50)
    # res = pool.map(get_tasks,urls)
    # t=[]
    # for x in res:
    #     if x:
    #         t.append(x)
    # print(len(t))
    # random.shuffle(user_tweet_tasks)
    # from multiprocessing.dummy import Pool
    # pool = Pool(3)
    # res = pool.map(task_one_new, data)





