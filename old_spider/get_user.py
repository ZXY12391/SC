import csv
import json
import random
import sys
import time
from json import JSONDecodeError
from urllib.parse import quote
import jsonpath
import requests
import os
from requests import ReadTimeout
# from OpenSSL.SSL import SysCallError
from requests import ConnectTimeout
from urllib3.exceptions import NewConnectionError
from requests.exceptions import ProxyError, SSLError, ReadTimeout
from bloom_filter.bloom_filter import bloom_filter_user_post
from config.headers import headerA, based_header
from logger.log import download_logger
from confusion.confusion import get_confusion_url
from logger import other_logger, download_logger
from keyword_new_simple.cookies_list import dx
#######################################
# spider account error codes          #
#######################################
SPIDER_ERROR_CODES = [32, 326, 130, 200]
PROXY_ERROR_CODE = 4

def parser_user_tweet(content, screen_id,):
    res = 0
    tweets = jsonpath.jsonpath(content, "$..tweet_results.result")
    datas = []
    # print('开始解析推文...')
    # starttime = datetime.datetime.now()
    for tweet_info in tweets:
        try:
            # t1 = datetime.datetime.now()
            user = tweet_info['core']['user_results']['result']
            user_id = user['rest_id']
            tweet = tweet_info['legacy']
            tweet_url = 'https://twitter.com/' + tweet['user_id_str'] + '/status/' + tweet['id_str']

            if user_id != str(screen_id):
                continue
                # # 如果该推文的作者不是目标用户，且不是转发则说明该tweet为目标用户评论别人的那篇推文，则不爬取该推文
                # if tweet['user_id_str'] != str(screen_id) and not tweet.get('retweeted_status_id_str'):
                #     print(tweet_url)
                #     continue
                # mysql判断是否存在
                # if UserTweetOper.is_tweet_exited(tweet_url):
                #     continue

                # # mongodb判断是否存在
                # if MongoUserTweetOper.query_tweet_is_existed(tweet_url):
                #     continue
            # 布隆过滤器判断是否存在
            if  bloom_filter_user_post.is_exist(tweet_url):
                continue
            original_author = None
            tweet_type = '原创'
            if tweet.get('in_reply_to_user_id_str'):
                tweet_type = '回复'
                original_author='https://twitter.com/' + tweet['in_reply_to_screen_name']
            if tweet.get('retweeted') or tweet.get('retweeted_status_result'):
                tweet_type = '转发'
                original_author='https://twitter.com/' + tweet['entities']['user_mentions'][0]['screen_name']
                # 带评论转发---user_id_str也为目标用户的screen_id
            if tweet.get('is_quote_status') and tweet['user_id_str'] == str(screen_id):
                tweet_type = '引用'
                original_author=tweet['quoted_status_permalink']['expanded'].split('/status')[0]
            # 爬取视屏链接信息
            video_urls = []
            try:
                media = tweet.get('extended_entities').get('media')
                for video in media:
                    video_url_list = [v.get('url') for v in video.get('video_info').get('variants') if v.get('content_type') == 'video/mp4']
                    video_urls.append(video_url_list[-1])
            except Exception:
                pass

            # 作者信息
            user = user['legacy']

            # register_time = time.strptime(user['created_at'], "%a %b %d %H:%M:%S +0000 %Y")
            # register_time = time.strftime("%Y-%m-%d %H:%M:%S", register_time)

            try:
                tweet_data = {}
                tweet_data['tweet_url'] = tweet_url
                bloom_filter_user_post.save(tweet_url)
                tweet_data['author_url'] = 'https://twitter.com/' + user['screen_name']
                tweet_data['avatar_url'] = user['profile_image_url_https']
                tweet_data['avatar_binary'] = None
                tweet_data['topic'] = None
                tweet_data['author_name'] = user['name']
                tweet_data['content'] = tweet['full_text']
                tweet_data['tweet_type'] = tweet_type
                tweet_data['tweet_language'] = tweet['lang']
                tweet_data['comment_count'] = int(tweet['reply_count'])
                tweet_data['retweet_count'] = int(tweet['retweet_count'])
                tweet_data['quote_count'] = int(tweet['quote_count'])
                tweet_data['praise_count'] = int(tweet['favorite_count'])
                tweet_data['publish_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.strptime(tweet['created_at'], "%a %b %d %H:%M:%S +0000 %Y"))
                tweet_data['fetch_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                tweet_data['tweet_id'] = tweet['id_str']
                tweet_data['screen_id'] = user_id
                tweet_data['screen_name'] = user['screen_name']
                tweet_data['tourist_crawl_status'] = 0  # 这里修改为只要插入redis就默认该推文的tourist已爬取，因为后面更新该状态太慢,1表示其tourist信息已爬取
                tweet_data['video_urls'] = video_urls
                tweet_data['original_author']=original_author
                # 新增推文图片--mysql没有
                img_list = tweet.get('entities').get('media')
                if img_list:
                    tweet_data['tweet_img_url'] = [img.get('media_url_https') for img in img_list]
                else:
                    tweet_data['tweet_img_url'] = None
                tweet_data['tweet_img_binary'] = None  # 推文图片单独爬取

            except Exception:
                continue

            datas.append(tweet_data)
            print(len(datas))
            if tweet_data['original_author']:
                user_task = {
                    'user_url': tweet_data['original_author'],
                    'user_name': None,
                    'important_user': 0,
                    'add_time': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
                    'update_time': None,

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

            res += 1


        except Exception:
            pass


    #print(datas)

    # 将数据写入 CSV 文件

    # 指定文件夹路径
    folder_path = "/data1/zxy/old_spider/data"

    # 根据文件夹路径和文件名构建完整的文件路径
    csv_file_path = os.path.join(folder_path, f'{screen_id}_tweets.csv')

    # csv_file_path = f'{screen_id}_tweets.csv'
    #<_io.TextIOWrapper name='D:\\project\\old_spider\\keyword_new_simple\\1558460786120351744_tweets.csv' mode='a' encoding='utf-8'>
    with open(csv_file_path, 'a', newline='', encoding='utf-8') as csvfile:
        print("CSV 文件路径：", csv_file_path)
        fieldnames = ['tweet_url', 'author_url', 'avatar_url', 'avatar_binary', 'topic', 'author_name', 'content',
                      'tweet_type', 'tweet_language', 'comment_count', 'retweet_count', 'quote_count',
                      'praise_count',
                      'publish_time', 'fetch_time', 'tweet_id', 'screen_id', 'screen_name', 'tourist_crawl_status',
                      'video_urls', 'original_author', 'tweet_img_url', 'tweet_img_binary']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        # 如果文件为空，写入表头
        if csvfile.tell() == 0:
            writer.writeheader()

        # 写入数据
        for tweet_data in datas:
            writer.writerow(tweet_data)
        print(f"写入表中{len(datas)}条数据")


    print(' {} 推文信息爬取 {} 条成功'.format(screen_id, res), file=sys.stderr)
    return res

def download_with_cookies(url, cookies, proxies, confusion=True):
    # 混淆（目标混淆，行为混淆）
    # if confusion:
    #     confusion_type = random.choice(['target', 'behavior'])
    #     crawl_confusion(url, confusion_type, cookies, proxies)
   #  headers_option={
   #  'accept':'*/*',
   #  'access-control-request-headers': 'authorization,x-csrf-token,x-twitter-active-user,x-twitter-auth-type,x-twitter-client-language,x-twitter-polling',
   # 'access-control-reques-method':'GET',
   #  }
    headers = {'x-csrf-token': cookies['ct0']}
    headers = {**based_header, **headers}
    # print(proxies['http'])
    try:
        resp = requests.get(url, proxies=proxies, headers=headers, cookies=cookies, timeout=10)
        return resp
    except ReadTimeout as e:
        download_logger.error('代理{}, 超时{}'.format(proxies, e), file=sys.stderr)
    except (NewConnectionError, ConnectionError, TimeoutError, ConnectTimeout) as e:
        download_logger.error('代理{}, 异常{}'.format(proxies, e), file=sys.stderr)
    except ProxyError as e:
        download_logger.error('代理{}, 代理出错{}'.format(proxies, e), file=sys.stderr)
    except SSLError as e:
        download_logger.error('代理{}, ssl链接{}'.format(proxies, e), file=sys.stderr)
    except Exception as e:
        download_logger.error('代理{}, 异常{}'.format(proxies, e), file=sys.stderr)
    finally:
        time.sleep(+random.randint(10, 15))
    return None

def user_tweet_process(screen_id,cookies,proxies):
    """
    爬取用户推文信息
    :param task:
    :return:
    """

    cursor = ''
    page_count = 1
    tweet_number = 1000  # 浏览器链接参数为20，但这里经测试可写大于20，一页获取的推文数量更多，这里写200则一页返回200条推文，这里也可以写成2000条，大概返回1600多条数据
    # first_url = 'https://twitter.com/i/api/2/timeline/profile/{}.json?include_profile_interstitial_type=1&include_blocking=1&include_blocked_by=1&include_followed_by=1&include_want_retweets=1&include_mute_edge=1&include_can_dm=1&include_can_media_tag=1&skip_status=1&cards_platform=Web-12&include_cards=1&include_ext_alt_text=true&include_quote_count=true&include_reply_count=1&tweet_mode=extended&include_entities=true&include_user_entities=true&include_ext_media_color=true&include_ext_media_availability=true&send_error_codes=true&simple_quoted_tweet=true&include_tweet_replies=true&count={}&userId={}&ext=mediaStats%2ChighlightedLabel'.format(screen_id, tweet_number, screen_id)
    first_url = 'https://twitter.com/i/api/graphql/CwLU7qTfeu0doqhSr6tW4A/UserTweetsAndReplies?variables=%7B%22userId%22%3A%22{}%22%2C%22count%22%3A{}%2C%22includePromotedContent%22%3Atrue%2C%22withCommunity%22%3Atrue%2C%22withSuperFollowsUserFields%22%3Atrue%2C%22withBirdwatchPivots%22%3Afalse%2C%22withDownvotePerspective%22%3Afalse%2C%22withReactionsMetadata%22%3Afalse%2C%22withReactionsPerspective%22%3Afalse%2C%22withSuperFollowsTweetFields%22%3Atrue%2C%22withVoice%22%3Atrue%2C%22withV2Timeline%22%3Afalse%2C%22__fs_interactive_text%22%3Afalse%2C%22__fs_dont_mention_me_view_api_enabled%22%3Afalse%7D'.format(screen_id, tweet_number)
    # 只爬5页
    print(first_url)
    while page_count < 100:
        if page_count%30==0:
            time.sleep(300)
        # next_url = 'https://twitter.com/i/api/2/timeline/profile/{}.json?include_profile_interstitial_type=1&include_blocking=1&include_blocked_by=1&include_followed_by=1&include_want_retweets=1&include_mute_edge=1&include_can_dm=1&include_can_media_tag=1&skip_status=1&cards_platform=Web-12&include_cards=1&include_ext_alt_text=true&include_quote_count=true&include_reply_count=1&tweet_mode=extended&include_entities=true&include_user_entities=true&include_ext_media_color=true&include_ext_media_availability=true&send_error_codes=true&simple_quoted_tweet=true&include_tweet_replies=true&count={}&cursor={}&userId={}&ext=mediaStats%2ChighlightedLabel'.format(screen_id, tweet_number, quote(cursor), screen_id)
        next_url = 'https://twitter.com/i/api/graphql/CwLU7qTfeu0doqhSr6tW4A/UserTweetsAndReplies?variables=%7B%22userId%22%3A%22{}%22%2C%22count%22%3A{}%2C%22cursor%22%3A%22{}%22%2C%22includePromotedContent%22%3Atrue%2C%22withCommunity%22%3Atrue%2C%22withSuperFollowsUserFields%22%3Atrue%2C%22withBirdwatchPivots%22%3Afalse%2C%22withDownvotePerspective%22%3Afalse%2C%22withReactionsMetadata%22%3Afalse%2C%22withReactionsPerspective%22%3Afalse%2C%22withSuperFollowsTweetFields%22%3Atrue%2C%22withVoice%22%3Atrue%2C%22withV2Timeline%22%3Afalse%2C%22__fs_interactive_text%22%3Afalse%2C%22__fs_dont_mention_me_view_api_enabled%22%3Afalse%7D'.format(screen_id, tweet_number, quote(cursor))
        url = first_url if page_count == 1 else next_url
        print("抓取 {} tweet第{}页推文信息（每页数据最多{}条推文）".format(screen_id, page_count, tweet_number))
        # import datetime
        # starttime = datetime.datetime.now()
        resp = download_with_cookies(url, cookies, proxies)
        print('ccc',resp.content)
        #
        # endtime = datetime.datetime.now()
        # print('resquest消耗时间：{}'.format((endtime - starttime).seconds))
        if resp is None:
            download_logger.error('user {} 第{}页推文信息抓取失败 '.format(screen_id, page_count), file=sys.stderr)
            return 0
        try:
            content = json.loads(resp.content)
            user_status_code = content.get("errors")[0].get('code') if content.get("errors") else 1
            if user_status_code == 22:
                other_logger.error('{}开启tweets隐私保护,第{}页推文信息抓取失败'.format(screen_id, page_count), file=sys.stderr)
                # SpiderAccountOper.free_using_account(account)
                return 1
            # 状态码为这些，表明爬虫账号出了问题，需要将该账号放入错误队列，并更新mongodb数据库中的alive字段
            if user_status_code in SPIDER_ERROR_CODES:
                # SpiderAccountOper.push_banned_account_to_error_queue(account)
                # MongoSpiderAccountOper.update_spider_account_status(account, {'alive': user_status_code})
                return 0
            entries = jsonpath.jsonpath(content, '$..entries[*]')
            # len(content) == 2表示无推文了，这两个元素为两个cursor，最后一个cursor用于翻页
            # 若继续下翻得到的cursor永远都一样了（目标用户有置顶推文时，当推文翻完后也为2）
            if len(entries) == 2:
                # SpiderAccountOper.free_using_account(account)
                print('user {} 所有推文爬取完毕'.format(screen_id))
                return 1
            cursor = entries[-1]['content']['value']
            res = parser_user_tweet(content,  screen_id)
            page_count += 1

            if res <= 1:
                print('当前页面未爬取推文仅{}条推文，故停止爬取下一页'.format(res), file=sys.stderr)
                # SpiderAccountOper.free_using_account(account)
                return 1
        except JSONDecodeError as e:
            other_logger.error('异常{},user {} 第{}页推文信息抓取失败,返回的不是json,返回的信息为：{}'.format(e, screen_id, page_count, resp.content))
            # SpiderAccountOper.free_using_account(account)
            return 1
        except Exception as e:
            other_logger.error('异常：{},user {} 第{}页推文信息抓取失败,目标账号可能出现异常,返回的信息为：{}'.format(e, screen_id, page_count, resp.content))
            # SpiderAccountOper.free_using_account(account)
            return 1
    # SpiderAccountOper.free_using_account(account)
def get_cookie_dict(x):
    print(x)
    xx = x.split(';')
    c_d = {}
    for xxx in xx:
        z = xxx.strip() #默认删除空格,可以在括号里设置
        # print(z)
        key = z.split('=')
        c_d[key[0]] = key[1]#划分属性值
    return c_d
# 定义一个函数来从文件中读取数字 id
def get_ids_from_file(file_path):
    ids = []
    with open(file_path, "r") as file:
        for line in file:
            # 使用 split(":") 分割每行，取得数字 id 部分，并去除可能存在的空格
            parts = line.strip().split(":")
            if len(parts) == 2:  # 确保每行都有正确的格式，包含用户名和数字 id
                id_str = parts[1].strip()
                if id_str.isdigit():  # 确保数字 id 是整数形式
                    ids.append(id_str)
    return ids


if __name__=='__main__':
    # account={
    #     'proxy': {'http': 'http://10.0.12.1:10843', 'https': 'http://10.0.12.1:10843'},
    #     'cookie': 'guest_id_marketing=v1%3A171204999143273772; guest_id_ads=v1%3A171204999143273772; guest_id=v1%3A171204999143273772; gt=1775092437792289058; _ga=GA1.2.53619784.1712049999; _gid=GA1.2.354382775.1712049999; _twitter_sess=BAh7CSIKZmxhc2hJQzonQWN0aW9uQ29udHJvbGxlcjo6Rmxhc2g6OkZsYXNo%250ASGFzaHsABjoKQHVzZWR7ADoPY3JlYXRlZF9hdGwrCEXdIZ6OAToMY3NyZl9p%250AZCIlMGIyMmE4YTMwNjJlNjBmODUzNGJiZmY2YmI5ZmVhODY6B2lkIiVkNzE5%250AMTNjYzhlNTdlNTA3MjljYzgxOTI1ZmQ0MWE4Mg%253D%253D--be0b44a7b048191d8dadfc16fb3c13ac7a83cc25; kdt=gszO8O9Iacu3sFnXwSuus40D8bye9bL4BGqugBZU; auth_token=c8f4b7b98d031063d401a6ad78da6019f00d6834; ct0=7fbc6d7dae89cfadcefedfe20422327f2b2a8b00857bad64b74bf2143e0ca5dc7d2d75a3609a3a4028840f90142204807ba617977a2ef15475acbcaaca89cbbaefcf431387658548a1148ac5af4b4b24; att=1-nrlB7I89EyiWPyv7V7NI6VuYFuzmtRw3CoD2eCDr; lang=id; twid=u%3D1526154558; personalization_id="v1_r2jOyx/deN/kkbZrUZnN/w=="'
    # }
    d2=random.choice(dx)
    print(d2)
    cookie=get_cookie_dict(d2['cookie'])
    proxy = d2['proxy']
    # cookie=get_cookie_dict(account['cookie'])
    # proxy=account['proxy']
    # 调用函数并获取数字 id 列表
    users = get_ids_from_file("user_id.txt")
    #users=['1558460786120351744']#,'401950182','1246206298544173058','1301867001338421249'['TaiwanGuard','roclove2020','FU86570497','FelixCh29980680']
    for user in users:
        print(user)
        user_tweet_process(user,cookie,proxy)

