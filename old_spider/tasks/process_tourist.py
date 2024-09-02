import json
import jsonpath
import random
import time
from json import JSONDecodeError
from urllib.parse import quote

from db.redis_db import get_redis_conn
from db.mongo_db import MongoSpiderAccountOper,MongoUserTweetOper
from utils.account_utils import select_normal_account
from logger.log import download_logger, other_logger
from crawlers.downloader import download_with_cookies
from parsers.parser_tourist_data import (parser_praise_or_retweet, parser_comment_reply, )
from db.redis_db import SpiderAccountOper
from exceptions.my_exceptions import NoSpiderAccountException
from config.settings import SPIDER_ERROR_CODES, PROXY_ERROR_CODE


conn = get_redis_conn()


def praise_retweet_comment_process(tweet, cookies=None, proxies=None, account=None):
    """
    爬取点赞者、转发者、评论者、回复者
    :param tweet_list:
    :param cookies:
    :param proxies:
    :return:
    """
    if not cookies:
        # # todo 选择正常的账号和代理
        # cookies, proxies, account = select_normal_account(conn)
        # if cookies is None:
        #     return 0
        # cookies = json.loads(cookies)
        spider_account = SpiderAccountOper.select_normal_account_from_redis()
        if spider_account is None:
            raise NoSpiderAccountException
        cookies, proxies, account = spider_account.cookies, spider_account.proxies, spider_account.account_name

    user_status_code = 1
    if tweet.get('praise_count') != 0:
        user_status_code = praise_retweet_process(tweet.get('author_url'), tweet.get('tweet_url'), 'praise', cookies, proxies)
    if tweet.get('retweet_count') != 0:
        user_status_code = praise_retweet_process(tweet.get('author_url'), tweet.get('tweet_url'), 'retweet', cookies, proxies)
    if tweet.get('comment_count') != 0:
        user_status_code = comment_process(tweet.get('author_url'), tweet.get('tweet_url'), tweet.get('screen_id'), cookies, proxies)
    if user_status_code in SPIDER_ERROR_CODES + [PROXY_ERROR_CODE]:
        SpiderAccountOper.push_banned_account_to_error_queue(account)
        MongoSpiderAccountOper.update_spider_account_status(account, {'alive': user_status_code})
        return 0
    # 爬取完后，将该账号从正在使用队里中删除
    SpiderAccountOper.free_using_account(account)
    return 1


def praise_retweet_process(personal_url, tweet_url, crawl_type, cookies, proxies):
    """
    点赞者和转发者（只能爬取一页）---只能爬取42个，参数是80但只返回了42个
    :param personal_url:
    :param tweet_url:
    :param crawl_type:
    :param cookies:
    :param proxies:
    :return:
    """
    nums = 80
    tweetId = tweet_url.split('/')[-1]
    likes_url = 'https://twitter.com/i/api/graphql/iFQiYNBAd4lHpwNAIZ5z-A/Favoriters?variables=%7B%22tweetId%22%3A%22{}%22%2C%22count%22%3A{}%2C%22withHighlightedLabel%22%3Afalse%2C%22withTweetQuoteCount%22%3Afalse%2C%22includePromotedContent%22%3Atrue%2C%22withTweetResult%22%3Afalse%2C%22withUserResults%22%3Afalse%2C%22withNonLegacyCard%22%3Atrue%7D'.format(tweetId, nums)
    # likes_url = 'https://twitter.com/i/api/graphql/VswKXfDN56opjszkm8fvMQ/Favoriters?variables=%7B%22tweetId%22%3A%22{}%22%2C%22count%22%3A{}%2C%22withHighlightedLabel%22%3Afalse%2C%22withTweetQuoteCount%22%3Afalse%2C%22includePromotedContent%22%3Atrue%2C%22withTweetResult%22%3Atrue%2C%22withReactions%22%3Afalse%2C%22withSuperFollowsTweetFields%22%3Afalse%2C%22withSuperFollowsUserFields%22%3Afalse%2C%22withUserResults%22%3Afalse%2C%22withBirdwatchPivots%22%3Afalse%7D'.format(tweetId, nums)
    retweets_url = 'https://twitter.com/i/api/graphql/pBu3jQwyMNVMEF9DNN9prQ/Retweeters?variables=%7B%22tweetId%22%3A%22{}%22%2C%22count%22%3A{}%2C%22withHighlightedLabel%22%3Afalse%2C%22withTweetQuoteCount%22%3Afalse%2C%22includePromotedContent%22%3Atrue%2C%22withTweetResult%22%3Afalse%2C%22withUserResults%22%3Afalse%2C%22withNonLegacyCard%22%3Atrue%7D'.format(tweetId, nums)
    url = likes_url if crawl_type == 'praise' else retweets_url
    print("抓取tweet {} 的 {} 信息".format(tweet_url, crawl_type))
    resp = download_with_cookies(url, cookies, proxies)
    # 这里返回的resp如果没有获取到推文信息，而是得到返回的错误信息，转为bool型后为false
    if resp is None:
        download_logger.error('tweet {} 的 {} 信息抓取失败'.format(tweet_url, crawl_type))
        return PROXY_ERROR_CODE
    try:
        content = json.loads(resp.content)

        user_status_code = content.get("errors")[0].get('code') if content.get("errors") else 1
        # 状态码为这些，表明爬虫账号出了问题，需要将该账号放入错误队列，并更新mongodb数据库中的alive字段
        if user_status_code in SPIDER_ERROR_CODES:
            return user_status_code

        parser_praise_or_retweet(personal_url, tweet_url, crawl_type, content, proxies)
    except JSONDecodeError as e:
        other_logger.error('异常{},可能返回的不是json数据, 返回的信息为：{}'.format(e, resp.content))
        return 1
    except Exception as e:
        other_logger.error('异常{},账号可能出现异常, 返回的信息为：{}'.format(e, resp.content))
        return 1


def comment_process(personal_url, tweet_url, screen_id, cookies, proxies):
    """评论（最多可爬取23页）
    当前评论信息并没有全部爬取下来，所有评论总数与该链接最后爬取的总数肯定对不上，因为对于评论的对象不是目标用户的情况以及其它的并没有爬取
    当前评论信息值爬取了对目标用户的评论以及一级页面的回复信息
    :param personal_url:
    :param tweet_url:
    :param screen_id:
    :param cookies:
    :param proxies:
    :return:
    """
    cursor = True
    page_count = 1
    nums = 20  # 浏览器参数为20，这里写20或大于20，最后返回的结果都只有12条
    tweetId = tweet_url.split('/')[-1]
    first_url = 'https://twitter.com/i/api/2/timeline/conversation/{}.json?include_profile_interstitial_type=1&include_blocking=1&include_blocked_by=1&include_followed_by=1&include_want_retweets=1&include_mute_edge=1&include_can_dm=1&include_can_media_tag=1&skip_status=1&cards_platform=Web-12&include_cards=1&include_ext_alt_text=true&include_quote_count=true&include_reply_count=1&tweet_mode=extended&include_entities=true&include_user_entities=true&include_ext_media_color=true&include_ext_media_availability=true&send_error_codes=true&simple_quoted_tweet=true&count={}&include_ext_has_birdwatch_notes=false&ext=mediaStats%2ChighlightedLabel'.format(tweetId, nums)
    while cursor:
        next_url = 'https://twitter.com/i/api/2/timeline/conversation/{}.json?include_profile_interstitial_type=1&include_blocking=1&include_blocked_by=1&include_followed_by=1&include_want_retweets=1&include_mute_edge=1&include_can_dm=1&include_can_media_tag=1&skip_status=1&cards_platform=Web-12&include_cards=1&include_ext_alt_text=true&include_quote_count=true&include_reply_count=1&tweet_mode=extended&include_entities=true&include_user_entities=true&include_ext_media_color=true&include_ext_media_availability=true&send_error_codes=true&simple_quoted_tweet=true&referrer=tweet&count={}&cursor={}&include_ext_has_birdwatch_notes=false&ext=mediaStats%2ChighlightedLabel'.format(tweetId, nums, quote(str(cursor)))
        url = first_url if page_count == 1 else next_url
        print("抓取tweet {} 第{}页评论信息（每页数据最多{}条评论）".format(tweet_url, page_count, nums))
        resp = download_with_cookies(url, cookies, proxies)
        if resp is None:
            download_logger.error('tweet {} 第{}页评论信息抓取失败 '.format(tweet_url, page_count))
            return PROXY_ERROR_CODE
        try:
            content = json.loads(resp.content)

            user_status_code = content.get("errors")[0].get('code') if content.get("errors") else 1
            # 状态码为这些，表明爬虫账号出了问题，需要将该账号放入错误队列，并更新mongodb数据库中的alive字段
            if user_status_code in SPIDER_ERROR_CODES:
                return user_status_code

            # 这里cursor返回false表示没有下一页评论了
            cursor = jsonpath.jsonpath(content, '$..cursor[value]')
            cursor = cursor[0] if cursor else False
            show_replies_cursor = jsonpath.jsonpath(content, '$..timelineCursor[value]')
            # print('show_replies_cursor', show_replies_cursor)
            # print('cursor', cursor)

            # 返回爬取到的新数据条数
            res = parser_comment_reply(personal_url, tweet_url, screen_id, content, 'comment', proxies)

            # 爬取当前页面的回复信息
            if show_replies_cursor:
                for replies_cursor in show_replies_cursor:
                    user_status_code = reply_process(personal_url, tweet_url, replies_cursor, screen_id, cookies, proxies)
                    if user_status_code in SPIDER_ERROR_CODES:
                        return user_status_code
            # 只爬取3页
            if page_count > 5:
                print('已爬取5页评论数据，故停止爬取下一页.')
                return 1
            page_count += 1
            if res <= 10:
                print('当前页面仅有{}评论未被爬取，故停止爬取下一页'.format(res))
                return 1

        except JSONDecodeError as e:
            other_logger.error('异常{},可能返回的不是json数据,返回的信息为：{}'.format(e, resp.content))
            return 1
        except Exception as e:
            other_logger.error('异常{},账号可能出现异常,返回的信息为：{}'.format(e, resp.content))
            return 1
    print('tweet {} 评论信息抓取完毕'.format(tweet_url))


def reply_process(personal_url, tweet_url, cursor, screen_id, cookies, proxies):
    """
    回复----目标用户对其它用户
    :param personal_url:
    :param tweet_url:
    :param cursor:
    :param screen_id:
    :param cookies:
    :param proxies:
    :return:
    """
    tweetId = tweet_url.split('/')[-1]
    page_count = 1
    while cursor:
        print("抓取tweet {} 第{}页回复信息".format(tweet_url, page_count))
        url = 'https://twitter.com/i/api/2/timeline/conversation/{}.json?include_profile_interstitial_type=1&include_blocking=1&include_blocked_by=1&include_followed_by=1&include_want_retweets=1&include_mute_edge=1&include_can_dm=1&include_can_media_tag=1&skip_status=1&cards_platform=Web-12&include_cards=1&include_ext_alt_text=true&include_quote_count=true&include_reply_count=1&tweet_mode=extended&include_entities=true&include_user_entities=true&include_ext_media_color=true&include_ext_media_availability=true&send_error_codes=true&simple_quoted_tweet=true&referrer=tweet&cursor={}&include_ext_has_birdwatch_notes=false&ext=mediaStats%2ChighlightedLabel'.format(tweetId, quote(cursor))
        resp = download_with_cookies(url, cookies, proxies)
        if resp is None:
            other_logger.error("tweet {} 第{}页回复信息失败".format(tweet_url, page_count))
            return PROXY_ERROR_CODE
        try:
            content = json.loads(resp.content)

            user_status_code = content.get("errors")[0].get('code') if content.get("errors") else 1
            # 状态码为这些，表明爬虫账号出了问题，需要将该账号放入错误队列，并更新mongodb数据库中的alive字段
            if user_status_code in SPIDER_ERROR_CODES:
                return user_status_code

            # 这里cursor返回false表示没有下一页评论了
            cursor = jsonpath.jsonpath(content, '$..timelineCursor[value]')
            cursor = cursor[0] if cursor else False
            # print('cursor', cursor)
            res = parser_comment_reply(personal_url, tweet_url, screen_id, content, 'reply', proxies)
            page_count += 1
            if page_count > 3:
                print('已爬取3页回复数据，故停止爬取下一页.')
                return 1
            if res <= 10:
                print('当前页面仅有{}回复未被爬取，故停止爬取下一页'.format(res))
                return 1
        except JSONDecodeError as e:
            other_logger.error('异常{},可能返回的不是json数据,返回的信息为：{}'.format(e, resp.content))
            return 0
        except Exception as e:
            other_logger.error('异常{},账号可能出现异常,返回的信息为：{}'.format(e, resp.content))
            return 0
    print('{} 回复信息爬取完毕'.format(tweet_url))

if __name__=='__main__':
    tweets = MongoUserTweetOper.get_tweet_tourist_tasks(nums=1)
    task = {'post_praise_task': 1, 'post_repost_task': 1, 'post_comment_task': 1}
    praise_retweet_comment_process(tweets[0])



    pass