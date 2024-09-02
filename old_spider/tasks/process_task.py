import json
import jsonpath
import random
import time
from json import JSONDecodeError
from urllib.parse import quote
from db.dao import AccountInfoOper
from db.mongo_db import MongoKeywordTaskOper, MongoUserInfoOper, MongoUserTaskOper, MongoUserTweetOper, MongoSpiderAccountOper

from db.redis_db import get_redis_conn, SpiderAccountOper
from utils.account_utils import select_normal_account
from utils.proxy_utils import get_one_proxy
from logger.log import download_logger, other_logger, parser_logger, spider_logger
from crawlers.downloader import download_with_cookies, download_without_cookies,download_with_cookies_keyword
from parsers.parser_data import (parser_keyword_tweet, parser_user_info, parser_relationship, parser_user_tweet,
                                 parser_user_topic, parser_keyword_user, parser_praise_or_retweet, parser_comment_reply)
from exceptions.my_exceptions import NoSpiderAccountException
from config.settings import SPIDER_ERROR_CODES
from celery_app.tourist_worker import tourist_worker
from filter_data.json_filter import FilterSecurity
from config.settings import PROXY_ERROR_CODE


conn = get_redis_conn()


def store_json(data_dict, file_path):
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(json.dumps(data_dict))


def keyword_tweet_process(search_keyword, crawl_type):
    """
    爬取关键词相关推文及其作者（Twitter搜索页面中的Top和Latest）
    :param search_keyword:
    :param crawl_type:
    :return:
    """
    print('爬取关键词 {} {}下的相关推文&用户信息...'.format(search_keyword, crawl_type))
    spider_account = SpiderAccountOper.select_normal_account_from_redis()
    if spider_account is None:
        raise NoSpiderAccountException
    cookies, proxies, account = spider_account.cookies, spider_account.proxies, spider_account.account_name
    cursor = ''
    page_count = 1
    tweet_nums = 200
    # 热门 top下的推文链接
    first_url = 'https://twitter.com/i/api/2/search/adaptive.json?include_profile_interstitial_type=1&include_blocking=1&include_blocked_by=1&include_followed_by=1&include_want_retweets=1&include_mute_edge=1&include_can_dm=1&include_can_media_tag=1&skip_status=1&cards_platform=Web-12&include_cards=1&include_ext_alt_text=true&include_quote_count=true&include_reply_count=1&tweet_mode=extended&include_entities=true&include_user_entities=true&include_ext_media_color=true&include_ext_media_availability=true&send_error_codes=true&simple_quoted_tweet=true&q={}&count={}&query_source=typed_query&pc=0&spelling_corrections=1&ext=mediaStats%2ChighlightedLabel'.format(quote(search_keyword), tweet_nums)
    # 最近 latest 推文链接
    if crawl_type == 'latest':
        first_url = 'https://twitter.com/i/api/2/search/adaptive.json?include_profile_interstitial_type=1&include_blocking=1&include_blocked_by=1&include_followed_by=1&include_want_retweets=1&include_mute_edge=1&include_can_dm=1&include_can_media_tag=1&skip_status=1&cards_platform=Web-12&include_cards=1&include_ext_alt_text=true&include_quote_count=true&include_reply_count=1&tweet_mode=extended&include_entities=true&include_user_entities=true&include_ext_media_color=true&include_ext_media_availability=true&send_error_codes=true&simple_quoted_tweet=true&q={}&tweet_search_mode=live&count={}&query_source=typed_query&pc=1&spelling_corrections=1&ext=mediaStats%2ChighlightedLabel'.format(quote(search_keyword), tweet_nums)
    while page_count < 100:
        # 热门 top下的推文链接
        next_url = 'https://twitter.com/i/api/2/search/adaptive.json?include_profile_interstitial_type=1&include_blocking=1&include_blocked_by=1&include_followed_by=1&include_want_retweets=1&include_mute_edge=1&include_can_dm=1&include_can_media_tag=1&skip_status=1&cards_platform=Web-12&include_cards=1&include_ext_alt_text=true&include_quote_count=true&include_reply_count=1&tweet_mode=extended&include_entities=true&include_user_entities=true&include_ext_media_color=true&include_ext_media_availability=true&send_error_codes=true&simple_quoted_tweet=true&q={}&count={}&query_source=typed_query&cursor={}&pc=0&spelling_corrections=1&ext=mediaStats%2ChighlightedLabel'.format(quote(search_keyword), tweet_nums, quote(cursor))
        # 最近 latest 推文链接
        if crawl_type == 'latest':
            next_url = 'https://twitter.com/i/api/2/search/adaptive.json?include_profile_interstitial_type=1&include_blocking=1&include_blocked_by=1&include_followed_by=1&include_want_retweets=1&include_mute_edge=1&include_can_dm=1&include_can_media_tag=1&skip_status=1&cards_platform=Web-12&include_cards=1&include_ext_alt_text=true&include_quote_count=true&include_reply_count=1&tweet_mode=extended&include_entities=true&include_user_entities=true&include_ext_media_color=true&include_ext_media_availability=true&send_error_codes=true&simple_quoted_tweet=true&q={}&tweet_search_mode=live&count={}&query_source=typed_query&cursor={}&pc=1&spelling_corrections=1&ext=mediaStats%2ChighlightedLabel'.format(quote(search_keyword), tweet_nums, quote(cursor))
        url = first_url if page_count == 1 else next_url
        print("抓取关键词 {} 第{}页推文信息（每页数据最多{}条推文）".format(search_keyword, page_count, tweet_nums))
        resp = download_with_cookies_keyword(url, cookies, proxies)
        if resp is None:
            download_logger.error('代理、链接或网络异常: {} ; 关键词 {} 第{}页信息抓取失败'.format(proxies, search_keyword, page_count))
            SpiderAccountOper.push_banned_account_to_error_queue(account)
            MongoSpiderAccountOper.update_spider_account_status(account, {'alive': PROXY_ERROR_CODE})
            return 0
        try:
            content = json.loads(resp.content)
            user_status_code = content.get("errors")[0].get('code') if content.get("errors") else 1
            # 状态码为这些，表明爬虫账号出了问题，需要将该账号放入错误队列，并更新mongodb数据库中的alive字段
            if user_status_code in SPIDER_ERROR_CODES:
                spider_logger.error(' {} 账号异常,返回的内容为:{}'.format(account, content))
                SpiderAccountOper.push_banned_account_to_error_queue(account)
                MongoSpiderAccountOper.update_spider_account_status(account, {'alive': user_status_code})
                return 0

            if not content['globalObjects']['tweets']:
                print('关键词：' + search_keyword + '搜索完成')
                update_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                MongoKeywordTaskOper.set_keyword_task_update_time(search_keyword, update_time)
                SpiderAccountOper.free_using_account(account)
                return 1
            cursor = jsonpath.jsonpath(content, '$..cursor[value]')[1]
            res = parser_keyword_tweet(content, account, proxies, search_keyword)
            page_count += 1
            if res <= 5:
                print('当前页面未爬取推文仅{}条推文，故停止爬取下一页'.format(res))
                update_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                MongoKeywordTaskOper.set_keyword_task_update_time(search_keyword, update_time)
                SpiderAccountOper.free_using_account(account)
                return 1
        except JSONDecodeError as e:
            download_logger.error('JSON异常，可能返回的不是json数据，返回的数据为:{}'.format(resp.content))
            SpiderAccountOper.free_using_account(account)
            return 0
    SpiderAccountOper.free_using_account(account)


def keyword_user_process(search_keyword):
    """
    爬取关键词相关用户（Twitter搜索页面中的People）
    :param search_keyword:
    :return:
    """
    print('爬取关键词 {} 相关用户...'.format(search_keyword))
    spider_account = SpiderAccountOper.select_normal_account_from_redis()
    if spider_account is None:
        raise NoSpiderAccountException
    cookies, proxies, account = spider_account.cookies, spider_account.proxies, spider_account.account_name
    # print(account)
    # print(proxies)
    # 这里crawl_nums写多少就会返回多少用户
    cursor = ''
    crawl_nums = 200
    page_count = 1
    #'https://api.twitter.com/2/search/adaptive.json?include_profile_interstitial_type=1&include_blocking=1&include_blocked_by=1&include_followed_by=1&include_want_retweets=1&include_mute_edge=1&include_can_dm=1&include_can_media_tag=1&include_ext_has_nft_avatar=1&include_ext_is_blue_verified=1&include_ext_verified_type=1&skip_status=1&cards_platform=Web-12&include_cards=1&include_ext_alt_text=true&include_ext_limited_action_results=false&include_quote_count=true&include_reply_count=1&tweet_mode=extended&include_ext_views=true&include_entities=true&include_user_entities=true&include_ext_media_color=true&include_ext_media_availability=true&include_ext_sensitive_media_warning=true&include_ext_trusted_friends_metadata=true&send_error_codes=true&simple_quoted_tweet=true&q=%E5%8F%B0%E6%B9%BE&result_filter=user&query_source=typed_query&count=20&requestContext=launch&pc=1&spelling_corrections=1&include_ext_edit_control=true&ext=mediaStats%2ChighlightedLabel%2ChasNftAvatar%2CvoiceInfo%2CbirdwatchPivot%2Cenrichments%2CsuperFollowMetadata%2CunmentionInfo%2CeditControl%2Cvibe'
    #url = 'https://api.twitter.com/2/search/adaptive.json?include_profile_interstitial_type=1&include_blocking=1&include_blocked_by=1&include_followed_by=1&include_want_retweets=1&include_mute_edge=1&include_can_dm=1&include_can_media_tag=1&include_ext_has_nft_avatar=1&include_ext_is_blue_verified=1&include_ext_verified_type=1&skip_status=1&cards_platform=Web-12&include_cards=1&include_ext_alt_text=true&include_ext_limited_action_results=false&include_quote_count=true&include_reply_count=1&tweet_mode=extended&include_ext_views=true&include_entities=true&include_user_entities=true&include_ext_media_color=true&include_ext_media_availability=true&include_ext_sensitive_media_warning=true&include_ext_trusted_friends_metadata=true&send_error_codes=true&simple_quoted_tweet=true&q=%E5%8F%B0%E6%B9%BE&result_filter=user&query_source=typed_query&count=20&requestContext=launch&pc=1&spelling_corrections=1&include_ext_edit_control=true&ext=mediaStats%2ChighlightedLabel%2ChasNftAvatar%2CvoiceInfo%2CbirdwatchPivot%2Cenrichments%2CsuperFollowMetadata%2CunmentionInfo%2CeditControl%2Cvibe'
    #'https://api.twitter.com/2/search/adaptive.json?include_profile_interstitial_type=1&include_blocking=1&include_blocked_by=1&include_followed_by=1&include_want_retweets=1&include_mute_edge=1&include_can_dm=1&include_can_media_tag=1&include_ext_has_nft_avatar=1&include_ext_is_blue_verified=1&include_ext_verified_type=1&skip_status=1&cards_platform=Web-12&include_cards=1&include_ext_alt_text=true&include_ext_limited_action_results=false&include_quote_count=true&include_reply_count=1&tweet_mode=extended&include_ext_views=true&include_entities=true&include_user_entities=true&include_ext_media_color=true&include_ext_media_availability=true&include_ext_sensitive_media_warning=true&include_ext_trusted_friends_metadata=true&send_error_codes=true&simple_quoted_tweet=true&q=台湾&result_filter=user&query_source=typed_query&count=20&requestContext=launch&pc=1&spelling_corrections=1&include_ext_edit_control=true&ext=mediaStats,highlightedLabel,hasNftAvatar,voiceInfo,birdwatchPivot,enrichments,superFollowMetadata,unmentionInfo,editControl,vibe'
    #first_url= 'https://api.twitter.com/2/search/adaptive.json?include_profile_interstitial_type=1&include_blocking=1&include_blocked_by=1&include_followed_by=1&include_want_retweets=1&include_mute_edge=1&include_can_dm=1&include_can_media_tag=1&include_ext_has_nft_avatar=1&include_ext_is_blue_verified=1&include_ext_verified_type=1&skip_status=1&cards_platform=Web-12&include_cards=1&include_ext_alt_text=true&include_ext_limited_action_results=false&include_quote_count=true&include_reply_count=1&tweet_mode=extended&include_ext_views=true&include_entities=true&include_user_entities=true&include_ext_media_color=true&include_ext_media_availability=true&include_ext_sensitive_media_warning=true&include_ext_trusted_friends_metadata=true&send_error_codes=true&simple_quoted_tweet=true&q=%E5%8F%B0%E6%B9%BE&result_filter=user&query_source=typed_query&count=20&requestContext=launch&pc=1&spelling_corrections=1&include_ext_edit_control=true&ext=mediaStats%2ChighlightedLabel%2ChasNftAvatar%2CvoiceInfo%2CbirdwatchPivot%2Cenrichments%2CsuperFollowMetadata%2CunmentionInfo%2CeditControl%2Cvibe'
    #'https://api.twitter.com/2/search/adaptive.json?include_profile_interstitial_type=1&include_blocking=1&include_blocked_by=1&include_followed_by=1&include_want_retweets=1&include_mute_edge=1&include_can_dm=1&include_can_media_tag=1&include_ext_has_nft_avatar=1&include_ext_is_blue_verified=1&include_ext_verified_type=1&skip_status=1&cards_platform=Web-12&include_cards=1&include_ext_alt_text=true&include_ext_limited_action_results=false&include_quote_count=true&include_reply_count=1&tweet_mode=extended&include_ext_views=true&include_entities=true&include_user_entities=true&include_ext_media_color=true&include_ext_media_availability=true&include_ext_sensitive_media_warning=true&include_ext_trusted_friends_metadata=true&send_error_codes=true&simple_quoted_tweet=true&q=台湾&result_filter=user&query_source=recent_search_click&count=20&requestContext=launch&pc=1&spelling_corrections=1&include_ext_edit_control=true&ext=mediaStats,highlightedLabel,hasNftAvatar,voiceInfo,birdwatchPivot,enrichments,superFollowMetadata,unmentionInfo,editControl,vibe'
    #first_url= 'https://api.twitter.com/2/search/adaptive.json?include_profile_interstitial_type=1&include_blocking=1&include_blocked_by=1&include_followed_by=1&include_want_retweets=1&include_mute_edge=1&include_can_dm=1&include_can_media_tag=1&include_ext_has_nft_avatar=1&include_ext_is_blue_verified=1&include_ext_verified_type=1&skip_status=1&cards_platform=Web-12&include_cards=1&include_ext_alt_text=true&include_ext_limited_action_results=false&include_quote_count=true&include_reply_count=1&tweet_mode=extended&include_ext_views=true&include_entities=true&include_user_entities=true&include_ext_media_color=true&include_ext_media_availability=true&include_ext_sensitive_media_warning=true&include_ext_trusted_friends_metadata=true&send_error_codes=true&simple_quoted_tweet=true&q={}&result_filter=user&query_source=recent_search_click&count=20&requestContext=launch&pc=1&spelling_corrections=1&include_ext_edit_control=true&ext=mediaStats'.format(quote(search_keyword))
    first_url = 'https://twitter.com/i/api/2/search/adaptive.json?include_profile_interstitial_type=1&include_blocking=1&include_blocked_by=1&include_followed_by=1&include_want_retweets=1&include_mute_edge=1&include_can_dm=1&include_can_media_tag=1&skip_status=1&cards_platform=Web-12&include_cards=1&include_ext_alt_text=true&include_quote_count=true&include_reply_count=1&tweet_mode=extended&include_entities=true&include_user_entities=true&include_ext_media_color=true&include_ext_media_availability=true&send_error_codes=true&simple_quoted_tweet=true&q={}&result_filter=user&count={}&query_source=typed_query&pc=1&spelling_corrections=1&ext=mediaStats%2ChighlightedLabel'.format(quote(search_keyword), crawl_nums)
    while True:
        #test
        #test
        #next_url=  'https://api.twitter.com/2/search/adaptive.json?include_profile_interstitial_type=1&include_blocking=1&include_blocked_by=1&include_followed_by=1&include_want_retweets=1&include_mute_edge=1&include_can_dm=1&include_can_media_tag=1&include_ext_has_nft_avatar=1&include_ext_is_blue_verified=1&include_ext_verified_type=1&skip_status=1&cards_platform=Web-12&include_cards=1&include_ext_alt_text=true&include_ext_limited_action_results=false&include_quote_count=true&include_reply_count=1&tweet_mode=extended&include_ext_views=true&include_entities=true&include_user_entities=true&include_ext_media_color=true&include_ext_media_availability=true&include_ext_sensitive_media_warning=true&include_ext_trusted_friends_metadata=true&send_error_codes=true&simple_quoted_tweet=true&q=台湾&result_filter=user&count=20&query_source=typed_query&cursor=DAAFCgABFqrlRMS_-TcLAAIAAAO8QUFBQUFDNk5zd2dBQUFDMEFBQUFBQUFBRlh3QUFBQUhBQUFBVmlXTW9BY0F5SUFVQ2hFWWxvZ29rQ2lKbUVnQUlPQUNBWUF3QkdEWndBVUNMUVVBQURJb0ZBUUF4a0FSaFFnQUF4SWdnQmhRQXpnalpUQmdnRVFrUUFKWUFKQUFxQVJBQUE0QUFNQUNnZ0FCQVVaSktBVVFoZ0FBQWdBd2dEUkVBRUVBZ0FPSWdDQklpWUFqQm9DSWdBZ1FBSUVKV0lJQkFCc0FvSUlSTkl4QVZRZ0VBQURnQUFBRkZnRlFCT0twQmdJZ0tRQWpCQUFnQUNLcUVJVlloRVNvU2dJUVlLV0FFZ3dEd0FEREVrRmdFbEFvQVVCUUFCQUlCQUFDQllTZ1FBTWtMWVFqa0RJQUdBUlFGQ0JDQ0FBUVRRQ1VCb1JRQUFoQUJFaUFBQUNBd2t2QUNRQ2dBbVJtZ0VCZ0NRRWdZbGdBZ0RCRUJBQ0Mwd0NCRUFBZ0Z3QWdBVGdNSThFcUlBQW9DUGNTRFFBeURBQUNBQlJBaFVrQmdhQUFFSUJnRUFCUWdrVW1SQm9BQUlKa0FJUVFCbEFZU0NrZ2t0SUNBQ2lBQ0VnQUFBQ0lESXdhTUlBREFBSVZoQ0FFQUFTMGNBRkFBSkVCNGFBSUN3SFE0QW9BQUVRQ3BFa01rb0dBQ29jQm1Rd05JRkpnTUdBWUVBQWdFdVFBQ2dORUJnQ2hJQUloSThBZ0pDcUdnQlNBaEFQZ0FFQUFoQllRQUN5QVFJVXdCSkVBRVV3VkNVZ0VJT0FHQUFJSUFBUUlBa0lCa0NBRU1HQUNDVFNBTVFnY09RRUFBQUFBQ1FaQ2dJWUFRVkFBSUNTQVJBQ0FnVU1RQW9hQUtCcUFRS0FLaUtnQUFtVU54S2dJQVJnQUVpQVFFSUFBVUJzRUFpQWdBSUF2QkkwQ0FBQkFFQW9CZ0FJNGhJQ0FFd0JCSUJrUUNHQUVDQUNFUkNrSUVBSWRaSjNnQVFBVUJCSWdnQVFZUktBRUFCRWNFRUJVaEFZUkFCRUlFRkJZQUFvSUVZQUFnQUFBQUFBS0JHb0p3b0FRTVNJRWdCS0NnaUJBZ0FvS0NRQWxBRUlVQVNDRUFnTlFBZ1FDS2dBQkFxQUNJUVJJR0JTUUcwb0lBQUNBSUFBQUNNQkFHQmdRWXBCQUVBc0FDeEpqZ0VnS2dFSVFDV2dHb2hBQkVCaEJLSUVJUUFCaElBZ29JSWhDSG5nSllnRVFDQUFZMEtxQUpDQT0AAA&pc=1&spelling_corrections=1&include_ext_edit_control=true&ext=mediaStats,highlightedLabel,hasNftAvatar,voiceInfo,birdwatchPivot,enrichments,superFollowMetadata,unmentionInfo,editControl,vibe'
        next_url = 'https://twitter.com/i/api/2/search/adaptive.json?include_profile_interstitial_type=1&include_blocking=1&include_blocked_by=1&include_followed_by=1&include_want_retweets=1&include_mute_edge=1&include_can_dm=1&include_can_media_tag=1&skip_status=1&cards_platform=Web-12&include_cards=1&include_ext_alt_text=true&include_quote_count=true&include_reply_count=1&tweet_mode=extended&include_entities=true&include_user_entities=true&include_ext_media_color=true&include_ext_media_availability=true&send_error_codes=true&simple_quoted_tweet=true&q={}&result_filter=user&count={}&query_source=typed_query&cursor={}&pc=0&spelling_corrections=1&ext=mediaStats%2ChighlightedLabel'.format(quote(search_keyword), crawl_nums, quote(cursor))
        url = first_url if page_count == 1 else next_url
        print("抓取关键词 {} 第{}页相关用户信息（每页数据最多{}个用户）".format(search_keyword, page_count, crawl_nums))
        resp = download_with_cookies_keyword(url, cookies, proxies)
        if resp is None:
            download_logger.error('代理、链接或网络异常: {} ; 关键词 {} 第{}页信息抓取失败'.format(proxies, search_keyword, page_count))
            SpiderAccountOper.push_banned_account_to_error_queue(account)
            MongoSpiderAccountOper.update_spider_account_status(account, {'alive': PROXY_ERROR_CODE})
            return 0
        try:
            content = json.loads(resp.content)
            print(content)
            user_status_code = content.get("errors")[0].get('code') if content.get("errors") else 1
            print(user_status_code)
            # 状态码为这些，表明爬虫账号出了问题，需要将该账号放入错误队列，并更新mongodb数据库中的alive字段
            if user_status_code in SPIDER_ERROR_CODES:
                spider_logger.error(' {} 账号异常,返回的内容为:{}'.format(account, content))
                SpiderAccountOper.push_banned_account_to_error_queue(account)
                MongoSpiderAccountOper.update_spider_account_status(account, {'alive': user_status_code})
                return 0

            if not content['globalObjects']['users']:
                print('关键词：' + search_keyword + '搜索完成')
                update_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                MongoKeywordTaskOper.set_keyword_task_update_time(search_keyword, update_time)
                SpiderAccountOper.free_using_account(account)
                return 1

            cursor = jsonpath.jsonpath(content, '$..cursor[value]')[1]
            res = parser_keyword_user(content, search_keyword)
            page_count += 1

            if res <= 10:
                print('当前页面未爬取相关用户仅{}条，故停止爬取下一页'.format(res))
                update_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                MongoKeywordTaskOper.set_keyword_task_update_time(search_keyword, update_time)
                SpiderAccountOper.free_using_account(account)
                return 1
        except JSONDecodeError as e:
            download_logger.error('JSON异常，可能返回的不是json数据，返回的数据为:{}'.format(resp.content))
            SpiderAccountOper.free_using_account(account)
            return 0


def user_info_process(task, proxies=None):
    """
    爬取用户档案信息
    :param task:
    :param cookies:
    :param proxies:
    :return:
    """
    user_url, user_tag, important_user = task.get('user_url'), task.get('user_tag'), task.get('important_user')
    proxies = get_one_proxy(conn) if proxies is None else proxies
    # print(proxies)
    # proxies = {"http": "http://10.0.12.1:10916", "https": "http://10.0.12.1:10916"}
    print('抓取用户个人主页URL：{} 的基本信息'.format(user_url))
    # print('使用Torsocks代理爬取数据...')
    screen_name = user_url[20:]  # 获取用户的screen_name
    # 该链接不需要登录就可爬取链接
    url = 'https://api.twitter.com/graphql/-xfUfZsnR_zqjFd-IfrN5A/UserByScreenName?variables=%7B%22screen_name%22%3A%22{}%22%2C%22withHighlightedLabel%22%3Atrue%7D'.format(quote(screen_name))
    # # 该链接需要登录才能爬取信息
    # url = 'https://twitter.com/i/api/graphql/Vf8si2dfZ1zmah8ePYPjDQ/UserByScreenNameWithoutResults?variables=%7B%22screen_name%22%3A%22{}%22%2C%22withHighlightedLabel%22%3Atrue%7D'.format(quote(screen_name))
    resp = download_without_cookies(url, proxies)
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
            MongoUserTaskOper.set_user_task_status(task.get('user_url'),
                                                   {'profile_crawl_status': 2, 'update_time': update_time})
            return 1, 'failed', 'failed', 'failed'  # 0表示需要重新爬取，1表示不需要(账号不存在)重新爬取

        if user_status_code == 63:
            parser_logger.error('url {} 异常（ User has been suspended）,采集基本信息失败'.format(user_url))
            # 更新用户档案信息爬取状态为2已完成
            update_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            MongoUserTaskOper.set_user_task_status(task.get('user_url'),
                                                   {'profile_crawl_status': 2, 'update_time': update_time})
            return 1, 'failed', 'failed', 'failed'
        # 解析页面
        screen_id, username, image_url = parser_user_info(user_url, user_tag, important_user, content, proxies)

        if screen_id == 0:
            parser_logger.error('url {} 异常,采集基本信息失败'.format(user_url))
            # 更新用户档案信息爬取状态为2已完成
            update_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            MongoUserTaskOper.set_user_task_status(task.get('user_url'),
                                                   {'profile_crawl_status': 2, 'update_time': update_time})
            return 1, 'failed', 'failed', 'failed'

        # 爬取话题
        flag = user_topic_process(screen_id, 'profile')
        if flag:
            pass
        else:
            other_logger.error('用户 {} topic信息需要重新抓取'.format(screen_id))

        print(' {} 基本信息抓取成功'.format(user_url))

        # 更新用户档案信息爬取状态为2已完成
        update_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        MongoUserTaskOper.set_user_task_status(task.get('user_url'),
                                               {'profile_crawl_status': 2, 'update_time': update_time})
        return 1, screen_id, username, image_url
    except JSONDecodeError as e:
        download_logger.error('JSON异常{}，可能返回的不是json数据，返回的数据为:{}'.format(e, resp.content))
        download_logger.error('url {} 基本信息抓取失败'.format(user_url))
    except Exception as e:
        download_logger.error('异常{},返回的数据为:{}'.format(e, resp.content))
        download_logger.error('url {} 基本信息抓取失败'.format(user_url))
    return 0, 'failed', 'failed', 'failed'


def user_topic_process(screen_id, crawl_type):
    """
    爬取用户话题信息
    :param screen_id:
    :param cookies:
    :param proxies:
    :return:
    """
    print('抓取用户 {} topic信息...'.format(screen_id))
    spider_account = SpiderAccountOper.select_normal_account_from_redis()
    if spider_account is None:
        raise NoSpiderAccountException
    cookies, proxies, account = spider_account.cookies, spider_account.proxies, spider_account.account_name

    # 该链接需要登录才能爬取信息-----测试完毕
    url = 'https://twitter.com/i/api/graphql/3AqdO1equ6AEPMEoA2v8Zw/FollowedTopics?variables=%7B%22userId%22%3A%22{}%22%2C%22count%22%3A20%2C%22withUserResults%22%3Afalse%2C%22withTweetResult%22%3Atrue%2C%22withBirdwatchPivots%22%3Afalse%2C%22withReactions%22%3Afalse%2C%22withSuperFollowsTweetFields%22%3Afalse%7D'.format(screen_id)
    resp = download_with_cookies(url, cookies, proxies)
    if resp is None:
        download_logger.error('代理或网络异常,用户 {} topic抓取失败'.format(screen_id))
        SpiderAccountOper.push_banned_account_to_error_queue(account)
        MongoSpiderAccountOper.update_spider_account_status(account, {'alive': PROXY_ERROR_CODE})
        return 0
    try:
        content = json.loads(resp.content)
        user_status_code = content.get("errors")[0].get('code') if content.get("errors") else 1
        # if user_status_code == 22:
        #     # 该情况在解析函数中写好了，这里就不写了
        #     other_logger.error('目标用户 {} 开启隐私保护,topic信息抓取失败'.format('screen_id'))
        #     return 1

        # 状态码为这些，表明爬虫账号出了问题，需要将该账号放入错误队列，并更新mongodb数据库中的alive字段
        if user_status_code in SPIDER_ERROR_CODES:
            SpiderAccountOper.push_banned_account_to_error_queue(account)
            MongoSpiderAccountOper.update_spider_account_status(account, {'alive': user_status_code})
            return 0
        # 解析话题数据
        flag = parser_user_topic(screen_id, content, crawl_type)

        SpiderAccountOper.free_using_account(account)
        return flag
    except JSONDecodeError as e:
        download_logger.error('JSON异常{}，可能返回的不是json数据，返回的数据为:{}'.format(e, resp.content))
        download_logger.error('url {} topic抓取失败'.format(screen_id))
    except Exception as e:
        download_logger.error('异常{},返回的数据为:{}'.format(e, resp.content))
        download_logger.error('用户 {} topic抓取失败'.format(screen_id))
    SpiderAccountOper.free_using_account(account)
    return 1


def user_relationship_process(task, crawl_type):
    """
    爬取用户关系信息（following、follower）
    :param task:
    :param crawl_type:
    :return:
    """
    person_url, user_tag = task.get('user_url'), task.get('user_tag')

    spider_account = SpiderAccountOper.select_normal_account_from_redis()
    if spider_account is None:
        raise NoSpiderAccountException
    cookies, proxies, account = spider_account.cookies, spider_account.proxies, spider_account.account_name

    # 判断是否需要爬取用户信息
    task_profile = MongoUserInfoOper.get_user_info_data(person_url)
    if task_profile:
        user_id, username, image_url = task_profile.get('screen_id'), task_profile.get('user_name'), task_profile.get('img_url'),
    else:
        # _flag表示该用户是否需要重新爬取---0表示需要重新爬取，1表示不需要(账号不存在)重新爬取
        _flag, user_id, username, image_url = user_info_process(task, proxies)
        if user_id == 'failed':
            SpiderAccountOper.free_using_account(account)
            return _flag

    cursor = ''
    page_count = 1
    nums = 200
    if crawl_type == 'follower':
        # first_url = 'https://api.twitter.com/graphql/mA9n2AcGj94QffGv3wXV1Q/Followers?variables=%7B%22userId%22%3A%22{}%22%2C%22count%22%3A{}%2C%22withHighlightedLabel%22%3Afalse%2C%22withTweetQuoteCount%22%3Afalse%2C%22includePromotedContent%22%3Afalse%2C%22withTweetResult%22%3Afalse%2C%22withUserResult%22%3Afalse%7D'.format(quote(user_id), nums)
        # 20220620更新
        #https://api.twitter.com/graphql/aw-ayt8IzqjZByg6HKr53w/Followers?variables={"userId":"1445375663993757700","count":20,"includePromotedContent":false,"withSuperFollowsUserFields":true,"withDownvotePerspective":false,"withReactionsMetadata":false,"withReactionsPerspective":false,"withSuperFollowsTweetFields":true}&features={"responsive_web_twitter_blue_verified_badge_is_enabled":true,"responsive_web_graphql_exclude_directive_enabled":false,"verified_phone_label_enabled":false,"responsive_web_graphql_timeline_navigation_enabled":true,"responsive_web_graphql_skip_user_profile_image_extensions_enabled":false,"tweetypie_unmention_optimization_enabled":true,"vibe_api_enabled":true,"responsive_web_edit_tweet_api_enabled":true,"graphql_is_translatable_rweb_tweet_is_translatable_enabled":true,"view_counts_everywhere_api_enabled":true,"longform_notetweets_consumption_enabled":true,"tweet_awards_web_tipping_enabled":false,"freedom_of_speech_not_reach_fetch_enabled":false,"standardized_nudges_misinfo":true,"tweet_with_visibility_results_prefer_gql_limited_actions_policy_enabled":false,"interactive_text_enabled":true,"responsive_web_text_conversations_enabled":false,"longform_notetweets_richtext_consumption_enabled":false,"responsive_web_enhance_cards_enabled":false}
        first_url = 'https://twitter.com/i/api/graphql/ysj_6Bszzl-X7e4bmvYpBA/Followers?variables=%7B%22userId%22%3A%22{}%22%2C%22count%22%3A{}%2C%22includePromotedContent%22%3Afalse%2C%22withSuperFollowsUserFields%22%3Atrue%2C%22withDownvotePerspective%22%3Afalse%2C%22withReactionsMetadata%22%3Afalse%2C%22withReactionsPerspective%22%3Afalse%2C%22withSuperFollowsTweetFields%22%3Atrue%7D&features=%7B%22dont_mention_me_view_api_enabled%22%3Atrue%2C%22interactive_text_enabled%22%3Atrue%2C%22responsive_web_uc_gql_enabled%22%3Afalse%2C%22vibe_tweet_context_enabled%22%3Afalse%2C%22responsive_web_edit_tweet_api_enabled%22%3Afalse%2C%22standardized_nudges_misinfo%22%3Afalse%2C%22responsive_web_enhance_cards_enabled%22%3Afalse%7D'.format(quote(user_id), nums)
    else:
        # first_url = 'https://api.twitter.com/graphql/oVUtpYtda5IGQ7sW8pE5VA/Following?variables=%7B%22userId%22%3A%22{}%22%2C%22count%22%3A{}%2C%22withHighlightedLabel%22%3Afalse%2C%22withTweetQuoteCount%22%3Afalse%2C%22includePromotedContent%22%3Afalse%2C%22withTweetResult%22%3Afalse%2C%22withUserResult%22%3Afalse%7D'.format(quote(user_id), nums)
        # 20220620更新
        first_url = 'https://twitter.com/i/api/graphql/ih3I-XV0ogyWjqsHqFQ9eA/Following?variables=%7B%22userId%22%3A%22{}%22%2C%22count%22%3A{}%2C%22includePromotedContent%22%3Afalse%2C%22withSuperFollowsUserFields%22%3Atrue%2C%22withDownvotePerspective%22%3Afalse%2C%22withReactionsMetadata%22%3Afalse%2C%22withReactionsPerspective%22%3Afalse%2C%22withSuperFollowsTweetFields%22%3Atrue%7D&features=%7B%22dont_mention_me_view_api_enabled%22%3Atrue%2C%22interactive_text_enabled%22%3Atrue%2C%22responsive_web_uc_gql_enabled%22%3Afalse%2C%22vibe_tweet_context_enabled%22%3Afalse%2C%22responsive_web_edit_tweet_api_enabled%22%3Afalse%2C%22standardized_nudges_misinfo%22%3Afalse%2C%22responsive_web_enhance_cards_enabled%22%3Afalse%7D'.format(quote(user_id), nums)
    # 这里不管爬取失败还是成功，全都返回1，即不管失败还是成功该任务都不再
    while page_count < 100:
        # 由于需要翻页次数可能过多，故这里需要选取新cookies
        if crawl_type == 'follower':
            # next_url = 'https://api.twitter.com/graphql/mA9n2AcGj94QffGv3wXV1Q/Followers?variables=%7B%22userId%22%3A%22' + quote(user_id) + '%22%2C%22count%22%3A{}%2C%22cursor%22%3A%22'.format(nums) + quote(cursor) + '%22%2C%22withHighlightedLabel%22%3Afalse%2C%22withTweetQuoteCount%22%3Afalse%2C%22includePromotedContent%22%3Afalse%2C%22withTweetResult%22%3Afalse%2C%22withUserResult%22%3Afalse%7D'
            # 20220620更新
            next_url = 'https://twitter.com/i/api/graphql/ysj_6Bszzl-X7e4bmvYpBA/Followers?variables=%7B%22userId%22%3A%22' + quote(user_id) + '%22%2C%22count%22%3A{}%2C%22cursor%22%3A%22{}%22%2C%22includePromotedContent%22%3Afalse%2C%22withSuperFollowsUserFields%22%3Atrue%2C%22withDownvotePerspective%22%3Afalse%2C%22withReactionsMetadata%22%3Afalse%2C%22withReactionsPerspective%22%3Afalse%2C%22withSuperFollowsTweetFields%22%3Atrue%7D&features=%7B%22dont_mention_me_view_api_enabled%22%3Atrue%2C%22interactive_text_enabled%22%3Atrue%2C%22responsive_web_uc_gql_enabled%22%3Afalse%2C%22vibe_tweet_context_enabled%22%3Afalse%2C%22responsive_web_edit_tweet_api_enabled%22%3Afalse%2C%22standardized_nudges_misinfo%22%3Afalse%2C%22responsive_web_enhance_cards_enabled%22%3Afalse%7D'.format(nums, cursor)
        else:
            # next_url = 'https://api.twitter.com/graphql/8O-9xzc2m0pCEtbr5hagUw/Following?variables=%7B%22userId%22%3A%22' + quote(user_id) + '%22%2C%22count%22%3A{}%2C%22cursor%22%3A%22'.format(nums) + quote(cursor) + '%22%2C%22withHighlightedLabel%22%3Afalse%2C%22withTweetQuoteCount%22%3Afalse%2C%22includePromotedContent%22%3Afalse%2C%22withTweetResult%22%3Afalse%2C%22withUserResult%22%3Afalse%7D'
            # 20220620更新
            next_url = 'https://twitter.com/i/api/graphql/ih3I-XV0ogyWjqsHqFQ9eA/Following?variables=%7B%22userId%22%3A%22' + quote(user_id) + '%22%2C%22count%22%3A{}%2C%22cursor%22%3A%22{}%22%2C%22includePromotedContent%22%3Afalse%2C%22withSuperFollowsUserFields%22%3Atrue%2C%22withDownvotePerspective%22%3Afalse%2C%22withReactionsMetadata%22%3Afalse%2C%22withReactionsPerspective%22%3Afalse%2C%22withSuperFollowsTweetFields%22%3Atrue%7D&features=%7B%22dont_mention_me_view_api_enabled%22%3Atrue%2C%22interactive_text_enabled%22%3Atrue%2C%22responsive_web_uc_gql_enabled%22%3Afalse%2C%22vibe_tweet_context_enabled%22%3Afalse%2C%22responsive_web_edit_tweet_api_enabled%22%3Afalse%2C%22standardized_nudges_misinfo%22%3Afalse%2C%22responsive_web_enhance_cards_enabled%22%3Afalse%7D'.format(nums, quote(cursor))

        url = first_url if page_count == 1 else next_url

        print("抓取 {} 第{}页{}社交关系".format(person_url, page_count, crawl_type))
        resp = download_with_cookies(url, cookies, proxies)
        if resp is None:
            # 网络或代理出问题
            download_logger.error('{} 的第{}页{}社交关系抓取失败'.format(person_url, page_count, crawl_type))
            SpiderAccountOper.push_banned_account_to_error_queue(account)
            MongoSpiderAccountOper.update_spider_account_status(account, {'alive': PROXY_ERROR_CODE})
            return 0
        try:
            content = json.loads(resp.content)

            if not content:
                other_logger.error("数据为空或者返回not json,抓取 {} 第{}页{}社交关系".format(person_url, page_count, crawl_type))
                SpiderAccountOper.free_using_account(account)
                return 1
            # # 存储json文件
            # store_json(content, './filter_data/json_data/{}_json.json'.format(crawl_type))
            # print('存储成功')
            # time.sleep(10000)

            # # 对信息进行过滤-----爬取关系过滤后会出问题，爬取不到数据
            # t1 = time.time()
            # try:
            #     FilterSecurity.traverseJson(content)
            # except Exception:
            #     pass
            #
            # t2 = time.time()
            # print('过滤用时：{}秒'.format(t2 - t1))

            user_status_code = content.get("errors")[0].get('code') if content.get("errors") else 1
            if user_status_code == 22:
                other_logger.error('{}开启tweets隐私保护,第{}页推文信息抓取失败'.format(person_url, page_count))
                SpiderAccountOper.free_using_account(account)
                return 1

            # 状态码为这些，表明爬虫账号出了问题，需要将该账号放入错误队列，并更新mongodb数据库中的alive字段
            if user_status_code in SPIDER_ERROR_CODES:
                SpiderAccountOper.push_banned_account_to_error_queue(account)
                MongoSpiderAccountOper.update_spider_account_status(account, {'alive': user_status_code})
                return 0

            res = parser_relationship(content, person_url, user_tag, crawl_type, proxies)
            page_count += 1
            if res <= 10:
                print('当前页面未爬取关系仅{}条，故停止爬取下一页'.format(res))
                SpiderAccountOper.free_using_account(account)
                return 1
            cursor = jsonpath.jsonpath(content, '$..value')[0]
            # 测试时发现，为0时表示关系采集完毕
            if cursor[:cursor.find('|')] == '0':
                print('{} 所有的社交关系{}已采集完毕'.format(person_url, crawl_type))
                SpiderAccountOper.free_using_account(account)
                return 1
            else:
                # 表示还有社交关系未采集完，需要使用下面的循环继续采集
                print('采集第{}页{}社交关系成功'.format(page_count - 1, crawl_type))
        except TypeError as e:
            other_logger.error('采集第{}页{}社交关系时发生异常:{}'.format(page_count - 1, crawl_type, e))
            SpiderAccountOper.free_using_account(account)
            return 1
    SpiderAccountOper.free_using_account(account)
    return 1


def user_tweet_process(task):
    """
    爬取用户推文信息
    :param task:
    :return:
    """
    person_url, user_tag = task.get('user_url'), task.get('user_tag')

    spider_account = SpiderAccountOper.select_normal_account_from_redis()
    if spider_account is None:
        raise NoSpiderAccountException
    cookies, proxies, account = spider_account.cookies, spider_account.proxies, spider_account.account_name

    # 判断是否需要爬取用户信息
    task_profile = MongoUserInfoOper.get_user_info_data(person_url)
    if task_profile:
        screen_id, username, image_url = task_profile.get('screen_id'), task_profile.get('user_name'), task_profile.get('img_url'),
    else:
        # _flag表示该用户是否需要重新爬取---0表示需要重新爬取，1表示不需要(账号不存在)重新爬取
        _flag, screen_id, username, image_url = user_info_process(task, proxies)
        if screen_id == 'failed':
            SpiderAccountOper.free_using_account(account)
            return _flag
    # 抓取推文信息
    cursor = ''
    page_count = 1
    tweet_number = 200  # 浏览器链接参数为20，但这里经测试可写大于20，一页获取的推文数量更多，这里写200则一页返回200条推文，这里也可以写成2000条，大概返回1600多条数据
    # first_url = 'https://twitter.com/i/api/2/timeline/profile/{}.json?include_profile_interstitial_type=1&include_blocking=1&include_blocked_by=1&include_followed_by=1&include_want_retweets=1&include_mute_edge=1&include_can_dm=1&include_can_media_tag=1&skip_status=1&cards_platform=Web-12&include_cards=1&include_ext_alt_text=true&include_quote_count=true&include_reply_count=1&tweet_mode=extended&include_entities=true&include_user_entities=true&include_ext_media_color=true&include_ext_media_availability=true&send_error_codes=true&simple_quoted_tweet=true&include_tweet_replies=true&count={}&userId={}&ext=mediaStats%2ChighlightedLabel'.format(screen_id, tweet_number, screen_id)
    first_url = 'https://twitter.com/i/api/graphql/CwLU7qTfeu0doqhSr6tW4A/UserTweetsAndReplies?variables=%7B%22userId%22%3A%22{}%22%2C%22count%22%3A{}%2C%22includePromotedContent%22%3Atrue%2C%22withCommunity%22%3Atrue%2C%22withSuperFollowsUserFields%22%3Atrue%2C%22withBirdwatchPivots%22%3Afalse%2C%22withDownvotePerspective%22%3Afalse%2C%22withReactionsMetadata%22%3Afalse%2C%22withReactionsPerspective%22%3Afalse%2C%22withSuperFollowsTweetFields%22%3Atrue%2C%22withVoice%22%3Atrue%2C%22withV2Timeline%22%3Afalse%2C%22__fs_interactive_text%22%3Afalse%2C%22__fs_dont_mention_me_view_api_enabled%22%3Afalse%7D'.format(screen_id, tweet_number)
    # 只爬5页
    while page_count < 10:
        # next_url = 'https://twitter.com/i/api/2/timeline/profile/{}.json?include_profile_interstitial_type=1&include_blocking=1&include_blocked_by=1&include_followed_by=1&include_want_retweets=1&include_mute_edge=1&include_can_dm=1&include_can_media_tag=1&skip_status=1&cards_platform=Web-12&include_cards=1&include_ext_alt_text=true&include_quote_count=true&include_reply_count=1&tweet_mode=extended&include_entities=true&include_user_entities=true&include_ext_media_color=true&include_ext_media_availability=true&send_error_codes=true&simple_quoted_tweet=true&include_tweet_replies=true&count={}&cursor={}&userId={}&ext=mediaStats%2ChighlightedLabel'.format(screen_id, tweet_number, quote(cursor), screen_id)
        next_url = 'https://twitter.com/i/api/graphql/CwLU7qTfeu0doqhSr6tW4A/UserTweetsAndReplies?variables=%7B%22userId%22%3A%22{}%22%2C%22count%22%3A{}%2C%22cursor%22%3A%22{}%22%2C%22includePromotedContent%22%3Atrue%2C%22withCommunity%22%3Atrue%2C%22withSuperFollowsUserFields%22%3Atrue%2C%22withBirdwatchPivots%22%3Afalse%2C%22withDownvotePerspective%22%3Afalse%2C%22withReactionsMetadata%22%3Afalse%2C%22withReactionsPerspective%22%3Afalse%2C%22withSuperFollowsTweetFields%22%3Atrue%2C%22withVoice%22%3Atrue%2C%22withV2Timeline%22%3Afalse%2C%22__fs_interactive_text%22%3Afalse%2C%22__fs_dont_mention_me_view_api_enabled%22%3Afalse%7D'.format(screen_id, tweet_number, quote(cursor))
        url = first_url if page_count == 1 else next_url
        print("抓取 {} tweet第{}页推文信息（每页数据最多{}条推文）".format(person_url, page_count, tweet_number))
        # import datetime
        # starttime = datetime.datetime.now()

        resp = download_with_cookies(url, cookies, proxies)
        print(resp.content)
        #
        # endtime = datetime.datetime.now()
        # print('resquest消耗时间：{}'.format((endtime - starttime).seconds))
        if resp is None:
            download_logger.error('user {} 第{}页推文信息抓取失败 '.format(person_url, page_count))
            SpiderAccountOper.push_banned_account_to_error_queue(account)
            MongoSpiderAccountOper.update_spider_account_status(account, {'alive': PROXY_ERROR_CODE})
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
                SpiderAccountOper.free_using_account(account)
                return 1
            # 状态码为这些，表明爬虫账号出了问题，需要将该账号放入错误队列，并更新mongodb数据库中的alive字段
            if user_status_code in SPIDER_ERROR_CODES:
                SpiderAccountOper.push_banned_account_to_error_queue(account)
                MongoSpiderAccountOper.update_spider_account_status(account, {'alive': user_status_code})
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
                SpiderAccountOper.free_using_account(account)
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
                SpiderAccountOper.free_using_account(account)
                return 1
        except JSONDecodeError as e:
            other_logger.error('异常{},user {} 第{}页推文信息抓取失败,返回的不是json,返回的信息为：{}'.format(e, person_url, page_count, resp.content))
            SpiderAccountOper.free_using_account(account)
            return 1
        except Exception as e:
            other_logger.error('异常：{},user {} 第{}页推文信息抓取失败,目标账号可能出现异常,返回的信息为：{}'.format(e, person_url, page_count, resp.content))
            SpiderAccountOper.free_using_account(account)
            return 1
    SpiderAccountOper.free_using_account(account)


if __name__ == '__main__':
    #OK 看完
    # keyword_user_process('taiwan')
    # keyword_tasks = ['侵略台灣', '入侵台灣', '武力攻臺', '武統']
    # for keyword_task in keyword_tasks:
    #     # todo 爬取关键词任务
    #     # from celery import group
    #     # group(keyword_worker.delay(keyword_task.get('keyword'), 'user'),
    #     #       keyword_worker.delay(keyword_task.get('keyword'), 'top'),
    #     #       keyword_worker.delay(keyword_task.get('keyword'), 'latest')
    #     #       )
    #     # keyword_worker.apply_async(args=[keyword_task.get('keyword'), 'user'], countdown=random.randint(1, 10))
    #     keyword_worker.apply_async(args=[keyword_task.get('keyword'), 'top'], countdown=random.randint(1, 10))
    #     keyword_worker.apply_async(args=[keyword_task.get('keyword'), 'latest'], countdown=random.randint(1, 10))

    # # # 我的cookie
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
    # # print(cookies)
    task = {
        'user_url': 'https://twitter.com/ncdinglis',
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
    # # seeds = []
    # # for url in urls:
    # #     task = {
    # #         'user_url': '',
    # #         'user_name': 'Netinho_91',
    # #         'important_user': 0,
    # #         'add_time': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
    # #         'update_time': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
    # #
    # #         'follower_task': 1,
    # #         'following_task': 1,
    # #         'post_task': 1,
    # #         'profile_task': 1,
    # #
    # #         'post_praise_task': 1,
    # #         'post_repost_task': 1,
    # #         'post_comment_task': 1,
    # #         'post_reply_task': 1,
    # #
    # #         'post_crawl_status': 0,
    # #         'following_crawl_status': 0,
    # #         'follower_crawl_status': 0,
    # #         'profile_crawl_status': 0,
    # #         'tourist_crawl_status': 0,
    # #         'user_tag': None,
    # #     }
    # #     task['user_url'] = url
    # #     seeds.append(task)
    # # import threading
    # #
    # # tasks = []
    # # for seed in seeds:
    # #     task = threading.Thread(target=user_relationship_process, args=(seed, 'follower'))
    # #     tasks.append(task)
    # #
    # # for task in tasks:
    # #     task.start()
    # #     time.sleep(random.randint(1, 3))
    # #
    # # for task in tasks:
    # #     task.join()
    #
    # # todo 解析点赞者、评论者的代码需要修改回去，现在正在爬取tourist表中的screen_id
    # # todo 为什么爬取推文信息的时候在服务器上跑，爬完一页需要20几分钟，但是在本地却只要30秒左右？？ 但是关系的爬取很快
    # # todo 难道是因为celery携程开得太多了？---已验证，将携程数改为20 还是很慢
    # # todo 或者是将推文插入celery worker队列的操作太耗时？（因为采关系的吴该操作，故有可能有该原因---已验证不是该原因，该程序在服务器上与celery同时跑，速度很快）
    # # todo 还有一个原因是不是因为堆积的任务太多造成celery出问题？？
    # user_tweet_process(task)
    #user_relationship_process(task, 'follower')
    # user_info_process(task)
    # keyword_user_process('taiwan')
    #keyword_tweet_process('台湾','latest')
    # user_topic_process('1402880970252980225', 'user_relationship',cookies, proxies)
    # task = {'user_url': 'https://twitter.com/664540966Mi', 'user_tag': 'fs'}
    # user_relationship_process(task, 'following')


    # personal_url = 'https://twitter.com/realCarlosEC'
    # tweet_url = 'https://twitter.com/realCarlosEC/status/1413300667301515264'
    # screen_id = '1327278498981896192'
    # comment_process(personal_url, tweet_url, screen_id, cookies, proxies)
    # comment_process('https://twitter.com/asadowaisi', 'https://twitter.com/asadowaisi/status/1397815667873566724', '336611577', cookies, proxies)