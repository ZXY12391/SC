import datetime

import requests
import time
import random
import urllib3
import json
import jsonpath
import pymongo
from bloom_filter.bloom_filter import bloom_filter_keyword_post,bloom_filter_user
from urllib import parse
from keyword_new_simple.mongodb import user_oper,keyword_post_oper,keyword_keyword_oper,keyword_user_oper
from keyword_new_simple.rz_db import RZ_fanhua_dataset
import re
headerA = [
        {
            'origin': 'https://twitter.com',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.92 Safari/537.36',
            'authorization': 'Bearer AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA',
        },
        {
            'origin': 'https://twitter.com',
            'user-agent': 'Opera/9.80 (Windows NT 6.0) Presto/2.12.388 Version/12.14',
            'authorization': 'Bearer AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA',

        },
        {
            'origin': 'https://twitter.com',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.75.14 (KHTML, like Gecko) Version/7.0.3 Safari/7046A194A',
            'authorization': 'Bearer AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA',
        }
    ]

page_len=100
# 连接MongoDB数据库
client = pymongo.MongoClient("mongodb://172.31.106.104:27017/")
db = client["DW"]
collection = db["tweets"]
collection1 = db["tweets_upload"]

def get_keywords_tweet(cookies,keyword,proxies):
    headers = headerA[0]
    tweet_list=[]
    #cookies:使用自己账号或redis读取数据库
    headers['x-csrf-token']=cookies['ct0']
    page=0
    cursor=None
    while True:
        #配置keyword
        if page==page_len:
            break
        if page==0:
            k = '"{}"'.format(keyword)
            #v = '{"rawQuery":' + k + ',"count":20,"querySource": "typed_query", "product": "Media"}'
            v = '{"rawQuery":' + k + ',"count":20,"querySource": "typed_query", "product": "Media"}'
            print(v)
            print("vvvvvvvvvvvvvvvvvvvvvvvvvvvvvv")
            f='{"responsive_web_graphql_exclude_directive_enabled":true,"verified_phone_label_enabled":false,"responsive_web_home_pinned_timelines_enabled":true,"creator_subscriptions_tweet_preview_api_enabled":true,"responsive_web_graphql_timeline_navigation_enabled":true,"responsive_web_graphql_skip_user_profile_image_extensions_enabled":false,"c9s_tweet_anatomy_moderator_badge_enabled":true,"tweetypie_unmention_optimization_enabled":true,"responsive_web_edit_tweet_api_enabled":true,"graphql_is_translatable_rweb_tweet_is_translatable_enabled":true,"view_counts_everywhere_api_enabled":true,"longform_notetweets_consumption_enabled":true,"responsive_web_twitter_article_tweet_consumption_enabled":false,"tweet_awards_web_tipping_enabled":false,"freedom_of_speech_not_reach_fetch_enabled":true,"standardized_nudges_misinfo":true,"tweet_with_visibility_results_prefer_gql_limited_actions_policy_enabled":true,"longform_notetweets_rich_text_read_enabled":true,"longform_notetweets_inline_media_enabled":true,"responsive_web_media_download_video_enabled":false,"responsive_web_enhance_cards_enabled":false}'
            #f = '{"rweb_lists_timeline_redesign_enabled":true,"responsive_web_graphql_exclude_directive_enabled":true,"verified_phone_label_enabled":false,"creator_subscriptions_tweet_preview_api_enabled":true,"responsive_web_graphql_timeline_navigation_enabled":true,"responsive_web_graphql_skip_user_profile_image_extensions_enabled":false,"tweetypie_unmention_optimization_enabled":true,"responsive_web_edit_tweet_api_enabled":true,"graphql_is_translatable_rweb_tweet_is_translatable_enabled":true,"view_counts_everywhere_api_enabled":true,"longform_notetweets_consumption_enabled":true,"responsive_web_twitter_article_tweet_consumption_enabled":false,"tweet_awards_web_tipping_enabled":false,"freedom_of_speech_not_reach_fetch_enabled":true,"standardized_nudges_misinfo":true,"tweet_with_visibility_results_prefer_gql_limited_actions_policy_enabled":true,"longform_notetweets_rich_text_read_enabled":true,"longform_notetweets_inline_media_enabled":true,"responsive_web_media_download_video_enabled":false,"responsive_web_enhance_cards_enabled":false}'
            first_url = 'https://twitter.com/i/api/graphql/lZ0GCEojmtQfiUQa5oJSEw/SearchTimeline?variables=' + parse.quote(v) + \
                        '&features=' + parse.quote(f)
            #print(first_url)
            #first_url='https://twitter.com/i/api/graphql/lZ0GCEojmtQfiUQa5oJSEw/SearchTimeline?variables=%7B%22rawQuery%22%3A%22%E5%B7%B4%E4%BB%A5%E6%B2%96%E7%AA%81%22%2C%22count%22%3A20%2C%22querySource%22%3A%22typed_query%22%2C%22product%22%3A%22Media%22%7D&features=%7B%22responsive_web_graphql_exclude_directive_enabled%22%3Atrue%2C%22verified_phone_label_enabled%22%3Afalse%2C%22responsive_web_home_pinned_timelines_enabled%22%3Atrue%2C%22creator_subscriptions_tweet_preview_api_enabled%22%3Atrue%2C%22responsive_web_graphql_timeline_navigation_enabled%22%3Atrue%2C%22responsive_web_graphql_skip_user_profile_image_extensions_enabled%22%3Afalse%2C%22c9s_tweet_anatomy_moderator_badge_enabled%22%3Atrue%2C%22tweetypie_unmention_optimization_enabled%22%3Atrue%2C%22responsive_web_edit_tweet_api_enabled%22%3Atrue%2C%22graphql_is_translatable_rweb_tweet_is_translatable_enabled%22%3Atrue%2C%22view_counts_everywhere_api_enabled%22%3Atrue%2C%22longform_notetweets_consumption_enabled%22%3Atrue%2C%22responsive_web_twitter_article_tweet_consumption_enabled%22%3Afalse%2C%22tweet_awards_web_tipping_enabled%22%3Afalse%2C%22freedom_of_speech_not_reach_fetch_enabled%22%3Atrue%2C%22standardized_nudges_misinfo%22%3Atrue%2C%22tweet_with_visibility_results_prefer_gql_limited_actions_policy_enabled%22%3Atrue%2C%22longform_notetweets_rich_text_read_enabled%22%3Atrue%2C%22longform_notetweets_inline_media_enabled%22%3Atrue%2C%22responsive_web_media_download_video_enabled%22%3Afalse%2C%22responsive_web_enhance_cards_enabled%22%3Afalse%7D'
            res = requests.get(first_url, headers=headers, cookies=cookies,proxies=proxies)
        else:
            if cursor:
                k = '"{}"'.format(keyword)
                v2 = '{"rawQuery":' + k +',"cursor":"' + cursor + '","count":20,"querySource":"recent_search_click","product":"Media"}'
                f = '{"rweb_lists_timeline_redesign_enabled":true,"responsive_web_graphql_exclude_directive_enabled":true,"verified_phone_label_enabled":false,"creator_subscriptions_tweet_preview_api_enabled":true,"responsive_web_graphql_timeline_navigation_enabled":true,"responsive_web_graphql_skip_user_profile_image_extensions_enabled":false,"tweetypie_unmention_optimization_enabled":true,"responsive_web_edit_tweet_api_enabled":true,"graphql_is_translatable_rweb_tweet_is_translatable_enabled":true,"view_counts_everywhere_api_enabled":true,"longform_notetweets_consumption_enabled":true,"responsive_web_twitter_article_tweet_consumption_enabled":false,"tweet_awards_web_tipping_enabled":false,"freedom_of_speech_not_reach_fetch_enabled":true,"standardized_nudges_misinfo":true,"tweet_with_visibility_results_prefer_gql_limited_actions_policy_enabled":true,"longform_notetweets_rich_text_read_enabled":true,"longform_notetweets_inline_media_enabled":true,"responsive_web_media_download_video_enabled":false,"responsive_web_enhance_cards_enabled":false}'

                next_url = 'https://twitter.com/i/api/graphql/NA567V_8AFwu0cZEkAAKcw/SearchTimeline?variables=' + parse.quote(
                    v2) + \
                           '&features=' + parse.quote(f)
                #print(f"这是请求的api{next_url}")
                res = requests.get(next_url, headers=headers, cookies=cookies,proxies=proxies)
            else:
                break
        if res is None:
            print('NO RES')
            break
        try:
            content = json.loads(res.content)
            #print(content)
            if not content:
                print('NO CONTENT')
                break
            user_status_code = content.get("errors")[0].get('code') if content.get("errors") else 1
            if user_status_code in [32, 326, 130, 200]:
                print(f"状态码：{user_status_code}")
                print('USER DONE')
                break
            tweets = jsonpath.jsonpath(content, '$..tweet_results')
            cursor = jsonpath.jsonpath(content, '$..content')[-1]['value']
            #print(f"这是{cursor}")
            print("推文数据格式：", type(tweets))
            print("游标数据格式：", type(cursor))
            #print(tweets)
            temp_list = parse_tweet(tweets, keyword)
            # 每次解析完一批推特后，将其插入到 MongoDB 中
            for tweet in temp_list:
                collection.insert_one(tweet)
            lens=len(temp_list)
            print(f"这次爬取的数量：{lens}")
            # 建议每次都插入数据库，参考之前的解析代码
            tweet_list += temp_list
            print(f"完成第{page}页")
            page += 1
            if page == 100:
                print('满了')
                break
            time.sleep(random.randint(1, 3))
        except Exception as e:
            print(f"page：{page} get_keywords_tweet{e}")
            break
            pass
    #进行翻页处理
    return tweet_list
def get_keywords_tweet_upload(cookies,keyword,proxies):
    headers = headerA[0]
    tweet_list=[]
    #cookies:使用自己账号或redis读取数据库
    headers['x-csrf-token']=cookies['ct0']
    page=0
    cursor=None
    while True:
        #配置keyword
        if page==page_len:
            break
        if page==0:
            k = '"{}"'.format(keyword)
            #v = '{"rawQuery":' + k + ',"count":20,"querySource": "typed_query", "product": "Media"}'
            v = '{"rawQuery":' + k + ',"count":20,"querySource": "typed_query", "product": "Media"}'
            print(v)
            print("vvvvvvvvvvvvvvvvvvvvvvvvvvvvvv")
            f='{"responsive_web_graphql_exclude_directive_enabled":true,"verified_phone_label_enabled":false,"responsive_web_home_pinned_timelines_enabled":true,"creator_subscriptions_tweet_preview_api_enabled":true,"responsive_web_graphql_timeline_navigation_enabled":true,"responsive_web_graphql_skip_user_profile_image_extensions_enabled":false,"c9s_tweet_anatomy_moderator_badge_enabled":true,"tweetypie_unmention_optimization_enabled":true,"responsive_web_edit_tweet_api_enabled":true,"graphql_is_translatable_rweb_tweet_is_translatable_enabled":true,"view_counts_everywhere_api_enabled":true,"longform_notetweets_consumption_enabled":true,"responsive_web_twitter_article_tweet_consumption_enabled":false,"tweet_awards_web_tipping_enabled":false,"freedom_of_speech_not_reach_fetch_enabled":true,"standardized_nudges_misinfo":true,"tweet_with_visibility_results_prefer_gql_limited_actions_policy_enabled":true,"longform_notetweets_rich_text_read_enabled":true,"longform_notetweets_inline_media_enabled":true,"responsive_web_media_download_video_enabled":false,"responsive_web_enhance_cards_enabled":false}'
            #f = '{"rweb_lists_timeline_redesign_enabled":true,"responsive_web_graphql_exclude_directive_enabled":true,"verified_phone_label_enabled":false,"creator_subscriptions_tweet_preview_api_enabled":true,"responsive_web_graphql_timeline_navigation_enabled":true,"responsive_web_graphql_skip_user_profile_image_extensions_enabled":false,"tweetypie_unmention_optimization_enabled":true,"responsive_web_edit_tweet_api_enabled":true,"graphql_is_translatable_rweb_tweet_is_translatable_enabled":true,"view_counts_everywhere_api_enabled":true,"longform_notetweets_consumption_enabled":true,"responsive_web_twitter_article_tweet_consumption_enabled":false,"tweet_awards_web_tipping_enabled":false,"freedom_of_speech_not_reach_fetch_enabled":true,"standardized_nudges_misinfo":true,"tweet_with_visibility_results_prefer_gql_limited_actions_policy_enabled":true,"longform_notetweets_rich_text_read_enabled":true,"longform_notetweets_inline_media_enabled":true,"responsive_web_media_download_video_enabled":false,"responsive_web_enhance_cards_enabled":false}'
            first_url = 'https://twitter.com/i/api/graphql/lZ0GCEojmtQfiUQa5oJSEw/SearchTimeline?variables=' + parse.quote(v) + \
                        '&features=' + parse.quote(f)
            #print(first_url)
            #first_url='https://twitter.com/i/api/graphql/lZ0GCEojmtQfiUQa5oJSEw/SearchTimeline?variables=%7B%22rawQuery%22%3A%22%E5%B7%B4%E4%BB%A5%E6%B2%96%E7%AA%81%22%2C%22count%22%3A20%2C%22querySource%22%3A%22typed_query%22%2C%22product%22%3A%22Media%22%7D&features=%7B%22responsive_web_graphql_exclude_directive_enabled%22%3Atrue%2C%22verified_phone_label_enabled%22%3Afalse%2C%22responsive_web_home_pinned_timelines_enabled%22%3Atrue%2C%22creator_subscriptions_tweet_preview_api_enabled%22%3Atrue%2C%22responsive_web_graphql_timeline_navigation_enabled%22%3Atrue%2C%22responsive_web_graphql_skip_user_profile_image_extensions_enabled%22%3Afalse%2C%22c9s_tweet_anatomy_moderator_badge_enabled%22%3Atrue%2C%22tweetypie_unmention_optimization_enabled%22%3Atrue%2C%22responsive_web_edit_tweet_api_enabled%22%3Atrue%2C%22graphql_is_translatable_rweb_tweet_is_translatable_enabled%22%3Atrue%2C%22view_counts_everywhere_api_enabled%22%3Atrue%2C%22longform_notetweets_consumption_enabled%22%3Atrue%2C%22responsive_web_twitter_article_tweet_consumption_enabled%22%3Afalse%2C%22tweet_awards_web_tipping_enabled%22%3Afalse%2C%22freedom_of_speech_not_reach_fetch_enabled%22%3Atrue%2C%22standardized_nudges_misinfo%22%3Atrue%2C%22tweet_with_visibility_results_prefer_gql_limited_actions_policy_enabled%22%3Atrue%2C%22longform_notetweets_rich_text_read_enabled%22%3Atrue%2C%22longform_notetweets_inline_media_enabled%22%3Atrue%2C%22responsive_web_media_download_video_enabled%22%3Afalse%2C%22responsive_web_enhance_cards_enabled%22%3Afalse%7D'
            res = requests.get(first_url, headers=headers, cookies=cookies,proxies=proxies)
        else:
            if cursor:
                k = '"{}"'.format(keyword)
                v2 = '{"rawQuery":' + k +',"cursor":"' + cursor + '","count":20,"querySource":"recent_search_click","product":"Media"}'
                f = '{"rweb_lists_timeline_redesign_enabled":true,"responsive_web_graphql_exclude_directive_enabled":true,"verified_phone_label_enabled":false,"creator_subscriptions_tweet_preview_api_enabled":true,"responsive_web_graphql_timeline_navigation_enabled":true,"responsive_web_graphql_skip_user_profile_image_extensions_enabled":false,"tweetypie_unmention_optimization_enabled":true,"responsive_web_edit_tweet_api_enabled":true,"graphql_is_translatable_rweb_tweet_is_translatable_enabled":true,"view_counts_everywhere_api_enabled":true,"longform_notetweets_consumption_enabled":true,"responsive_web_twitter_article_tweet_consumption_enabled":false,"tweet_awards_web_tipping_enabled":false,"freedom_of_speech_not_reach_fetch_enabled":true,"standardized_nudges_misinfo":true,"tweet_with_visibility_results_prefer_gql_limited_actions_policy_enabled":true,"longform_notetweets_rich_text_read_enabled":true,"longform_notetweets_inline_media_enabled":true,"responsive_web_media_download_video_enabled":false,"responsive_web_enhance_cards_enabled":false}'

                next_url = 'https://twitter.com/i/api/graphql/NA567V_8AFwu0cZEkAAKcw/SearchTimeline?variables=' + parse.quote(
                    v2) + \
                           '&features=' + parse.quote(f)
                #print(f"这是请求的api{next_url}")
                res = requests.get(next_url, headers=headers, cookies=cookies,proxies=proxies)
            else:
                break
        if res is None:
            print('NO RES')
            break
        try:
            content = json.loads(res.content)
            #print(content)
            if not content:
                print('NO CONTENT')
                break
            user_status_code = content.get("errors")[0].get('code') if content.get("errors") else 1
            if user_status_code in [32, 326, 130, 200]:
                print(f"状态码：{user_status_code}")
                print('USER DONE')
                break
            tweets = jsonpath.jsonpath(content, '$..tweet_results')
            cursor = jsonpath.jsonpath(content, '$..content')[-1]['value']
            #print(f"这是{cursor}")
            print("推文数据格式：", type(tweets))
            print("游标数据格式：", type(cursor))
            #print(tweets)
            temp_list = parse_tweet(tweets, keyword)
            # 每次解析完一批推特后，将其插入到 MongoDB 中
            for tweet in temp_list:
                collection1.insert_one(tweet)
            lens=len(temp_list)
            print(f"这次爬取的数量：{lens}")
            # 建议每次都插入数据库，参考之前的解析代码
            tweet_list += temp_list
            print(f"完成第{page}页")
            page += 1
            if page == 100:
                print('满了')
                break
            time.sleep(random.randint(1, 3))
        except Exception as e:
            print(f"page：{page} get_keywords_tweet{e}")
            break
            pass
    #进行翻页处理
    return tweet_list
def get_keywords_tweet_people(cookies,keyword,proxies,people):
    headers = headerA[0]
    tweet_list=[]
    #cookies:使用自己账号或redis读取数据库
    headers['x-csrf-token']=cookies['ct0']
    page=0
    cursor=None
    while True:
        #配置keyword
        if page==page_len:
            break
        if page==0:
            k = '"{}"'.format(keyword+'(from:@{})'.format(people))
            v = '{"rawQuery":' + k + ',"count":20,"querySource":"recent_search_click","product":"Media"}'
            print(v)
            f = '{"rweb_lists_timeline_redesign_enabled":true,"responsive_web_graphql_exclude_directive_enabled":true,"verified_phone_label_enabled":false,"creator_subscriptions_tweet_preview_api_enabled":true,"responsive_web_graphql_timeline_navigation_enabled":true,"responsive_web_graphql_skip_user_profile_image_extensions_enabled":false,"tweetypie_unmention_optimization_enabled":true,"responsive_web_edit_tweet_api_enabled":true,"graphql_is_translatable_rweb_tweet_is_translatable_enabled":true,"view_counts_everywhere_api_enabled":true,"longform_notetweets_consumption_enabled":true,"responsive_web_twitter_article_tweet_consumption_enabled":false,"tweet_awards_web_tipping_enabled":false,"freedom_of_speech_not_reach_fetch_enabled":true,"standardized_nudges_misinfo":true,"tweet_with_visibility_results_prefer_gql_limited_actions_policy_enabled":true,"longform_notetweets_rich_text_read_enabled":true,"longform_notetweets_inline_media_enabled":true,"responsive_web_media_download_video_enabled":false,"responsive_web_enhance_cards_enabled":false}'
            first_url = 'https://twitter.com/i/api/graphql/NA567V_8AFwu0cZEkAAKcw/SearchTimeline?variables=' + parse.quote(v) + \
                        '&features=' + parse.quote(f)
            res = requests.get(first_url, headers=headers, proxies=proxies, cookies=cookies)
        else:
            if cursor:
                k = '"{}"'.format(keyword + '(from:@{})'.format(people))
                v2 = '{"rawQuery":' + k + ',"cursor":"' + cursor + '","count":20,"querySource":"recent_search_click","product":"Media"}'
                f = '{"rweb_lists_timeline_redesign_enabled":true,"responsive_web_graphql_exclude_directive_enabled":true,"verified_phone_label_enabled":false,"creator_subscriptions_tweet_preview_api_enabled":true,"responsive_web_graphql_timeline_navigation_enabled":true,"responsive_web_graphql_skip_user_profile_image_extensions_enabled":false,"tweetypie_unmention_optimization_enabled":true,"responsive_web_edit_tweet_api_enabled":true,"graphql_is_translatable_rweb_tweet_is_translatable_enabled":true,"view_counts_everywhere_api_enabled":true,"longform_notetweets_consumption_enabled":true,"responsive_web_twitter_article_tweet_consumption_enabled":false,"tweet_awards_web_tipping_enabled":false,"freedom_of_speech_not_reach_fetch_enabled":true,"standardized_nudges_misinfo":true,"tweet_with_visibility_results_prefer_gql_limited_actions_policy_enabled":true,"longform_notetweets_rich_text_read_enabled":true,"longform_notetweets_inline_media_enabled":true,"responsive_web_media_download_video_enabled":false,"responsive_web_enhance_cards_enabled":false}'
                next_url = 'https://twitter.com/i/api/graphql/NA567V_8AFwu0cZEkAAKcw/SearchTimeline?variables=' + parse.quote(
                    v2) + \
                           '&features=' + parse.quote(f)
                res = requests.get(next_url, headers=headers, proxies=proxies, cookies=cookies)
            else:
                break
        if res is None:
            print('NO RES')
            break
        try:
            content = json.loads(res.content)
            if not content:
                print('NO CONTENT')
                break
            user_status_code = content.get("errors")[0].get('code') if content.get("errors") else 1
            if user_status_code in [32, 326, 130, 200]:
                print('USER DONE')
                break
            tweets = jsonpath.jsonpath(content, '$..tweet_results')
            cursor = jsonpath.jsonpath(content, '$..content')[-1]['value']
            print(cursor)
            print("推文数据格式：", type(tweets))
            print("游标数据格式：", type(cursor))
            temp_list=parse_tweet(tweets,keyword)
            print(len(temp_list))
            #建议每次都插入数据库，参考之前的解析代码
            tweet_list+=temp_list
            page+=1
            if page==100:
                print('满了')
                break
            time.sleep(random.randint(1,3))
        except Exception as e:
            print(e)
            break
            pass
    #进行翻页处理
    return tweet_list

def get_keywords_tweet_people_no_p(cookies,keyword,people):
    headers = headerA[0]
    tweet_list=[]
    #cookies:使用自己账号或redis读取数据库
    headers['x-csrf-token']=cookies['ct0']
    page=0
    cursor=None
    while True:
        #配置keyword
        if page==page_len:
            break
        if page==0:
            k = '"{}"'.format(keyword+'(from:@{})'.format(people))
            v = '{"rawQuery":' + k + ',"count":20,"querySource":"recent_search_click","product":"Media"}'
            print(v)
            f = '{"rweb_lists_timeline_redesign_enabled":true,"responsive_web_graphql_exclude_directive_enabled":true,"verified_phone_label_enabled":false,"creator_subscriptions_tweet_preview_api_enabled":true,"responsive_web_graphql_timeline_navigation_enabled":true,"responsive_web_graphql_skip_user_profile_image_extensions_enabled":false,"tweetypie_unmention_optimization_enabled":true,"responsive_web_edit_tweet_api_enabled":true,"graphql_is_translatable_rweb_tweet_is_translatable_enabled":true,"view_counts_everywhere_api_enabled":true,"longform_notetweets_consumption_enabled":true,"responsive_web_twitter_article_tweet_consumption_enabled":false,"tweet_awards_web_tipping_enabled":false,"freedom_of_speech_not_reach_fetch_enabled":true,"standardized_nudges_misinfo":true,"tweet_with_visibility_results_prefer_gql_limited_actions_policy_enabled":true,"longform_notetweets_rich_text_read_enabled":true,"longform_notetweets_inline_media_enabled":true,"responsive_web_media_download_video_enabled":false,"responsive_web_enhance_cards_enabled":false}'
            first_url = 'https://twitter.com/i/api/graphql/NA567V_8AFwu0cZEkAAKcw/SearchTimeline?variables=' + parse.quote(v) + \
                        '&features=' + parse.quote(f)
            res = requests.get(first_url, headers=headers, cookies=cookies)
        else:
            if cursor:
                k = '"{}"'.format(keyword + '(from:@{})'.format(people))
                v2 = '{"rawQuery":' + k + ',"cursor":"' + cursor + '","count":20,"querySource":"recent_search_click","product":"Media"}'
                f = '{"rweb_lists_timeline_redesign_enabled":true,"responsive_web_graphql_exclude_directive_enabled":true,"verified_phone_label_enabled":false,"creator_subscriptions_tweet_preview_api_enabled":true,"responsive_web_graphql_timeline_navigation_enabled":true,"responsive_web_graphql_skip_user_profile_image_extensions_enabled":false,"tweetypie_unmention_optimization_enabled":true,"responsive_web_edit_tweet_api_enabled":true,"graphql_is_translatable_rweb_tweet_is_translatable_enabled":true,"view_counts_everywhere_api_enabled":true,"longform_notetweets_consumption_enabled":true,"responsive_web_twitter_article_tweet_consumption_enabled":false,"tweet_awards_web_tipping_enabled":false,"freedom_of_speech_not_reach_fetch_enabled":true,"standardized_nudges_misinfo":true,"tweet_with_visibility_results_prefer_gql_limited_actions_policy_enabled":true,"longform_notetweets_rich_text_read_enabled":true,"longform_notetweets_inline_media_enabled":true,"responsive_web_media_download_video_enabled":false,"responsive_web_enhance_cards_enabled":false}'
                next_url = 'https://twitter.com/i/api/graphql/NA567V_8AFwu0cZEkAAKcw/SearchTimeline?variables=' + parse.quote(
                    v2) + \
                           '&features=' + parse.quote(f)
                res = requests.get(next_url, headers=headers, cookies=cookies)
            else:
                break
        if res is None:
            print('NO RES')
            break
        try:
            content = json.loads(res.content)
            if not content:
                print('NO CONTENT')
                break
            user_status_code = content.get("errors")[0].get('code') if content.get("errors") else 1
            if user_status_code in [32, 326, 130, 200]:
                print('USER DONE')
                break
            tweets = jsonpath.jsonpath(content, '$..tweet_results')
            cursor = jsonpath.jsonpath(content, '$..content')[-1]['value']
            print(cursor)
            temp_list=parse_tweet(tweets,keyword)
            print(len(temp_list))
            #建议每次都插入数据库，参考之前的解析代码
            tweet_list+=temp_list
            page+=1
            if page==100:
                print('满了')
                break
            time.sleep(random.randint(1,3))
        except Exception as e:
            print(e)
            break
            pass
    #进行翻页处理
    return tweet_list

def get_keywords_tweet_rz(cookies,keyword,proxies):
    headers = headerA[0]
    tweet_list=[]
    #cookies:使用自己账号或redis读取数据库
    headers['x-csrf-token']=cookies['ct0']
    page=0
    cursor=None
    while True:
        #配置keyword
        if page==page_len:
            break
        if page==0:
            k = '"{}"'.format(keyword)
            v = '{"rawQuery":' + k + ',"count":20,"querySource":"recent_search_click","product":"Media"}'
            print(v)
            f = '{"rweb_lists_timeline_redesign_enabled":true,"responsive_web_graphql_exclude_directive_enabled":true,"verified_phone_label_enabled":false,"creator_subscriptions_tweet_preview_api_enabled":true,"responsive_web_graphql_timeline_navigation_enabled":true,"responsive_web_graphql_skip_user_profile_image_extensions_enabled":false,"tweetypie_unmention_optimization_enabled":true,"responsive_web_edit_tweet_api_enabled":true,"graphql_is_translatable_rweb_tweet_is_translatable_enabled":true,"view_counts_everywhere_api_enabled":true,"longform_notetweets_consumption_enabled":true,"responsive_web_twitter_article_tweet_consumption_enabled":false,"tweet_awards_web_tipping_enabled":false,"freedom_of_speech_not_reach_fetch_enabled":true,"standardized_nudges_misinfo":true,"tweet_with_visibility_results_prefer_gql_limited_actions_policy_enabled":true,"longform_notetweets_rich_text_read_enabled":true,"longform_notetweets_inline_media_enabled":true,"responsive_web_media_download_video_enabled":false,"responsive_web_enhance_cards_enabled":false}'
            first_url = 'https://twitter.com/i/api/graphql/NA567V_8AFwu0cZEkAAKcw/SearchTimeline?variables=' + parse.quote(v) + \
                        '&features=' + parse.quote(f)
            res = requests.get(first_url, headers=headers, proxies=proxies, cookies=cookies)
        else:
            if cursor:
                k = '"{}"'.format(keyword)
                v2 = '{"rawQuery":' + k + ',"cursor":"' + cursor + '","count":20,"querySource":"recent_search_click","product":"Media"}'
                f = '{"rweb_lists_timeline_redesign_enabled":true,"responsive_web_graphql_exclude_directive_enabled":true,"verified_phone_label_enabled":false,"creator_subscriptions_tweet_preview_api_enabled":true,"responsive_web_graphql_timeline_navigation_enabled":true,"responsive_web_graphql_skip_user_profile_image_extensions_enabled":false,"tweetypie_unmention_optimization_enabled":true,"responsive_web_edit_tweet_api_enabled":true,"graphql_is_translatable_rweb_tweet_is_translatable_enabled":true,"view_counts_everywhere_api_enabled":true,"longform_notetweets_consumption_enabled":true,"responsive_web_twitter_article_tweet_consumption_enabled":false,"tweet_awards_web_tipping_enabled":false,"freedom_of_speech_not_reach_fetch_enabled":true,"standardized_nudges_misinfo":true,"tweet_with_visibility_results_prefer_gql_limited_actions_policy_enabled":true,"longform_notetweets_rich_text_read_enabled":true,"longform_notetweets_inline_media_enabled":true,"responsive_web_media_download_video_enabled":false,"responsive_web_enhance_cards_enabled":false}'
                next_url = 'https://twitter.com/i/api/graphql/NA567V_8AFwu0cZEkAAKcw/SearchTimeline?variables=' + parse.quote(
                    v2) + \
                           '&features=' + parse.quote(f)
                res = requests.get(next_url, headers=headers, proxies=proxies, cookies=cookies)
            else:
                break
        if res is None:
            print('NO RES')
            break
        try:
            content = json.loads(res.content)
            if not content:
                print('NO CONTENT')
                break
            user_status_code = content.get("errors")[0].get('code') if content.get("errors") else 1
            if user_status_code in [32, 326, 130, 200]:
                print('USER DONE')
                break
            tweets = jsonpath.jsonpath(content, '$..tweet_results')
            cursor = jsonpath.jsonpath(content, '$..content')[-1]['value']
            print(cursor)
            temp_list=parse_tweet_rz(tweets,keyword)
            print(len(temp_list))
            #建议每次都插入数据库，参考之前的解析代码
            tweet_list+=temp_list
            page+=1
            if page==100:
                print('满了')
                break
            time.sleep(random.randint(1,3))
        except Exception as e:
            print(e)
            break
            pass
    #进行翻页处理
    return tweet_list

def remove_until_since_and_space(text):
    # 匹配并替换掉 until 和 since 及其后面的时间部分
    result = re.sub(r'(until|since):[0-9]{4}-[0-9]{2}-[0-9]{2}', '', text)
    # 去除末尾的空格
    result = result.strip()
    return result
def parse_tweet(tweets,keyword):
    tweet_list=[]
    for u in tweets:
        try:
            user=u['result']['core']["user_results"]
            tweet_url = 'https://twitter.com/' + user['result']['legacy']["screen_name"] + '/status/' + u['result']['rest_id']
            if bloom_filter_keyword_post.is_exist(tweet_url):
                # print('in')
                continue
            #(tweet_url)
            d={}
            user_dict={}
            d['tweet_url']=tweet_url
            d['Url']=tweet_url
            d['tid']=u['result']['rest_id']
            d['user_url']='https://twitter.com/'+user['result']['legacy']["screen_name"]
            d["source"]=u['result']["source"]
            d['Publish_time']=u['result']["legacy"]['created_at']
            d['Content']=u['result']["legacy"]['full_text']
            d['lang'] = u['result']["legacy"]['lang']
            d['favorite_count'] = u['result']["legacy"]['favorite_count']
            d['quote_count'] = u['result']["legacy"]['quote_count']
            d['reply_count'] = u['result']["legacy"]['reply_count']
            d['retweet_count'] = u['result']["legacy"]['retweet_count']
            d['quote_count'] = u['result']["legacy"]['quote_count']
            keyword_clean=remove_until_since_and_space(keyword)
            d['Keyword']=keyword_clean
            d['Featch_time']=str(datetime.datetime.now().date())
            keyword_post_oper.update(d)
            #keyword_keyword_oper.update(d)
            print(d)
            user_url = 'https://twitter.com/' + user['result']['legacy']["screen_name"]
            if not bloom_filter_user.is_exist(user_url):
                try:
                    user_dict['user_url']='https://twitter.com/'+user['result']['legacy']["screen_name"]
                    user_dict['screen_name']=user['result']['legacy']["screen_name"]
                    user_dict['description']=user['result']['legacy']["description"]
                    user_dict['Publish_time']=user['result']['legacy']["created_at"]
                    user_dict['favourites_count']=user['result']['legacy']["favourites_count"]
                    user_dict['followers_count'] = user['result']['legacy']["followers_count"]
                    user_dict['friends_count'] = user['result']['legacy']["friends_count"]
                    user_dict['name'] = user['result']['legacy']["name"]
                    user_dict['profile_image_url_https'] = user['result']['legacy']["profile_image_url_https"]
                    user_dict['statuses_count'] = user['result']['legacy']["statuses_count"]
                    user_dict['Featch_time'] = str(datetime.datetime.now().date())
                #     print(u)
                    bloom_filter_user.save(user_dict['user_url'])
                    user_oper.update(user_dict)
                    #keyword_user_oper.update(user_dict)
                except:
                    print(user_dict)
            bloom_filter_keyword_post.save(tweet_url)
            tweet_list.append(d)
        except Exception as e:
            print(f"parse_tweet{e}")
            pass
    return tweet_list

def parse_tweet_rz(tweets,keyword):
    tweet_list=[]
    for u in tweets:
        try:
            user=u['result']['core']["user_results"]
            tweet_url = 'https://twitter.com/' + user['result']['legacy']["screen_name"] + '/status/' + u['result']['rest_id']
            if bloom_filter_keyword_post.is_exist(tweet_url):
                continue
            print(tweet_url)
            d={}
            user_dict={}
            d['tweet_url']=tweet_url
            d['tid']=u['result']['rest_id']
            d['user_url']='https://twitter.com/'+user['result']['legacy']["screen_name"]
            d["source"]=u['result']["source"]
            d['created_at']=u['result']["legacy"]['created_at']
            d['full_text']=u['result']["legacy"]['full_text']
            d['lang'] = u['result']["legacy"]['lang']
            d['favorite_count'] = u['result']["legacy"]['favorite_count']
            d['quote_count'] = u['result']["legacy"]['quote_count']
            d['reply_count'] = u['result']["legacy"]['reply_count']
            d['retweet_count'] = u['result']["legacy"]['retweet_count']
            d['quote_count'] = u['result']["legacy"]['quote_count']
            d['keyword']=keyword
            d['featch_time']=str(datetime.datetime.now().date())
            #keyword_post_oper.update(d)
            RZ_fanhua_dataset.update(d)
            print(d)
            # user_url = 'https://twitter.com/' + user['result']['legacy']["screen_name"]
            # if not bloom_filter_user.is_exist(user_url):
            #     try:
            #         user_dict['user_url']='https://twitter.com/'+user['result']['legacy']["screen_name"]
            #         user_dict['screen_name']=user['result']['legacy']["screen_name"]
            #         user_dict['description']=user['result']['legacy']["description"]
            #         user_dict['created_at']=user['result']['legacy']["created_at"]
            #         user_dict['favourites_count']=user['result']['legacy']["favourites_count"]
            #         user_dict['followers_count'] = user['result']['legacy']["followers_count"]
            #         user_dict['friends_count'] = user['result']['legacy']["friends_count"]
            #         user_dict['name'] = user['result']['legacy']["name"]
            #         user_dict['profile_image_url_https'] = user['result']['legacy']["profile_image_url_https"]
            #         user_dict['statuses_count'] = user['result']['legacy']["statuses_count"]
            #     #     print(u)
            #         bloom_filter_user.save(user_dict['user_url'])
            #         #user_oper.update(user_dict)
            #         keyword_user_oper.update(user_dict)
            #     except:
            #         print(user_dict)
            bloom_filter_keyword_post.save(tweet_url)
            tweet_list.append(d)
        except Exception as e:
            print(e)
            pass
    return tweet_list

if __name__=='__main__':
    x = '_ga_BYKEBDM7DS=GS1.1.1668430218.1.0.1668430218.0.0.0; des_opt_in=Y; guest_id_ads=v1%3A167783074997576169; guest_id_marketing=v1%3A167783074997576169; guest_id=v1%3A167783074997576169; kdt=MfPbhUgSFd1xvSnljvBUlgFM2ok5Hxe6Uullcxju; auth_token=b42066a4904fe2cd3a68e6c466110b286a27462c; ct0=4ae8a8724f6315ff33704e538d2857d1c3847d7d9f74a6129d4ebf9f77c570116cd4abb50fbcdc36e82ac64d70d887bcc52819fdc0406302370bc56803601e872b5ade59699b4c92e76d28840b51e12b; twid=u%3D1417123113267986438; mbox=PC#66ed333c25a041b5a7d08733b0d0be1d.35_0#1741594902|session#af482dfb641c480b8ecb7a4cc42a046b#1678351962; _ga_34PHSZMC42=GS1.1.1678350038.2.1.1678350109.0.0.0; _ga=GA1.2.1361456031.1667122339; lang=en; _gid=GA1.2.1804735022.1689581266; personalization_id="v1_T7Yi9AxYIR8XhE0x4LT9Zw=="'
    xx = x.split(';')
    c_d = {}
    for xxx in xx:
        z = xxx.strip()
        # print(z)
        key = z.split('=')
        c_d[key[0]] = key[1]
    proxies = {'http': 'http://10.0.12.1:10885', 'https': 'http://10.0.12.1:10885'}
    cookies=c_d#'输入你的cookie或者从redis中读取（参照之前的代码）'#需要转换成dict形式
    keyword='蔡徐坤'
    tweet_list=get_keywords_tweet(cookies=cookies,keyword=keyword,proxies=proxies)
    print(tweet_list)

    #得到推文列表
    #根据需要存储进入数据库或存在本地（存数据库代码参考其他）















