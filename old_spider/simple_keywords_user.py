import pprint

import requests
import time
import random
import urllib3
import json
import jsonpath

from urllib import parse

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

page_len=1

def get_keywords_user(cookies,keyword,proxies):
    headers = headerA[0]
    user_list=[]
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
            v = '{"rawQuery":' + k + ',"count":20,"querySource":"recent_search_click","product":"People"}'
            print(v)
            f = '{"rweb_lists_timeline_redesign_enabled":true,"responsive_web_graphql_exclude_directive_enabled":true,"verified_phone_label_enabled":false,"creator_subscriptions_tweet_preview_api_enabled":true,"responsive_web_graphql_timeline_navigation_enabled":true,"responsive_web_graphql_skip_user_profile_image_extensions_enabled":false,"tweetypie_unmention_optimization_enabled":true,"responsive_web_edit_tweet_api_enabled":true,"graphql_is_translatable_rweb_tweet_is_translatable_enabled":true,"view_counts_everywhere_api_enabled":true,"longform_notetweets_consumption_enabled":true,"responsive_web_twitter_article_tweet_consumption_enabled":false,"tweet_awards_web_tipping_enabled":false,"freedom_of_speech_not_reach_fetch_enabled":true,"standardized_nudges_misinfo":true,"tweet_with_visibility_results_prefer_gql_limited_actions_policy_enabled":true,"longform_notetweets_rich_text_read_enabled":true,"longform_notetweets_inline_media_enabled":true,"responsive_web_media_download_video_enabled":false,"responsive_web_enhance_cards_enabled":false}'
            first_url = 'https://twitter.com/i/api/graphql/NA567V_8AFwu0cZEkAAKcw/SearchTimeline?variables=' + parse.quote(v) + \
                        '&features=' + parse.quote(f)
            res = requests.get(first_url, headers=headers, proxies=proxies, cookies=cookies)
        else:
            if cursor:
                k = '"{}"'.format(keyword)
                v2 = '{"rawQuery":' + k +',"cursor":"' + cursor + '","count":20,"querySource":"recent_search_click","product":"People"}'
                f = '{"rweb_lists_timeline_redesign_enabled":true,"responsive_web_graphql_exclude_directive_enabled":true,"verified_phone_label_enabled":false,"creator_subscriptions_tweet_preview_api_enabled":true,"responsive_web_graphql_timeline_navigation_enabled":true,"responsive_web_graphql_skip_user_profile_image_extensions_enabled":false,"tweetypie_unmention_optimization_enabled":true,"responsive_web_edit_tweet_api_enabled":true,"graphql_is_translatable_rweb_tweet_is_translatable_enabled":true,"view_counts_everywhere_api_enabled":true,"longform_notetweets_consumption_enabled":true,"responsive_web_twitter_article_tweet_consumption_enabled":false,"tweet_awards_web_tipping_enabled":false,"freedom_of_speech_not_reach_fetch_enabled":true,"standardized_nudges_misinfo":true,"tweet_with_visibility_results_prefer_gql_limited_actions_policy_enabled":true,"longform_notetweets_rich_text_read_enabled":true,"longform_notetweets_inline_media_enabled":true,"responsive_web_media_download_video_enabled":false,"responsive_web_enhance_cards_enabled":false}'
                next_url = 'https://twitter.com/i/api/graphql/NA567V_8AFwu0cZEkAAKcw/SearchTimeline?variables=' + parse.quote(
                    v2) + \
                           '&features=' + parse.quote(f)
                res = requests.get(next_url, headers=headers, proxies=proxies, cookies=cookies)
            else:
                break
        if res is None:
            break
        try:
            content = json.loads(res.content)
            if not content:
                break
            user_status_code = content.get("errors")[0].get('code') if content.get("errors") else 1
            if user_status_code in [32, 326, 130, 200]:
                break
            users = jsonpath.jsonpath(content, '$..user_results')
            cursor = jsonpath.jsonpath(content, '$..content')[-1]['value']
            user_temp_list=parse_user(users)
            user_list+=user_temp_list
            page+=1
            if len(user_temp_list)<=10:
                break
            time.sleep(random.randint(1,3))
        except:
            break
            pass
    #进行翻页处理
    return user_list

def parse_user(users):
    user_list=[]
    for u in users:
        try:
            user_list.append(u['result']["legacy"])
        # 这个中包含用户信息如下：['can_dm', 'can_media_tag', 'created_at', 'default_profile', 'default_profile_image', 'description', 'entities', 'fast_followers_count', 'favourites_count', 'followers_count', 'friends_count', 'has_custom_timelines', 'is_translator', 'listed_count', 'location', 'media_count', 'name', 'normal_followers_count', 'pinned_tweet_ids_str', 'possibly_sensitive', 'profile_banner_url', 'profile_image_url_https', 'profile_interstitial_type', 'screen_name', 'statuses_count', 'translator_type', 'url', 'verified', 'want_retweets', 'withheld_in_countries']
        # 根据需要筛选
        except:
            pass
    return user_list

import random

from keyword_new_simple.cookies_list import d
from db.mongo_db import client

cbd=client['Fqdl']

def get_cookie_dict(x):
    print(x)
    xx = x.split(';')
    c_d = {}
    for xxx in xx:
        z = xxx.strip()
        # print(z)
        key = z.split('=')
        c_d[key[0]] = key[1]
    return c_d

def task_one_new(user_task):
    try:
        time.sleep(random.randint(1, 2))
        d2 = random.choice(d)
        print(d2)
        cookies = get_cookie_dict(d2['cookie'])
        proxies = d2['proxy']
        # for user_task in user_profile_tasks:
        print(user_task.get('twitter_url') + '需要爬取档案信息')
        user_name=cbd['t_data'].find_one({'user_url':user_task.get('twitter_url')})['user_name']
        user_list=get_keywords_user(cookies,user_name,proxies)[0:5]
        pprint.pprint(user_list)
        if len(user_list)==0:
            print('没有')
            return None
        if len(user_list)==1:
            cbd['RZ_multi_user_tasks'].update_one({'twitter_url': user_task.get('twitter_url')},
                                                  {'$set': {'profile': 3}})
        for x in user_list:
            if x['screen_name']==user_task.get('twitter_url').replace('https://twitter.com/','').replace('https://www.twitter.com/',''):
                continue
            x['father_url']=user_task.get('twitter_url')
            x['img_tasks'] = 0
            cbd['t_data_similer'].insert_one(x)
            cbd['RZ_multi_user_tasks'].update_one({'twitter_url':user_task.get('twitter_url')},{'$set':{'profile': 2}})
    except Exception as e:
        print(e)
def get_tasks(task):
    task = cbd['RZ_multi_user_tasks'].find_one({'twitter_url': task,'profile':1})
    if not task:
        return None
    return task

if __name__=='__main__':
    user_profile_tasks = list(cbd['RZ_multi_user_tasks'].find({'profile': 1}))
    # p = r'E:\RZ项目\川大\川大\新建 XLSX 工作表.xlsx'
    # import pandas as pd
    # data = pd.read_excel(io=p)
    # data = data.loc[:, ["new_url"]]
    # data = data.to_dict(orient='records')
    # data = [_['new_url'] for _ in data]
    # user_profile_tasks_ = []
    # for u in user_profile_tasks:
    #     if u['facebook_url'] in data:
    #         user_profile_tasks_.append(u)
    # print(len(user_profile_tasks_))
    # random.shuffle(user_profile_tasks_)
    # # # task_one_new(user_profile_tasks[])
    random.shuffle(user_profile_tasks)
    print(len(user_profile_tasks))
    from multiprocessing.dummy import Pool
    pool = Pool(2)
    res = pool.map(task_one_new, user_profile_tasks)