import requests
import time
import random
import urllib3
import json
import jsonpath

# from OpenSSL.SSL import SysCallError
from requests import ConnectTimeout
from urllib3.exceptions import NewConnectionError
from requests.exceptions import ProxyError, SSLError, ReadTimeout

from config.headers import headerA, based_header
from logger.log import download_logger
from confusion.confusion import get_confusion_url


def crawl_confusion(real_url, confusion_type, cookies, proxies):
    """
    采集混淆（目标混淆，行为混淆）
    :return:
    """
    url = get_confusion_url(real_url, confusion_type)
    if not url:
        return
    download_with_cookies(url, cookies, proxies, False)


# 不需要登录的请求
def download_without_cookies(url, proxies):
    try:
        resp = requests.get(url, proxies=proxies, headers=headerA[0], timeout=10)
        return resp
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
    finally:
        time.sleep(random.randint(5, 10))
        pass
    return None

from urllib import parse
# 需要登录的请求
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
        download_logger.error('代理{}, 超时{}'.format(proxies, e))
    except (NewConnectionError, ConnectionError, TimeoutError, ConnectTimeout) as e:
        download_logger.error('代理{}, 异常{}'.format(proxies, e))
    except ProxyError as e:
        download_logger.error('代理{}, 代理出错{}'.format(proxies, e))
    except SSLError as e:
        download_logger.error('代理{}, ssl链接{}'.format(proxies, e))
    except Exception as e:
        download_logger.error('代理{}, 异常{}'.format(proxies, e))
    finally:
        time.sleep(random.randint(10, 15))
    return None

def download_with_cookies_keyword(url, cookies, proxies, confusion=True):
    import httpx
    based_header_ = {
        'origin': 'https://twitter.com',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36',
        'authorization': 'Bearer AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA',
    }
    headers = {**based_header_}
    proxies=proxies['http']
    print(type(proxies))
    print(url)
    try:
        url_token = 'https://api.twitter.com/1.1/guest/activate.json'
        token = json.loads(httpx.post(url_token, headers=headers, proxies=proxies).text)['guest_token']
        print(token)
        headers['x-guest-token'] = token
        time.sleep(0.1)
        resp = httpx.get(url, proxies=proxies, headers=headers, timeout=10)#, cookies=cookies
        print(resp.content)
        return resp
    except :
        pass
    finally:
        time.sleep(random.randint(10, 15))
    return None




if __name__=='__main__':
    pass
    # headers = {'x-csrf-token': 'xsaxasxasx'}
    # headers = {**based_header}
    # print(headers)
    # headers = {
    #     'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:93.0) Gecko/20100101 Firefox/93.0',
    #     'Accept': '*/*',
    #     'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
    #     'x-guest-token': '',
    #     'x-twitter-client-language': 'zh-cn',
    #     'x-twitter-active-user': 'yes',
    #     'x-csrf-token': '25ea9d09196a6ba850201d47d7e75733',
    #     'Sec-Fetch-Dest': 'empty',
    #     'Sec-Fetch-Mode': 'cors',
    #     'Sec-Fetch-Site': 'same-origin',
    #     'authorization': 'Bearer AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA',
    #     'Referer': 'https://twitter.com/',
    #     'Connection': 'keep-alive',
    # }
    # proxies={'http': 'http://10.0.12.1:10961', 'https': 'http://10.0.12.1:10961'}
    # url_token = 'https://api.twitter.com/1.1/guest/activate.json'
    # token = json.loads(requests.post(url_token, headers=headers, proxies=proxies).text)['guest_token']
    # print(token)
    # headers['x-guest-token'] = token
    # q = '(from:twitter) until:2021-01-02 since:2021-01-01'
    # first_url='https://api.twitter.com/2/search/adaptive.json?include_profile_interstitial_type=1&include_blocking=1&include_blocked_by=1&include_followed_by=1&include_want_retweets=1&include_mute_edge=1&include_can_dm=1&include_can_media_tag=1&include_ext_has_nft_avatar=1&include_ext_is_blue_verified=1&include_ext_verified_type=1&skip_status=1&cards_platform=Web-12&include_cards=1&include_ext_alt_text=true&include_ext_limited_action_results=false&include_quote_count=true&include_reply_count=1&tweet_mode=extended&include_ext_views=true&include_entities=true&include_user_entities=true&include_ext_media_color=true&include_ext_media_availability=true&include_ext_sensitive_media_warning=true&include_ext_trusted_friends_metadata=true&send_error_codes=true&simple_quoted_tweet=true&q=quoted_tweet_id:1633385119317917696&vertical=tweet_detail_quote&count=20&requestContext=launch&pc=1&spelling_corrections=1&include_ext_edit_control=true&ext=mediaStats,highlightedLabel,hasNftAvatar,voiceInfo,birdwatchPivot,enrichments,superFollowMetadata,unmentionInfo,editControl,vibe'
    # #'https://api.twitter.com/2/search/adaptive.json?include_profile_interstitial_type=1&include_blocking=1&include_blocked_by=1&include_followed_by=1&include_want_retweets=1&include_mute_edge=1&include_can_dm=1&include_can_media_tag=1&include_ext_has_nft_avatar=1&include_ext_is_blue_verified=1&include_ext_verified_type=1&skip_status=1&cards_platform=Web-12&include_cards=1&include_ext_alt_text=true&include_ext_limited_action_results=false&include_quote_count=true&include_reply_count=1&tweet_mode=extended&include_ext_views=true&include_entities=true&include_user_entities=true&include_ext_media_color=true&include_ext_media_availability=true&include_ext_sensitive_media_warning=true&include_ext_trusted_friends_metadata=true&send_error_codes=true&simple_quoted_tweet=true&q=台湾&result_filter=user&query_source=recent_search_click&count=20&requestContext=launch&pc=1&spelling_corrections=1&include_ext_edit_control=true&ext=mediaStats,highlightedLabel,hasNftAvatar,voiceInfo,birdwatchPivot,enrichments,superFollowMetadata,unmentionInfo,editControl,vibe'#'https://api.twitter.com/2/search/adaptive.json?include_profile_interstitial_type=1&include_blocking=1&include_blocked_by=1&include_followed_by=1&include_want_retweets=1&include_mute_edge=1&include_can_dm=1&include_can_media_tag=1&include_ext_has_nft_avatar=1&include_ext_is_blue_verified=1&include_ext_verified_type=1&skip_status=1&cards_platform=Web-12&include_cards=1&include_ext_alt_text=true&include_ext_limited_action_results=false&include_quote_count=true&include_reply_count=1&tweet_mode=extended&include_ext_views=true&include_entities=true&include_user_entities=true&include_ext_media_color=true&include_ext_media_availability=true&include_ext_sensitive_media_warning=true&include_ext_trusted_friends_metadata=true&send_error_codes=true&simple_quoted_tweet=true&q={}&result_filter=user&query_source=recent_search_click&count=20&requestContext=launch&pc=1&spelling_corrections=1&include_ext_edit_control=true&ext=mediaStats'
    # #url = "https://twitter.com/i/api/2/search/adaptive.json?include_profile_interstitial_type=1&include_blocking=1&include_blocked_by=1&include_followed_by=1&include_want_retweets=1&include_mute_edge=1&include_can_dm=1&include_can_media_tag=1&skip_status=1&cards_platform=Web-12&include_cards=1&include_ext_alt_text=true&include_quote_count=true&include_reply_count=1&tweet_mode=extended&include_entities=true&include_user_entities=true&include_ext_media_color=true&include_ext_media_availability=true&send_error_codes=true&simple_quoted_tweet=true&q={}&count=20&query_source=typed_query&pc=1&spelling_corrections=1&ext=mediaStats%2ChighlightedLabel%2CvoiceInfo"
    # res = requests.get(first_url.format(parse.quote(q)), headers=headers,proxies=proxies)
    # print(res.text)


#OK 页面下载器