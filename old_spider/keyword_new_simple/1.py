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

page_len = 10

def get_user_tweets(cookies, user_id):
    headers = headerA[0]
    tweets_list = []
    # cookies: Use your own account or read from the database, like Redis
    proxies = {'http': 'http://10.0.12.1:10885', 'https': 'http://10.0.12.1:10885'}
    headers['x-csrf-token'] = cookies['ct0']
    page = 0
    cursor = None
    while True:
        if page == page_len:
            break
        if page == 0:
            v = '{"userId":"' + user_id + '","count":20,"tabType":"tweets","mediaType":"GIF","containerType":"ProfileTimeline","timelineCursor":null,"withReactions":false,"includeReplies":false,"withHighlightedLabel":true,"withTweetQuoteCount":true,"includePromotedContent":false,"withTweetResult":false}'
            f = '{"disable_experiments":true,"disable_search_autocomplete":true,"responsive_web_mentions_new":true,"responsive_web_mentions_new_slot":false,"responsive_web_graphql_ruf_v2":true,"responsive_web_performance_v2_3":true,"responsive_web_scroll_reset_top":true,"responsive_web_scroll_reset_bottom":false}'
            first_url = 'https://twitter.com/i/api/graphql/CAAAAAAAAANV64WAAABJLJ-QAAAAAFAAAAiyoSmAAF0AAAAFAlRhAXEAAAAIAAAAAAAI6C4SMxYAAAAA/ProfileTimeline?variables=' + parse.quote(v) + '&count=20&userId=' + user_id + '&withHighlightedLabel=true&withTweetQuoteCount=true'
            res = requests.get(first_url, headers=headers, proxies=proxies, cookies=cookies)
        else:
            if cursor:
                v2 = '{"userId":"' + user_id + '","count":20,"tabType":"tweets","mediaType":"GIF","containerType":"ProfileTimeline","timelineCursor":"' + cursor + '","withReactions":false,"includeReplies":false,"withHighlightedLabel":true,"withTweetQuoteCount":true,"includePromotedContent":false,"withTweetResult":false}'
                next_url = 'https://twitter.com/i/api/graphql/CAAAAAAAAANV64WAAABJLJ-QAAAAAFAAAAiyoSmAAF0AAAAFAlRhAXEAAAAIAAAAAAAI6C4SMxYAAAAA/ProfileTimeline?variables=' + parse.quote(v2) + '&count=20&userId=' + user_id + '&withHighlightedLabel=true&withTweetQuoteCount=true'
                res = requests.get(next_url, headers=headers, proxies=proxies, cookies=cookies)
            else:
                break
        if res is None:
            break
        try:
            content = json.loads(res.content)
            if not content:
                break
            tweets_status_code = content.get("errors")[0].get('code') if content.get("errors") else 1
            if tweets_status_code in [32, 326, 130, 200]:
                break
            tweets = jsonpath.jsonpath(content, '$..tweet_results')
            cursor = jsonpath.jsonpath(content, '$..content')[-1]['value']
            tweet_temp_list = parse_tweets(tweets)
            if len(tweet_temp_list) <= 10:
                break
            tweets_list += tweet_temp_list
            page += 1
            time.sleep(random.randint(1, 3))
        except:
            break
            pass
    return tweets_list

def parse_tweets(tweets):
    tweet_list = []
    for tweet in tweets:
        try:
            tweet_list.append(tweet['result']['legacy'])
            # This contains tweet information like: ['created_at', 'id_str', 'text', 'full_text', 'truncated', 'display_text_range', 'entities', 'extended_entities', 'source', 'in_reply_to_status_id_str', 'in_reply_to_user_id_str', 'in_reply_to_screen_name', 'user', 'geo', 'coordinates', 'place', 'contributors', 'is_quote_status', 'quoted_status_id_str', 'quoted_status', 'quoted_status_permalink', 'retweet_count', 'favorite_count', 'favorited', 'retweeted', 'possibly_sensitive', 'lang']
            # Customize as needed
        except:
            pass
    return tweet_list

if __name__ == '__main__':
    cookies = get_cookie_dict(d2['cookie'])
    cookies = 'YourCookieHere'  # Use your own cookie or read from the database (refer to previous code)
    user_id = 'UserIDHere'  # Specify the user ID whose tweets you want to retrieve
    tweets_list = get_user_tweets(cookies=cookies, user_id=user_id)
    print(tweets_list)
    # Get the list of tweets
    # Store in the database or locally as needed (refer to other code for database storage)
