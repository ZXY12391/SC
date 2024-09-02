import json
import random
import re
import random
import pymongo
Host="172.31.106.104"
MongoDB={
    "host":Host,
    "port":27017,

}
import requests

first_num = random.randint(55, 76)
third_num = random.randint(0, 3800)
fourth_num = random.randint(0, 140)
proxy = {
    'http': 'http://10.0.12.1:10835',
    'https': 'http://10.0.12.1:10835'
}
x_guest_token_list = []

class FakeChromeUA:
    os_type = [
        '(Windows NT 6.1; WOW64)', '(Windows NT 10.0; WOW64)', '(X11; Linux x86_64)',
        '(Macintosh; Intel Mac OS X 10_12_6)'
    ]

    chrome_version = 'Chrome/{}.0.{}.{}'.format(first_num, third_num, fourth_num)

    @classmethod
    def get_ua(cls):
        return ' '.join(['Mozilla/5.0', random.choice(cls.os_type), 'AppleWebKit/537.36',
                         '(KHTML, like Gecko)', cls.chrome_version, 'Safari/537.36']
                        )


def get_user_id_by_screen_name(screen_name):
    '''
     - 根据 screen_name 查询 user_id 的函数，该函数用于根据 screen_name 获取下面的 follow 函数中需要填写的 user_id （在 Twitter 上名为 rest_id）
    :param screen_name: 用户的 screen_name
    :return: 用户的 user_id
    '''


    # 再获取具体信息
    get_url = 'https://twitter.com/i/api/graphql/NimuplG1OB7Fd2btCLdBOw/UserByScreenName'

    if len(x_guest_token_list) == 0:
        # 先请求拿到 x-guest-token
        x_guest_token_headers = {
            'authority': 'api.twitter.com',
            'authorization': 'Bearer AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA',
            'origin': 'https://twitter.com',
            'referer': 'https://twitter.com/',
            'user-agent': FakeChromeUA.get_ua(),
        }

        x_guest_token_response = requests.post('https://api.twitter.com/1.1/guest/activate.json',headers=x_guest_token_headers,proxies=proxy).json()
        x_guest_token_list.append(x_guest_token_response['guest_token'])

    # 头部信息
    get_rest_id_headers = {
        'authority': 'api.twitter.com',
        'authorization': 'Bearer AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA',
        'origin': 'https://twitter.com',
        'referer': 'https://twitter.com/',
        'user-agent': FakeChromeUA.get_ua(),
        'x-guest-token' : x_guest_token_list[0]
    }

    params = {
        "variables" : json.dumps({
            "screen_name": screen_name,
            "withSafetyModeUserFields": True
        }),
        "features" : json.dumps({
            "hidden_profile_likes_enabled":True,
            "hidden_profile_subscriptions_enabled":True,
            "responsive_web_graphql_exclude_directive_enabled":True,
            "verified_phone_label_enabled":False,
            "subscriptions_verification_info_is_identity_verified_enabled":True,
            "subscriptions_verification_info_verified_since_enabled":True,
            "highlights_tweets_tab_ui_enabled":True,
            "responsive_web_twitter_article_notes_tab_enabled":False,
            "creator_subscriptions_tweet_preview_api_enabled":True,
            "responsive_web_graphql_skip_user_profile_image_extensions_enabled":False,
            "responsive_web_graphql_timeline_navigation_enabled":True
        }),
    }

    response = requests.get(get_url,headers=get_rest_id_headers, params=params,proxies=proxy)
    if response.status_code == 200 and response.json().get("data") != None:
        #print(response.json())
        return response.json()["data"]["user"]["result"]["rest_id"]
    else:
        x_guest_token_list.pop()
        get_user_id_by_screen_name(screen_name)

if __name__ == "__main__":
    with open("user.txt", "r") as file:
        user_ids = []
        for line in file:
            screen_name = line.strip()
            user_id = get_user_id_by_screen_name(screen_name)
            if user_id:
                username = line.strip()
                user_ids.append(user_id)
                print(f"{username}:{user_id}")

# 打印所有用户 id
print("User IDs:")
for user_id in user_ids:
    print(user_id)

# 将用户 id 写入到新的文本文件中
with open("user_ids.txt", "w") as outfile:
    for user_id in user_ids:
        outfile.write(user_id + "\n")
