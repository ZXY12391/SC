import random
import time

from keyword_list import keyword_1,keyword2
from cookies_list import d
from get_keyword_tweets import get_keywords_tweet_people
from datetime import datetime as dt
import datetime

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

def fanti(text):
    import zhconv
    text1 = zhconv.convert(text, 'zh-tw')
    return text1

# def get_times():
#     from datetime import datetime
#     today = datetime.now()
#     import datetime
#     time = datetime.timedelta(days=30)
#     print("今天的日期:", today - time)
#     return today-time,today-time


if __name__=='__main__':
    # print(datetime.datetime.now().date())
    keyword_list=['台北','台中','臺北','臺中']
    # d2=random.choice(d)
    for key in ['拜登 美国大选']:#,'蔡英文','韩国瑜'

        d2=random.choice(d)
        print(d2)
        cookies=get_cookie_dict(d2['cookie'])
        proxies = d2['proxy']
        keyword=fanti(key)
        try:
            tweet_list = get_keywords_tweet_people(cookies=cookies, keyword=keyword, proxies=proxies, people=people)
        except:
            pass
        flag=next
        time.sleep(3)










































