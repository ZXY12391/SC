import random
import time

from keyword_list import keyword_1,keyword2
from cookies_list import d
from get_keyword_tweets import get_keywords_tweet_rz
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

'until:2020-11-28 since:2020-10-28'
if __name__=='__main__':
    # print(datetime.datetime.now().date())
    keyword_list=keyword_1
    # d2=random.choice(d)
    k=['#台湾独立','']
    for key in ['#自由中国','#台湾独立','#台湾建国']:#,'蔡英文','韩国瑜','#中共国 可恶','蓝卫兵','#反共',
        d2 = random.choice(d)
        print(d2)
        cookies = get_cookie_dict(d2['cookie'])
        proxies = d2['proxy']
        keyword = key # +fanti(' 反对')
        try:
            tweet_list = get_keywords_tweet_rz(cookies=cookies, keyword=keyword, proxies=proxies)
        except:
            pass
        # datestart = '2023-09-30'
        # datestart = dt.strptime(datestart, '%Y-%m-%d')
        # flag = datestart
        # for x in range(30):
        #     t = datetime.timedelta(days=30)
        #     next=flag-t
        #     d2=random.choice(d)
        #     print(d2)
        #     cookies=get_cookie_dict(d2['cookie'])
        #     proxies = d2['proxy']
        #     keyword=key+' until:{} since:{}'.format(flag.date(),next.date())#+fanti(' 反对')
        #     try:
        #         tweet_list = get_keywords_tweet_rz(cookies=cookies, keyword=keyword, proxies=proxies)
        #     except:
        #         pass
        #     flag=next
        #     time.sleep(3)




