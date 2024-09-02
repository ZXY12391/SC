import random
import time

from keyword_list import keyword_1,keyword2
from cookies_list import dx
from get_keyword_tweets import get_keywords_tweet
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
    # d2=random.choice(d)
    for key in ['俄乌战争','俄乌冲突','俄罗斯 乌克兰']:#['停火協議','空襲','戰爭與和平','以色列','乌克兰','猶太人','以巴分治','休戰協議','巴以沖突','俄烏戰爭','以巴分治','兩岸和平','哈瑪斯']:#,'蔡英文','拜登 赢','蔡英文 赢','赖清德 选举','侯友宜 选举','柯文哲 选举','
        datestart = '2023-12-05'
        datestart = dt.strptime(datestart, '%Y-%m-%d')
        flag = datestart
        for x in range(30):
            t = datetime.timedelta(days=30)
            next = flag - t
            d2=random.choice(dx)
            print(d2)
            cookies=get_cookie_dict(d2['cookie'])
            proxies = d2['proxy']
            keyword=key+' until:{} since:{}'.format(flag.date(),next.date())
            try:
                tweet_list = get_keywords_tweet(cookies=cookies, keyword=keyword, proxies=proxies)
            except:
                pass
            flag=next
            time.sleep(3)

    for key in ['巴以战争','巴以冲突','以色列 巴勒斯坦']:#['停火協議','空襲','戰爭與和平','以色列','乌克兰','猶太人','以巴分治','休戰協議','巴以沖突','俄烏戰爭','以巴分治','兩岸和平','哈瑪斯']:#,'蔡英文','拜登 赢','蔡英文 赢','赖清德 选举','侯友宜 选举','柯文哲 选举','
        datestart = '2023-12-05'
        datestart = dt.strptime(datestart, '%Y-%m-%d')
        flag = datestart
        for x in range(30):
            t = datetime.timedelta(days=7)
            next = flag - t
            d2=random.choice(dx)
            print(d2)
            cookies=get_cookie_dict(d2['cookie'])
            proxies = d2['proxy']
            keyword=key+' until:{} since:{}'.format(flag.date(),next.date())
            try:
                tweet_list = get_keywords_tweet(cookies=cookies, keyword=keyword, proxies=proxies)
            except:
                pass
            flag=next
            time.sleep(3)