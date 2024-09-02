import random
import time

from keyword_list import keyword_1,keyword2
from cookies_list import d
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


'until:2020-11-28 since:2020-10-28'
if __name__=='__main__':
    # print(datetime.datetime.now().date())
    keyword_list=keyword_1
    # d2=random.choice(d)亚运会建设',
    for key in ['杭州亚运会','亚运会','杭州亚运会安保','杭州亚运会保安','杭州亚运会 整治','杭州亚运会 安全','杭州亚运会 中共','杭州亚运会 网络攻击','杭州亚运会 黑客','杭州亚运会 勒索软件','杭州亚运会 APT']:#,'蔡英文','韩国瑜'
        datestart = '2023-10-08'
        datestart = dt.strptime(datestart, '%Y-%m-%d')
        flag = datestart
        for x in range(4):
            t = datetime.timedelta(days=2)
            next=flag-t
            d2=random.choice(d)
            print(d2)
            cookies=get_cookie_dict(d2['cookie'])
            proxies = d2['proxy']
            keyword=key+' until:{} since:{}'.format(flag.date(),next.date())#+fanti(' 反对')
            try:
                tweet_list = get_keywords_tweet(cookies=cookies, keyword=keyword, proxies=proxies)#proxies)
            except Exception as e:
                print(e)
                pass
            flag=next
            time.sleep(3)