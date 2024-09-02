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
    for key in ['選舉','胜利','失败','支持','反对']:#,'蔡英文','拜登 赢','蔡英文 赢','赖清德 选举','侯友宜 选举','柯文哲 选举','
        datestart = '2023-09-10'
        datestart = dt.strptime(datestart, '%Y-%m-%d')
        flag = datestart
        for x in range(30):
            t = datetime.timedelta(days=10)
            next=flag-t
            d2=random.choice(d)
            print(d2)
            cookies=get_cookie_dict(d2['cookie'])
            proxies = d2['proxy']
            t='(柯文哲 OR 侯友谊 OR 赖清德 OR 蔡英文 OR 郭台铭)'
            t2='(敗灯 OR 癡呆 OR 川普 OR 白痴登 OR 破锣西 OR 拜登 OR 特朗普 OR 德桑蒂斯 OR 海莉 OR 黑莉)'
            keyword=t2+' until:{} since:{}'.format(flag.date(),next.date())#+fanti(' 反对')
            try:
                tweet_list = get_keywords_tweet(cookies=cookies, keyword=keyword, proxies=proxies)
            except:
                pass
            flag=next
            time.sleep(3)
    # pass

#6192

