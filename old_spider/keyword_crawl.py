import random
import time
import argparse
from keyword_new_simple.cookies_list import dx
from keyword_new_simple.get_keyword_tweets import get_keywords_tweet_upload
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
def crawl(keyword_list,datestart,num_months):
    for key in keyword_list:
        datestart = dt.strptime(datestart, '%Y-%m-%d')
        flag = datestart
        for x in range(num_months):
            t = datetime.timedelta(days=30)
            next = flag - t
            d2=random.choice(dx)
            print(d2)
            cookies=get_cookie_dict(d2['cookie'])
            proxies = d2['proxy']
            keyword=key+' until:{} since:{}'.format(flag.date(),next.date())
            try:
                tweet_list = get_keywords_tweet_upload(cookies=cookies, keyword=keyword, proxies=proxies)
            except:
                pass
            flag=next
            time.sleep(3)
if __name__=='__main__':
    parser = argparse.ArgumentParser(description='Crawl tweets based on keywords.')
    parser.add_argument('keyword_list', type=str, help='The list of keywords.')
    parser.add_argument('datestart', type=str, help='The start date in YYYY-MM-DD format.')
    parser.add_argument('num_months', type=int, help='The number of months to crawl.')
    args = parser.parse_args()
    crawl(args.keyword_list, args.datestart, args.num_months)
