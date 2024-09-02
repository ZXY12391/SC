import random
from keyword_list import keyword_1,keyword2
from cookies_list import d
from get_keyword_tweets import get_keywords_tweet
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

if __name__=='__main__':
    keyword_list=keyword_1
    # d2=random.choice(d)
    for key in keyword_list:
        d2=random.choice(d)
        print(d2)
        cookies=get_cookie_dict(d2['cookie'])
        proxies = d2['proxy']
        keyword=fanti(key+' 选举')
        try:
            tweet_list = get_keywords_tweet(cookies=cookies, keyword=keyword, proxies=proxies)
        except:
            pass
    pass







