import random
from keyword_list import keyword_1,keyword2
from cookies_list import d
from get_keyword_tweets import get_keywords_tweet_people,get_keywords_tweet_people_no_p
from mongodb import user_oper,cdb

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
    p=r'E:/RZ项目/RZ/10.19.xlsx'
    import pandas as pd
    data = pd.read_excel(io=p)
    data = data.loc[:, ["target", "user_url"]]
    x=[]
    y=[]
    for each in data.target:
        x.append(each)
    for each in data.user_url:
        y.append(each)
    data={}
    for i in range(len(x)):
        data[y[i].replace('https://twitter.com/','')]=[x[i]]
    #'EZ2p8' 明天从这里开始
    f=0
    for x in data:
        print(x)
        if x=='EZ2p8':
            f=1
        if f!=1:
            continue
        print(data[x])
        try:
            d2=random.choice(d)
            print(d2)
            cookies=get_cookie_dict(d2['cookie'])
            proxies = d2['proxy']
            keyword=data[x][0]
            #tweet_list = get_keywords_tweet_people_no_p(cookies=cookies, keyword=keyword,people=x)
            tweet_list = get_keywords_tweet_people(cookies=cookies, keyword=keyword, proxies=proxies,people=x)
        except Exception as e:
            print(e)
            pass
    pass