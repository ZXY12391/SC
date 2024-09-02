#1 work 和 experence 的exls文件用户读出来

#2 把profile 2 的读出来看有多少

#3 profile 3  和 1中选有twitter的

#4 public 里面选出 profile 2 的



#1
import pprint
import random


def choose_one(url):
    p=cbd['t_data'].find_one({'user_url':url})
    if not p:
        return None
    s={}
    s['url']=url
    s['name']=p['user_name']
    return s

if __name__=='__main__':
    from config.settings import mongo_uri, mongo_name, PROXY_ERROR_CODE
    from logger.log import db_logger
    import pymongo
    client = pymongo.MongoClient(mongo_uri)
    cbd = client['Fqdl']
    p = r'E:\RZ项目\川大\川大\新建 XLSX 工作表.xlsx'
    import pandas as pd
    data = pd.read_excel(io=p)
    data = data.loc[:, ["new_url"]]
    data = data.to_dict(orient='records')
    p = r'E:\RZ项目\川大\川大\123.xlsx'
    data2 = pd.read_excel(io=p)
    data2 = data2.loc[:, ["new_url"]]
    data2 = data2.to_dict(orient='records')
    data = [_['new_url'] for _ in data]
    data2=[_['new_url'] for _ in data2]
    data=data+data2
    print('1',len(data))
    data=list(set(data))
    print('2',len(data))
    user_profile_tasks = list(cbd['RZ_multi_user_tasks'].find({'profile': 2,'tweets':1}))
    user_profile_tasks += list(cbd['RZ_multi_user_tasks'].find({'profile': 1,'tweets':1}))
    user_profile_tasks += list(cbd['RZ_multi_user_tasks'].find({'profile': 3,'tweets':1}))
    select=[]
    for x in user_profile_tasks:
        url=x['facebook_url']
        if url in data:
            select.append(x)
    print('select',len(select))
    #public select
    user_profile_tasks_p = list(cbd['RZ_multi_public_tasks'].find({'profile': 2}))
    user_profile_tasks_p += list(cbd['RZ_multi_public_tasks'].find({'profile': 1,'tweets':1}))
    user_profile_tasks_p += list(cbd['RZ_multi_public_tasks'].find({'profile': 3,'tweets':1}))
    print('public',len(user_profile_tasks_p))
    select=select+user_profile_tasks_p
    print(select[0].keys())
    urls=[_['twitter_url'] for _ in select]
    dx=[]
    print('here')
    from multiprocessing.dummy import Pool
    pool = Pool(50)
    print(len(urls))
    res = pool.map(choose_one, urls)
    for r in res:
        if r:
            dx.append(r)
    print('dx',len(dx))
    get=[_['url'] for _ in dx]
    user_profile_tasks_c = list(cbd['RZ_multi_user_tasks'].find({'profile':3,'tweets': 1}))
    user_profile_tasks_c+= list(cbd['RZ_multi_public_tasks'].find({'profile':3}))
    expend=[_['twitter_url'] for _ in user_profile_tasks_c if _['twitter_url'] not in get]
    print('len',len(expend))
    from multiprocessing.dummy import Pool
    pool = Pool(50)
    res = pool.map(choose_one, expend)
    dx2=[]
    for r in res:
        if r:
            dx2.append(r)
    print(len(dx2))
    k=random.sample(dx2,500)
    print(len(k))
    dx=dx+k
    print(len(dx))
    data2 = pd.DataFrame(data=dx, columns=list(dx[0].keys()))
    # PATH为导出文件的路径和文件名
    data2.to_csv('./label.csv',encoding='utf-8-sig')
    # for u in urls:
    #     p=cbd['t_data'].find_one({'user_url':u})
    #     s={}
    #     s['url']=u
    #     s['name']=p['user_name']
    #     d.append(s)
    #     print('1')
    # print('here')






    # print(len(select))
    # data2 = pd.DataFrame(data=select, columns=list(select[0].keys()))
    # # PATH为导出文件的路径和文件名
    # data2.to_csv('./new/.csv',
    #              encoding='utf-8-sig')

































