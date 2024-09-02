import random
from keyword_list import keyword_1,keyword2
from cookies_list import d
from get_keyword_tweets import get_keywords_tweet_people
from mongodb import user_oper,cdb
import pandas as pd

def export_people():
    data = cdb['RZ_post'].find({'featch_time': '2023-09-27'}, ['user_url'])
    data = list(set([_['user_url'] for _ in data]))
    from pprint import pprint
    pprint(len(data))
    i = 0
    import os
    filePath = r'E:\Twitter\old_spider\keyword_new_simple\data_0927'
    filenames = os.listdir(filePath)
    filePath = r'E:\Twitter\old_spider\keyword_new_simple\data'
    filenames += os.listdir(filePath)
    for x in data:
        datas = list(cdb['RZ_post'].find({'user_url': x}))
        if not datas:
            continue
        for d in datas:
            del d['_id']
        s = datas[0]['user_url'].replace('https://twitter.com/', '')
        if s in filenames:
            print(s)
            print('in')
            continue
        print('not in')
        data2 = pd.DataFrame(data=datas, columns=list(datas[0].keys()))
        # PATH为导出文件的路径和文件名
        data2.to_csv('./test_people/{}.csv'.format(datas[0]['user_url'].replace('https://twitter.com/', '')),
                     encoding='utf-8-sig')


if __name__=='__main__':
    # p = r'E:/RZ项目/RZ/10.19.xlsx'
    # import pandas as pd
    # data = pd.read_excel(io=p)
    # data = data.loc[:, ["target", "user_url","stance"]]
    # data=data.to_dict(orient='records')
    #
    # print(len(data))
    # for each in data:
    #     print(each['user_url'])
    #     print(each['stance'])
    #     datas = list(cdb['RZ_post'].find({'user_url': each['user_url']}))
    #     print(len(datas))
    #     data2 = pd.DataFrame(data=datas, columns=list(datas[0].keys()))
    #     # PATH为导出文件的路径和文件名
    #     data2.to_csv('./new/{}_{}_{}.csv'.format(each['user_url'].replace('https://twitter.com/', ''),each['target'],each['stance']),
    #                  encoding='utf-8-sig')
    #
    # p = r'E:/RZ项目/RZ/待标.xlsx'
    # import pandas as pd
    # data_ = pd.read_excel(io=p)
    # data_ = data_.loc[:, ["target", "user_url", "stance"]]
    # data_ = data_.to_dict(orient='records')
    # print(len(data_))
    #
    # for each in data_:
    #     print(each['user_url'])
    #     print(each['stance'])
    #     datas = list(cdb['RZ_post'].find({'user_url': 'https://twitter.com/'+each['user_url']}))
    #     print(len(datas))
    datas=list(cdb['RZ_profle_stage2_truth_test'].find({},['user_url','Occupation','gender','introduction','loction']))
    print(len(datas))
    data2 = pd.DataFrame(data=datas, columns=list(datas[0].keys()))
    # PATH为导出文件的路径和文件名
    data2.to_csv('./data_old.csv',
                 encoding='utf-8-sig')

    # for each in data.target:
    #     x.append(each)
    # for each in data.user_url:
    #     y.append(each)
    # data = {}
    # for i in range(len(x)):
    #     data[y[i].replace('https://twitter.com/', '')] = [x[i]]


    # data = list(cdb['RZ_post'].find({'featch_time':'2023-10-05'}))
    # print(len(data))
    # # data = list(set([_['user_url'] for _ in data]))
    # # from pprint import pprint
    # # pprint(len(data))
    # # i = 0
    # # import os
    # # # filePath = r'E:\Twitter\old_spider\keyword_new_simple\data_0927'
    # # # filenames=os.listdir(filePath)
    # # # filePath = r'E:\Twitter\old_spider\keyword_new_simple\data'
    # # # filenames += os.listdir(filePath)
    # # datas_=[]
    # # for x in data:
    # #     datas=list(cdb['RZ_post'].find({'user_url': x}))
    # #     if not datas:
    # #         continue
    # #     for d in datas:
    # #         del d['_id']
    # #     datas=datas[0:20]
    # #     datas_+=datas
    # data2 = pd.DataFrame(data=data, columns=list(data[0].keys()))
    # # PATH为导出文件的路径和文件名
    # data2.to_csv('./{}.csv'.format('tweets_new_to_wh'), encoding='utf-8-sig')






