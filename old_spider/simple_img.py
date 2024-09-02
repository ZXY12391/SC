import time
import base64
import requests
import random

# d={
#     'proxy':{'http': 'http://10.0.12.1:10926', 'https': 'http://10.0.12.1:10926'},
#     'cookie':'mbox=PC#5ede305b874b4a9ab92022b4db0a4508.35_0#1741595283|session#4f4794e5d82d467ba7dfa43d40a2ebf7#1678352343; _ga_34PHSZMC42=GS1.1.1678350046.3.1.1678350494.0.0.0; _ga=GA1.2.1945272692.1669812117; guest_id=v1%3A168543812976418058; guest_id_marketing=v1%3A168543812976418058; guest_id_ads=v1%3A168543812976418058; kdt=imzajsQCi0HDxIXeZeoyOKYYdNOupx47I8PLqpr9; auth_token=c92444532fd78da176f9d88ec4bcb6ebae53d6c9; ct0=bdd185eb9f8d941793abbf525ea3f21216767bb9910edcdb10610cf0064b7e50e00a4d03ce3434aaa57161802cab335987d0abf2d6a4c15609b6597a5f3ac3a66a1e1278a2890142b749f4b391db50e1; twid=u%3D1309043493940137992; lang=en; external_referer=padhuUp37zhD6%2F29CpQtyhGQCUl05AFo|0|8e8t2xd8A2w%3D; _gid=GA1.2.972153973.1693558283; personalization_id="v1_rsF0+j7UFI8uR1huriAzIA=="'
# }



headerA = [
    {
        'origin': 'https://twitter.com',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.92 Safari/537.36',
        'authorization': 'Bearer AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA',
    },
    {
        'origin': 'https://twitter.com',
        'user-agent': 'Opera/9.80 (Windows NT 6.0) Presto/2.12.388 Version/12.14',
        'authorization': 'Bearer AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA',

    },
    {
        'origin': 'https://twitter.com',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.75.14 (KHTML, like Gecko) Version/7.0.3 Safari/7046A194A',
        'authorization': 'Bearer AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA',
    }
]

def download_without_cookies(url, proxies):
    try:
        resp = requests.get(url, proxies=proxies, headers=headerA[0], timeout=10)
        return resp
    except:
        pass
    finally:
        time.sleep(random.randint(5, 10))
        pass
    return None

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

def crawl_avatar_image(user_url, avatar_url,proxy):
    if not avatar_url:
        return 1
    count = 1
    avatar_image_data = {}
    while count < 4:
        resp = download_without_cookies(avatar_url, proxy)
        if resp is None:
            # download_logger.error('{} 头像 {} 抓取第{}次失败'.format(user_url, avatar_url, count))
            avatar_image_data['avatar_binary'] = None
            avatar_image_data['avatar_bs64encode'] = None
            count += 1
        else:
            # 二进制文件
            avatar_image_data['avatar_binary'] = resp.content
            # base64编码可在离线环境下展示，方便前端展示（可直接存链接或者base64编码后的图片）
            encodestr = base64.b64encode(resp.content)  # 得到 byte 编码的数据
            avatar_image_data['avatar_bs64encode'] = 'data:image/jpeg;base64,%s' % encodestr.decode("utf-8")
            print(avatar_image_data['avatar_bs64encode'])
            return avatar_image_data['avatar_bs64encode']
    return None

def get_one(avatar_url):
    p=[10802, 10803, 10805, 10806, 10810, 10811, 10812, 10813, 10814, 10815, 10816, 10817, 10818, 10819, 10820, 10821, 10822, 10823, 10824, 10825, 10826, 10827, 10828, 10829, 10830, 10831, 10832, 10833, 10834, 10835, 10836, 10837, 10838, 10839, 10840, 10841, 10842, 10843, 10844, 10845, 10846, 10847, 10848, 10849, 10852, 10853, 10854, 10856, 10857, 10858, 10859, 10860, 10861, 10862, 10863, 10864, 10865, 10866, 10867, 10868, 10869, 10870, 10871, 10872, 10873, 10874, 10875, 10876, 10877, 10878, 10879]
    pp={'http': 'http://10.0.12.1:{}'.format(random.choice(p)), 'https': 'http://10.0.12.1:{}'.format(random.choice(p))}
    url=avatar_url.replace('normal','400x400')
    print(avatar_url)
    data=crawl_avatar_image(avatar_url=url,proxy=pp,user_url=None)
    if data:
        # if 'default_profile_images' in avatar_url:
        #     cbd['t_data'].update_many({'profile_image_url_https': avatar_url},
        #                                      {'$set': {'avatar_bs64encode': data, 'img_tasks': 1}})
        cbd['t_data'].update_one({'avatar_url':avatar_url},{'$set':{'avatar_bs64encode':data,'img_tasks':1}})
        print('DONE')
        return True
    return None

if __name__=='__main__':
    from config.settings import mongo_uri, mongo_name, PROXY_ERROR_CODE
    from logger.log import db_logger
    import pymongo
    client = pymongo.MongoClient(mongo_uri)
    cbd = client['Fqdl']
    #user_img_tasks = list(cbd['t_data_similer'].find({'img_tasks':0}))
    print('get data')
    #user_img_tasks=[_['profile_image_url_https'] for _ in user_img_tasks if 'avatar_bs64encode' not in user_img_tasks]
    u=['https://twitter.com/ruthymunoz','https://twitter.com/insecteens','https://twitter.com/Ws2Et1','https://twitter.com/CallMeRoy115',
       'https://twitter.com/AlbertLiao13','https://twitter.com/Hao_ye_AK4783']
    for x in u:
        d=cbd['t_data'].find_one({'user_url':x})
        print(d)
        get_one(d['avatar_url'])
    # print(len(u))
    # # for user_task in user_profile_tasks:
    # from multiprocessing.dummy import Pool
    # pool = Pool(50)
    # res = pool.map(get_one, u)








