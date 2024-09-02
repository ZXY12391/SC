mongo_host = '172.31.106.104'
mongo_port = '27017'
mongo_user = 'rzuser'
mongo_pass = '172031106104'
mongo_name = 'RzDB'
mongo_type = 'mongodb'
mongo_uri = '{}://{}:{}@{}:{}/{}'.format(mongo_type, mongo_user, mongo_pass, mongo_host, mongo_port, mongo_name)
print(mongo_uri)

import pymongo

myclient = pymongo.MongoClient(mongo_uri)
cdb = myclient['RzDB']
# import pymongo
# mongo_host = '10.0.12.2'
# mongo_port = '27017'
# mongo_user = 'fquser'
# mongo_pass = 'VPS123db!'
# mongo_name = 'Fqdl'
# mongo_type = 'mongodb'
# mongo_uri = '{}://{}:{}@{}:{}/{}'.format(mongo_type, mongo_user, mongo_pass, mongo_host, mongo_port, mongo_name)
# myclient = pymongo.MongoClient(mongo_uri)
# cdb = myclient['Fqdl']

class mongodber():
    key = None

    @classmethod
    def update_or_insert(cls, conditions, attrs_maps):
        col = cdb[cls.key]
        col.update_one(conditions, {'$set': attrs_maps}, True)
    @classmethod
    def test(cls):
        pass
    @classmethod
    def basic_find(cls,condition,attrs):
        col = cdb[cls.key]
        res=col.find(condition,attrs)
        return list(res)

    @classmethod
    def find_one(cls,condition):
        col = cdb[cls.key]
        res=col.find_one(condition)
        return res

    @classmethod
    def basic_find_num(cls, condition, attrs,num):
        col = cdb[cls.key]
        res = col.find(condition, attrs).limit(num)
        return list(res)


class user_oper(mongodber):
    key = 'RZ_user_1205'#_1205
    @classmethod
    def update(cls, data):
        con = {'user_url': data['user_url']}
        cls.update_or_insert(conditions=con, attrs_maps=data)

    @classmethod
    def get_urser(cls):
        data=cls.basic_find([],['screen_name'])
        return data




class keyword_post_oper(mongodber):
    key = 'RZ_post_1205'#_1205
    @classmethod
    def update(cls, data):
        con = {'tweet_url': data['tweet_url']}
        cls.update_or_insert(conditions=con, attrs_maps=data)

    @classmethod
    def find(cls, data):
        con = {'user_url': data}
        res=cls.basic_find(condition=con,attrs=[])
        return res

class keyword_keyword_oper(mongodber):
    key = 'YAYUN_keyword'

    @classmethod
    def update(cls, data):
        con = {'tweet_url': data['tweet_url']}
        cls.update_or_insert(conditions=con, attrs_maps=data)

    @classmethod
    def find_all(cls):
        #con = {'tweet_url': data['tweet_url']}
        data=cls.basic_find({'featch_time': '2023-10-08'}, ['full_text'])#'featch_time':'2023-09-24'
        data=[_['full_text'] for _ in data]
        return data

    @classmethod
    def find_all_two(cls):
        # con = {'tweet_url': data['tweet_url']}
        data = cls.basic_find({'featch_time':'2023-10-08'}, ['full_text','tweet_url','user_url'])
        # data = [_['full_text'] for _ in data]
        return list(data)


class keyword_user_oper(mongodber):
    key = 'YAYUN_user'

    @classmethod
    def update(cls, data):
        con = {'user_url': data['user_url']}
        cls.update_or_insert(conditions=con, attrs_maps=data)

    @classmethod
    def get_urser(cls):
        data=cls.basic_find([],['screen_name'])
        return data