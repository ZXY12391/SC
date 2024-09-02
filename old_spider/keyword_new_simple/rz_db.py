import pymongo
mongo_host = '10.0.12.2'
mongo_port = '27017'
mongo_user = 'fquser'
mongo_pass = 'VPS123db!'
mongo_name = 'Fqdl'
mongo_type = 'mongodb'
mongo_uri = '{}://{}:{}@{}:{}/{}'.format(mongo_type, mongo_user, mongo_pass, mongo_host, mongo_port, mongo_name)
myclient = pymongo.MongoClient(mongo_uri)
cdb = myclient['Fqdl']

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


class RZ_fanhua_dataset(mongodber):
    key = 'RZ_fanhua_keyword'

    @classmethod
    def update(cls, data):
        con = {'tweet_url': data['tweet_url']}
        cls.update_or_insert(conditions=con, attrs_maps=data)























