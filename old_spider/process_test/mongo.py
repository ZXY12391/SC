mongo_host = '10.0.12.2'
mongo_port = '27017'
mongo_user = 'fquser'
mongo_pass = 'VPS123db!'
mongo_name = 'Fqdl'
mongo_type = 'mongodb'
mongo_uri = '{}://{}:{}@{}:{}/{}'.format(mongo_type, mongo_user, mongo_pass, mongo_host, mongo_port, mongo_name)

import pymongo

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
    def basic_find_sort(cls, condition, attrs,sort_con):
        col = cdb[cls.key]
        res = col.find(condition, attrs)
        return list(res)



class keyword_post_task_oper(mongodber):
    key='weibo_data_key_post'

    @classmethod
    def update(cls, data):
        con = {'id': data['id']}
        cls.update_or_insert(conditions=con, attrs_maps=data)

    @classmethod
    def get_datas(cls):
        datas = cls.basic_find({}, attrs={})
        # print(datas[0].keys())
        # print(datas[1].keys())
        # datas = list(set(datas))
        return datas

    @classmethod
    def get_datas_ids(cls):
        datas=cls.basic_find({},attrs=['id'])
        ids=[_['id'] for _ in datas]
        return ids

    @classmethod
    def get_datas_ids_flag_3_post_task(cls):
        datas = cls.basic_find_sort({'status':0,'flag':1}, attrs=['id','count'],sort_con='count')
        # ids = [_['id'] for _ in datas]
        return datas

    @classmethod
    def get_post_task(cls):
        datas=cls.basic_find({'status':0},attrs=['id'])
        ids = [_['id'] for _ in datas]
        return

    @classmethod
    def get_post_task_flag_0(cls):
        datas = cls.basic_find({'status': 0,'flag':3,}, attrs=['id'])
        ids = [_['id'] for _ in datas]
        return ids

    @classmethod
    def get_finish_post_task(cls):
        datas = cls.basic_find({'flag':3,'status': 1}, attrs=['id'])
        ids = [_['id'] for _ in datas]
        return ids

    @classmethod
    def get_finish_intertaction_post_task(cls):
        datas = cls.basic_find({'flag':0,'status': 2}, attrs=['id'])
        ids = [_['id'] for _ in datas]
        return ids

    @classmethod
    def get_intertaction_task_test(cls,id):
        datas = cls.basic_find({'id': id}, attrs={})
        return datas
        pass

class action_transformer_test_task_oper(mongodber):
    key='twibot20'

    @classmethod
    def update(cls, data):
        con = {'id': data['id']}
        cls.update_or_insert(conditions=con, attrs_maps=data)

    @classmethod
    def get_datas(cls):
        datas = cls.basic_find({}, attrs=['ID','label'])
        # print(datas[0].keys())
        # print(datas[1].keys())
        # datas = list(set(datas))
        return list(datas)

    @classmethod
    def get_datas_by_id(cls,id):
        datas = cls.basic_find({'ID': id}, attrs={})
        return datas


class action_transformer_test_task_oper_post(mongodber):
    key = 'twibot20_post'

    @classmethod
    def select_datas(cls,id):
        datas = cls.basic_find({'screen_id':id}, attrs={})
        # print(datas[0].keys())
        # print(datas[1].keys())
        # datas = list(set(datas))
        return datas

class name_get(mongodber):
    key = 'twibot20_relationship'
    @classmethod
    def select_datas(cls,id):
        datas = cls.basic_find({'user_url':id}, attrs={})
        # print(datas[0].keys())
        # print(datas[1].keys())
        # datas = list(set(datas))
        return datas




if __name__=='__main__':
    a=name_get.select_datas('https://twitter.com/sunnyhowlader5')
    print(a)