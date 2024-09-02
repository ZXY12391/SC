import time, datetime


def now():
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())


def get_timestamp(minutes=0):
    t = (datetime.datetime.now() + datetime.timedelta(minutes=minutes)).timestamp()
    return int(round(t * 1000))


def get_data_nums():
    from db.mongo_db import MongoCommonOper
    print('时间：{}'.format(now()))
    collections = ['T_user_post', 'T_user_relationship', 'T_user_tourist', 'T_keyword_post_info', 'T_user_profile',
                   'F_user_post', 'F_user_relationship', 'F_user_tourist', 'F_keyword_post_info', 'F_user_profile'
                   ]
    nums = 0
    post_nums = 0
    friend_nums = 0
    for collection in collections:
        count = MongoCommonOper.get_collection_data_count(collection)
        if collection in ['T_user_post', 'F_user_post']:
            post_nums += count
        if collection in ['T_user_relationship', 'F_user_relationship']:
            friend_nums += count
        print('{} 表共 {} 条数据.'.format(collection, count))
        nums += count
    print("推文：{}".format(post_nums))
    print("关系：{}".format(friend_nums))
    print('T平台总数据共 {} 条.'.format(nums))


def reset_celery_task_number():
    """
    重置爬虫状态表中的任务数量
    :return:
    """
    from db.mongo_db import MongoCommonOper
    tasks = ['F_tourist_spider', 'F_relationship_spider', 'F_post_spider', 'F_profile_spider', 'F_keyword_search_spider']
    for task_name in tasks:
        MongoCommonOper.insert_or_update_one_data('crawler_status', {'name': task_name}, {'crawl_interval': 24})


def get_confusion_info_from_mongo():
    from db.mongo_db import MongoCommonOper
    res = MongoCommonOper.query_datas_by_condition_limit('T_user_tourist', {}, 100000)
    users = set()
    for r in res:
        url = r.get('user_url')
        screen_id = r.get('screen_id')
        if url not in users and screen_id:
            users.add((url, screen_id))
    users = list(users)
    print(users)
    with open('../confusion/T_confusion_info.txt', 'w', encoding='utf-8') as f:
        for user in users:
            f.write(user[0] + ',' + user[1] + '\n')


def add_normal_user_tasks_from_relationship():
    from bloom_filter.bloom_filter import bloom_filter_user
    from db.mongo_db import MongoCommonOper, MongoUserTaskOper
    existed_user_tasks = MongoCommonOper.query_datas_by_condition('T_user_task', {})
    existed_user_tasks = [t.get('user_url') for t in existed_user_tasks]
    print('已有任务加载完毕...共{}条'.format(len(existed_user_tasks)))

    users = MongoCommonOper.query_datas_by_condition_limit('T_user_relationship', {}, 500000)
    print('新增任务查询成功...共{}条'.format(len(users)))
    add_tasks = []
    for index, user_task in enumerate(users):
        if user_task.get('relationship_user_url') in existed_user_tasks:
            continue
        # 存入用户任务表
        task = {
            'user_url': user_task.get('relationship_user_url'),
            'user_name': user_task.get('user_name'),
            'important_user': 0,
            'add_time': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
            'update_time': None,

            'follower_task': 1,
            'following_task': 1,
            'post_task': 1,
            'profile_task': 1,

            'post_praise_task': 1,
            'post_repost_task': 1,
            'post_comment_task': 1,
            'post_reply_task': 1,

            'post_crawl_status': 0,
            'following_crawl_status': 0,
            'follower_crawl_status': 0,
            'profile_crawl_status': 0,
            'tourist_crawl_status': 0,

            'user_tag': None,
        }
        add_tasks.append(task)
        existed_user_tasks.append(user_task.get('relationship_user_url'))
        if index % 500 == 0:
            print(index)
            MongoCommonOper.insert_many_data('T_user_task', add_tasks)
            print('新增{}条任务成功!'.format(len(add_tasks)))
            if len(existed_user_tasks) > 560000:
                break
            add_tasks = []


def add_normal_user_tasks_from_tourist():
    from bloom_filter.bloom_filter import bloom_filter_user
    from db.mongo_db import MongoCommonOper, MongoUserTaskOper
    existed_user_tasks = MongoCommonOper.query_datas_by_condition('T_user_task', {})
    existed_user_tasks = [t.get('user_url') for t in existed_user_tasks]
    print('已有任务加载完毕...共{}条'.format(len(existed_user_tasks)))

    users = MongoCommonOper.query_datas_by_condition_limit('T_user_tourist', {}, 1000000)
    print('新增任务查询成功...共{}条'.format(len(users)))
    add_tasks = []
    for index, user_task in enumerate(users):
        if user_task.get('user_url') in existed_user_tasks:
            continue
        # 存入用户任务表
        task = {
            'user_url': user_task.get('user_url'),
            'user_name': user_task.get('user_name'),
            'important_user': 0,
            'add_time': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
            'update_time': None,

            'follower_task': 1,
            'following_task': 1,
            'post_task': 1,
            'profile_task': 1,

            'post_praise_task': 1,
            'post_repost_task': 1,
            'post_comment_task': 1,
            'post_reply_task': 1,

            'post_crawl_status': 0,
            'following_crawl_status': 0,
            'follower_crawl_status': 0,
            'profile_crawl_status': 0,
            'tourist_crawl_status': 0,

            'user_tag': None,
        }
        add_tasks.append(task)
        existed_user_tasks.append(user_task.get('user_url'))
        if index % 500 == 0:
            print(index)
            MongoCommonOper.insert_many_data('T_user_task', add_tasks)
            print('新增{}条任务成功!'.format(len(add_tasks)))
            if len(existed_user_tasks) > 560000:
                break
            add_tasks = []


def get_current_timetamp():
    res = int(time.time())
    r = 40 * 24 * 60 * 60
    print(res)
    return int(time.time())


def timestamp2datetime(timestamp):
    # 转换成localtime
    time_local = time.localtime(timestamp)
    # 转换成新的时间格式(2016-05-05 20:28:54)
    dt = time.strftime("%Y-%m-%d %H:%M:%S", time_local)
    return dt


def datetime2timestamp(dt):
    # mysql获取时间戳unix_timestamp(now())
    # 转换成时间数组
    timeArray = time.strptime(dt, "%Y-%m-%d %H:%M:%S")
    # 转换成时间戳
    timestamp = time.mktime(timeArray)
    return int(timestamp)


def static_important_user_relationship():
    """
    将es中的重复关系去重
    :return:
    """
    import random
    from db.mongo_db import MongoUserTaskOper
    from db.es_db import es_query, es_delete
    important_users = MongoUserTaskOper.get_user_tasks_by_condition({"important_user": 1}, nums=600)
    print("重点用户数量:", len(important_users))
    random.shuffle(important_users)
    for idx, user in enumerate(important_users):
        index = 't_user_relationship_3'
        condition = [
            {"term": {"user_url": user.get("user_url")}},
            # {"term": {"tourist_crawl_status": 1}},

            # {"term": {"is_useful": "0"}},
            # {"term": {"is_useful": "0"}},
            # {"match": {"author_url": {"query": "https://twitter.com/RinhoaSakura", "operator": "and"}}},
            # {"match": {"comment": {"query": 1, "operator": "and"}}},
        ]
        size = 100000
        result = es_query(index, condition, size)
        print("*" * 20, idx, "*" * 20)
        print(user.get("user_url"))
        print("following:{}".format(len([1 for r in result if r.get("relationship_type") == 0])))
        print("follower:{}".format(len([1 for r in result if r.get("relationship_type") == 1])))

        tmp_list = set()
        _ids = []
        for r in result:
            tmp = r.get("user_url") + r.get("relationship_user_url") + "@@@@@@@@@@" + str(r.get("relationship_type"))
            if tmp in tmp_list:
                _ids.append(r)
                continue
            tmp_list.add(tmp)
        print("去重后following:{}".format(len([1 for r in tmp_list if r.split("@@@@@@@@@@")[-1] == "0"])))
        print("去重后follower:{}".format(len([1 for r in tmp_list if r.split("@@@@@@@@@@")[-1] == "1"])))
        for id in _ids:
            es_delete(index, id.get("_id"))
        print("*" * 44)


def get_nums():
    from db.mongo_db import MongoCommonOper
    print('时间：{}'.format(now()))
    collections = ['twibot20_profile', 'twibot20_post', 'twibot20_relationship', 'twibot20_tourist']
    for collection in collections:
        count = MongoCommonOper.get_collection_data_count(collection)
        print('{} 表共 {} 条数据.'.format(collection, count))


if __name__ == '__main__':
    get_nums()
    # get_data_nums()
    # static_important_user_relationship()
    # dt = get_current_timetamp()
    # print(dt)
    # current_datetime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    # print(current_datetime)
    # # res = datetime2timestamp(current_datetime)
    # temp = int((dt - 1633833546) / (24 * 60 * 60))
    # print(temp)

    # print(timestamp2datetime(1633772857))
    # print(datetime2timestamp('2021-11-26 21:00:54'))
    # get_current_timetamp()

    # reset_celery_task_number()
    # get_confusion_info_from_mongo()
    # add_normal_user_tasks_from_tourist()

"""
在001和10.36服务器上部署post的情况下（协程都开的10）：
# 修改代码加快post follower following profile 任务分配（每种任务单独分配）频率的情况下的数据，follower、following分配频率为35，协程为40，profile为15，关系不去重
时间：2021-12-03 10:40:19
T_user_post 表共 118574931 条数据.
T_user_relationship 表共 875364253 条数据.
T_user_tourist 表共 113898613 条数据.
T_keyword_post_info 表共 932989 条数据.
T_user_profile 表共 524178 条数据.
总数据共 1109294964 条.

时间：2021-12-15 15:14:57
T_user_post 表共 122405477 条数据.
T_user_relationship 表共 876131766 条数据.
T_user_tourist 表共 214013532 条数据.
T_keyword_post_info 表共 945041 条数据.
T_user_profile 表共 524190 条数据.
总数据共 1214020006 条.
"""
