import json
from db.redis_db import get_redis_conn, acquire_lock, release_lock


conn = get_redis_conn()


def push_task_to_redis(key, task):
    try:
        pickled_task = json.dumps(task)
        # 使用列表
        conn.lpush(key, pickled_task)
        # 使用集合
        # conn.sadd(key, pickled_task)
        return True
    except Exception as e:
        print(task, ' 插入失败')
        return False


def get_task_from_redis(key):
    # # 加锁--待测试
    # lock_name = key
    # iden = acquire_lock(conn, 'user_task')
    # if not iden:
    #     return

    pickled_task = conn.rpop(key)
    if not pickled_task:
        print('{} 任务队列为空'.format(key))
        return None
    # conn.lpush(key, pickled_task)
    task = json.loads(pickled_task)

    # # 释放锁--待测试
    # release_lock(conn, lock_name, iden)

    return task
