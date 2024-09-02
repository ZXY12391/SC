import os

from datetime import timedelta
from kombu import Exchange, Queue
from celery.schedules import crontab

from config.settings import REDIS_DB, REDIS_HOST, REDIS_PORT, REDIS_PASS

# # import
CELERY_IMPORTS = (
    'celery_app.tourist_worker',
    'celery_app.task_workers',
    'celery_app.beat_tasks',
)

# Broker and Backend
BROKER_URL = 'redis://{}:{}/{}'.format(REDIS_HOST, REDIS_PORT, REDIS_DB)
CELERY_RESULT_BACKEND = 'redis://{}:{}/{}'.format(REDIS_HOST, REDIS_PORT, REDIS_DB)

# 时间日期设置
CELERY_TIMEZONE = 'Asia/Shanghai'  # 指定时区，不指定默认为 'UTC'
# CELERY_TIMEZONE = 'America/New_York'  # 美国纽约时区
CELERY_ENABLE_UTC = True

# 每个worker执行了多少次任务后就会死掉，建议数量大一些
CELERYD_MAX_TASKS_PER_CHILD = 100

# 关闭限速
CELERY_DISABLE_RATE_LIMITS = True

# celery worker 每次去BROKER中预取任务的数量
CELERYD_PREFETCH_MULTIPLIER = 2

# 设置默认的队列名称，如果一个消息不符合其他的队列就会放在默认队列里面，如果什么都不设置的话，数据都会发送到默认的队列中
CELERY_DEFAULT_QUEUE = "default"

# # 日志
# base_path = os.path.dirname(os.path.dirname(__file__))
# celery_log_path = os.path.join(base_path, 'logs')
# CELERYD_LOG_FILE = celery_log_path,
# CELERYBEAT_LOG_FILE = celery_log_path,

# 数据格式
CELERY_TASK_SERIALIZER = 'json'
CELERY_RESULT_SERIALIZER = 'json'
CELERY_ACCEPT_CONTENT = ['json']

# 任务过期时间
CELERY_TASK_RESULT_EXPIRES = 60 * 60 * 24
# 非常重要,有些情况下可以防止死锁
CELERYD_FORCE_EXECV = True
# # 规定完成任务的时间，在60s内完成任务，否则执行该任务的worker将被杀死，任务移交给父进程
# CELERYD_TASK_TIME_LIMIT = 60


# 调度
CELERYBEAT_SCHEDULE = {
    'keyword_beat_task': {
        'task': 'celery_app.beat_tasks.keyword_task',
        'schedule': crontab(minute=0, hour=9),  # 每天早上 9 点 00 分执行一次
        'options': {  # 设置Task的一些属性, 参见apply_async的参数
            'queue': 'celery_twitter_beat_task_queue'
        }
    },
    # 'important_user_beat_task': {
    #     'task': 'celery_app.beat_tasks.important_user_task',
    #     'schedule': crontab(hour=7, minute=30),  # crontab(hour=7, minute=30, day_of_week=1)每周一早上7点半执行一次
    #     'args': (600,),  # 任务函数参数
    #     'options': {  # 设置Task的一些属性, 参见apply_async的参数
    #         'queue': 'celery_twitter_beat_task_queue'
    #     }
    # },
    # # 从mongodb任务表T_user_task表读取任务放入redis任务队列（post、follower、following、profile） （1）
    # 'distribute_mongo2redis_beat_task': {
    #     'task': 'celery_app.beat_tasks.distribute_task_mongo2redis_queue',
    #     'schedule': timedelta(minutes=20),  # 每 30 分钟执行一次 hours=12, minutes=60,
    #     'args': (5,),  # 任务函数参数
    #     'options': {  # 设置Task的一些属性, 参见apply_async的参数
    #         'queue': 'celery_twitter_beat_task_queue'
    #     }
    # },
    # # 将redis任务队列twitter:task中的任务（post、follower、following）放入对应的worker中  （2）
    # 'distribute_redis2worker_beat_task': {
    #     'task': 'celery_app.beat_tasks.distribute_task_redis2worker_queue',
    #     'schedule': timedelta(minutes=10),  # 每 40分钟执行一次 hours=12, minutes=60,
    #     'options': {  # 设置Task的一些属性, 参见apply_async的参数
    #         'queue': 'celery_twitter_beat_task_queue'
    #     }
    # },  这里将follower、following、post三个任务的分配一起完成，需要修改为模块化单独分配即修改为T_user_profile这种--已修改，
    # 故将之前的任务分配方案注释
    'user_profile_beat_task': {
        'task': 'celery_app.beat_tasks.user_profile_task',
        'schedule': timedelta(seconds=30),  # 每 5分钟执行一次 hours=12, minutes=60,
        'args': (1,),  # 任务函数参数
        'options': {  # 设置Task的一些属性, 参见apply_async的参数
            'queue': 'celery_twitter_beat_task_queue'
        }
    },
    # post、follower、following任务分配模块化后的代码
    'user_post_beat_task': {
        'task': 'celery_app.beat_tasks.user_post_task',
        'schedule': timedelta(minutes=10),  # 每 5分钟执行一次 hours=12, minutes=60,
        'args': (10,),  # 任务函数参数--这里为每次分配的任务数
        'options': {  # 设置Task的一些属性, 参见apply_async的参数
            'queue': 'celery_twitter_beat_task_queue'
        }
    },
    'user_follower_beat_task': {
        'task': 'celery_app.beat_tasks.user_follower_task',
        'schedule': timedelta(minutes=5),  # 每 5分钟执行一次 hours=12, minutes=60,
        'args': (30,),  # 任务函数参数
        'options': {  # 设置Task的一些属性, 参见apply_async的参数
            'queue': 'celery_twitter_beat_task_queue'
        }
    },
    'user_following_beat_task': {
        'task': 'celery_app.beat_tasks.user_following_task',
        'schedule': timedelta(minutes=5),  # 每 5分钟执行一次 hours=12, minutes=60,
        'args': (30,),  # 任务函数参数
        'options': {  # 设置Task的一些属性, 参见apply_async的参数
            'queue': 'celery_twitter_beat_task_queue'
        }
    },
    # 'tweet_image_beat_task': {
    #     'task': 'celery_app.beat_tasks.tweet_image_task',
    #     'schedule': timedelta(minutes=6),  # 每 5分钟执行一次 hours=12, minutes=60,
    #     'args': (5,),  # 任务函数参数
    #     'options': {  # 设置Task的一些属性, 参见apply_async的参数
    #         'queue': 'celery_twitter_beat_task_queue'
    #     }
    # },
    # 'relationship_avatar_beat_task': {
    #     'task': 'celery_app.beat_tasks.relationship_avatar_task',
    #     'schedule': timedelta(minutes=7),  # 每 5分钟执行一次 hours=12, minutes=60,
    #     'args': (5,),  # 任务函数参数
    #     'options': {  # 设置Task的一些属性, 参见apply_async的参数
    #         'queue': 'celery_twitter_beat_task_queue'
    #     }
    # },
    'tourist_avatar_beat_task': {
        'task': 'celery_app.beat_tasks.tweet_tourist_task',
        'schedule': timedelta(minutes=8),  # 每 5分钟执行一次 hours=12, minutes=60,
        'args': (5,),  # 任务函数参数
        'options': {  # 设置Task的一些属性, 参见apply_async的参数
            'queue': 'celery_twitter_beat_task_queue'
        }
    },
    # 'relationship_topic_beat_task': {
    #     'task': 'celery_app.beat_tasks.relationship_topic_task',
    #     'schedule': timedelta(minutes=8),  # 每 5分钟执行一次 hours=12, minutes=60,
    #     'args': (5,),  # 任务函数参数
    #     'options': {  # 设置Task的一些属性, 参见apply_async的参数
    #         'queue': 'celery_twitter_beat_task_queue'
    #     }
    # },
    'tourist_topic_beat_task': {
        'task': 'celery_app.beat_tasks.tourist_topic_task',
        'schedule': timedelta(minutes=9),  # 每 5分钟执行一次 hours=12, minutes=60,
        'args': (5,),  # 任务函数参数
        'options': {  # 设置Task的一些属性, 参见apply_async的参数
            'queue': 'celery_twitter_beat_task_queue'
        }
    },
    'del_expire_using_spider_beat_task': {
        'task': 'celery_app.beat_tasks.del_expire_using_spider_worker',
        'schedule': timedelta(hours=4),  # 每 5分钟执行一次 hours=12, minutes=60,
        # 'args': (5,),  # 任务函数参数
        'options': {  # 设置Task的一些属性, 参见apply_async的参数
            'queue': 'celery_twitter_beat_task_queue'
        }
    },
    'del_error_account_beat_task': {
        'task': 'celery_app.beat_tasks.del_error_account_task',
        'schedule': timedelta(hours=24),  # 修改为一天执行一次
        'options': {
            'queue': 'celery_twitter_beat_task_queue'
        }
    },
}

# 消息队列
CELERY_QUEUES = (
    # 默认队列
    Queue('default', Exchange('default'), routing_key='default'),

    Queue('celery_twitter_keyword_queue_RZ', exchange=Exchange('keyword', type='direct'),
          routing_key='keyword_routing_key_paper_RZ'),

    Queue('celery_twitter_profile_queue_RZ', exchange=Exchange('profile_paper', type='direct'),
          routing_key='profile_routing_key_RZ'),
    Queue('celery_twitter_post_queue_RZ', exchange=Exchange('post_paper', type='direct'),
          routing_key='post_routing_key_RZ'),
    Queue('celery_twitter_following_queue_RZ', exchange=Exchange('following_paper', type='direct'),
          routing_key='following_routing_key_RZ'),
    Queue('celery_twitter_follower_queue_RZ', exchange=Exchange('follower_paper', type='direct'),
          routing_key='follower_routing_key_RZ'),
    Queue('celery_twitter_post_tourist_queue_RZ', exchange=Exchange('post_tourist_paper', type='direct'),
          routing_key='post_tourist_routing_key_RZ'),

    # Queue('celery_twitter_tweet_image_queue', exchange=Exchange('tweet_image', type='direct'),
    #       routing_key='tweet_image_routing_key'),
    # Queue('celery_twitter_avatar_image_queue', exchange=Exchange('avatar_image', type='direct'),
    #       routing_key='avatar_image_routing_key'),
    Queue('celery_twitter_user_topic_queue_RZ', exchange=Exchange('user_topic', type='direct'),
          routing_key='user_topic_routing_key_RZ'),
    # # 2022-07-12 王森-sc任务
    # Queue('celery_twitter_keyword_queue_sc', exchange=Exchange('keyword_sc', type='direct'),
    #       routing_key='keyword_routing_key_sc'),
    # # 2022-08-15 负面立场数据收集
    # Queue('celery_twitter_post_queue_stance', exchange=Exchange('post', type='direct'), routing_key='post_routing_key_stance'),

)

# 路由
CELERY_ROUTES = {
    'celery_app.task_workers.keyword_worker': {'queue': 'celery_twitter_keyword_queue_RZ',
                                               'routing_key': 'keyword_routing_key_RZ'},

    'celery_app.task_workers.user_profile_worker': {'queue': 'celery_twitter_profile_queue_RZ',
                                                    'routing_key': 'profile_routing_key_RZ'},
    'celery_app.task_workers.user_post_worker': {'queue': 'celery_twitter_post_queue_RZ',
                                                 'routing_key': 'post_routing_key_RZ'},
    'celery_app.task_workers.user_following_worker': {'queue': 'celery_twitter_following_queue_RZ',
                                                      'routing_key': 'following_routing_key_RZ'},
    'celery_app.task_workers.user_follower_worker': {'queue': 'celery_twitter_follower_queue_RZ',
                                                     'routing_key': 'follower_routing_key_RZ'},
    'celery_app.tourist_worker.tourist_worker': {'queue': 'celery_twitter_post_tourist_queue_RZ',
                                                 'routing_key': 'post_tourist_routing_key_RZ'},
    # 'celery_app.task_workers.tweet_image_worker': {'queue': 'celery_twitter_tweet_image_queue',
    #                                                'routing_key': 'tweet_image_routing_key'},
    # 'celery_app.task_workers.avatar_image_worker': {'queue': 'celery_twitter_avatar_image_queue',
    #                                                 'routing_key': 'avatar_image_routing_key'},
    'celery_app.task_workers.user_topic_worker': {'queue': 'celery_twitter_user_topic_queue_RZ',
                                                  'routing_key': 'user_topic_routing_key_RZ'},
    # #  2022-07-12 王森-sc任务
    # 'celery_app.task_workers.keyword_worker_sc': {'queue': 'celery_twitter_keyword_queue_sc',
    #                                            'routing_key': 'keyword_routing_key_sc'},
    # # 2022-08-15 负面立场数据收集
    # 'celery_app.task_workers.user_post_worker_stance': {'queue': 'celery_twitter_post_queue_stance',
    #                                                     'routing_key': 'post_routing_key_stance'},
}


#beat_task指定定时任务执行，定时任务使用apply_async生成任务传到指定的队列queue，
# worker再从队列中读取任务执行，产生结果回到原来的函数中，同一个任务下一个函数里面产生多个具体子任务队列来处理
#队列与队列通过CELERY_QUEUES，CELERY_ROUTES，生成任务是CELERYBEAT_SCHEDULE执行定时任务时，apply_async将任务发出去，任务再到woker时按照函数执行


#celery_twitter_beat_task_queue定时任务队列，如何指定woker，生成任务，但是woker来执行定时任务，route指定了queue是task就在那个队列中处理，但是在apply_async中也可单独指定队列

#keyword_worker 关键字
#user_post_worker
#user_following_worker
#user_follower_worker
#tourist_worker
#user_topic_worker
