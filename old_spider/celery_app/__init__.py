from celery import Celery, platforms

platforms.C_FORCE_ROOT = True

app = Celery('cert')  # 创建 Celery 实例
app.config_from_object('celery_app.celery_config')  # 通过 Celery 实例加载配置模块


# 启动worker监听进程：
# celery -A celery_app worker -l info -P eventlet -c 100
# --pool=solo 参数指定后只会跑一个线程
# 启动定时任务
# celery -A celery_app beat -s ./beat_result/result
