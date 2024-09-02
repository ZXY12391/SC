"""
1、启动celery的事件功能: celery -A proj control enable_events
2、import app到该文件下
3、完成task_monitor中的Crawler_Status类下面的任务开始，成功，失败的sql功能
4、启动该文件 python twitter_spider_monitor.py
"""
from celery_app import app
from task_monitor.monitor import Crawler_Status, my_monitor

if __name__ == '__main__':
    print('爬虫状态监控启动')
    my_monitor(app)

"""
（flower页面介绍：https://www.jianshu.com/p/4a408657ef76）
"""