from db.mongo_db import MongoCrawlerStatusOper
from task_monitor.settings import task_2_spider
from task_monitor.utils import now


class Crawler_Status:
    """
    状态表里面的crawl_status字段，有任务开始就加一，任务结束就减一，使用$inc来实现，
    当crwal_status》0说明有任务，爬虫运行中，当等于0，说明任务结束
    """

    @classmethod
    def start(cls, spider_name):
        """
        爬虫开始启动，修改状态表
        :return:
        """
        MongoCrawlerStatusOper.inc_crawler_status(spider_name)
        MongoCrawlerStatusOper.update_crawl_status_info(spider_name, {'update_time': now(), 'exception': ''})

    @classmethod
    def success(cls, spider_name):
        """
        爬虫结束，修改状态表
        :param spider_name:
        :return:
        """
        MongoCrawlerStatusOper.sub_crawler_status(spider_name)
        MongoCrawlerStatusOper.update_crawl_status_info(spider_name, {'update_time': now(), 'exception': ''})

    @classmethod
    def failed(cls, spider_name, exception):
        """
        爬虫结束，修改状态表
        :param spider_name:
        :return:
        """
        MongoCrawlerStatusOper.sub_crawler_status(spider_name)
        MongoCrawlerStatusOper.update_crawl_status_info(spider_name, {'update_time': now(), 'exception': exception})


def my_monitor(app):
    """
    监控函数
    :param app:
    :return:
    """
    # uuid_task_map用来存储 任务id与任务队列名称的映射，因为任务结束后得到的只有任务id，没有任务名称，所以得记录
    uuid_task_map = {}
    state = app.events.State()

    def recv_tasks(event):
        # 接收到任务后回调函数，
        state.event(event)
        task = state.tasks.get(event['uuid'])
        # print('TASK: %s[%s] %s' % (task.name, task.uuid, task.info(),))
        short_uuid = task.uuid.split('-')[0]
        if task.name in task_2_spider.keys():
            Crawler_Status.start(task_2_spider[task.name])
            uuid_task_map[short_uuid] = task_2_spider[task.name]
            print('{}接收到任务:{}'.format(task.name, task.uuid))

    def success_tasks(event):
        # 任务执行成功后回调函数
        state.event(event)
        task = state.tasks.get(event['uuid'])
        short_uuid = task.uuid.split('-')[0]
        if short_uuid in uuid_task_map.keys():
            Crawler_Status.success(uuid_task_map[short_uuid])
            uuid_task_map.pop(short_uuid)
            print('{}任务执行成功'.format(task.uuid))

    def failed_tasks(event):
        # 任务执行失败后回调函数
        state.event(event)
        task = state.tasks.get(event['uuid'])
        short_uuid = task.uuid.split('-')[0]
        if short_uuid in uuid_task_map.keys():
            Crawler_Status.failed(uuid_task_map[short_uuid], task.exception)
            uuid_task_map.pop(short_uuid)
            print('{}任务执行失败'.format(task.uuid))

    with app.connection() as connection:
        recv = app.events.Receiver(connection, handlers={
            'task-failed': failed_tasks,
            'task-received': recv_tasks,
            'task-succeeded': success_tasks,
        })
        recv.capture(limit=None, timeout=None, wakeup=True)

