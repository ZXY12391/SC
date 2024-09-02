# @Author  : zg0dsss 
# @Time    : 2021/11/18 15:59
# @Function:

from subprocess import call

# 获取当前环境的python解释器路径
import sys
import os

py_path = sys.executable
file_path = os.path.abspath(os.curdir)
# print(file_path)
# 在shell中执行字符串所代表的命令
tasks_dict = {
    'beat': 'celery -A celery_app worker -l info -P gevent -c 10 -Q celery_twitter_beat_task_queue > ./logs/beat_task.log 2>&1 &',
    'keyword': 'celery -A celery_app worker -l info -P gevent -c 10 -Q celery_twitter_keyword_queue > ./logs/keyword.log 2>&1 &',
    'profile': 'celery -A celery_app worker -l info -P gevent -c 40 -Q celery_twitter_profile_queue > ./logs/profile.log 2>&1 &',
    'post': 'celery -A celery_app worker -l info -P gevent -c 10 -Q celery_twitter_post_queue > ./logs/post.log 2>&1 &',
    'following': 'celery -A celery_app worker -l info -P gevent -c 10 -Q celery_twitter_following_queue > ./logs/following.log 2>&1 &',
    'follower': 'celery -A celery_app worker -l info -P gevent -c 10 -Q celery_twitter_follower_queue > ./logs/follower.log 2>&1 &',
    'tourist': 'celery -A celery_app worker -l info -P gevent -c 30 -Q celery_twitter_post_tourist_queue > ./logs/tourist.log 2>&1 &',
    'tweet_image': 'celery -A celery_app worker -l info -P gevent -c 5 -Q celery_twitter_tweet_image_queue > ./logs/tweet_image.log 2>&1 &',
    'avatar': 'celery -A celery_app worker -l info -P gevent -c 10 -Q celery_twitter_avatar_image_queue > ./logs/avatar.log 2>&1 &',
    'topic': 'celery -A celery_app worker -l info -P gevent -c 10 -Q celery_twitter_user_topic_queue > ./logs/topic.log 2>&1 &',
    'enable_events': 'celery -A celery_app control enable_events',
    'monitor': 'nohup python -u twitter_spider_monitor.py > ./logs/monitor.log 2>&1 &'}


def run():
    tasks = ['beat', 'keyword', 'profile', 'post', 'following', 'follower', 'tourist', 'enable_events', 'monitor']
    task = input('Task name:')
    # 调用shell环境, 执行命令
    # cmd = """cd {};
    #         {}
    #         """.format(file_path, task)
    cmd = "{}".format(tasks_dict.get(task))
    call(cmd, shell=True)


if __name__ == '__main__':
    run()
    # task = input('Task name:')
    # print(task)