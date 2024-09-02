import time

from celery_app.beat_tasks import (keyword_task, tweet_tourist_task, tweet_image_task, tourist_avatar_task,
                                   relationship_avatar_task, tourist_topic_task, relationship_topic_task,
                                   distribute_task_mongo2redis_queue, distribute_task_redis2worker_queue,user_following_worker,user_follower_worker )
import pandas as pd

if __name__ == '__main__':
    path = open('./result2.csv')
    taks_list=[]
    df = pd.read_csv(path)
    for x in df.iterrows():
        # print(x[1])
        for i in range(4):
            if x[1][f'rela{i+1}']==0:
                #print(x[1][f'node{i+1}'])
                taks_list.append(x[1][f'node{i+2}'])
    #fans
    taks_list=list(set(taks_list))
    print(len(taks_list))
    print(len(taks_list))
    f=0
    for x in taks_list:
        print(x)
        task = {
                'user_url': x,
                'user_name': None,
                'important_user': 0,
                'add_time': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
                'update_time': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),

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
        user_following_worker.apply_async(args=[task])








    #单个账号频率
    #多少号
    #好友列表能不能采完
    #二次回复
    #

    # # 关键词
    # keyword_task()
    #
    # # 推文图片爬取
    # tweet_image_task()
    #
    # # 头像爬取
    # tourist_avatar_task()
    # relationship_avatar_task()
    #
    # # 话题爬取
    # tourist_topic_task()
    # relationship_topic_task()
    #
    # 推文点赞者评论者爬取
    # tweet_tourist_task(nums=300000)

    # # 分配任务(mongodb任务分配到redis任务队列中)
    # distribute_task_mongo2redis_queue(nums=10)
    # # 分配任务（redis任务队列中分配到celery worker任务队列中，待celery worker来执行）
    # distribute_task_redis2worker_queue()

    # while True:
    #     # 推文图片爬取
    #     tweet_image_task()
    #
    #     # 头像爬取
    #     tourist_avatar_task()
    #     relationship_avatar_task()
    #
    #     # # 话题爬取
    #     # tourist_topic_task()
    #     # relationship_topic_task()
    #     time.sleep(0.1 * 60 * 60)