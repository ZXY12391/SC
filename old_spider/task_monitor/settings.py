task_2_spider = {
    'celery_app.task_workers.user_following_worker': 'T_relationship_spider',
    'celery_app.task_workers.user_follower_worker': 'T_relationship_spider',
    'celery_app.task_workers.user_profile_worker': 'T_profile_spider',
    'celery_app.task_workers.user_post_worker': 'T_post_spider',
    'celery_app.tourist_worker.tourist_worker': 'T_tourist_spider',
    'celery_app.task_workers.keyword_worker': 'T_keyword_search_spider',
}
# celery_app.task_workers.keyword_worker
#OK 看完
