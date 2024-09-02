#!/bin/sh
# celery -A celery_app worker -l info -P gevent -c 10 -Q celery_twitter_post_queue_stance
celery -A celery_app beat -s ./beat_result/result -l info > ./logs/celery_beat_log.log 2>&1 &
celery -A celery_app worker -l info -P gevent -c 3 -Q celery_twitter_beat_task_queue_RZ > ./logs/beat_task.log 2>&1 &
celery -A celery_app worker -l info -P gevent -c 10 -Q celery_twitter_keyword_queue_RZ > ./logs/keyword.log 2>&1 &
celery -A celery_app worker -l info -P gevent -c 2 -Q celery_twitter_profile_queue_RZ > ./logs/profile.log 2>&1 &
celery -A celery_app worker -l info -P gevent -c 10 -Q celery_twitter_post_queue_RZ > ./logs/post.log 2>&1 &
celery -A celery_app worker -l info -P gevent -c 10 -Q celery_twitter_follower_queue_RZ > ./logs/follower.log 2>&1 &
celery -A celery_app worker -l info -P gevent -c 10 -Q celery_twitter_following_queue_RZ > ./logs/following.log 2>&1 &
celery -A celery_app worker -l info -P gevent -c 50 -Q celery_twitter_post_tourist_queue_RZ > ./logs/tourist.log 2>&1 &
celery -A celery_app worker -l info -P gevent -c 10 -Q celery_twitter_user_topic_queue_RZ > ./logs/topic.log 2>&1 &