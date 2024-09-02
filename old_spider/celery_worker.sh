#!/bin/sh
case $1 in
beat) celery -A celery_app worker -l info -P gevent -c 3 -Q celery_twitter_beat_task_queue > ./logs/beat_task.log 2>&1 &;;
keyword) celery -A celery_app worker -l info -P gevent -c 10 -Q celery_twitter_keyword_queue_RZ > ./logs/keyword.log 2>&1 &;;
profile) celery -A celery_app worker -l info -P gevent -c 2 -Q celery_twitter_profile_queue_RZ > ./logs/profile.log 2>&1 &;;
post) celery -A celery_app worker -l info -P gevent -c 10 -Q celery_twitter_post_queue_RZ > ./logs/post.log 2>&1 &;;
following) celery -A celery_app worker -l info -P gevent -c 10 -Q celery_twitter_following_queue_RZ > ./logs/following.log 2>&1 &;;
follower) celery -A celery_app worker -l info -P gevent -c 10 -Q celery_twitter_follower_queue_RZ > ./logs/follower.log 2>&1 &;;
tourist) celery -A celery_app worker -l info -P gevent -c 50 -Q celery_twitter_post_tourist_queue_RZ > ./logs/tourist.log 2>&1 &;;
topic) celery -A celery_app worker -l info -P gevent -c 10 -Q celery_twitter_user_topic_queue_RZ > ./logs/topic.log 2>&1 &;;
enable_events) celery -A celery_app control enable_events;;
monitor) nohup python -u twitter_spider_monitor.py > ./logs/monitor.log 2>&1 &;;
*) echo "require mongo2redis|redis2worker|keyword|profile|post|following|follower|tourist|tweet_image|avatar|topic|monitor|enable_events" ;;
esac

# celery -A celery_app worker -l info -P gevent -c 10 -Q celery_twitter_post_queue_stance

celery -A celery_app worker -l info -P gevent -c 10 -Q celery_twitter_post_queue_paper
celery -A celery_app worker -l info -P gevent -c 10 -Q celery_twitter_follower_queue_paper
celery -A celery_app worker -l info -P gevent -c 10 -Q celery_twitter_following_queue_paper