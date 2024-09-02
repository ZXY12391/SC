#!/bin/sh
celery -A celery_app beat -s ./beat_result/result -l info > ./logs/celery_beat_log.log 2>&1 &;;