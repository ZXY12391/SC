#!/bin/sh


nohup python -u simple_tweets.py >> test.txt 2>&1 &
wait
nohup python -u simple_tweets_temp.py >> test.txt 2>&1 &
wait

#profile 2里面选 xlsx 全部 然后在 1 2 3里面选一些出来 1500

#public p 2 里面选 500 个出来


