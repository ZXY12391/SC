# -*- coding: utf-8 -*-
# @Time    : 2021-11-15 16:17
# @Author  : lldzyshwjx
import time
import jsonpath

from collections.abc import Iterable
from db.mongo_db import MongoUserTweetTouristOper,MongoUserTaskOper

from bloom_filter.bloom_filter import bloom_filter_post


def check_contain_chinese(check_str):
    for ch in check_str:
        if u'\u4e00' <= ch <= u'\u9fff':
            return True
    return False

def insert_object_user(task):
    seg = ['location', 'introduction', 'user_name', 'user_url']
    try:
        user_url = task[seg[3]]
        if MongoUserTaskOper.is_exit_user_task(user_url=user_url):
            return ''
        user_task = {
            'user_url': user_url,
            'user_name': None,
            'important_user': 0,
            'add_time': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
            'update_time': None,

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
        # if 'Taiwan' in task[seg[0]]:
        #     MongoUserTaskOper.insert_user_task(user_task)
        # elif check_contain_chinese(task[seg[1]]) or check_contain_chinese(task[seg[2]]):
        MongoUserTaskOper.insert_user_task(user_task)
    except:
        return ''

def parser_praise_or_retweet(target_url, tweet_url, crawl_type, content, proxies):
    # 此时content为一个字典，这里递归查找entries键的所有子元素（以列表形式返回），[]为子元素操作符，*表示任意元素
    # 找不到则entries为False
    res = 0
    persons = jsonpath.jsonpath(content, '$..entries[*]')
    # 当没有点赞者、转发者时返回的是
    if not persons:
        print('tweet {} 无人 {} '.format(tweet_url, crawl_type))
        return 0
    if not isinstance(persons, Iterable):
        return 0
    datas = []
    for index, person in enumerate(persons):
        try:
            screen_id = person['content']['itemContent']['user']['rest_id']
            person = person['content']['itemContent']['user']['legacy']
        except KeyError:
            continue
        person_url = 'https://twitter.com/' + person['screen_name']
        # 由于转发者和点赞者都只有能爬取一页信息，故这里不需要判断其是否已爬取
        try:
            person_img_url = person['profile_image_url_https']
        except KeyError:
            person_img_url = ''

        try:
            website = person['entities']['url']['urls'][0]['expanded_url']
        except KeyError:
            website = ''

        register_time = time.strptime(person['created_at'], "%a %b %d %H:%M:%S +0000 %Y")
        register_time = time.strftime("%Y-%m-%d %H:%M:%S", register_time)
        data = {}
        if crawl_type == 'praise':
            data['praise'] = 1
            data['retweet'] = 0
        else:
            data['retweet'] = 1
            data['praise'] = 0
        data['comment'] = 0
        data['comment_content'] = None
        data['comment_time'] = None
        data['reply'] = 0
        data['reply_content'] = None
        data['reply_time'] = None
        data['tweet_url'] = tweet_url
        data['author_url'] = target_url
        data['user_url'] = person_url  # 转发者或者点赞者
        data['screen_name'] = person['screen_name']
        data['screen_id'] = screen_id
        data['user_name'] = person['name']
        data['avatar_url'] = person_img_url
        data['register_time'] = register_time
        # data['avatar_binary'] = None
        # data['avatar_bs64encode'] = None

        data['introduction'] = person['description']
        data['location'] = person['location']
        data['website'] = website
        data['following_count'] = person['friends_count']
        data['followers_count'] = person['followers_count']
        data['tweets_count'] = person['statuses_count']
        data['media_tweets_count'] = person['media_count']
        data['favourites_tweets_count'] = person['favourites_count']
        data['is_protected'] = 1 if person['protected'] else 0
        data['fetch_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        # 新增，mysql没有
        data['banner_url'] = person.get('profile_banner_url')  # 没有背景图片的用户无该字段,get没有就返回None
        # data['banner_binary'] = None  # 没有背景图片的用户无该字段
        data['is_verified'] = 1 if person['verified'] else 0  # 是否认证，类似于机构认证那种
        # data['topic'] = None
        data['is_default_profile'] = 1 if person['default_profile'] else 0
        data['is_default_profile_image'] = 1 if person['default_profile_image'] else 0
        # data['user_tag'] = user_tag
        datas.append(data)
        insert_object_user(data)
        # if crawl_type == 'praise':
        #     MongoUserTweetTouristOper.insert_or_update_tweet_praise(data)
        # else:
        #     MongoUserTweetTouristOper.insert_or_update_tweet_retweet(data)
        res += 1
    if datas:
        if crawl_type == 'praise':
            MongoUserTweetTouristOper.insert_many_praise_data(datas)
        else:
            MongoUserTweetTouristOper.insert_many_retweet_data(datas)
    print('抓取 {} 信息{}条成功'.format(crawl_type, res))


def parser_comment_reply(target_url, tweet_url, screen_id, content, crawl_type, proxies):
    res = 0
    tweetId = tweet_url.split('/')[-1]
    # 该页面users中的，statuses_count为用户的推文数量
    comments = jsonpath.jsonpath(content, '$..tweets[*]')
    datas = []
    for index, comment in enumerate(comments):
        try:
            # 'in_reply_to_user_id_str'：表示该评论，评论的推文id
            # 'in_reply_to_status_id_str'：表示该评论，评论的推文的作者id
            # 'id_str'：该评论内容的id
            user_type = 'user_id_str'  # 其他人评论推文
            # 如果为目标推文则跳过---因为该评论页面将目标推文的信息也存好的，且该目标推文的记录无in_reply_to_status_id_str字段
            if comment['id_str'] == tweetId:
                continue
            # 目标用户回复自己的情况也跳过
            if comment['user_id_str'] == screen_id and comment['in_reply_to_user_id_str'] == screen_id:
                continue
            # 如果其它用户评论回复的推文不为目标推文，也跳过
            if comment['user_id_str'] != screen_id and comment['in_reply_to_status_id_str'] != tweetId:
                continue
            # 目标用户回复其它用户的情况---即该回复的对象需要记录，说明目标用户与该用户有交互行为
            if comment['user_id_str'] == screen_id and comment['in_reply_to_user_id_str'] != screen_id:
                user_type = 'in_reply_to_user_id_str'  # 即该类型的评论定义为：回复

            # # 如果评论的回复数大于1则需要进一步爬取完整回复信息（评论数为1的已经在一级评论页面显示），这里不是指show replies
            # if comment['reply_count'] > 1:
            #     print('该评论又有多个评论')
            #     print('如果需要爬取所有的内容')

            # 评论信息的url、时间
            cmt_url = 'https://twitter.com/' + comment['user_id_str'] + '/status/' + comment['id_str']
            cmt_time = time.strptime(comment['created_at'], "%a %b %d %H:%M:%S +0000 %Y")
            cmt_time = time.strftime("%Y-%m-%d %H:%M:%S", cmt_time)

            # 评论者（user_id_str）或被目标用户回复者信息（in_reply_to_user_id_str）
            person = jsonpath.jsonpath(content, '$..users.{}'.format(comment[user_type]))
            if person:
                person = person[0]
                person_name = person.get('name')
                person_url = 'https://twitter.com/' + person.get('screen_name')
            else:
                person_name = ''
                person_url = ''
                continue
            # 判断该数据是否已爬取---使用用户言论布隆过滤器来判断
            if bloom_filter_post.is_exist(cmt_url):
                continue

            try:
                person_img_url = person['profile_image_url_https']
                # pic_task = {}
                # pic_task['table'] = 'post_tourist_info'
                # pic_task['url'] = person_img_url
                # push_picture_task(json.dumps(pic_task))
                # resp = crawl_twitter(person_img_url, proxies)
                # person_img = resp.content  # 得到头像的二进制数据
            except KeyError:
                person_img_url = ''
                # person_img = ''

            try:
                website = person['entities']['url']['urls'][0]['expanded_url']
            except KeyError:
                website = ''

            register_time = time.strptime(person['created_at'], "%a %b %d %H:%M:%S +0000 %Y")
            register_time = time.strftime("%Y-%m-%d %H:%M:%S", register_time)

            data = {}
            if user_type == 'user_id_str':
                data['comment'] = 1
                data['comment_time'] = cmt_time
                data['comment_content'] = comment['full_text']
                data['reply'] = 0
                data['reply_time'] = None
                data['reply_content'] = None
            else:
                data['reply'] = 1
                data['reply_time'] = cmt_time
                data['reply_content'] = comment['full_text']
                data['comment'] = 0
                data['comment_time'] = None
                data['comment_content'] = None

            data['retweet'] = 0
            data['praise'] = 0
            data['tweet_url'] = tweet_url
            data['author_url'] = target_url
            data['user_url'] = person_url  # 评论者或目标用户回复的那个人
            data['screen_name'] = person.get('screen_name')
            data['screen_id'] = comment['user_id_str']
            data['user_name'] = person.get('name')
            data['avatar_url'] = person_img_url
            data['register_time'] = register_time
            # data['avatar_binary'] = None
            # data['avatar_bs64encode'] = None
            data['introduction'] = person.get('description')
            data['location'] = person.get('location')
            data['website'] = website
            data['following_count'] = person.get('friends_count')
            data['followers_count'] = person.get('followers_count')
            data['tweets_count'] = person.get('statuses_count')
            data['media_tweets_count'] = person.get('media_count')
            data['favourites_tweets_count'] = person.get('favourites_count')
            data['is_protected'] = 1 if person.get('protected') else 0
            data['fetch_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            # 新增，mysql没有
            data['banner_url'] = person.get('profile_banner_url')  # 没有背景图片的用户无该字段,get没有就返回None
            # data['banner_binary'] = None  # 没有背景图片的用户无该字段
            data['is_verified'] = 1 if person.get('verified') else 0  # 是否认证，类似于机构认证那种
            # data['topic'] = None
            data['is_default_profile'] = 1 if person.get('default_profile') else 0
            data['is_default_profile_image'] = 1 if person.get('default_profile_image') else 0
            # data['user_tag'] = user_tag
            datas.append(data)
            insert_object_user(data)
            # # 存数据
            # if user_type == 'user_id_str':
            #     # 评论者
            #     MongoUserTweetTouristOper.insert_or_update_tweet_comment(data)
            # else:
            #     # 目标用户回复那个人
            #     MongoUserTweetTouristOper.insert_or_update_tweet_reply(data)

            # 存布隆过滤器
            bloom_filter_post.save(cmt_url)
            res += 1
        except Exception as e:
            # 异常:'in_reply_to_status_id_str'无该字段---这种情况为该推文为回复别人的推文，然后该页面返回的数据中包含别人的推文（该推文无该字段）
            # print('异常:{}'.format(e))
            continue
    if datas:
        MongoUserTweetTouristOper.insert_many_comment_reply_data(datas)
    print('{} 当前页面{}信息爬取{}条成功'.format(tweet_url, crawl_type, res))
    return res