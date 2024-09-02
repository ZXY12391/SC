import base64
import time
import datetime
import jsonpath
import json
import gridfs
from collections.abc import Iterable
from bson import binary
from db.mongo_db import MongoCommonOper, MongoUserTweetTouristOper, MongoUserInfoOper, MongoUserTweetOper, MongoUserRelationshipOper, MongoKeywordTweetOper, MongoUserTaskOper
from db.dao import KeywordsOper, UserRelationshipOper, UserTweetOper
from db.models import PostInfoSearchResults, UserInfoSearchResults
from utils.image_utils import crawl_avatar_image
from parsers.store_data import (store_keyword_tweet_info, store_keyword_user_info, store_user_info,
                                store_relationship_info, store_user_tweet_info, )
from crawlers.downloader import download_without_cookies
from logger.log import parser_logger, download_logger
from celery_app.tourist_worker import tourist_worker

from bloom_filter.bloom_filter import bloom_filter_keyword_post, bloom_filter_post, bloom_filter_following, bloom_filter_follower

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
        # if 'Taiwan' in task[seg[0]] or '台湾' in task[seg[0]] :
        #     MongoUserTaskOper.insert_user_task(user_task)
        # elif check_contain_chinese(task[seg[1]]) or check_contain_chinese(task[seg[2]]):
        # MongoUserTaskOper.insert_user_task(user_task)
    except:
        return ''


# 解析用户基本（档案）信息---需要进行持续更新
def parser_user_info(user_url, user_tag, important_user, content, proxies):
    try:
        user = content['data']['user']['legacy']
    except Exception as e:
        return 0, 0, 0
    screen_id = content['data']['user']['rest_id']
    register_time = time.strptime(user['created_at'], "%a %b %d %H:%M:%S +0000 %Y")
    register_time = time.strftime("%Y-%m-%d %H:%M:%S", register_time)
    image_url = user['profile_image_url_https'].replace('normal', '400x400')
    try:
        # default_profile_normal.png为平台默认的图片，即用户没有设置头像
        resp = download_without_cookies(image_url, proxies)
        if resp is None:
            avatar_data = crawl_avatar_image(user_url, image_url, 'profile')
            avatar_binary = avatar_data.get('avatar_binary')
            avatar_bs64encode = avatar_data.get('avatar_bs64encode')
        else:
            # 二进制文件
            avatar_binary = resp.content
            # base64编码可在离线环境下展示，方便前端展示（可直接存链接或者base64编码后的图片）
            encodestr = base64.b64encode(resp.content)  # 得到 byte 编码的数据
            avatar_bs64encode = 'data:image/jpeg;base64,%s' % encodestr.decode("utf-8")
    except Exception as e:
        avatar_bs64encode = None
        avatar_binary = None

    if user.get('entities').get('url'):
        website = user['entities']['url']['urls'][0]
        website = website.get('expanded_url', '') if website.get('expanded_url') else website.get('url', '')
    else:
        website = ''
    user_data = {}
    user_data['user_url'] = user_url
    user_data['screen_id'] = screen_id
    user_data['screen_name'] = user['screen_name']
    user_data['user_name'] = user['name']
    user_data['avatar_url'] = image_url
    user_data['register_time'] = register_time
    user_data['avatar_binary'] = avatar_binary  # 在datagrip中二进制<failed to load> org.bson.types.Binary， Navicat中没问题
    user_data['avatar_bs64encode'] = avatar_bs64encode
    user_data['introduction'] = user['description']
    user_data['location'] = user['location']
    user_data['website'] = website
    user_data['following_count'] = user['friends_count']
    user_data['followers_count'] = user['followers_count']
    user_data['tweets_count'] = user['statuses_count']
    user_data['media_tweets_count'] = user['media_count']
    user_data['favourites_tweets_count'] = user['favourites_count']
    user_data['is_protected'] = 1 if user['protected'] else 0
    user_data['fetch_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    # 新增的属性--mysql没有
    user_data['banner_url'] = user.get('profile_banner_url')  # 没有背景图片的用户无该字段,get没有就返回None
    user_data['banner_binary'] = None  # 没有背景图片的用户无该字段
    user_data['is_verified'] = 1 if user['verified'] else 0  # 是否认证，类似于机构认证那种
    # user_data['topic'] = []  # 这里不能赋值为[]。否则在对用户信息进行更新时，会更新之前的topic信息为[]--topic在爬取基本信息后爬
    user_data['is_default_profile'] = 1 if user['default_profile'] else 0
    user_data['is_default_profile_image'] = 1 if user['default_profile_image'] else 0
    # 添加用户标签
    user_data['user_tag'] = user_tag
    # 生日
    try:
        birthday = content['data']['user']['legacy_extended_profile'].get('birthdate')
        user_data['birthday'] = {'year': birthday.get('year', ''), 'month': birthday.get('month', ''),
                                 'day': birthday.get('day', '')} if birthday else None
    except Exception as e:
        user_data['birthday'] = None
    # 添加是否为机器人字段
    user_data['is_bot'] = None
    # # 新增是否为重点用户字段
    # user_data['important_user'] = important_user
    # # 存储mysql
    # store_user_info(user_data)

    # 存mongodb
    MongoUserInfoOper.insert_or_update_user_basic_info(user_data)
    return screen_id, user['name'], image_url


def parser_user_topic(screen_id, content, crawl_type):
    topics = jsonpath.jsonpath(content, '$..entries[*]')
    if not topics:
        print('用户 {} 开启隐私保护,topic信息抓取失败'.format(screen_id))
        if crawl_type == 'profile':
            MongoUserInfoOper.insert_or_update_user_topic_info(screen_id, {'topic': []})
        if crawl_type == 'relationship':
            MongoUserRelationshipOper.insert_or_update_topic(screen_id, {'topic': []})
        if crawl_type == 'tourist':
            MongoUserTweetTouristOper.insert_or_update_topic(screen_id, {'topic': []})
        return 1
    user_topics = []
    for topic in topics:
        if 'messagePrompt' in topic.get('entryId') or 'TopicsModule' in topic.get('entryId'):
            continue
        topic = topic.get('content').get('itemContent').get('topic')
        user_topics.append({'name': topic.get('name'), 'description': topic.get('description')})
    topic_data = {'topic': user_topics}
    # 更新mongodb
    if crawl_type == 'profile':
        MongoUserInfoOper.insert_or_update_user_topic_info(screen_id, topic_data)
    if crawl_type == 'relationship':
        MongoUserRelationshipOper.insert_or_update_topic(screen_id, topic_data)
    if crawl_type == 'tourist':
        MongoUserTweetTouristOper.insert_or_update_topic(screen_id, topic_data)
    print('用户 {} topic信息抓取成功'.format(screen_id))
    return 1


def parser_user_tweet(content, person_url, screen_id, username, user_tag, image_url, task):
    res = 0
    tweets = jsonpath.jsonpath(content, "$..tweet_results.result")
    datas = []
    # print('开始解析推文...')
    # starttime = datetime.datetime.now()
    for tweet_info in tweets:
        try:
            # t1 = datetime.datetime.now()
            user = tweet_info['core']['user_results']['result']
            user_id = user['rest_id']
            tweet = tweet_info['legacy']
            tweet_url = 'https://twitter.com/' + tweet['user_id_str'] + '/status/' + tweet['id_str']

            if user_id != str(screen_id):
                continue
                # # 如果该推文的作者不是目标用户，且不是转发则说明该tweet为目标用户评论别人的那篇推文，则不爬取该推文
                # if tweet['user_id_str'] != str(screen_id) and not tweet.get('retweeted_status_id_str'):
                #     print(tweet_url)
                #     continue
                # mysql判断是否存在
                # if UserTweetOper.is_tweet_exited(tweet_url):
                #     continue

                # # mongodb判断是否存在
                # if MongoUserTweetOper.query_tweet_is_existed(tweet_url):
                #     continue
            # 布隆过滤器判断是否存在
            # if bloom_filter_post.is_exist(tweet_url):
            #     continue
            original_author = None
            tweet_type = '原创'
            if tweet.get('in_reply_to_user_id_str'):
                tweet_type = '回复'
                original_author='https://twitter.com/' + tweet['in_reply_to_screen_name']
            if tweet.get('retweeted') or tweet.get('retweeted_status_result'):
                tweet_type = '转发'
                original_author='https://twitter.com/' + tweet['entities']['user_mentions'][0]['screen_name']
                # 带评论转发---user_id_str也为目标用户的screen_id
            if tweet.get('is_quote_status') and tweet['user_id_str'] == str(screen_id):
                tweet_type = '引用'
                original_author=tweet['quoted_status_permalink']['expanded'].split('/status')[0]
            # 爬取视屏链接信息
            video_urls = []
            try:
                media = tweet.get('extended_entities').get('media')
                for video in media:
                    video_url_list = [v.get('url') for v in video.get('video_info').get('variants') if v.get('content_type') == 'video/mp4']
                    video_urls.append(video_url_list[-1])
            except Exception:
                pass

            # 作者信息
            user = user['legacy']

            # register_time = time.strptime(user['created_at'], "%a %b %d %H:%M:%S +0000 %Y")
            # register_time = time.strftime("%Y-%m-%d %H:%M:%S", register_time)

            try:
                print(tweet_url)
                tweet_data = {}
                tweet_data['tweet_url'] = tweet_url
                tweet_data['author_url'] = 'https://twitter.com/' + user['screen_name']
                tweet_data['avatar_url'] = user['profile_image_url_https']
                tweet_data['avatar_binary'] = None
                tweet_data['topic'] = None
                tweet_data['author_name'] = user['name']
                tweet_data['content'] = tweet['full_text']
                tweet_data['tweet_type'] = tweet_type
                tweet_data['tweet_language'] = tweet['lang']
                tweet_data['comment_count'] = int(tweet['reply_count'])
                tweet_data['retweet_count'] = int(tweet['retweet_count'])
                tweet_data['quote_count'] = int(tweet['quote_count'])
                tweet_data['praise_count'] = int(tweet['favorite_count'])
                tweet_data['publish_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.strptime(tweet['created_at'], "%a %b %d %H:%M:%S +0000 %Y"))
                tweet_data['fetch_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                tweet_data['tweet_id'] = tweet['id_str']
                tweet_data['screen_id'] = user_id
                tweet_data['screen_name'] = user['screen_name']
                tweet_data['tourist_crawl_status'] = 0  # 这里修改为只要插入redis就默认该推文的tourist已爬取，因为后面更新该状态太慢,1表示其tourist信息已爬取
                tweet_data['video_urls'] = video_urls
                tweet_data['original_author']=original_author
                # 新增推文图片--mysql没有
                img_list = tweet.get('entities').get('media')
                if img_list:
                    tweet_data['tweet_img_url'] = [img.get('media_url_https') for img in img_list]
                else:
                    tweet_data['tweet_img_url'] = None
                tweet_data['tweet_img_binary'] = None  # 推文图片单独爬取
                # 加入用户标签
                tweet_data['user_tag'] = user_tag
            except Exception:
                continue

            datas.append(tweet_data)
            # if tweet_data['original_author']:
            #     user_task = {
            #         'user_url': tweet_data['original_author'],
            #         'user_name': None,
            #         'important_user': 0,
            #         'add_time': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
            #         'update_time': None,
            #
            #         'follower_task': 1,
            #         'following_task': 1,
            #         'post_task': 1,
            #         'profile_task': 1,
            #
            #         'post_praise_task': 1,
            #         'post_repost_task': 1,
            #         'post_comment_task': 1,
            #         'post_reply_task': 1,
            #
            #         'post_crawl_status': 0,
            #         'following_crawl_status': 0,
            #         'follower_crawl_status': 0,
            #         'profile_crawl_status': 0,
            #         'tourist_crawl_status': 0,
            #
            #         'user_tag': None,
            #     }
            #     # MongoUserTaskOper.insert_user_task(user_task)
            # 经测试，获取内容部分耗时0秒，即基本不耗时
            # t3 = datetime.datetime.now()
            # print('获取内容部分消耗时间：{}秒'.format((t3 - t2).seconds))
            # # 存储数据mysql
            # store_user_tweet_info(tweet_data)

            # # mongodb存储数据
            # MongoUserTweetOper.insert_or_update_user_tweet_info(tweet_data)
            # 放入布隆过滤器---经测试保存布隆过滤器耗时1-2秒,---但是尝试去掉这部分后还是很慢，为啥？协程设为5时为0，为10时有时为0有时为1
            bloom_filter_post.save(tweet_url)
            res += 1
            # t4 = datetime.datetime.now()
            # print('保存布隆过滤器消耗时间：{}秒'.format((t4 - t3).seconds))

            # 对于转推和引用的推文不爬取点赞者、评论者、转发者---tourist_crawl_status还是为0
            # if tweet.get('tweet_type') == '转发' or tweet.get('tweet_type') == '引用':
            #     continue
            # 对于评论者、转发者、点赞数都为1的也不爬取---tourist_crawl_status还是为0
            # tourist_number = 1
            # if tweet_data.get('praise_count') >= tourist_number or tweet_data.get('retweet_count') >= tourist_number or tweet_data.get('comment_count') >= tourist_number:
            #     # tweet_list.append(tweet_data)
            #     # t4 = datetime.datetime.now()
            #     if tweet_data.get('_id'):
            #         tweet_data.pop('_id')
            #     tourist_worker.apply_async(args=[tweet_data])  # 协程数为20.测试证明放入worker耗时1-8秒，  协程设为5、10时为0，
                # t5 = datetime.datetime.now()
                # print('放入worker消耗时间：{}秒'.format((t5 - t4).seconds))
        except Exception:
            pass

    # endtime = datetime.datetime.now()
    # print('解析推文消耗时间：{}秒'.format((endtime - starttime).seconds))
    # 协程数为20、10的时候很耗时，为5的时候也很慢200-300多秒，为1的时候需要80-100多秒（去掉）
    # 直接调用函数运行大概需要60多秒（存布隆过滤器和worker），不存大概30-50秒
    if datas:
        # 插入数据库时间已统计，为0秒，即不消耗时间
        # t1 = datetime.datetime.now()
        MongoUserTweetOper.insert_many_tweet_data(datas)
        # t2 = datetime.datetime.now()
        # print('插入数据库消耗时间：{}秒'.format((t2 - t1).seconds))

        # 注意这里使用mongodb批量插入后，后修改datas列表中的元素，会给每个元素加上一个_id字段，值为mongodb中的Object类型
        # 这里由于tweet_list和datas中的元素在python中使用同一地址，故tweet_list中的元素也会都被加上_id字段
    print(' {} 推文信息爬取 {} 条成功'.format(person_url, res))
    return res


def parser_relationship(content, personal_url, user_tag, crawl_type, proxies):
    bloom_filter_relationship = bloom_filter_follower if crawl_type == 'follower' else bloom_filter_following
    res = 0
    # 找不到则follows为False
    follows = jsonpath.jsonpath(content, '$..entries[*]')
    if not isinstance(follows, Iterable):
        return 0
    datas = []  # 新增批量插入功能
    # bot_datas = []  # 杨舟师姐的机器人信息
    # starttime = datetime.datetime.now()
    for follow in follows:
        try:
            # 20220620更新
            screen_id = follow['content']['itemContent']['user_results']['result']['rest_id']
            follow = follow['content']['itemContent']['user_results']['result']['legacy']
        except KeyError:
            continue

        relationship_user_url = 'https://twitter.com/' + follow['screen_name']
        relationship_type = 1 if crawl_type == 'following' else 0
        # mysql查询
        # if UserRelationshipOper.is_relationship_exited(personal_url, relationship_user_url, relationship_type):
        #     continue

        # # mongodb查询
        # if MongoUserRelationshipOper.query_relationship_is_existed(personal_url, relationship_user_url, relationship_type):
        #     continue

        # 布隆过滤器
        if bloom_filter_relationship.is_exist(personal_url + relationship_user_url + str(relationship_type)):
            continue
        try:
            image_url = follow['profile_image_url_https']
            # todo 将头像链接放入头像任务队列爬取头像
            # pic_task = {}
            # pic_task['table'] = 'osn_relationship'
            # pic_task['url'] = image_url
            # push_picture_task(json.dumps(pic_task))
        except KeyError:
            image_url = None
        if follow.get('entities').get('url'):
            website = follow['entities']['url']['urls'][0]
            website = website.get('expanded_url', '') if website.get('expanded_url') else website.get('url', '')
        else:
            website = ''

        register_time = time.strptime(follow['created_at'], "%a %b %d %H:%M:%S +0000 %Y")
        register_time = time.strftime("%Y-%m-%d %H:%M:%S", register_time)

        relationship_data = {}
        relationship_data['user_url'] = personal_url
        relationship_data['relationship_type'] = relationship_type
        relationship_data['relationship_user_url'] = relationship_user_url
        relationship_data['screen_id'] = screen_id
        relationship_data['screen_name'] = follow['screen_name']
        relationship_data['user_name'] = follow['name']
        relationship_data['avatar_url'] = image_url
        relationship_data['register_time'] = register_time
        relationship_data['avatar_binary'] = None
        relationship_data['avatar_bs64encode'] = None
        relationship_data['introduction'] = follow['description']
        relationship_data['location'] = follow['location']
        relationship_data['website'] = website
        relationship_data['following_count'] = follow['friends_count']
        relationship_data['followers_count'] = follow['followers_count']
        relationship_data['tweets_count'] = follow['statuses_count']
        relationship_data['media_tweets_count'] = follow['media_count']
        relationship_data['favourites_tweets_count'] = follow['favourites_count']
        relationship_data['is_protected'] = 1 if follow['protected'] else 0
        relationship_data['fetch_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        # 新增，mysql没有
        relationship_data['banner_url'] = follow.get('profile_banner_url')  # 没有背景图片的用户无该字段,get没有就返回None
        relationship_data['banner_binary'] = None  # 没有背景图片的用户无该字段
        relationship_data['is_verified'] = 1 if follow['verified'] else 0  # 是否认证，类似于机构认证那种
        relationship_data['topic'] = None
        relationship_data['is_default_profile'] = 1 if follow['default_profile'] else 0
        relationship_data['is_default_profile_image'] = 1 if follow['default_profile_image'] else 0
        # relationship_data['user_tag'] = user_tag  # 关系不需要用户标签
        datas.append(relationship_data)  # 新增计划批量插入
        insert_object_user(relationship_data)
        # # 存储mysql
        # store_relationship_info(relationship_data)

        # # 存储杨舟师姐的机器人检测的数据----部署时序注释
        # bot_data = {}
        # bot_data['crawl_status'] = 1
        # bot_data['is_bot'] = 1
        # bot_data['source'] = 'twitter'
        # bot_data['url'] = relationship_user_url
        # bot_datas.append(bot_data)

        # MongoUserRelationshipOper.insert_or_update_yz_bot_data(bot_data)  # 单条插入

        # # 存储mongodb
        # MongoUserRelationshipOper.insert_or_update_user_relationship_info(relationship_data)
        # 放入布隆过滤器
        bloom_filter_relationship.save(personal_url + relationship_user_url + str(relationship_type))
        res += 1
    # endtime = datetime.datetime.now()
    # print('解析关系消耗时间：{}'.format((endtime - starttime).seconds))
    if datas:
        MongoUserRelationshipOper.insert_many_relationship_data(datas)
    # if bot_datas:
    #     for d in bot_datas:
    #         if d.get('_id'):
    #             d.pop('_id')
    #     MongoCommonOper.insert_many_data('social_bot_task', bot_datas)
    print(' {} 的{}社交关系采集{}条成功'.format(personal_url, crawl_type, res))
    return res


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
        data['avatar_binary'] = None
        data['avatar_bs64encode'] = None

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
        data['banner_binary'] = None  # 没有背景图片的用户无该字段
        data['is_verified'] = 1 if person['verified'] else 0  # 是否认证，类似于机构认证那种
        data['topic'] = None
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
            data['avatar_binary'] = None
            data['avatar_bs64encode'] = None
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
            data['banner_binary'] = None  # 没有背景图片的用户无该字段
            data['is_verified'] = 1 if person.get('verified') else 0  # 是否认证，类似于机构认证那种
            data['topic'] = None
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

def is_jianti_sentence(s):
    import zhconv
    s_ = zhconv.convert(s, 'zh-hans')
    if s_ == s:
        return True
    else:
        return False

def parser_keyword_tweet(content, account, proxies, search_keyword):
    """
    关键词相关推文及作者信息解析：目前不支持对已采集推文进行更新，因为太耗费资源了
    :param content:
    :param account:
    :param proxies:
    :param search_keyword:
    :return:
    """
    res = 0
    tweets = jsonpath.jsonpath(content, '$..tweets[*]')
    datas = []
    for tweet in tweets:
        try:
            tweet_url = 'https://twitter.com/' + tweet['user_id_str'] + '/status/' + tweet['id_str']
            # mysql
            # if KeywordsOper.is_tweet_exited(tweet_url):
            #     continue

            # # mongodb--如果该推文已爬取，则继续
            # if MongoKeywordTweetOper.query_tweet_is_existed(tweet_url):
            #     continue

            # 关键词推文--布隆过滤器
            if bloom_filter_keyword_post.is_exist(tweet_url):
                continue

            tweet_content = tweet['full_text']
            if is_jianti_sentence(tweet_content):
                continue

            tweet_type = '原创'
            # 回复
            if tweet.get('in_reply_to_user_id_str'):
                tweet_type = '回复'
            # 如果为转发推文
            if tweet.get('retweeted_status_id_str'):
                tweet_type = '转发'
            # 带评论转发---user_id_str也为目标用户的screen_id
            if tweet.get('is_quote_status'):
                tweet_type = '引用'
            reply_count = tweet['reply_count']
            retweet_count = tweet['retweet_count']
            quote_count = tweet['quote_count']
            favorite_count = tweet['favorite_count']
            publish_time = tweet['created_at'].replace('+0000 ', '')
            publish_time = datetime.datetime.strptime(publish_time, '%a %b %d %H:%M:%S %Y')
            tweet_language = tweet['lang']
            # 作者信息
            user = jsonpath.jsonpath(content, "$..%s" % tweet['user_id_str'])
            user = user[0] if user else {}
            # 如果没有找到该推文的作者则不爬取该推文数据
            if not user:
                continue
            tweet_author_url = 'https://twitter.com/' + user['screen_name']
            image_url = user['profile_image_url_https']
            user_name = user['name']
            register_time = time.strptime(user['created_at'], "%a %b %d %H:%M:%S +0000 %Y")
            register_time = time.strftime("%Y-%m-%d %H:%M:%S", register_time)
            # tweet信息（存在的数据就根据主键更新，不存在则插入）
            tweet_data = {}
            tweet_data['tweet_url'] = tweet_url
            tweet_data['author_url'] = tweet_author_url
            tweet_data['img_url'] = image_url
            # tweet_data['img_binary'] = b''
            tweet_data['author_name'] = user_name
            tweet_data['content'] = tweet_content
            tweet_data['tweet_type'] = tweet_type
            tweet_data['tweet_language'] = tweet_language
            tweet_data['comment_count'] = int(reply_count)
            tweet_data['retweet_count'] = int(retweet_count)
            tweet_data['quote_count'] = int(quote_count)
            tweet_data['praise_count'] = int(favorite_count)
            tweet_data['publish_time'] = publish_time
            tweet_data['fetch_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            tweet_data['search_keyword'] = search_keyword
            tweet_data['tweet_id'] = tweet['id_str']
            tweet_data['screen_id'] = user['id_str']
            tweet_data['screen_name'] = user['screen_name']
            tweet_data['spider_account'] = account
            datas.append(tweet_data)
            # # 存储推文信息--mysql
            # store_keyword_tweet_info(tweet_data)

            # # 存储推文信息--mongodb
            # MongoKeywordTweetOper.insert_or_update_keyword_tweet_info(tweet_data)
            # 将推文链接存储到布隆过滤器
            bloom_filter_keyword_post.save(tweet_url)
            res += 1

            # tweet作者信息（存在的数据就根据主键更新，不存在则插入）
            # tweet_author_data = {}
            # tweet_author_data['user_url'] = tweet_author_url
            # tweet_author_data['screen_id'] = user['id_str']
            # tweet_author_data['screen_name'] = user['screen_name']
            # tweet_author_data['user_name'] = user['name']
            # tweet_author_data['img_url'] = image_url
            # tweet_author_data['register_time'] = register_time
            # # tweet_author_data['avatar_binary'] = b''
            # # tweet_author_data['avatar_bs64encode'] = ''
            # tweet_author_data['introduction'] = user['description']
            # tweet_author_data['location'] = user['location']
            # tweet_author_data['website'] = user['entities']['url']['urls'][0]['expanded_url'] if user.get('entities').get('url') else ''
            # tweet_author_data['following_count'] = user['friends_count']
            # tweet_author_data['followers_count'] = user['followers_count']
            # tweet_author_data['tweets_count'] = user['statuses_count']
            # tweet_author_data['media_tweets_count'] = user['media_count']
            # tweet_author_data['favourites_tweets_count'] = user['favourites_count']
            # tweet_author_data['is_protected'] = 1 if user['protected'] else 0
            # tweet_author_data['search_keyword'] = search_keyword
            # tweet_author_data['fetch_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            #
            # # 新增的属性--mysql没有----还未测试
            # tweet_author_data['banner_url'] = user.get('profile_banner_url')  # 没有背景图片的用户无该字段,get没有就返回None
            # tweet_author_data['banner_binary'] = b''  # 没有背景图片的用户无该字段
            # tweet_author_data['is_verified'] = 1 if user['verified'] else 0  # 是否认证，类似于机构认证那种
            # # tweet_author_data['topic'] = []  # 这里不能赋值为[]。否则在对用户信息进行更新时，会更新之前的topic信息为[]
            # tweet_author_data['is_default_profile'] = 1 if user['default_profile'] else 0
            # tweet_author_data['is_default_profile_image'] = 1 if user['default_profile_image'] else 0
            # 添加用户标签---占时先不加
            # user_data['user_tag'] =

            # 生日先不爬
            # try:
            #     birthday = content['data']['user']['legacy_extended_profile'].get('birthdate')
            #     if birthday:
            #         user_data['birthday'] = {'year': birthday.get('year', ''), 'month': birthday.get('month', ''),
            #                                  'day': birthday.get('day', '')}
            #     else:
            #         user_data['birthday'] = None
            # except Exception as e:
            #     user_data['birthday'] = None
            # 存储推文作者信息
            # store_keyword_user_info(tweet_author_data)

            # 将该推文的作者加入任务表
            user_task = {
                'user_url': tweet_author_url,
                'user_name': user['name'],
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
            MongoUserTaskOper.insert_user_task(user_task)
        except Exception as e:
            print('异常:{}'.format(e))
            continue
    if datas:
        MongoKeywordTweetOper.insert_many_keyword_tweet_data(datas)
    print('关键词 {} 相关推文信息爬取 {}条成功'.format(search_keyword, res))
    return res


def parser_keyword_user(content, search_keyword):
    res = 0
    users = jsonpath.jsonpath(content, '$..users[*]')
    if not users:
        print('关键词 {} 无相关用户信息'.format(search_keyword))
        return
    for user in users:
        user_url = 'https://twitter.com/' + user['screen_name']
        if MongoUserTaskOper.is_exit_user_task(user_url):
            res += 1
            continue
        screen_id = user['id_str']
        try:
            introduction = user['description']
        except KeyError:
            introduction = ''
        try:
            location = user['location']
        except KeyError:
            location = ''
        try:
            website = user['entities']['url']['urls'][0]['expanded_url']
        except KeyError:
            website = ''
        # tweet数量
        # tweet_count = user['statuses_count']

        createtime = user['created_at'].replace('+0000 ', '')
        createtime = datetime.datetime.strptime(createtime, '%a %b %d %H:%M:%S %Y')
        following_count = user['friends_count']  # 关注数
        followee_count = user['followers_count']  # 粉丝数
        userimage = user['profile_image_url_https']

        # 存入用户任务表
        user_task = {
            'user_url': user_url,
            'user_name': user['name'],
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
        MongoUserTaskOper.insert_user_task(user_task)
    print(' {} 相关用户信息抓取成功,共{}条.'.format(search_keyword, len(users) - res))
    return len(users) - res

if __name__=='__main__':
    #OK
    url=[
        'https://twitter.com/kuomintang',
        'https://twitter.com/JohnnyChiang12',
        'https://twitter.com/WananChiang',
        'https://twitter.com/houyuih',
       ' https://twitter.com/KweiBo',
        'https://twitter.com/eric_chu0607',
        'https://twitter.com/hsuchiaohsin',
        'https://twitter.com/lushiowyen',
        'https://twitter.com/iingwen',
        'https://twitter.com/TaipeiShihChung',
        'https://twitter.com/eballgogogo',
        'https://twitter.com/DPPonline',
        'https://twitter.com/sikunyou',
        'https://twitter.com/bikhim',
        'https://twitter.com/Military_idv_tw',
        'https://twitter.com/chimaichen',
        'https://twitter.com/ChingteLai',
        'https://twitter.com/pinyutw2020',
        'https://twitter.com/minorta',
        'https://twitter.com/KP_Taiwan',
        'https://twitter.com/miaopoya' ,
        'https://twitter.com/Shan_Huang33',
        'https://twitter.com/TsaiPK',
        'https://twitter.com/ambtomchou',
    ]
    for user_url in url:
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
        MongoUserTaskOper.insert_user_task(user_task)

#现阶段采集逻辑
#(1)get task（keyword or seed or sub-task）
#(2)relation check in task
#(3)tweet orignal node in task
#(4)tourist check in task
# 重复（1，2，3，4）
#task 中基本上都是需要的






