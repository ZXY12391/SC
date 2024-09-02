import base64

from utils.proxy_utils import get_one_proxy
from crawlers.downloader import download_without_cookies
from db.redis_db import get_redis_conn
from db.mongo_db import MongoUserTweetOper, MongoUserRelationshipOper, MongoUserTweetTouristOper
from logger.log import download_logger, other_logger


# 测试完毕
def crawl_tweet_image(tweet_url, img_urls):
    flag = 1
    tweet_image_datas = []
    for img_url in img_urls:
        conn = get_redis_conn()
        count = 1
        tweet_image_data = {'img_url': img_url}
        while count < 4:
            proxy = get_one_proxy(conn)
            resp = download_without_cookies(img_url, proxy)
            if resp is None:
                download_logger.error('{} 推文图片 {} 抓取失败'.format(tweet_url, img_url))
                tweet_image_data['img_binary'] = None
                tweet_image_data['image_bs64encode'] = None
                count += 1
                continue
            else:
                # 二进制文件
                tweet_image_data['img_binary'] = resp.content
                # base64编码可在离线环境下展示，方便前端展示（可直接存链接或者base64编码后的图片）
                encodestr = base64.b64encode(resp.content)  # 得到 byte 编码的数据
                tweet_image_data['image_bs64encode'] = 'data:image/jpeg;base64,%s' % encodestr.decode("utf-8")
                break
        # if tweet_image_data.get('img_binary'):
        #     flag = 1
        tweet_image_datas.append(tweet_image_data)
    image_data = {'tweet_img_binary': tweet_image_datas}
    MongoUserTweetOper.insert_or_update_tweet_image(tweet_url, image_data)
    return flag


# 爬取头像---测试完毕--头像更新需要选择所有符合条件的
def crawl_avatar_image(user_url, avatar_url, crawl_type):
    if not avatar_url:
        return 1
    conn = get_redis_conn()
    count = 1
    avatar_image_data = {}
    while count < 4:
        proxy = get_one_proxy(conn)
        resp = download_without_cookies(avatar_url, proxy)
        if resp is None:
            download_logger.error('{} 头像 {} 抓取第{}次失败'.format(user_url, avatar_url, count))
            avatar_image_data['avatar_binary'] = None
            avatar_image_data['avatar_bs64encode'] = None
            count += 1
        else:
            # 二进制文件
            avatar_image_data['avatar_binary'] = resp.content
            # base64编码可在离线环境下展示，方便前端展示（可直接存链接或者base64编码后的图片）
            encodestr = base64.b64encode(resp.content)  # 得到 byte 编码的数据
            avatar_image_data['avatar_bs64encode'] = 'data:image/jpeg;base64,%s' % encodestr.decode("utf-8")
            break
    # 更新头像---关系表
    if crawl_type == 'relationship':
        MongoUserRelationshipOper.insert_or_update_avatar(user_url, avatar_image_data)
    # 更新头像---评论者转发者表
    if crawl_type == 'tourist':
        MongoUserTweetTouristOper.insert_or_update_avatar(user_url, avatar_image_data)
    if crawl_type == 'profile':
        return avatar_image_data
    flag = 1 if avatar_image_data['avatar_binary'] else 0
    return flag
