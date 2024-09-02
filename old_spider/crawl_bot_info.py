# -*- coding: utf-8 -*-
# @Time    : 2021-10-15 16:16
# @Author  : lldzyshwjx
import base64
import time
import json

from json import JSONDecodeError
from urllib.parse import quote
from db.redis_db import get_redis_conn
from utils.proxy_utils import get_one_proxy
from db.mongo_db import MongoCommonOper
from crawlers.downloader import download_without_cookies

conn = get_redis_conn()


# 爬取头像---测试完毕--头像更新需要选择所有符合条件的
def crawl_avatar_image(avatar_url):
    if not avatar_url:
        return 1
    conn = get_redis_conn()
    count = 1
    avatar_image_data = {}
    while count < 4:
        proxy = get_one_proxy(conn)
        resp = download_without_cookies(avatar_url, proxy)
        if resp is None:
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

    return avatar_image_data


# 解析用户基本（档案）信息---需要进行持续更新
def parser_user_info(screen_name, content, proxies):
    try:
        user = content['data']['user']['legacy']
    except Exception as e:
        return 0

    image_url = user['profile_image_url_https']
    try:
        # default_profile_normal.png为平台默认的图片，即用户没有设置头像
        resp = download_without_cookies(image_url, proxies)
        if resp is None:
            avatar_data = crawl_avatar_image(image_url)
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
    data = {'avatar_bs64encode': avatar_bs64encode, 'avatar_binary': avatar_binary, 'avatar_url': image_url}
    MongoCommonOper.insert_or_update_one_data('T_bot_info', {'account': screen_name}, data)


def user_info_process(user_url, proxies=None):
    """
    爬取用户档案信息
    :param task:
    :param cookies:
    :param proxies:
    :return:
    """
    proxies = get_one_proxy(conn) if proxies is None else proxies
    print('抓取用户个人主页URL：{} 的基本信息'.format(user_url))
    screen_name = user_url[20:]  # 获取用户的screen_name
    # 该链接不需要登录就可爬取链接
    url = 'https://api.twitter.com/graphql/-xfUfZsnR_zqjFd-IfrN5A/UserByScreenName?variables=%7B%22screen_name%22%3A%22{}%22%2C%22withHighlightedLabel%22%3Atrue%7D'.format(
        quote(screen_name))
    # # 该链接需要登录才能爬取信息
    # url = 'https://twitter.com/i/api/graphql/Vf8si2dfZ1zmah8ePYPjDQ/UserByScreenNameWithoutResults?variables=%7B%22screen_name%22%3A%22{}%22%2C%22withHighlightedLabel%22%3Atrue%7D'.format(quote(screen_name))
    resp = download_without_cookies(url, proxies)
    if resp is None:
        print('代理或网络异常，url {} 基本信息抓取失败'.format(user_url))
        return 0
    try:
        content = json.loads(resp.content)

        # 判断该用户url是否存在
        user_status_code = content.get("errors")[0].get('code') if content.get("errors") else 1
        if user_status_code == 50:
            print('url {} 不存在,采集基本信息失败'.format(user_url))
            return 1

        if user_status_code == 63:
            print('url {} 异常（ User has been suspended）,采集基本信息失败'.format(user_url))
            return 1

        # 解析页面
        parser_user_info(screen_name, content, proxies)
        print(' {} 基本信息抓取成功'.format(user_url))

    except Exception as e:
        print('异常{},返回的数据为:{}'.format(e, resp.content))
        print('url {} 基本信息抓取失败'.format(user_url))
    return 0


if __name__ == '__main__':

    # tasks = MongoCommonOper.query_datas_by_condition('T_bot_info', {'avatar_bs64encode': None, 'enable': 1})
    # for user in tasks:
    #     user_info_process("https://twitter.com/" + user.get('account'))
    #     print(user.get('account'))
    data = {'avatar_bs64encode': None, 'avatar_binary': None, 'avatar_url': None}
    MongoCommonOper.update_muti_data('T_bot_info', {}, data)
