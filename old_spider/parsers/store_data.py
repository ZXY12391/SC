from config.settings import project_tables_dict
from db.models import (PostInfoSearchResults, UserInfoSearchResults, UserInfoResults, UserRelationshipResults,
                       PostInfoResults, )
from db.dao import KeywordsOper, UserInfoOper, UserRelationshipOper, UserTweetOper

"""
该模块为存储mysql的模块
"""


def store_keyword_tweet_info(tweet_data):
    """
    project_tables_dict中每个表的key为标准命名的属性，value为项目数据库表的字段名字
    :param tweet_data: 字典，键为标准命名属性
    :return:
    """
    data = PostInfoSearchResults()
    keyword_tweet_attrs = [(key, value) for key, value in project_tables_dict.get(data.get_entity_name()).items() if value]
    for key1, key2 in keyword_tweet_attrs:
        setattr(data, key2, tweet_data.get(key1))  # tweet_data.get如果不存在key1键则返回None，即如果未爬取该字段数据库为空
    # 这里直接使用插入，因为解析时已经判断了该链接是否已存储，对于已存储的数据直接跳过了
    KeywordsOper.insert_one_entity(data)


def store_keyword_user_info(tweet_author_data):
    """
    project_tables_dict中每个表的key为标准命名的属性，value为项目数据库表的字段名字
    :param tweet_author_data: 字典，键为标准命名属性
    :return:
    """
    data = UserInfoSearchResults()
    keyword_user_attrs = [(key, value) for key, value in project_tables_dict.get(data.get_entity_name()).items() if value]
    for key1, key2 in keyword_user_attrs:
        setattr(data, key2, tweet_author_data.get(key1))
    # 这里直接使用插入，因为解析时已经判断了该链接是否已存储，对于已存储的数据直接跳过了
    KeywordsOper.insert_one_entity(data)


def store_user_info(user_data):
    """
    project_tables_dict中每个表的key为标准命名的属性，value为项目数据库表的字段名字
    :param user_data: 字典，键为标准命名属性
    :return:
    """
    data = UserInfoResults()
    user_attrs = [(key, value) for key, value in project_tables_dict.get(data.get_entity_name()).items() if value]
    for key1, key2 in user_attrs:
        setattr(data, key2, user_data.get(key1))
    # # 方法1：适用于user_url为主键
    # UserInfoOper.insert_or_update_user_info_by_primary(data)
    # 方法2：根据条件更新
    UserInfoOper.insert_or_update_user_info_by_conditions(data, user_data)


def store_relationship_info(relationship_data):
    """
    project_tables_dict中每个表的key为标准命名的属性，value为项目数据库表的字段名字
    :param relationship_data: 字典，键为标准命名属性
    :return:
    """
    data = UserRelationshipResults()
    user_attrs = [(key, value) for key, value in project_tables_dict.get(data.get_entity_name()).items() if value]
    for key1, key2 in user_attrs:
        setattr(data, key2, relationship_data.get(key1))
    # 这里可直接插入数据（因为已做过是否存在判断）
    UserRelationshipOper.insert_one_entity(data)
    # # 需要进行更新时---根据filter条件
    # UserRelationshipOper.insert_or_update_relationship_info(data, relationship_data)
    # # 需要进行更新时---根据主键（可以为联和主键）
    # UserRelationshipOper.insert_or_update_entity_by_primary_key(data)


def store_user_tweet_info(tweet_data):
    """
    project_tables_dict中每个表的key为标准命名的属性，value为项目数据库表的字段名字
    :param tweet_data: 字典，键为标准命名属性
    :return:
    """
    data = PostInfoResults()
    user_tweet_attrs = [(key, value) for key, value in project_tables_dict.get(data.get_entity_name()).items() if value]
    for key1, key2 in user_tweet_attrs:
        setattr(data, key2, tweet_data.get(key1))  # tweet_data.get如果不存在key1键则返回None，即如果未爬取该字段数据库为空
    # 这里直接使用插入，因为解析时已经判断了该链接是否已存储，对于已存储的数据直接跳过了
    UserTweetOper.insert_one_entity(data)
