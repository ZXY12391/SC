import time
import random
from datetime import timedelta

from pymysql.err import IntegrityError as PymysqlIntegrityError

from sqlalchemy import text
from sqlalchemy.exc import IntegrityError as SqlalchemyIntegrityError
from sqlalchemy.exc import InvalidRequestError

from logger import db_logger
from db.basic import get_db_session
from config.settings import project_tables_dict
from db.models import (KeywordTasks, PostInfoSearchResults, UserTasks, UserInfoResults, UserRelationshipResults,
                       PostInfoResults, AccountInfoForSpider,
                       TwitterCookies, TwitterBotInfo)


class CommonOperation:
    @classmethod
    def get_entity_by_conditions(cls, model, conditions):
        """
        根据条件获取一个实体
        :param model: sqlalchemy model
        :param conditions: 字符串，sql查询条件
        :return: 符合查询条件的一个实体
        """
        with get_db_session() as db:
            try:
                return db.query(model).filter(text(conditions)).first()
            except Exception as e:
                db_logger.error('exception {} is raied'.format(e))

    @classmethod
    def get_entities_by_conditions(cls, model, conditions):
        """
        根据条件获取多个实体
        :param model: sqlalchemy model
        :param conditions: 字符串，sql查询条件
        :return: 列表，符合查询条件多个实体列表
        """
        with get_db_session() as db:
            try:
                return db.query(model).filter(text(conditions)).all()
            except Exception as e:
                db_logger.error('exception {} is raied'.format(e))

    @classmethod
    def get_attrs_by_conditions(cls, model, attrs, conditions):
        """
        根据查询条件获取实体的多个属性
        :param model: sqlalchemy model
        :param attrs: 列表，查询的属性--key为标准的表字段名
        :param conditions: 字符串，sql查询条件
        :return: 列表，符合条件的实体及其查询的属性
        """
        model_attrs = list()
        for attr in attrs:
            model_attr = getattr(model, project_tables_dict.get(model.__name__).get(attr))
            if model_attr:
                model_attrs.append(model_attr)

        with get_db_session() as db:
            try:
                return db.query(*model_attrs).filter(text(conditions)).all()
            except Exception as e:
                db_logger.error('exception {} is raied'.format(e))

    @classmethod
    def insert_one_entity(cls, data):
        """
        插入一条数据
        :param data: 待插入数据库的一个实体对象
        :return: 是否成功
        """
        rs = 1
        with get_db_session() as db:
            try:
                db.add(data)
                db.commit()
            except SqlalchemyIntegrityError:
                rs = 0
            except Exception as e:
                db_logger.error('exception {} is raied'.format(e))
                rs = 0
        return rs

    @classmethod
    def insert_multiple_entity(cls, datas):
        """
        插入多条数据
        :param datas: 列表，待插入数据库的多个实体对象列表
        :return: 插入成功个数
        """
        count = len(datas)
        with get_db_session() as db:
            try:
                db.add_all(datas)
                db.commit()
            except (SqlalchemyIntegrityError, PymysqlIntegrityError,
                    InvalidRequestError):
                count = 0
                for data in datas:
                    count += cls.insert_one_entity(data)
        return count

    @classmethod
    def insert_or_update_entity_by_primary_key(cls, data):
        """
        根据主键插入或更新数据
        :param data: 待插入数据库的一个实体对象
        :return:
        """
        with get_db_session() as db_session:
            try:
                # merge的作用是合并，查找primary key是否一致，一致则合并，不一致则新建
                # 数据库中有该记录，则更新该记录，如果不存在该记录，则进行insert操作
                db_session.merge(data)
                db_session.commit()
            except Exception as e:
                db_logger.error('exception {} is raied'.format(e))

    @classmethod
    def insert_or_update_entity_by_conditions(cls, model, conditions, data, attr_maps):
        """
        根据条件插入或更新数据（适用于主键为id且此时又需要更新或插入数据的情况）
        :param conditions:
        :param model:
        :param data: 待插入或更新数据库的一个实体对象
        :param attr_maps: 待插入或更新数据库的一个实体对象对应的字典--key为标准的表字段名
        :return:
        """
        entity = cls.get_entity_by_conditions(model, conditions)
        if not entity:
            cls.insert_one_entity(data)
            return

        with get_db_session() as db:
            try:
                for attr, value in attr_maps.items():
                    setattr(entity, project_tables_dict.get(model.__name__).get(attr), value)
                db.merge(entity)
                db.commit()
            except Exception as e:
                db_logger.error('exception {} is raied'.format(e))

    @classmethod
    def update_entity_attrs_by_conditions(cls, model, conditions, attr_maps):
        """
        更新符合条件的一个实体的属性，如果数据库中没有符合该条件的实体则什么都不做
        :param model: sqlalchemy model
        :param conditions: 字符串，sql查询条件
        :param attr_maps: 字典，需要更新的属性信息--key为标准的表字段名
        :return:
        """
        entity = cls.get_entity_by_conditions(model, conditions)
        if not entity:
            return

        with get_db_session() as db:
            try:
                for attr, value in attr_maps.items():
                    setattr(entity, project_tables_dict.get(model.__name__).get(attr), value)
                db.merge(entity)
                db.commit()
            except Exception as e:
                db_logger.error('exception {} is raied'.format(e))

    @classmethod
    def update_entity_attrs(cls, model, entity, attr_maps):
        """
        更新一个实体的属性
        :param model: sqlalchemy model
        :param entity: 需要更新的数据实体对象
        :param attr_maps: 字典，需要更新的属性信息--key为标准的表字段名
        :return:
        """
        with get_db_session() as db:
            try:
                for attr, value in attr_maps.items():
                    setattr(entity, project_tables_dict.get(model.__name__).get(attr), value)
                db.merge(entity)
                db.commit()
            except Exception as e:
                db_logger.error('exception {} is raied'.format(e))


class TaskTablesOper(CommonOperation):

    @classmethod
    def get_keyword_tasks(cls):
        model = KeywordTasks
        conditions = "%s=0" % project_tables_dict.get(model.__name__).get('crawl_status')
        return cls.get_entities_by_conditions(model=model, conditions=conditions)

    @classmethod
    def update_keyword_task_status(cls, keyword, status_value):
        model = KeywordTasks
        conditions = "%s='%s'" % (project_tables_dict.get(model.__name__).get('keyword'), keyword)
        # 需要修改的属性
        attr_maps = {'crawl_status': status_value, }
        # 更新时间字段
        if project_tables_dict.get(model.__name__).get('update_time'):
            attr_maps['update_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

        return cls.update_entity_attrs_by_conditions(model=model, conditions=conditions, attr_maps=attr_maps)

    @classmethod
    def get_relationship_tasks(cls):
        model = UserTasks
        conditions = "%s=0" % project_tables_dict.get(model.__name__).get('crawl_status')
        return cls.get_entities_by_conditions(model=model, conditions=conditions)

    @classmethod
    def update_relationship_task_status(cls, user_url, status_value):
        model = UserTasks
        conditions = "%s='%s'" % (project_tables_dict.get(model.__name__).get('user_url'), user_url)
        # 需要修改的属性
        attr_maps = {'crawl_status': status_value, }
        # 更新时间字段
        if project_tables_dict.get(model.__name__).get('update_time'):
            attr_maps['update_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

        return cls.update_entity_attrs_by_conditions(model=model, conditions=conditions, attr_maps=attr_maps)

    @classmethod
    def get_user_tasks(cls):
        model = UserTasks
        conditions = "%s=0" % project_tables_dict.get(model.__name__).get('crawl_status')
        return cls.get_entities_by_conditions(model=model, conditions=conditions)

    @classmethod
    def update_user_task_status(cls, user_url, status_value):
        model = UserTasks
        conditions = "%s='%s'" % (project_tables_dict.get(model.__name__).get('user_url'), user_url)
        # 需要修改的属性
        attr_maps = {'crawl_status': status_value}
        # 更新时间字段
        if project_tables_dict.get(model.__name__).get('update_time'):
            attr_maps['update_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

        return cls.update_entity_attrs_by_conditions(model=model, conditions=conditions, attr_maps=attr_maps)

    @classmethod
    def get_user_tweet_tasks(cls):
        model = UserTasks
        conditions = "%s=0" % project_tables_dict.get(model.__name__).get('crawl_status')
        return cls.get_entities_by_conditions(model=model, conditions=conditions)

    @classmethod
    def update_user_tweet_task_status(cls, user_url, status_value):
        model = UserTasks
        conditions = "%s='%s'" % (project_tables_dict.get(model.__name__).get('user_url'), user_url)
        # 需要修改的属性
        attr_maps = {'crawl_status': status_value}
        # 更新时间字段
        if project_tables_dict.get(model.__name__).get('update_time'):
            attr_maps['update_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

        return cls.update_entity_attrs_by_conditions(model=model, conditions=conditions, attr_maps=attr_maps)


# 包括关键词推文及相应的作者
class KeywordsOper(CommonOperation):

    @classmethod
    def is_tweet_exited(cls, tweet_url):
        model = PostInfoSearchResults
        conditions = "%s='%s'" % (project_tables_dict.get(model.__name__).get('tweet_url'), tweet_url)
        if cls.get_entity_by_conditions(model, conditions):
            return True
        return False


# 用户档案信息
class UserInfoOper(CommonOperation):

    @classmethod
    def insert_or_update_user_info_by_conditions(cls, data, data_dict):
        model = UserInfoResults
        conditions = "%s='%s'" % (project_tables_dict.get(model.__name__).get('user_url'), data_dict.get('user_url'))
        return cls.insert_or_update_entity_by_conditions(model, conditions, data, data_dict)

    @classmethod
    def insert_or_update_user_info_by_primary(cls, data):
        return cls.insert_or_update_entity_by_primary_key(data)


# 用户关系信息
class UserRelationshipOper(CommonOperation):

    @classmethod
    def is_relationship_exited(cls, user_url, relationship_user_url, relationship_type):
        with get_db_session() as db:
            model = UserRelationshipResults
            rs = db.query(model).filter_by(user_url=user_url, relationship_user_url=relationship_user_url,
                                           relationship_type=relationship_type).first()
        return rs

    @classmethod
    def insert_or_update_relationship_info(cls, data, data_dict):
        model = UserRelationshipResults
        with get_db_session() as db:
            entity = db.query(model).filter_by(user_url=data_dict['user_url'],
                                               relationship_user_url=data_dict['relationship_user_url'],
                                               relationship_type=data_dict['relationship_type']).first()
            if entity:
                return cls.update_entity_attrs(model, entity, data_dict)
            else:
                cls.insert_one_entity(data)


# 包括关键词推文及相应的作者
class UserTweetOper(CommonOperation):

    @classmethod
    def is_tweet_exited(cls, tweet_url):
        model = PostInfoResults
        conditions = "%s='%s'" % (project_tables_dict.get(model.__name__).get('tweet_url'), tweet_url)
        if cls.get_entity_by_conditions(model, conditions):
            return True
        return False


class AccountInfoOper(CommonOperation):

    @classmethod
    def get_proxy_cookies(cls, alive):
        with get_db_session() as db_session:
            rs = db_session.query(AccountInfoForSpider).filter_by(site='twitter', alive=alive).all()
        return rs

    @classmethod
    def set_account_status(cls, account, status):
        with get_db_session() as db_session:
            db_session.query(AccountInfoForSpider).filter_by(site='twitter', account=account).update({AccountInfoForSpider.alive: status})
            db_session.commit()
            print('更新twitter爬虫账号{}的状态成功！'.format(account))

    @classmethod
    def get_all_account(cls, alive):
        with get_db_session() as db_session:
            rs = db_session.query(AccountInfoForSpider).filter_by(site='twitter', alive=alive).all()
        return rs

    @classmethod
    def get_all_proxy(cls, alive):
        with get_db_session() as db_session:
            rs = db_session.query(AccountInfoForSpider).filter_by(site='twitter', alive=alive).all()
        return rs


# 培育表
class TwitterCookiesOper(CommonOperation):

    @classmethod
    def get_all_cookies(cls):
        with get_db_session() as db_session:
            rs = db_session.query(TwitterCookies).all()
        return rs

    @classmethod
    def update_bot_cookies(cls, conditions, data):
        cls.update_entity_attrs_by_conditions(TwitterCookies, conditions, data)

    @classmethod
    def insert_cookies_info(cls):
        cls.insert_one_entity()


# 培育表
class TwitterBotInfoOper(CommonOperation):

    @classmethod
    def get_bot_info(cls, account):
        with get_db_session() as db_session:
            rs = db_session.query(TwitterBotInfo).filter_by(account=account).first()
        return rs

    @classmethod
    def get_all_bot_info(cls):
        with get_db_session() as db_session:
            rs = db_session.query(TwitterBotInfo).all()
        return rs

    @classmethod
    def get_bots_by_condition(cls, conditions):
        return cls.get_entities_by_conditions(TwitterBotInfo, conditions)
