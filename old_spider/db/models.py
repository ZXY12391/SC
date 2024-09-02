from db.basic import Base
from db.tables import (search_keyword_tasks, account_info_for_spider, post_info_search_results, post_info_results,
                       user_info_search_results, user_tasks, user_info_results, user_relationship_results,
                       twitter_cookies, twitter_bot_info)

__all__ = ['KeywordTasks', 'AccountInfoForSpider', 'PostInfoSearchResults', 'UserInfoSearchResults', 'UserTasks',
           'UserInfoResults', 'UserRelationshipResults', 'PostInfoResults', 'TwitterCookies', 'TwitterBotInfo']

#######################################
# models                              #
#######################################
class AccountInfoForSpider(Base):
    __table__ = account_info_for_spider

    @classmethod
    def get_entity_name(cls):
        return cls.__name__


class KeywordTasks(Base):
    __table__ = search_keyword_tasks

    @classmethod
    def get_entity_name(cls):
        return cls.__name__


class PostInfoSearchResults(Base):
    __table__ = post_info_search_results

    @classmethod
    def get_entity_name(cls):
        return cls.__name__


class UserInfoSearchResults(Base):
    __table__ = user_info_search_results

    @classmethod
    def get_entity_name(cls):
        return cls.__name__


class UserTasks(Base):
    __table__ = user_tasks

    @classmethod
    def get_entity_name(cls):
        return cls.__name__


class UserInfoResults(Base):
    __table__ = user_info_results

    @classmethod
    def get_entity_name(cls):
        return cls.__name__


class UserRelationshipResults(Base):
    __table__ = user_relationship_results

    @classmethod
    def get_entity_name(cls):
        return cls.__name__


class PostInfoResults(Base):
    __table__ = post_info_results

    @classmethod
    def get_entity_name(cls):
        return cls.__name__


class TwitterCookies(Base):
    __table__ = twitter_cookies

    @classmethod
    def get_entity_name(cls):
        return cls.__name__


class TwitterBotInfo(Base):
    __table__ = twitter_bot_info

    @classmethod
    def get_entity_name(cls):
        return cls.__name__