import os
import warnings
from contextlib import contextmanager

from sqlalchemy.pool import NullPool
from sqlalchemy import create_engine, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker

from config.settings import db_host, db_port, db_name, db_pass, db_type, db_user
from logger import db_logger


__all__ = ['get_db_session', 'metadata', 'Base', 'create_db']


def create_db():
    mysql_engine = create_engine('{}+pymysql://{}:{}@{}:{}'.format(db_type, db_user,
                                                                   db_pass, db_host,
                                                                   db_port))
    mysql_engine.execute("CREATE DATABASE IF NOT EXISTS {};".format(db_name))


def get_engine():
    password = os.getenv('DB_PASS', db_pass)
    connect_str = "{}+pymysql://{}:{}@{}:{}/{}?charset=utf8mb4".format(
        db_type, db_user, password, db_host, db_port, db_name)
    engine = create_engine(connect_str, encoding='utf-8', poolclass=NullPool, echo=False)
    return engine


def create_all():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        # 如果数据库不存在则创建数据库
        create_db()
        # 如果表不存在则创建表，存在则啥也不做
        metadata.create_all()
        db_logger.info('init db successfully!')
        print('init db successfully!')


eng = get_engine()
Base = declarative_base()
metadata = MetaData(get_engine())
SessionFactory = sessionmaker(bind=eng, expire_on_commit=False)
global_session = SessionFactory()
# scoped_session use registry design pattern，
# during the life of a single process,
# just one session exists.
Session = scoped_session(SessionFactory)


@contextmanager
def get_db_session():
    try:
        my_session = Session()
        try:
            yield my_session
        except Exception as e:
            db_logger.error('db operate error: {}'.format(e))
            my_session.rollback()
        finally:
            my_session.close()
    except Exception as e:
        db_logger.error('uncatched exceptions {}'.format(e))
        pass
