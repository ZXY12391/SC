import os
import logging
import logging.config as log_conf
from config.settings import log_name, log_dir

__all__ = ['db_logger', 'download_logger', 'parser_logger', 'other_logger', 'spider_logger']

abs_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), log_dir)

if not os.path.exists(abs_path):
    os.mkdir(abs_path)
log_path = os.path.join(abs_path, log_name)


log_config = {
    'version': 1.0,
    'formatters': {
        'detail': {
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            'datefmt': "%Y-%m-%d %H:%M:%S"
        },
        'simple': {
            'format': '%(name)s - %(levelname)s - %(message)s',
        },
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'level': 'INFO',
            'formatter': 'detail'
        },
        'file': {
            'class': 'logging.handlers.RotatingFileHandler',
            'maxBytes': 1024 * 1024 * 2,
            'backupCount': 10,
            'filename': log_path,
            'level': 'INFO',
            'formatter': 'detail',
            'encoding': 'utf-8',
        },
    },
    'loggers': {
        'download_logger': {
            'handlers': ['console', 'file'],
            'level': 'INFO',
        },
        'parser_logger': {
            'handlers': ['console', 'file'],
            'level': 'DEBUG',
        },
        'other_logger': {
            'handlers': ['console', 'file'],
            'level': 'INFO',
        },
        'db_logger': {
            'handlers': ['console', 'file'],
            'level': 'INFO',
        },
        'spider_logger': {
            'handlers': ['console', 'file'],
            'level': 'INFO',
        }
    }
}

log_conf.dictConfig(log_config)

db_logger = logging.getLogger('db_logger')
parser_logger = logging.getLogger('parser_logger')
download_logger = logging.getLogger('download_logger')
other_logger = logging.getLogger('other_logger')
spider_logger = logging.getLogger('spider_logger')

#OK


