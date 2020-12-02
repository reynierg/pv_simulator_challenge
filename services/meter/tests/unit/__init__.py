import logging
import logging.config
import os

from . import constants as constants_for_tests

LOG_FILE_NAME = "unit_tests.log"
LOGGER_NAME = "unit_tests"
LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "handlers": {
        "console": {
            "level": logging.DEBUG,
            "class": "logging.StreamHandler",
            "formatter": "verbose"
        },
        "loghandler": {
            "level": logging.DEBUG,
            "class": "logging.handlers.RotatingFileHandler",
            "filename": os.path.join(os.path.dirname(os.path.abspath(__file__)), LOG_FILE_NAME),
            "maxBytes": 1024 * 1024 * 2,  #2Mb
            "backupCount": 5,
            "formatter": "verbose"
        }
    },
    "formatters": {
        "verbose": {
            "format": "%(levelname)s|%(threadName)s|%(asctime)s|%(filename)s:%(lineno)s - %(funcName)10s(): %(message)s",
            "datefmt": "%d/%b/%Y %H:%M:%S"
        },
    },
    "loggers": {
        LOGGER_NAME: {
            "handlers": ["loghandler", "console"],
            "level": logging.DEBUG,
            "propagate": False
        }
    }
}

logging.config.dictConfig(LOGGING)
logger = logging.getLogger(LOGGER_NAME)
