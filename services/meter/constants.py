import logging

MQ_SENDER_TYPE = "PikaMQSender"
DEFAULT_AMQP_URI = "amqp://guest:guest@localhost"
DEFAULT_MQ_NAME = "reynier_queue"

# Specifies the time that a "MessagingThread" thread will block a queue waiting for a generated
# meter power value to be sent:
WAIT_FOR_ITEM_TIMEOUT = 1

# The randomly generated Meter value must meet the following condition:
# MIN_METER_VALUE <= meter_value <= MAX_METER_VALUE
MIN_METER_VALUE = 0
MAX_METER_VALUE = 9000

MAX_METER_QUEUE_SIZE = 50

LOGGER_NAME = "meter"
DEFAULT_LOG_LEVEL = "info"
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
            "level": "",
            "class": "logging.handlers.RotatingFileHandler",
            "filename": "app.log",
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
            "level": "",
            "propagate": False
        }
    }
}

LOGGING_LEVELS_MAPPING = {
    "critical": logging.CRITICAL,
    "fatal": logging.FATAL,
    "error": logging.ERROR,
    "warning": logging.WARNING,
    "warn": logging.WARN,
    "info": logging.INFO,
    "debug": logging.DEBUG
}
