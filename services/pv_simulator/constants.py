import logging

MQ_RECEIVER_TYPE = "PikaMQReceiver"
DEFAULT_AMQP_URI = "amqp://guest:guest@localhost"
DEFAULT_MQ_NAME = "reynier_queue"

"""
The following values represent times of a day for which PV power values measures are greater than 0, 
and that fall under the PV power values curve, as depicted in the corresponding picture in the 
challenge(PV Power/minute).
The values represent a time in the day in minutes, measured from 00:00
Explanation:
336  -> 05:36
480  -> 08:00
499  -> 08:19
557  -> 09:17
576  -> 09:36
624  -> 10:24
672  -> 11:12
749  -> 12:29
864  -> 14:24
960  -> 16:00
1037 -> 17:17
1056 -> 17:36
1104 -> 18:24
1152 -> 19:12
1181 -> 19:41
1200 -> 20:00
1267 -> 21:07
"""
MINUTES_DATA_SET = [
    336, 480, 499, 557, 576,
    624, 672, 749, 864, 960,
    1037, 1056, 1104, 1152, 1181,
    1200, 1267
]

"""
The following values represent PV power values that maps to the previously specified 
times(specified in MINUTES_DATA_SET). The mapping is one to one and represent Watts:
"""
PV_POWER_VALUES_DATA_SET = [
    0, 300, 500, 1000, 1500,
    2000, 2500, 3000, 3250, 3000,
    2500, 2000, 1500, 1000, 500,
    250, 0
]

RESULTS_LOGGER_NAME = "results_values"
LOGGER_NAME = "pvsimulator"
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
            # "level": logging.DEBUG,
            "class": "logging.handlers.RotatingFileHandler",
            "filename": "app.log",
            "maxBytes": 1024 * 1024 * 2,  #2Mb
            "backupCount": 5,
            "formatter": "verbose"
        },
        "resultsloghandler": {
            "level": logging.INFO,
            "class": "logging.handlers.RotatingFileHandler",
            "filename": "results_values.log",
            "maxBytes": 1024 * 1024 * 2,  #2Mb
            "backupCount": 1000,
            "formatter": "resultsformatter"
        },
    },
    "formatters": {
        "verbose": {
            "format": "%(levelname)s|%(threadName)s|%(asctime)s|"
                      "%(filename)s:%(lineno)s - %(funcName)10s(): %(message)s",
            "datefmt": "%d/%b/%Y %H:%M:%S"
        },
        "resultsformatter": {
            "format": "%(message)s"
        },
    },
    "loggers": {
        LOGGER_NAME: {
            "handlers": ["loghandler", "console"],
            "level": "",
            "propagate": False
        },
        RESULTS_LOGGER_NAME: {
            "handlers": ["resultsloghandler"],
            "level": logging.INFO,
            "propagate": False
        }
    },
    # "root": {
    #     "level": logging.INFO,
    #     "handlers": ["loghandler", "console"],
    # }
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
