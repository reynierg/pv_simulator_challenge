import logging
import logging.config
import os
import pathlib
import random
import typing

from services.meter import constants as constants


def random_meter_value_generator() -> typing.Generator[int, None, None]:
    """Generates a random Meter value between [constants.MIN_METER_VALUE, constants.MAX_METER_VALUE].
    """

    while True:
        yield random.randint(constants.MIN_METER_VALUE, constants.MAX_METER_VALUE)


def initialize_logger(current_dir: pathlib.Path) -> logging.Logger:
    """Initialize the main logger to be used in the application.

    Parameters
    ----------
    current_dir : pathlib.Path
        Refers the parent directory

    Returns
    -------
    logging.Logger
        logger to be used
    """

    # print("initialize_logger()")
    logging_leve: int = constants\
        .LOGGING_LEVELS_MAPPING\
        .get(os.getenv("LOG_LEVEL", constants.DEFAULT_LOG_LEVEL), constants.DEFAULT_LOG_LEVEL)

    log_file_handler = constants.LOGGING["handlers"]["loghandler"]
    log_file_handler["level"] = logging_leve
    file_path = str(current_dir / "data" / log_file_handler["filename"])
    # print(f"file_path: {file_path}")
    log_file_handler["filename"] = file_path
    constants.LOGGING["loggers"][constants.LOGGER_NAME]["level"] = logging_leve

    # print("constants.LOGGING:")
    # pprint.pprint(constants.LOGGING)
    logging.config.dictConfig(constants.LOGGING)
    logger: logging.Logger = logging.getLogger(constants.LOGGER_NAME)
    logger.debug(f"initialize_logger(current_dir={current_dir})")
    return logger


def initialize_nose_logger() -> logging.Logger:
    """Configures the logger to be used by the "nose" package.
    """

    print("initialize_nose_logger()")
    logger_config = {
        "version": 1,
        "disable_existing_loggers": False,
        "handlers": {
            "console": {
                "level": logging.DEBUG,
                "class": "logging.StreamHandler",
                "formatter": "verbose"
            }
        },
        "formatters": {
            "verbose": {
                "format": "%(levelname)s|%(threadName)s|%(asctime)s|"
                          "%(filename)s:%(lineno)s - %(funcName)10s(): %(message)s",
                "datefmt": "%d/%b/%Y %H:%M:%S"
            },
        },
        "loggers": {
            "nose.core": {
                "handlers": ["console"],
                "level": logging.DEBUG,
                "propagate": False
            }
        }
    }

    logging.config.dictConfig(logger_config)
    return logging.getLogger("nose.core")
