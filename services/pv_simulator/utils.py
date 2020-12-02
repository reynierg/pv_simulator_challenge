import logging
import logging.config
import os
import pathlib
import pprint
import sys
import typing

import services.pv_simulator.constants as constants


def initialize_loggers(current_dir_path: pathlib.Path) -> logging.Logger:
    print("initialize_loggers()")
    logging_leve: int = constants\
        .LOGGING_LEVELS_MAPPING\
        .get(os.getenv("LOG_LEVEL", constants.DEFAULT_LOG_LEVEL), constants.DEFAULT_LOG_LEVEL)

    log_file_handler = constants.LOGGING["handlers"]["loghandler"]
    log_file_handler["level"] = logging_leve
    file_path = str(current_dir_path / "data" / log_file_handler["filename"])
    print(f"file_path: {file_path}")
    log_file_handler["filename"] = file_path
    constants.LOGGING["loggers"][constants.LOGGER_NAME]["level"] = logging_leve

    results_file_handler = constants.LOGGING["handlers"]["resultsloghandler"]
    file_path = str(current_dir_path / "data" / results_file_handler["filename"])
    results_file_handler["filename"] = file_path

    print("constants.LOGGING")
    pprint.pprint(constants.LOGGING)
    logging.config.dictConfig(constants.LOGGING)
    main_logger = logging.getLogger(constants.LOGGER_NAME)
    main_logger.info("Logger was successfully initialized")
    # results_logger = logging.getLogger(constants.RESULTS_LOGGER_NAME)
    return main_logger


def initialize_nose_logger() -> logging.Logger:
    """
    Configures the logger to be used by the "nose" package.
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
