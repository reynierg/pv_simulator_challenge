import logging
import os
import pathlib
import queue
import threading
import typing

from dotenv import load_dotenv

import services.meter.constants as constants
from services.meter.main_loop import MainLoop
from services.meter.typing_custom_protocols import MessagingThreadProtocol
from services.meter.messaging_thread import MessagingThread
from services.meter.mq_sender import MQSender, MQSenderFactory
from services.meter import utils


current_dir: pathlib.Path = pathlib.Path(__file__).parent.absolute()

# Load into the corresponding environments variables, the content read from the specified ".env" file:
load_dotenv(dotenv_path=f"{current_dir}/.env")


def get_test_modules_names() -> typing.List[str]:
    """Gets the list of test modules to be run

    Returns
    -------
    list
        list of tests modules to be run by the test runner.
    """

    from services.meter.tests.unit import constants_for_tests
    return constants_for_tests.TESTS_MODULES


def create_messaging_thread(meter_values_queue: queue.Queue) -> MessagingThreadProtocol:
    """Creates a `MessagingThread` to be used to handle the generated Meter power values.

    Parameters
    ----------
    meter_values_queue : queue.Queue
        Queue from were will a `MessagingThread` consume the pending Meter's generated values.

    Returns
    -------
    MessagingThreadProtocol:
        the instantiated `MessagingThread`
    """

    # Build a AMQP message queue sender to be used by the messaging thread. Pass to it the same Event passed to the
    # created messaging thread, to be able to gracefully handle execution abortion:
    event = threading.Event()
    mq_sender = MQSenderFactory.get_mq_sender(constants.MQ_SENDER_TYPE, alertable_event=event)
    return MessagingThread(meter_values_queue, event, mq_sender)


def main(sys_argv: typing.List[str]) -> None:
    """Meter simulator execution entry point.

    Parameters
    ----------
    sys_argv : list
        contains the list of arguments passed to the CLI during its execution. The first argument contains the
        executed script name.
    """

    main_logger: typing.Optional[logging.Logger] = None
    try:
        must_exit_after_24h = os.getenv("MUST_EXIT_AFTER_24H", "0")
        must_exit_after_24h = \
            True if must_exit_after_24h.isdecimal() and int(must_exit_after_24h) == 1 else False

        main_logger = utils.initialize_logger(current_dir)
        main_loop: MainLoop = MainLoop(constants.LOGGER_NAME,
                                       constants.MAX_METER_QUEUE_SIZE,
                                       current_dir,
                                       must_exit_after_24h,
                                       create_messaging_thread,
                                       utils.random_meter_value_generator,
                                       tests_modules_names_provider=get_test_modules_names)

        main_loop.handle_arguments(sys_argv)
    except KeyboardInterrupt:
        if main_logger is not None:
            main_logger.exception("Required to abort:")
        else:
            import traceback
            traceback.print_exc()

    except Exception:
        if main_logger is not None:
            main_logger.exception("Error:")
        else:
            import traceback
            traceback.print_exc()
