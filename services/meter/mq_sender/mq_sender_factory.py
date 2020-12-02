import logging
import os

import services.meter.constants as constants
from services.meter.mq_sender import MQSender

DUMMY_MQ_SENDER_NAME = "DummyMQSender"
PIKA_MQ_SENDER_NAME = "PikaMQSender"

IMPORT_INDEX = {
    PIKA_MQ_SENDER_NAME: "from services.meter.mq_sender.pika_mq_sender import PikaMQSender",
    DUMMY_MQ_SENDER_NAME: "from services.meter.mq_sender.dummy_mq_sender import DummyMQSender",
}

CONFIG = {
    PIKA_MQ_SENDER_NAME: {
        # Delay environment variable evaluation. Wait for that the .env file's content has been loaded
        # into the process's environment, when the corresponding logic load it in main.py:
        "queue_name": lambda: os.getenv("QUEUE_NAME", constants.DEFAULT_MQ_NAME),
        "amqp_uri": lambda: os.getenv("AMQP_URI", constants.DEFAULT_AMQP_URI)
    },
    DUMMY_MQ_SENDER_NAME: dict(),
}


class InvalidMQSenderException(Exception):
    """Exception to be thrown when the `MQSender` type specified to be instantiated, is not recognized"""

    def __init__(self, msg):
        super().__init__(msg)


class MQSenderFactory:
    """Encapsulates logic for the instantiation of a concrete `MQSender` implementation."""

    @staticmethod
    def get_mq_sender(mq_sender_type: str, **kwargs) -> MQSender:
        """Encapsulates logic for instantiation of a concrete `MQSender` implementation.

        Parameters
        ----------
        mq_sender_type : str
            Specifies the type of the `MQSender` requested, to be instantiated.
        kwargs : dict
            Contains additional parameters to be passed during instantiation, to the concrete implementation of
            the `MQSender` being requested.

        Raises
        ------
        InvalidMQSenderException
            If an un-recognized `MQSender` type was specified to be instantiate.

        Returns
        -------
        MQSender
            Concrete implementation of the `MQSender` requested.
        """

        logger = logging.getLogger(constants.LOGGER_NAME)
        logger.debug(f"MQSenderFactory.get_mq_sender(mq_sender_type={mq_sender_type}")
        import_statement = IMPORT_INDEX.get(mq_sender_type)
        if import_statement is None:
            raise InvalidMQSenderException(f"Specified mq_sender_type is invalid: {mq_sender_type}")

        exec(import_statement)
        # Evaluate environment variables values if required:
        config = {
            k: v() if callable(v) else v for k, v in CONFIG.get(mq_sender_type).items()
        }

        if len(kwargs):
            config.update(kwargs)

        # TODO: Remove the following line, for not log the Message Queue's username and password:
        logger.info(f"config: {config}")
        return eval(mq_sender_type)(**config)
