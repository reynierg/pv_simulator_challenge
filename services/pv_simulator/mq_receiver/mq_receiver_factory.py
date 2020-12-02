import logging
import os

import services.pv_simulator.constants as constants
from services.pv_simulator.mq_receiver import MQReceiver

PIKA_MQ_RECEIVER_NAME = "PikaMQReceiver"

IMPORT_INDEX = {
    PIKA_MQ_RECEIVER_NAME: "from services.pv_simulator.mq_receiver.pika_mq_receiver import PikaMQReceiver"
}

CONFIG = {
    PIKA_MQ_RECEIVER_NAME: {
        # Delay environment variable evaluation. Wait for that the .env file's content has been loaded
        # into the process's environment, when the corresponding logic load it in main.py:
        "queue_name": lambda: os.getenv("QUEUE_NAME", constants.DEFAULT_MQ_NAME),
        "amqp_uri": lambda: os.getenv("AMQP_URI", constants.DEFAULT_AMQP_URI)
    }
}


class InvalidMQReceiverException(Exception):
    """Exception to be thrown when the `MQReceiver` type specified to be instantiated, is not recognized"""

    def __init__(self, msg):
        super().__init__(msg)


class MQReceiverFactory:
    """Encapsulates logic for the instantiation of a concrete `MQReceiver` implementation."""

    @staticmethod
    def get_mq_receiver(mq_receiver_type: str, **kwargs) -> MQReceiver:
        """Encapsulates logic for instantiation of a concrete `MQReceiver` implementation.

        Parameters
        ----------
        mq_receiver_type : str
            Specifies the type of the `MQReceiver` requested, to be instantiated.
        kwargs : dict
            Contains additional parameters to be passed during instantiation, to the concrete implementation of
            the `MQReceiver` being requested.

        Raises
        ------
        InvalidMQReceiverException
            If an un-recognized `MQReceiver` type was specified to be instantiate.

        Returns
        -------
        MQReceiver
            Concrete implementation of the `MQReceiver` requested.
        """

        logger = logging.getLogger(constants.LOGGER_NAME)
        logger.debug(f"MQReceiverFactory.get_mq_receiver(mq_receiver_type={mq_receiver_type}")
        import_statement = IMPORT_INDEX.get(mq_receiver_type)
        if import_statement is None:
            raise InvalidMQReceiverException(f"Specified mq_receiver_type is invalid: {mq_receiver_type}")

        exec(import_statement)
        # Evaluate environment variables values if required:
        config = {
            k: v() if callable(v) else v for k, v in CONFIG.get(mq_receiver_type).items()
        }

        if len(kwargs):
            config.update(kwargs)

        # TODO: Remove the following line, for not log the Message Queue's username and password:
        logger.info(f"config: {config}")
        return eval(mq_receiver_type)(**config)
