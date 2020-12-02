import abc
import logging
import typing

import services.pv_simulator.constants as constants


class MQReceiver(abc.ABC):
    """Abstract class that define the interface, common to all Message Queue receivers

    It must be implemented by all concrete implementations of `MQReceiver`.
    It also implements some logic, common to all of them.
    """

    def __init__(self, logger_name=constants.LOGGER_NAME):
        """

        Parameters
        ----------
        logger_name : str, optional
            Name of the logger to be used,
        """

        self._log = logging.getLogger(logger_name)
        self._log.debug("MQReceiver.__init__()")

    @abc.abstractmethod
    def get_message(self, ack_on_receive: bool = False) -> typing.Optional[typing.Dict[str, str]]:
        """Connects to the corresponding AMQP broker, and tries to get a message from it.

        Parameters
        ----------
        ack_on_receive : bool, optional
            Specifies if the message should be acknowledge, without waiting to see if it was successfully processed
            (default is False)

        Returns
        -------
        dict
            a dict with the message's payload
        """

        raise NotImplementedError()

    @abc.abstractmethod
    def ack_message(self, msg_delivery_tag: str) -> None:
        """Acknowledges to the AMQP broker, a previously acquired message, represented by `msg_delivery_tag`.

        When the message is acknowledge, the AMQP broker will be frees to drop it.

        Parameters
        ----------
        msg_delivery_tag : str
            Uniquely identifies the message to be acknowledge, acquired over a connection channel.
        """

        raise NotImplementedError()

    @abc.abstractmethod
    def close_connection(self) -> None:
        """Close the connection with the AMQP broker."""

        raise NotImplementedError()
