import abc
import logging
import typing
import uuid

import services.meter.constants as constants


class MQSender(abc.ABC):
    """Abstract class that define the interface, common to all Message Queue senders

    It must be implemented by all concrete implementations of `MQSender`.
    It also implements some logic, common to all of them.
    """

    def __init__(self, logger_name=constants.LOGGER_NAME):
        """

        Parameters
        ----------
        logger_name : str, optionL
            Name of the logger to be used,
        """

        self._log = logging.getLogger(logger_name)
        self._log.debug("MQSender.__init__()")

    def post_message(self, message_payload: typing.Dict[str, str]) -> None:
        """Post or publish a message with the corresponding AMQP broker.

        Parameters
        ----------
        message_payload : dict
            Message to be sent to the AMQP broker
        """

        self._log.debug(f"MQSender.post_message(message_payload: {message_payload})")
        message_payload = self.__tag_message_with_id(message_payload)
        self._post_message(message_payload)
        self._log.info(f"Message with id '{message_payload['id']}' was successfully sent")

    @abc.abstractmethod
    def close_connection(self) -> None:
        """Close the connection with the AMQP broker."""

        raise NotImplementedError()

    @abc.abstractmethod
    def _post_message(self, message_payload: typing.Dict[str, str]) -> None:
        """Low level function used to send a message to a AMQP broker.

        Should be implemented by all concrete implementations of `MQSender`.

        Parameters
        ----------
        message_payload : dict
            Message to be sent to the AMQP broker
        """

        raise NotImplementedError()

    def __tag_message_with_id(self, message_payload: typing.Dict[str, str]) -> typing.Dict[str, str]:
        """Tags a message's payload with an unique id for message identification and tracing.

        Parameters
        ----------
        message_payload : dict
            Message to be tag with an unique id

        Returns
        -------
        dict
            dict with the tagged message
        """

        self._log.debug("MQSender.__tag_message_with_id()")
        message_payload.update({
            "id": str(uuid.uuid4())
        })
        return message_payload
