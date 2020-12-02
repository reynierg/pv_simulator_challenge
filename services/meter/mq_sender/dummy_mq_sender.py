import typing

import services.meter.constants as constants
from services.meter.mq_sender import MQSender


class DummyMQSender(MQSender):
    """Dummy implementation of a `MQSender`.

    Is intended to be used for testing purpose.
    """

    def __init__(self, *args, **kwargs):
        logger_name = kwargs.get("logger_name") or constants.LOGGER_NAME
        super().__init__(logger_name)
        self._log.debug(f"{self.__class__.__name__}.__init__()")
        self._observer: MQSender = kwargs.get("observer")

    def _post_message(self, message_payload: typing.Dict[str, str]) -> None:
        """Forward the method call to the `self._observer`.

        Parameters
        ----------
        message_payload : dict
            Message to be sent to the AMQP broker
        """

        self._log.debug(f"{self.__class__.__name__}._post_message(message_payload={message_payload})")
        if self._observer is not None:
            self._observer._post_message(message_payload)

    def close_connection(self) -> None:
        """Close the connection with the AMQP broker."""

        self._log.debug(f"{self.__class__.__name__}.close_connection()")
        # The dummy doesn't need to close any connection.
