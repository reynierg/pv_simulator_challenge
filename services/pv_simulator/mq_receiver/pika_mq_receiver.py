import json
import typing

import pika
import tenacity

from services.pv_simulator.mq_receiver import MQReceiver


class PikaMQReceiver(MQReceiver):
    """Provides logic to receive data from a AMQP broker, using the 'pika' library."""

    DELIVERY_TAG = "delivery_tag"

    def __init__(self, *args, **kwargs):
        """

        Parameters
        ----------
        **kwargs
            ``amqp_uri`` : str
                Uri to be used to communicate with the AMQP broker.
            ``queue_name`` : str
                Name of the queue that should be used in the AMQP broker, to store the messages.
            ``check_if_must_exit`` : callable
                Function to be called to determine when the `self._retrying_policy` must stop re-trying.
        """

        super().__init__()
        self._amqp_uri: str = kwargs.get("amqp_uri")
        self._queue_name: str = kwargs.get("queue_name")

        check_if_must_exit_callback: typing.Callable[[], bool] = kwargs.get("check_if_must_exit")
        if check_if_must_exit_callback is None:
            check_if_must_exit = lambda retry_state: False
        else:
            check_if_must_exit = lambda retry_state: check_if_must_exit_callback()
        self._check_if_must_exit: typing.Callable[[tenacity.RetryCallState], bool] = check_if_must_exit

        self._cnx: typing.Optional[pika.BlockingConnection] = None
        self._channel: typing.Optional[pika.channel.Channel] = None

        # Define re-trying policy to be used, when fails to successfully communicate with the AMQP broker:
        self._retrying_policy = tenacity.Retrying(
            wait=tenacity.wait_random_exponential(multiplier=0.5, max=30),  # Random exponential back-off before retry
            retry=tenacity.retry_if_exception_type(pika.exceptions.AMQPConnectionError),
            stop=tenacity.stop_any(self._check_if_must_exit),
            after=lambda _, __, ___: self._log.warning(f"Unable to connect to AMQP server with uri: {self._amqp_uri}")
        )

        self._log.debug(f"{self.__class__.__name__}.__init__(queue_name={self._queue_name})")

    def _reconnect(self) -> None:
        """Tries to re-connect to the AMQP broker"""

        self._log.debug(f"{self.__class__.__name__}._reconnect()")
        if self._cnx is None or self._cnx.is_closed:
            self._cnx = pika.BlockingConnection(pika.URLParameters(self._amqp_uri))
            self._channel = self._cnx.channel()
            self._channel.queue_declare(queue=self._queue_name)

    def _read_message(self, ack_on_receive: bool = False) -> typing.Optional[typing.Dict[str, str]]:
        """Reads a message from the AMQP broker.

        Parameters
        ----------
        ack_on_receive : bool, optional
            Specifies if the acquired message should be acknowledged upon reception.

        Returns
        -------
        dict:
            dict with message data, or None if no message was pending to be processed.
        """

        self._log.debug(f"{self.__class__.__name__}._read_message(ack_on_receive={ack_on_receive})")
        method_frame, header, body = self.channel.basic_get(self._queue_name)
        if method_frame:
            # An available message was successfully acquired:
            delivery_tag = method_frame.delivery_tag
            msg_payload = json.loads(body.decode())
            if ack_on_receive:
                self._ack_message(delivery_tag)
            else:
                msg_payload[self.DELIVERY_TAG] = delivery_tag

            return msg_payload

        return None

    def _reconnect_and_read_message(self, ack_on_receive: bool = False) -> typing.Optional[typing.Dict[str, str]]:
        """Tries to re-connect to the AMQP broker, and read the next pending message from it.

        Parameters
        ----------
        ack_on_receive : bool, optional
            Specifies if the acquired message should be acknowledged upon reception.

        Returns
        -------
        dict:
            dict with message data, or None if no message was pending to be processed.
        """

        self._log.debug(f"{self.__class__.__name__}._reconnect_and_read_message(ack_on_receive={ack_on_receive})")
        # Re-connect and re-try:
        self._reconnect()
        return self._read_message(ack_on_receive)

    def get_message(self, ack_on_receive: bool = False) -> typing.Optional[typing.Dict[str, str]]:
        """Tries to get a message from the AMQP broker.

        If an error occurs during communication with the AMQP broker, it will re-try the operation.

        Parameters
        ----------
        ack_on_receive : bool, optional
            Specifies if the message should be acknowledged, without waiting to see if it was successfully processed
            (default is False)

        Returns
        -------
        dict
            a dict with the message's payload
        """

        self._log.debug(f"{self.__class__.__name__}.get_message()")
        try:
            return self._read_message(ack_on_receive)
        except pika.exceptions.ConnectionClosed:
            # Re-connect and re-try:
            self._log.exception("Error:")
            return self._retrying_policy.call(self._reconnect_and_read_message, ack_on_receive=ack_on_receive)
        except pika.exceptions.AMQPConnectionError:
            # Re-connect and re-try:
            self._log.exception("Error:")
            return self._retrying_policy.call(self._reconnect_and_read_message, ack_on_receive=ack_on_receive)

    def _ack_message(self, msg_delivery_tag: str) -> None:
        """Acknowledges with the AMQP broker, a previously received message

        When the message is acknowledge, the AMQP broker will be free to drop it.

        Parameters
        ----------
        msg_delivery_tag : str
            Unique identifier in the corresponding channel, used to identify the message to be acknowledged.
        """

        self._log.debug(f"{self.__class__.__name__}._ack_message(msg_delivery_tag={msg_delivery_tag})")
        self.channel.basic_ack(msg_delivery_tag)

    def _reconnect_and_ack_message(self, msg_delivery_tag: str) -> None:
        """Tries to reconnect to the AMQP broker, and acknowledge a previously received message

        When the message is acknowledge, the AMQP broker will be free to drop it.

        Parameters
        ----------
        msg_delivery_tag : str
            Unique identifier in the corresponding channel, used to identify the message to be acknowledged.
        """

        self._log.debug(f"{self.__class__.__name__}._reconnect_and_ack_message(msg_delivery_tag={msg_delivery_tag})")
        self._reconnect()
        self._ack_message(msg_delivery_tag)

    def ack_message(self, msg_delivery_tag: str) -> None:
        """Acknowledges to the AMQP broker, a previously acquired message, represented by `msg_delivery_tag`.

        When the message is acknowledge, the AMQP broker will be free to drop it.
        If an error occurs during communication with the AMQP broker, it will re-try the operation.

        Parameters
        ----------
        msg_delivery_tag : str
            Uniquely identifies the message to be acknowledge, acquired over a connection channel.
        """

        self._log.debug(f"{self.__class__.__name__}.ack_message(msg_delivery_tag={msg_delivery_tag})")
        try:
            self._ack_message(msg_delivery_tag)
        except pika.exceptions.ConnectionClosed:
            # Re-connect and re-try:
            self._log.exception("Error:")
            self._retrying_policy.call(self._reconnect_and_ack_message, msg_delivery_tag=msg_delivery_tag)
        except pika.exceptions.AMQPConnectionError:
            # Re-connect and re-try:
            self._log.exception("Error:")
            self._retrying_policy.call(self._reconnect_and_ack_message, msg_delivery_tag=msg_delivery_tag)

    def close_connection(self) -> None:
        """Close the connection with the AMQP broker."""

        self._log.debug(f"{self.__class__.__name__}.close_connection()")
        if self._cnx is None:
            return

        try:
            self._cnx.close()
            self._channel = None
            self._cnx = None
        except pika.exceptions.ConnectionWrongStateError:
            self._log.exception("Error trying to close connection to the AMQP Server:")

    def _get_connection(self) -> pika.BlockingConnection:
        """Returns an existing connection, or creates a new one

        Returns
        -------
        pika.BlockingConnection
            Connection to be used in communication with the AMQP server.
        """

        self._log.debug(f"{self.__class__.__name__}._get_connection()")
        if self._cnx is None:
            self._cnx: pika.BlockingConnection = \
                pika.BlockingConnection(pika.URLParameters(self._amqp_uri)
            )

        return self._cnx

    def _get_channel(self) -> pika.channel.Channel:
        """Return an existing channel, or creates a new one.

        If a channel is created, also a queue with the name `self._queue_name` will be created if it already
        doesn't exist.

        Returns
        -------
        pika.channel.Channel
            Channel to be used in communication with the AMQP server.
        """

        self._log.debug(f"{self.__class__.__name__}._get_channel()")
        if self._channel is None:
            self._channel = self.connection.channel()
            self._channel.queue_declare(queue=self._queue_name)

        return self._channel

    @property
    def connection(self) -> pika.BlockingConnection:
        """Creates a connection if one already doesn't exist.

        Returns
        -------
        pika.BlockingConnection
            a connection to use in further communication with the AMQP broker
        """

        return self._get_connection()

    @property
    def channel(self) -> pika.channel.Channel:
        """Creates a connection channel if one already doesn't exist.

        Returns
        -------
        pika.channel.Channel
            a connection channel to use in further communication with the AMQP broker
        """

        return self._get_channel()
