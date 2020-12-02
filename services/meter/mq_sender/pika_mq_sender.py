import json
import typing

import pika
import tenacity

from services.meter.mq_sender import MQSender


class PikaMQSender(MQSender):
    """Provides logic to send data to a AMQP broker, using the 'pika' library."""

    def __init__(self, *args, **kwargs):
        """

        Parameters
        ----------
        **kwargs
            ``amqp_uri`` : str
                Uri to be used to communicate with the AMQP broker.
            ``queue_name`` : str
                Name of the queue that should be used in the AMQP broker, to store the messages.
            ``alertable_event`` : threading.Event
                Event to be used to determine when the `self._retrying_policy` must stop re-trying.
        """

        super().__init__()
        self._amqp_uri: str = kwargs.get("amqp_uri")
        self._queue_name: str = kwargs.get("queue_name")
        self._alertable_event = kwargs.get("alertable_event")
        self._cnx: typing.Optional[pika.BlockingConnection] = None
        self._channel: typing.Optional[pika.channel.Channel] = None

        # Define re-trying policy to be used, when fails to successfully communicate with the AMQP broker:
        self._retrying_policy = tenacity.Retrying(
            wait=tenacity.wait_random_exponential(multiplier=0.5, max=30),  # Random exponential back-off before retry
            retry=tenacity.retry_if_exception_type(pika.exceptions.AMQPConnectionError),
            stop=tenacity.stop.stop_when_event_set(self._alertable_event),
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

    def _publish(self, message_payload: typing.Dict[str, str]) -> None:
        """Post a message to the corresponding AMQP server.

        Parameters
        ----------
        message_payload : dict
            Message to be post.
        """

        self._log.debug(f"{self.__class__.__name__}._publish(message_payload={message_payload})")
        self.channel.basic_publish(
            exchange='',
            routing_key=self._queue_name,
            body=json.dumps(message_payload)
        )

    def _reconnect_and_publish(self, message_payload: typing.Dict[str, str]) -> None:
        """Tries to re-connect to the AMQP broker and post to it the specified message

        Parameters
        ----------
        message_payload : dict
            Message to be sent to the AMQP broker
        """

        self._log.debug(f"{self.__class__.__name__}._reconnect_and_publish(message_payload={message_payload})")
        # Re-connect and re-try:
        self._reconnect()
        self._publish(message_payload)

    def _post_message(self, message_payload: typing.Dict[str, str]) -> None:
        """Send a message to the corresponding AMQP broker.

        The default exchange, empty exchange "" would be used, so for the routing key will be enough with the name
        of the queue to be used.

        Parameters
        ----------
        message_payload : dict
            Message to be send to the AMQP broker

        Raises
        ------
        tenacity.RetryError:
            Will be raised when, while re-trying an operation that failed because an error in communication with
            the AMQP broker, the "stop" condition of the self._retrying_policy is fulfilled. Specifically, this will
            happen when the `self._alertable_event` gets signaled by the main execution thread.
        """

        self._log.debug(f"{self.__class__.__name__}._post_message(message_payload={message_payload})")
        try:
            self._publish(message_payload)
        except pika.exceptions.ConnectionClosed:
            # Re-connect and re-try:
            self._log.exception("Error:")
            self._retrying_policy.call(self._reconnect_and_publish, message_payload=message_payload)
        except pika.exceptions.AMQPConnectionError:
            # Re-connect and re-try:
            self._log.exception("Error:")
            self._retrying_policy.call(self._reconnect_and_publish, message_payload=message_payload)
        # except Exception as ex:
        #     self._log.exception("Error:")
        #     self._log.info(f"ex's type: {type(ex)}")
        #     raise ex

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
            self._cnx = pika.BlockingConnection(pika.URLParameters(self._amqp_uri))

        return self._cnx

    def _get_channel(self) -> pika.channel.Channel:
        """Return an existing channel, or creates a new one.

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

    @property
    def queue_name(self):
        """Gets the name of the queue being used

        Returns
        -------
        str
            queue's name
        """

        return self._queue_name

    @property
    def amqp_uri(self):
        """Gets the connection uri to be used to connect to the AMQP broker.

        Returns
        -------
        str
            AMQP broker's connection uri
        """

        return self._amqp_uri
