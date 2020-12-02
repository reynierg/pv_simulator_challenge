import queue
import threading
import time
import typing
import unittest

from services.meter.messaging_thread import MessagingThread
from services.meter.mq_sender import DUMMY_MQ_SENDER_NAME, MQSender, MQSenderFactory
from services.meter.tests.unit import LOGGER_NAME


class MessagingThreadTestCase(unittest.TestCase):
    """Provides test cases related to the functioning of a `MessagingThread`."""

    def setUp(self) -> None:
        """Will be executed by the test runner, before a test case method gets executed.

        Creates a dummy `MQSender`, that will proxy every received message to the specified observer object.
        Initialize a `MessagingThread` with the created dummy MQSender, and fires its execution.
        """

        self._wait_time_out: int = 1
        self._dummy_mq_sender: MQSender = self._build_dummy_mq_sender()
        self._thread: MessagingThread = \
            self._create_thread(self._dummy_mq_sender, self._wait_time_out)

        self._thread.start()

    def tearDown(self) -> None:
        """Will be executed by the test runner, after a test case method finish its execution.

        Asks the previously created and started "MessagingThread" to abort its execution, and waits 3 second for
        it to finish.
        """

        if self._thread is not None and self._thread.is_alive():
            self._thread.stop()
            self._thread.join(self._wait_time_out + 2)

    def _build_dummy_mq_sender(self) -> MQSender:
        """Creates a dummy `MQSender` for testing purpose.

        The dummy `MQSender` will proxy every received message to the specified "observer" object.
        The observer object being used, is the instance of the `MessagingThreadTestCase` that is currently running.

        Returns
        -------
        MQSender
            A dummy `MQSender` implementation.
        """

        return MQSenderFactory\
            .get_mq_sender(DUMMY_MQ_SENDER_NAME,
                           logger_name=LOGGER_NAME,
                           observer=self)

    def _create_thread(self, mq_sender: MQSender, wait_timeout: int) -> MessagingThread:
        """Creates a `MessagingThread` execution thread.

        To it is being passed a `queue.Queue` to be used, for push to it the, the generated Meter's values to be sent.
        This `queue.Queue` is used to store a generated Meter's value that the `MessagingThread` thread
        will pick up and send using the `mq_sender` specified.

        Parameters
        ----------
        mq_sender : MQSender
            Specifies the the object to be used to communicate with the AMQP server.
        wait_timeout : int
            Species the the `wait_timeout` that sleeps the `MessagingThread` waiting for a pending meter
            value in the `queue.Queue` yo be sent.

        Returns
        -------
        MessagingThread
            A `MessagingThread` that will process the `queue.Queue` pending Meter's values to be sent.
        """

        self._thread_queue: queue.Queue = queue.Queue()
        self._thread_event: threading.Event = threading.Event()
        thread: MessagingThread = \
            MessagingThread(self._thread_queue,
                            self._thread_event,
                            mq_sender,
                            LOGGER_NAME,
                            wait_timeout)

        return thread

    def _post_message(self, message_payload: typing.Dict[str, str]) -> None:
        """Callback that will be called by a dummy `MQSender` with a message payload.

        This method will get called when the corresponding `MessagingThread` thread,
        tries to send a message through the corresponding `MQSender`.

        Parameters
        ----------
        message_payload : dict
            Message's payload.
        """

        self._received_message_payload = message_payload

    def test_abort_thread_execution(self) -> None:
        """Verifies that a `MessagingThread` aborts properly its executions when so asked.
        """

        self._thread.stop()
        self._thread.join(self._wait_time_out + 2)
        self.assertFalse(self._thread.is_alive())

    def test_thread_post_message_through_mq(self) -> None:
        """Verifies that a `MessagingThread` use a `MQSender` to sent a Meter's generated value.

        Verifies that when a meter value is put in the `queue.Queue` being used for communication with a running
        `MessagingThread` thread, the thread picks it up, and sends it using the corresponding dummy `MQSender`.
        """

        meter_value: int = 100
        self._thread_queue.put(meter_value)
        time.sleep(2)
        self.assertEqual(meter_value,
                         self._received_message_payload[self._thread.METER_POWER_FIELD_NAME])
