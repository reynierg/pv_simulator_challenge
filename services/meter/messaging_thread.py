import logging
from queue import Queue, Empty
import threading

import tenacity

import services.meter.constants as constants
from services.meter.typing_custom_protocols import MQSenderProtocol


class MessagingThread(threading.Thread):
    """Send Meter's generated power values to a AMQP broker.

    Implements logic for in a dedicated execution thread, process generated meter values,
    and send them to a AMQP broker. The power meter values to be sent are acquired from the
    provided "queue".
    """

    METER_POWER_FIELD_NAME = "meter_power"

    def __init__(self,
                 queue: Queue,
                 event: threading.Event,
                 msg_queue_sender: MQSenderProtocol,
                 logger_name: str = constants.LOGGER_NAME,
                 queue_wait_timeout: int = constants.WAIT_FOR_ITEM_TIMEOUT):
        """Creates a new execution thread and initialize its internal data.

        Parameters
        ----------
        queue : queue.Queue
            Queue from which will be pick-up the Meter's generated values to be sent to the AMQP broker.
        event : threading.Event
            Will be used to implement graceful shutdown of the execution thread.
            The execution thread will continually verify if the event has been set, to in that case abort execution.
        msg_queue_sender : MQSenderProtocol
            Object to be used to post the Meter's generated power values to the AMQP broker.
        logger_name : str, optional
            Name of the logger to be use.
        queue_wait_timeout : int
            Time to wait blocked in the `queue`, waiting for a generated Meter's power value.
        """

        threading.Thread.__init__(self, name=self.__class__.__name__)
        self._log: logging.Logger = logging.getLogger(logger_name)
        self._log.debug(f"{self.__class__.__name__}.__init__()")
        # Initialize event to graceful shutdown of the messaging thread:
        self._queue: Queue = queue
        self._event: threading.Event = event
        self._msg_queue_sender: MQSenderProtocol = msg_queue_sender
        self._queue_wait_timeout: int = queue_wait_timeout

    @property
    def mq_sender(self) -> MQSenderProtocol:
        """Gets the `MQSender` being used in communication with the AMQP broker.

        Returns
        -------
        MQSenderProtocol:
            `MQSender` being used in communication with the AMQP broker
        """

        return self._msg_queue_sender

    def run(self) -> None:
        """Execution thread entry point.

        Will continually pick-up Meter's generated power values from the `queue` and sent them to the
        AMQP broker for further processing.
        """

        self._log.debug(f"{self.__class__.__name__}.run()")
        while not self._event.is_set():
            self._log.debug("Trying to get Meter power value from queue...")
            meter_power_value = None
            try:
                meter_power_value = self._queue.get(timeout=self._queue_wait_timeout)
                self._log.info(f"meter_power_value: {meter_power_value}")
                msg_payload = {
                    self.METER_POWER_FIELD_NAME: meter_power_value
                }
                self._msg_queue_sender.post_message(msg_payload)
            except Empty:
                self._log.debug("Queue is empty")
            except tenacity.RetryError:
                # If we're here, is because has been requested to abort execution.
                self._log.info("Giving up trying to communicate with the AMQP broker, because execution abortion has "
                               "been requested.")
            except Exception:
                self._log.exception("An Unknown Error has happened:")
            finally:
                if meter_power_value is not None:
                    self._queue.task_done()

            if self._event.wait(1):
                self._log.info("Event was signaled")
                break

        self._log.info(f"Aborting thread: '{self.name}' execution")

    def stop(self) -> None:
        """Notifies this instance of the `MessagingThread` thread, that it should abort execution.

        Implements graceful shutdown.
        """

        self._log.debug(f"{self.__class__.__name__}.stop()")
        self._log.info(f"Asking thread '{self.name}' to abort execution")
        self._event.set()
