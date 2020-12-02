import unittest

from services.meter.mq_sender import (
    PIKA_MQ_SENDER_NAME,
    MQSender,
    MQSenderFactory,
    InvalidMQSenderException
)
from services.meter.tests.unit import LOGGER_NAME


class MQSenderFactoryTestCase(unittest.TestCase):
    """Provides test cases related to the functioning of the `MQSenderFactory`"""

    def test_get_pika_mq_sender(self) -> None:
        """Verifies that `MQSenderFactory`, correctly provides an instance of a `PikaMQSender`, when asked to do so.

        Also verifies that the corresponding arguments for `queue_name` and `amqp_uri` are being passed to the
        constructor of the class "PikaMQSender" as expected.
        """

        from services.meter.mq_sender.mq_sender_factory import CONFIG
        from services.meter.mq_sender.pika_mq_sender import PikaMQSender

        mq_sender: MQSender = MQSenderFactory.get_mq_sender(PIKA_MQ_SENDER_NAME, logger_name=LOGGER_NAME)
        self.assertIsInstance(mq_sender, PikaMQSender)
        self.assertEqual(CONFIG[PIKA_MQ_SENDER_NAME]["queue_name"](),
                         mq_sender.queue_name,
                         "Message queue's name should match")
        self.assertEqual(CONFIG[PIKA_MQ_SENDER_NAME]["amqp_uri"](),
                         mq_sender.amqp_uri,
                         "Message queue's connection uri should match")

    def test_get_invalid_mq_sender(self) -> None:
        """Verifies that `MQSenderFactory` throws an exception, when the specified MQSender type is invalid.

        The exception that will be throw will be `InvalidMQSenderException`
        """

        invalid_mq_sender_type: str = "IAmANotExistentMQSenderType"
        self.assertRaises(InvalidMQSenderException,
                          MQSenderFactory.get_mq_sender,
                          invalid_mq_sender_type,
                          logger_name=LOGGER_NAME)
