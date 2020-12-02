import unittest

import services.meter.constants as constants
import services.meter.utils as utils


class MeterValueGeneratorTestCase(unittest.TestCase):
    """Provides test cases related to the functioning of a Meter's values generator."""

    def test_random_meter_value_generator(self) -> None:
        """Verifies that the generated Meter value is in the expected interval.

        The generated value is expected to be in the range [constants.MIN_METER_VALUE, constants.MAX_METER_VALUE].
        """

        generator = utils.random_meter_value_generator()
        random_meter_value: int = next(generator)
        self.assertGreaterEqual(random_meter_value, constants.MIN_METER_VALUE)
        self.assertLessEqual(random_meter_value, constants.MAX_METER_VALUE)
