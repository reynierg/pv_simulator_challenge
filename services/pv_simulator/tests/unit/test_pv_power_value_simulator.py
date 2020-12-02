import unittest

import services.pv_simulator.constants as constants
from services.pv_simulator.pv_power_value_calculator import PVPowerValueCalculator
from services.pv_simulator.tests.unit import LOGGER_NAME


class PVPowerValueSimulatorTestCase(unittest.TestCase):
    MINUTES_AT_05_37 = 5 * 60 + 37
    MINUTES_AT_14_24 = 14 * 60 + 24
    MINUTES_AT_21_07 = 21 * 60 + 7
    MINUTES_AT_00_00 = 24 * 60

    def setUp(self) -> None:
        self._pv_power_calculator: PVPowerValueCalculator = \
            PVPowerValueCalculator(constants.MINUTES_DATA_SET,
                                   constants.PV_POWER_VALUES_DATA_SET,
                                   logger_name=LOGGER_NAME)

    def test_before_05_37_pv_power_value_is_0(self):
        """Verifies that from 00:00 to 05:37 the calculated PV power value is 0"""

        pv_power_value_expected_before_05_37 = 0
        step = 5
        for time_minute in range(0, self.MINUTES_AT_05_37, step):
            self.assertEqual(pv_power_value_expected_before_05_37,
                             self._pv_power_calculator.get_pv_power_value(time_minute))

    def test_from_05_37_to_14_24_pv_power_value_continuously_increase(self):
        """Verifies that from 05:37 to 14:24 the calculated PV power value continuously increase"""

        # Verify that every 5 minutes the calculated PV power value continuously increase
        # (is grater than the previously calculated):
        step = 5
        prev_pv_power_value = 0
        minutes = self.MINUTES_AT_05_37
        while minutes <= self.MINUTES_AT_14_24:
            pv_power_value_at_minutes = self._pv_power_calculator.get_pv_power_value(minutes)
            self.assertGreater(pv_power_value_at_minutes, prev_pv_power_value)

            prev_pv_power_value = pv_power_value_at_minutes
            minutes += step

    def test_from_14_24_to_21_07_pv_power_value_continuously_decrease(self):
        """Verifies that from 14:24 to 21:07 the calculated PV power value continuously decrease"""

        # Verify that every 5 minutes the calculated PV power value continuously decrease
        # (is lower than the previously calculated):
        step = 5
        prev_pv_power_value = self._pv_power_calculator.get_pv_power_value(self.MINUTES_AT_14_24)
        minutes = self.MINUTES_AT_14_24 + 10
        while minutes <= self.MINUTES_AT_21_07:
            pv_power_value_at_minutes = self._pv_power_calculator.get_pv_power_value(minutes)
            self.assertLess(pv_power_value_at_minutes, prev_pv_power_value)

            prev_pv_power_value = pv_power_value_at_minutes
            minutes += step

    def test_from_21_07_to_00_00_pv_power_value_is_0(self):
        """Verifies that from 21:07 to 00:00 the calculated PV power value is 0"""

        minutes = self.MINUTES_AT_21_07
        step = 5
        while minutes <= self.MINUTES_AT_00_00:
            self.assertEqual(0., self._pv_power_calculator.get_pv_power_value(minutes))
            minutes += step
