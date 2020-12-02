from functools import lru_cache
import logging
import typing

import services.pv_simulator.constants as constants


class PVPowerValueCalculator:
    """Generates PV power values based on the time of the day."""

    def __init__(self, timestamps_in_min: typing.List[int],
                 pv_power_values: typing.List[int],
                 logger_name=constants.LOGGER_NAME):
        """
        Parameters
        ----------
        timestamps_in_min : list
            List of timestamps during the day. The first value will represent the last time that the PV power value
            was 0, before starting to increase. The last value will be the first time that the PV power value was 0,
            while decreasing his value. Is ordered in ascending order.
        pv_power_values : list
            List of PV power values during the day. The first and the last value will be 0. The count of items in this
            list and in `timestamps_in_min` must be the same.
        logger_name : str
        """

        self._log = logging.getLogger(logger_name)
        self._log\
            .debug(f"{self.__class__.__name__}"
                   f".__init__(timestamps_in_min={timestamps_in_min}, pv_power_values={pv_power_values})")

        if len(timestamps_in_min) != len(pv_power_values):
            raise Exception(f"Lists timestamps_in_min and pv_power_values must have the same count of items")

        self._timestamps_in_min = timestamps_in_min
        self._min_minute = min(timestamps_in_min)
        self._log.debug(f"self._min_minute={self._min_minute}")
        self._max_minute = max(timestamps_in_min)
        self._log.debug(f"self._max_minute={self._max_minute}")
        self._pv_power_values = pv_power_values
        self._precalculated_pw_values = dict()
        for idx, item in enumerate(timestamps_in_min):
            # Hold a dict with minutes as key and PV power values as associated values:
            self._precalculated_pw_values[item] = pv_power_values[idx]

    def _calculate_slope(self, pt1_idx: int, pt2_idx: int) -> float:
        """Calculates the slope of a line specified by two points.

        The points are represented by the values stored in the object attribute `self._timestamps_in_min` in the
        indices indicated by the arguments `pt1_idx` and `pt2_idx`.

        Parameters
        ----------
        pt1_idx : int
            The index of the first point in the timestamps list
        pt2_idx : int
            The index of the second point in the timestamps list

        Returns
        -------
        float
             the slope of the line
        """

        self._log.debug(f"{self.__class__.__name__}._calculate_slope(pt1_idx={pt1_idx}, pt2_idx={pt2_idx})")
        x1, x2 = self._timestamps_in_min[pt1_idx], self._timestamps_in_min[pt2_idx]
        y1, y2 = self._pv_power_values[pt1_idx], self._pv_power_values[pt2_idx]

        divisor = x2 - x1
        if divisor == 0:
            return 0

        return (y2 - y1) / divisor

    def _find_timestamp_segment(self, minute: int) -> int:
        """Finds the index of the timestamps segment where fall the specified `minute` value.

        The search is effectuated in the list of daily timestamps `self._timestamps_in_min`.
        The index of the first minute in the time segment would be returned.
        Examples:
        1.
        self._timestamps_in_min = [0, 10, 20, 30]
        minute = 8
        Like 0 < 8 < 10, 0 will be returned
        2.
        self._timestamps_in_min = [0, 10, 20, 30]
        minute = 22
        Like 20 < 22 < 30, 2 will be returned

        Parameters
        ----------
        minute : int
            The minute for which we want to determine his time interval

        Returns
        -------
        int
           index of the timestamps segment where the `minute` is included.
        """

        self._log.debug(f"{self.__class__.__name__}._find_timestamp_segment(minute={minute})")
        return self._find_timestamps_segment_index(minute, 0, len(self._timestamps_in_min) - 1)

    def _find_timestamps_segment_index(self, minute: int, start_idx: int, stop_idx: int) -> int:
        """Finds the index of the timestamps segment where fall the specified `minute` value.

        A binary search is effectuated in the list of daily timestamps `self._timestamps_in_min`.

        Parameters
        ----------
        minute : int
            The minute for which we want to determine his time interval
        start_idx : int
            The index of the list where the search should start
        stop_idx : int
            The index of the list where the search should end

        Returns
        -------
        int
           index of the timestamps segment where the `minute` is included.
        """

        self._log.debug(f"{self.__class__.__name__}._find_timestamps_segment_index(minute={minute}, "
                        f"start_idx={start_idx}, stop_idx={stop_idx})")
        middle_index = start_idx + (stop_idx - start_idx) // 2
        if minute > self._timestamps_in_min[middle_index]:
            # Search to the right of middle_index:
            if minute < self._timestamps_in_min[middle_index + 1]:
                # Container minutes interval founded:
                return middle_index

            return self._find_timestamps_segment_index(minute, middle_index + 1, stop_idx)
        else:
            # Search to the right of middle_index:
            if minute > self._timestamps_in_min[middle_index - 1]:
                # Container minutes interval founded:
                return middle_index - 1

            return self._find_timestamps_segment_index(minute, start_idx, middle_index - 1)

    @lru_cache(maxsize=120)
    def get_pv_power_value(self, minute: int) -> float:
        """Calculates a PV power value.

        The PV power value is calculated interpolating the values on the lists of daily timestamps and PV power values
        as depicted in the curve in the graphic.

        Parameters
        ----------
        minute : int
            The daily timestamp for which a PV power value will be estimated.

        Returns
        -------
        float
             estimated PV power value.
        """

        self._log.debug(f"{self.__class__.__name__}.get_pv_power_value(minute={minute})")
        # PV value must be 0 in accordance to the graphic curve in the following interval:
        if minute <= self._min_minute or minute >= self._max_minute:
            return 0

        pw_value = self._precalculated_pw_values.get(minute)
        if pw_value is not None:
            return pw_value

        # Get the index of the start minute of the timestamps segment, in which fall the value of
        # "minute" variable.
        time_segment_start_idx = self._find_timestamp_segment(minute)

        """
        Calculate the slope of the curve P1P2. This will represent the slope of the segment of the curve
        where is the point, whose X coordinate is represented by the value contained in "minute" variable.
        If the points P1 and P2 are near enough, the segment between P1 and P2 of the curve, could be
        assumed to be represented as a lineal function, instead of a polynomial curve.
        """
        slope = self._calculate_slope(time_segment_start_idx, time_segment_start_idx + 1)
        # Calculate the PV power value:
        # Py = slope * (Px - P1x) + P1y:
        return slope * (minute - self._timestamps_in_min[time_segment_start_idx]) +\
               self._pv_power_values[time_segment_start_idx]
