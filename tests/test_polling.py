from __future__ import annotations

import unittest
import time

from execution.polling import remaining_cycle_sleep_sec, sleep_until_next_cycle


class PollingCadenceTests(unittest.TestCase):
    def test_remaining_sleep_targets_start_to_start_interval(self) -> None:
        self.assertEqual(
            remaining_cycle_sleep_sec(
                cycle_started_monotonic=100.0,
                interval_sec=8.0,
                now_monotonic=103.5,
            ),
            4.5,
        )

    def test_remaining_sleep_is_zero_when_cycle_overruns(self) -> None:
        self.assertEqual(
            remaining_cycle_sleep_sec(
                cycle_started_monotonic=100.0,
                interval_sec=8.0,
                now_monotonic=112.0,
            ),
            0.0,
        )

    def test_sleep_until_next_cycle_returns_actual_sleep(self) -> None:
        slept: list[float] = []
        started = time.monotonic()
        sleep_sec = sleep_until_next_cycle(
            cycle_started_monotonic=started,
            interval_sec=10.0,
            sleeper=slept.append,
        )

        self.assertGreater(sleep_sec, 9.0)
        self.assertEqual(slept, [sleep_sec])


if __name__ == "__main__":
    unittest.main()
