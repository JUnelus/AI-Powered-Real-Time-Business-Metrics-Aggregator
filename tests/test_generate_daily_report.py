import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pandas as pd

from automation.generate_daily_report import SOURCE_REQUIRED_COLUMNS, run_data_quality_checks


MONDAY = datetime(2026, 9, 28, 13, tzinfo=timezone.utc)


def source_data(now: datetime, age_hours: float) -> pd.DataFrame:
    return pd.DataFrame([{
        "symbol": "AAPL",
        "shortName": "Apple",
        "regularMarketPrice": 197.2,
        "regularMarketChangePercent": 0.8,
        "regularMarketVolume": 52000000,
        "marketCap": 3000000000000,
        "regularMarketTime": (now - timedelta(hours=age_hours)).timestamp(),
    }])


class DataQualityChecksTests(unittest.TestCase):
    def test_weekday_freshness_and_report(self):
        for weekday in (1, 2, 3, 4):  # Tuesday through Friday
            now = MONDAY + timedelta(days=weekday)
            for age in (12, 35.99, 36):
                with self.subTest(weekday=weekday, age=age):
                    report = run_data_quality_checks(source_data(now, age), True, now_utc=now)
                    self.assertEqual(report["status"], "pass")
                    self.assertEqual(report["freshness_max_age_hours"], 36)
                    self.assertEqual(report["data_age_hours"], age)
                    self.assertEqual(report["checked_at"], now.isoformat())
            for age in (36.01, 65.86):
                with self.subTest(weekday=weekday, age=age):
                    with self.assertRaisesRegex(RuntimeError, r"Freshness check failed:.*\(limit 36h\)"):
                        run_data_quality_checks(source_data(now, age), True, now_utc=now)

    def test_monday_accepts_friday_market_close(self):
        for age in (65, 65.86, 70):
            with self.subTest(age=age):
                report = run_data_quality_checks(source_data(MONDAY, age), True, now_utc=MONDAY)
                self.assertEqual(report["status"], "pass")
                self.assertEqual(report["freshness_max_age_hours"], 84)
                self.assertEqual(report["data_age_hours"], age)

    def test_weekend_and_monday_allowance_and_boundary(self):
        for weekday in (0, 5, 6):
            now = MONDAY + timedelta(days=weekday)
            for age in (48, 83.99, 84):
                with self.subTest(weekday=weekday, age=age):
                    report = run_data_quality_checks(source_data(now, age), True, now_utc=now)
                    self.assertEqual(report["status"], "pass")
                    self.assertEqual(report["freshness_max_age_hours"], 84)
            for age in (84.01, 100):
                with self.subTest(weekday=weekday, age=age):
                    with self.assertRaisesRegex(RuntimeError, r"Freshness check failed:.*\(limit 84h\)"):
                        run_data_quality_checks(source_data(now, age), True, now_utc=now)

    def test_default_clock_uses_same_time_for_check_and_report(self):
        with patch("automation.generate_daily_report.datetime", wraps=datetime) as clock:
            clock.now.return_value = MONDAY
            report = run_data_quality_checks(source_data(MONDAY, 65.86), True)
            clock.now.assert_called_once_with(timezone.utc)
        self.assertEqual(report["freshness_max_age_hours"], 84)
        self.assertEqual(report["checked_at"], MONDAY.isoformat())

    def test_weekday_is_determined_in_utc(self):
        # Still Monday locally, but Tuesday in UTC: the normal limit applies.
        local_now = datetime(2026, 9, 28, 23, tzinfo=timezone(timedelta(hours=-4)))
        report = run_data_quality_checks(source_data(local_now, 12), True, now_utc=local_now)
        self.assertEqual(report["freshness_max_age_hours"], 36)
        self.assertEqual(report["checked_at"], "2026-09-29T03:00:00+00:00")

    def test_schema_drift_is_rejected(self):
        for column in sorted(SOURCE_REQUIRED_COLUMNS):
            with self.subTest(column=column):
                source = source_data(MONDAY, 65).drop(columns=[column])
                with self.assertRaisesRegex(RuntimeError, f"Schema drift detected.*{column}"):
                    run_data_quality_checks(source, True, now_utc=MONDAY)

    def test_invalid_numeric_columns_are_rejected(self):
        for column in ("regularMarketPrice", "regularMarketChangePercent", "regularMarketVolume", "marketCap"):
            with self.subTest(column=column):
                source = source_data(MONDAY, 65).assign(**{column: "invalid"})
                with self.assertRaisesRegex(RuntimeError, f"Data quality check failed for '{column}'"):
                    run_data_quality_checks(source, True, now_utc=MONDAY)

    def test_missing_valid_timestamps_are_rejected(self):
        source = source_data(MONDAY, 65).assign(regularMarketTime="invalid")
        with self.assertRaisesRegex(RuntimeError, "has no valid timestamps"):
            run_data_quality_checks(source, True, now_utc=MONDAY)

    def test_millisecond_timestamps_remain_supported(self):
        source = source_data(MONDAY, 65.86)
        source["regularMarketTime"] *= 1000
        report = run_data_quality_checks(source, True, now_utc=MONDAY)
        self.assertEqual(report["data_age_hours"], 65.86)
        self.assertEqual(report["freshness_max_age_hours"], 84)

    def test_disabled_freshness_still_reports_applicable_threshold(self):
        for weekday, limit in ((0, 84), (4, 36)):
            now = MONDAY + timedelta(days=weekday)
            with self.subTest(weekday=weekday):
                report = run_data_quality_checks(source_data(now, 100), False, now_utc=now)
                self.assertEqual(report["freshness_max_age_hours"], limit)
                self.assertEqual(report["data_age_hours"], 100)
                source = source_data(now, 100).assign(regularMarketTime="invalid")
                report = run_data_quality_checks(source, False, now_utc=now)
                self.assertEqual(report["freshness_max_age_hours"], limit)
                self.assertIsNone(report["data_age_hours"])
                self.assertIsNone(report["latest_market_time"])


if __name__ == "__main__":
    unittest.main()
