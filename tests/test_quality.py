"""
Unit tests for Data Quality Validation Rules in Weather Pipeline.
Runs with standard Python unittest:
python -m unittest tests/test_quality.py
"""

import unittest
import pandas as pd
from src.pipeline import validate_and_clean, EXPECTED


class TestDataQualityRules(unittest.TestCase):

    def setUp(self):
        self.valid_record = {
            "city": "Mumbai",
            "latitude": 19.0760,
            "longitude": 72.8777,
            "observed_at": "2026-10-12T12:00:00Z",
            "temperature_c": 30.5,
            "relative_humidity_pct": 65.0,
            "precipitation_mm": 0.0,
            "wind_speed_kmh": 12.0,
            "weather_code": 0.0,
            "fetched_at_utc": "2026-10-12T12:05:00Z",
        }

    def test_valid_record_accepted(self):
        df = pd.DataFrame([self.valid_record], columns=EXPECTED)
        clean, rejected, quality = validate_and_clean(df)
        self.assertEqual(len(clean), 1)
        self.assertEqual(len(rejected), 0)
        self.assertEqual(quality.iloc[0]["quality_pass_pct"], 100.0)

    def test_temperature_anomaly_rejected(self):
        bad_rec = dict(self.valid_record)
        bad_rec["temperature_c"] = 999.0  # Above 65 deg C
        df = pd.DataFrame([bad_rec], columns=EXPECTED)
        clean, rejected, _ = validate_and_clean(df)
        self.assertEqual(len(clean), 0)
        self.assertEqual(len(rejected), 1)
        self.assertIn("temperature_out_of_plausible_range", rejected.iloc[0]["quality_issues"])

    def test_humidity_out_of_range_rejected(self):
        bad_rec = dict(self.valid_record)
        bad_rec["relative_humidity_pct"] = 150.0  # Above 100%
        df = pd.DataFrame([bad_rec], columns=EXPECTED)
        clean, rejected, _ = validate_and_clean(df)
        self.assertEqual(len(clean), 0)
        self.assertEqual(len(rejected), 1)
        self.assertIn("humidity_out_of_range", rejected.iloc[0]["quality_issues"])

    def test_negative_precipitation_rejected(self):
        bad_rec = dict(self.valid_record)
        bad_rec["precipitation_mm"] = -5.0
        df = pd.DataFrame([bad_rec], columns=EXPECTED)
        clean, rejected, _ = validate_and_clean(df)
        self.assertEqual(len(clean), 0)
        self.assertEqual(len(rejected), 1)
        self.assertIn("negative_precipitation", rejected.iloc[0]["quality_issues"])

    def test_negative_wind_speed_rejected(self):
        bad_rec = dict(self.valid_record)
        bad_rec["wind_speed_kmh"] = -15.0
        df = pd.DataFrame([bad_rec], columns=EXPECTED)
        clean, rejected, _ = validate_and_clean(df)
        self.assertEqual(len(clean), 0)
        self.assertEqual(len(rejected), 1)
        self.assertIn("negative_wind_speed", rejected.iloc[0]["quality_issues"])

    def test_missing_fields_rejected(self):
        bad_rec = dict(self.valid_record)
        bad_rec["city"] = ""
        bad_rec["temperature_c"] = None
        df = pd.DataFrame([bad_rec], columns=EXPECTED)
        clean, rejected, _ = validate_and_clean(df)
        self.assertEqual(len(clean), 0)
        self.assertEqual(len(rejected), 1)
        self.assertIn("missing_or_invalid_city", rejected.iloc[0]["quality_issues"])
        self.assertIn("missing_or_invalid_temperature_c", rejected.iloc[0]["quality_issues"])

    def test_duplicate_records_deduplicated(self):
        # Two records with exact same city and timestamp
        rec1 = dict(self.valid_record)
        rec2 = dict(self.valid_record)
        rec2["fetched_at_utc"] = "2026-10-12T12:06:00Z"
        df = pd.DataFrame([rec1, rec2], columns=EXPECTED)
        clean, rejected, quality = validate_and_clean(df)
        self.assertEqual(len(clean), 1)
        self.assertEqual(len(rejected), 0)
        self.assertEqual(quality.iloc[0]["duplicate_records_removed"], 1)


if __name__ == "__main__":
    unittest.main()
