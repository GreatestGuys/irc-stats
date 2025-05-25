import unittest
import os
import time
import datetime
from web.logs import LogQueryEngine 

class TestLogQueryEngine(unittest.TestCase):
    def setUp(self):
        current_dir = os.path.dirname(os.path.abspath(__file__))
        sample_log_path = os.path.join(current_dir, "..", "data", "sample_log.json")
        self.log_engine = LogQueryEngine(log_file_path=sample_log_path)
        self.assertTrue(self.log_engine.logs, "Logs should be loaded from sample file for tests.")
        self.sample_date_dt = datetime.datetime(2023, 3, 15) 
        self.sample_timestamp = time.mktime(self.sample_date_dt.timetuple())
        self.sample_month_coarse_key_ts = time.mktime(datetime.datetime(2023, 3, 1).timetuple())

    def test_initialization_no_file(self):
        engine = LogQueryEngine(log_file_path="non_existent_log_file.json")
        self.assertEqual(engine.logs, [])

    def test_get_all_log_entries(self):
        entries = self.log_engine.get_all_log_entries()
        self.assertEqual(len(entries), 4)

    def test_count_occurrences_simple(self):
        self.assertEqual(self.log_engine.count_occurrences("Cosmo"), 2)
        self.assertEqual(self.log_engine.count_occurrences("message"), 1)
        self.assertEqual(self.log_engine.count_occurrences("TNANK"), 1)

    def test_count_occurrences_case_insensitive(self):
        self.assertEqual(self.log_engine.count_occurrences("cosmo", ignore_case=True), 2)
        self.assertEqual(self.log_engine.count_occurrences("tnank", ignore_case=True), 1)

    def test_count_occurrences_nick_filter(self):
        self.assertEqual(self.log_engine.count_occurrences("Hello", nick="Cosmo"), 1)
        self.assertEqual(self.log_engine.count_occurrences("Cosmo", nick="Graham"), 1)
        self.assertEqual(self.log_engine.count_occurrences("search", nick="Jesse"), 1)
        self.assertEqual(self.log_engine.count_occurrences("warm", nick="Graham", ignore_case=True), 1)
        self.assertEqual(self.log_engine.count_occurrences("nonexistent", nick="Cosmo"), 0)
    
    def test_count_occurrences_no_match(self):
        self.assertEqual(self.log_engine.count_occurrences("nonexistentterm"), 0)

    def test_get_logs_by_day(self):
        logs_by_day = self.log_engine.get_logs_by_day()
        self.assertIn((2023, 3, 15), logs_by_day)
        self.assertEqual(len(logs_by_day[(2023, 3, 15)]), 4)

    def test_get_valid_days(self):
        valid_days = self.log_engine.get_valid_days()
        self.assertEqual(valid_days, [(2023, 3, 15)])

    def test_get_all_days(self):
        all_days = self.log_engine.get_all_days()
        self.assertEqual(all_days, [(2023, 3, 15)])

    def test_query_logs_simple_match(self):
        results = self.log_engine.query_logs("Hello")
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["x"], self.sample_timestamp)
        self.assertEqual(results[0]["y"], 1)

        results_tnank = self.log_engine.query_logs("TNANK", ignore_case=True)
        self.assertEqual(len(results_tnank), 1)
        self.assertEqual(results_tnank[0]["x"], self.sample_timestamp)
        self.assertEqual(results_tnank[0]["y"], 1)

    def test_query_logs_no_match(self):
        results = self.log_engine.query_logs("nonexistentterm")
        self.assertEqual(len(results), 1) 
        self.assertEqual(results[0]["x"], self.sample_timestamp)
        self.assertEqual(results[0]["y"], 0)

    def test_query_logs_nick_filter(self):
        results = self.log_engine.query_logs("Cosmo", nick="Graham")
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["y"], 1)

        results_cosmo_msgs = self.log_engine.query_logs("message", nick="Cosmo")
        self.assertEqual(len(results_cosmo_msgs), 1)
        self.assertEqual(results_cosmo_msgs[0]["y"], 1)

    def test_query_logs_cumulative(self):
        results = self.log_engine.query_logs("Hello", cumulative=True)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["y"], 1)

    def test_query_logs_coarse_time(self):
        results = self.log_engine.query_logs("Hello", coarse=True)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["x"], self.sample_month_coarse_key_ts)
        self.assertEqual(results[0]["y"], 1)

        results_all_coarse = self.log_engine.query_logs(".", coarse=True)
        self.assertEqual(len(results_all_coarse), 1)
        self.assertEqual(results_all_coarse[0]["x"], self.sample_month_coarse_key_ts)
        self.assertEqual(results_all_coarse[0]["y"], 4)

    def test_query_logs_normalize_non_cumulative(self):
        results = self.log_engine.query_logs("Hello", normalize=True, cumulative=False)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["x"], self.sample_timestamp)
        self.assertAlmostEqual(results[0]["y"], 1.0/4.0)

        results_cosmo = self.log_engine.query_logs("Cosmo", normalize=True, cumulative=False)
        self.assertEqual(len(results_cosmo), 1)
        self.assertEqual(results_cosmo[0]["x"], self.sample_timestamp)
        self.assertAlmostEqual(results_cosmo[0]["y"], 2.0/4.0)

    def test_query_logs_normalize_cumulative(self):
        results = self.log_engine.query_logs("Hello", normalize=True, cumulative=True)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["x"], self.sample_timestamp)
        self.assertAlmostEqual(results[0]["y"], 1.0/4.0) 

        results_cosmo = self.log_engine.query_logs("Cosmo", normalize=True, cumulative=True)
        self.assertEqual(len(results_cosmo), 1)
        self.assertEqual(results_cosmo[0]["x"], self.sample_timestamp)
        self.assertAlmostEqual(results_cosmo[0]["y"], 2.0/4.0)

    def test_search_day_logs_simple(self):
        results = self.log_engine.search_day_logs("Hello")
        self.assertEqual(len(results), 1)
        day_tuple, index, line, start_char, end_char = results[0]
        self.assertEqual(day_tuple, (2023, 3, 15))
        self.assertEqual(line["message"], "Hello everyone!")
        self.assertEqual(line["message"][start_char:end_char], "Hello")

    def test_search_day_logs_no_match(self):
        results = self.log_engine.search_day_logs("nonexistentsearchterm")
        self.assertEqual(len(results), 0)

    def test_search_results_to_chart(self):
        chart_data = self.log_engine.search_results_to_chart("Hello")
        self.assertEqual(len(chart_data), 1) 
        self.assertEqual(chart_data[0]["key"], "")
        values = chart_data[0]["values"]
        self.assertEqual(len(values), 1) 
        self.assertEqual(values[0]["x"], self.sample_month_coarse_key_ts)
        self.assertEqual(values[0]["y"], 1)

        chart_data_all = self.log_engine.search_results_to_chart(".")
        values_all = chart_data_all[0]["values"]
        self.assertEqual(len(values_all), 1)
        self.assertEqual(values_all[0]["x"], self.sample_month_coarse_key_ts)
        self.assertEqual(values_all[0]["y"], 4)

if __name__ == "__main__":
    unittest.main()

