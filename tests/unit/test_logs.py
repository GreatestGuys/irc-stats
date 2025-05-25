import unittest
import os
import datetime
import time
from web.logs import LogQueryEngine

class TestLogQueryEngine(unittest.TestCase):
    def setUp(self):
        # Construct the path to the sample log file
        # This assumes tests are run from the project root directory
        base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))) # Should give project root
        sample_log_path = os.path.join(base_dir, 'tests', 'data', 'sample_log.json')
        # Fallback for simpler execution environment if the above is too complex
        if not os.path.exists(sample_log_path):
            sample_log_path = os.path.join('tests', 'data', 'sample_log.json')

        self.log_engine = LogQueryEngine(log_file_path=sample_log_path)
        self.assertTrue(self.log_engine.logs, "Logs should be loaded from sample file")
        # Expected day for all entries in sample_log.json
        self.expected_day_tuple = (2023, 3, 15)
        self.expected_timestamp_day_start = time.mktime(datetime.datetime(2023, 3, 15, 0, 0, 0).timetuple())
        self.expected_timestamp_month_start = time.mktime(datetime.datetime(2023, 3, 1, 0, 0, 0).timetuple())


    def test_initialization_no_file(self):
        # Test that LogQueryEngine initializes with empty logs if file not found
        non_existent_path = os.path.join('tests', 'data', 'non_existent_log.json')
        temp_engine = LogQueryEngine(log_file_path=non_existent_path)
        self.assertEqual(temp_engine.logs, [])

    def test_count_occurrences_simple(self):
        # "Cosmo" appears as a nick twice, and in one message by Graham.
        # Search for "Cosmo" in messages.
        count = self.log_engine.count_occurrences("Cosmo")
        self.assertEqual(count, 2) # "Hi Cosmo!" and "Another message from Cosmo."

        count_search = self.log_engine.count_occurrences("search")
        self.assertEqual(count_search, 1) # "Testing the new search functionality."

    def test_count_occurrences_nick_filter(self):
        # Nick "Cosmo" has two messages: "Hello everyone!" and "Another message from Cosmo."
        count_cosmo_message = self.log_engine.count_occurrences("message", nick="Cosmo")
        self.assertEqual(count_cosmo_message, 1) # "Another message from Cosmo."
        
        count_cosmo_hello = self.log_engine.count_occurrences("Hello", nick="Cosmo")
        self.assertEqual(count_cosmo_hello, 1) # "Hello everyone!"

        count_graham_cosmo = self.log_engine.count_occurrences("Cosmo", nick="Graham")
        self.assertEqual(count_graham_cosmo, 1) # "Hi Cosmo! TNANK for the warm welcome."

    def test_count_occurrences_no_match(self):
        count = self.log_engine.count_occurrences("nonexistentword")
        self.assertEqual(count, 0)
        
        count_nick_no_match = self.log_engine.count_occurrences("Hello", nick="Jesse") # Jesse's message is "Testing..."
        self.assertEqual(count_nick_no_match, 0)

    def test_count_occurrences_case_insensitivity(self):
        count_case_sensitive = self.log_engine.count_occurrences("hello") # Default is case sensitive
        self.assertEqual(count_case_sensitive, 0)
        
        count_case_insensitive = self.log_engine.count_occurrences("hello", ignore_case=True)
        self.assertEqual(count_case_insensitive, 1) # "Hello everyone!"

    def test_get_valid_days(self):
        valid_days = self.log_engine.get_valid_days()
        # All logs are on 2023-03-15
        expected_days = [self.expected_day_tuple]
        self.assertEqual(valid_days, expected_days)

    def test_get_all_days(self):
        all_days = self.log_engine.get_all_days()
        # All logs are on 2023-03-15, so range is just that day
        expected_days = [self.expected_day_tuple]
        self.assertEqual(all_days, expected_days)
        
    def test_get_logs_by_day(self):
        logs_by_day = self.log_engine.get_logs_by_day()
        self.assertIn(self.expected_day_tuple, logs_by_day)
        self.assertEqual(len(logs_by_day[self.expected_day_tuple]), 4) # All 4 entries are on this day

        # Check one of the log messages
        self.assertEqual(logs_by_day[self.expected_day_tuple][0]['message'], "Hello everyone!")

    def test_query_logs_simple_match(self):
        # Test querying for "Hello"
        results = self.log_engine.query_logs("Hello")
        self.assertEqual(len(results), 1) 
        self.assertEqual(results[0]['x'], self.expected_timestamp_day_start)
        self.assertEqual(results[0]['y'], 1) # One occurrence of "Hello"

        # Test querying for "Cosmo" (in messages)
        results_cosmo = self.log_engine.query_logs("Cosmo")
        self.assertEqual(len(results_cosmo), 1)
        self.assertEqual(results_cosmo[0]['x'], self.expected_timestamp_day_start)
        self.assertEqual(results_cosmo[0]['y'], 2) # "Hi Cosmo!" and "Another message from Cosmo."

    def test_query_logs_no_match(self):
        results = self.log_engine.query_logs("nonexistentword")
        # query_logs fills in days, so we expect one entry for the day, with 0 count
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]['x'], self.expected_timestamp_day_start)
        self.assertEqual(results[0]['y'], 0)

    def test_query_logs_case_insensitivity(self):
        results_sensitive = self.log_engine.query_logs("hello")
        self.assertEqual(len(results_sensitive), 1)
        self.assertEqual(results_sensitive[0]['y'], 0)

        results_insensitive = self.log_engine.query_logs("hello", ignore_case=True)
        self.assertEqual(len(results_insensitive), 1)
        self.assertEqual(results_insensitive[0]['x'], self.expected_timestamp_day_start)
        self.assertEqual(results_insensitive[0]['y'], 1)

    def test_query_logs_nick_filter(self):
        # Nick Cosmo's message: "Hello everyone!" and "Another message from Cosmo."
        results = self.log_engine.query_logs("message", nick="Cosmo")
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]['x'], self.expected_timestamp_day_start)
        self.assertEqual(results[0]['y'], 1) # "Another message from Cosmo."

        results_graham = self.log_engine.query_logs("Cosmo", nick="Graham")
        self.assertEqual(len(results_graham), 1)
        self.assertEqual(results_graham[0]['x'], self.expected_timestamp_day_start)
        self.assertEqual(results_graham[0]['y'], 1) # "Hi Cosmo! TNANK for the warm welcome."
        
        results_jesse_hello = self.log_engine.query_logs("Hello", nick="Jesse")
        self.assertEqual(len(results_jesse_hello), 1)
        self.assertEqual(results_jesse_hello[0]['y'], 0)


    def test_query_logs_cumulative(self):
        # Search for "o", appears in "Hello everyone!", "Hi Cosmo!", "Another message from Cosmo."
        # Day 1: 3 occurrences
        results = self.log_engine.query_logs("o", cumulative=True, ignore_case=True)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]['x'], self.expected_timestamp_day_start)
        self.assertEqual(results[0]['y'], 3) # Total 3 "o"s on that day

    def test_query_logs_coarse_time(self):
        # Coarse time groups by month. All data is March 2023.
        results = self.log_engine.query_logs("Hello", coarse=True)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]['x'], self.expected_timestamp_month_start) # Timestamp for start of March
        self.assertEqual(results[0]['y'], 1) # One "Hello" in that month

    def test_query_logs_normalize(self):
        # Test with "welcome". Graham: "Hi Cosmo! TNANK for the warm welcome." (1 line)
        # Total lines on that day: 4
        # Normalized: 1/4 = 0.25
        # Note: query_logs normalizes based on total lines in the window, not just matched lines.
        # For a single day, it's matched_on_day / total_on_day
        results = self.log_engine.query_logs("welcome", normalize=True, ignore_case=True)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]['x'], self.expected_timestamp_day_start)
        self.assertAlmostEqual(results[0]['y'], 1/4)

        # Test with "Cosmo" (in messages): "Hi Cosmo!", "Another message from Cosmo." (2 lines)
        # Normalized: 2/4 = 0.5
        results_cosmo = self.log_engine.query_logs("Cosmo", normalize=True)
        self.assertEqual(len(results_cosmo), 1)
        self.assertEqual(results_cosmo[0]['x'], self.expected_timestamp_day_start)
        self.assertAlmostEqual(results_cosmo[0]['y'], 2/4)

    def test_search_day_logs_simple(self):
        # Search for "Hello"
        results = self.log_engine.search_day_logs("Hello")
        self.assertEqual(len(results), 1)
        day_tuple, index, line, start_char, end_char = results[0]
        self.assertEqual(day_tuple, self.expected_day_tuple)
        # self.assertEqual(index, 0) # This depends on the internal order within the day which is preserved.
        self.assertEqual(line['message'], "Hello everyone!")
        self.assertEqual(line['nick'], "Cosmo")
        self.assertEqual(start_char, 0)
        self.assertEqual(end_char, 5)

        # Search for "Cosmo" (in messages)
        results_cosmo = self.log_engine.search_day_logs("Cosmo")
        self.assertEqual(len(results_cosmo), 2)
        # First result (reverse sorted by day, then by log order)
        self.assertEqual(results_cosmo[0][2]['message'], "Another message from Cosmo.")
        self.assertEqual(results_cosmo[0][3], 19) # Start index of "Cosmo"
        self.assertEqual(results_cosmo[0][4], 24) # End index of "Cosmo"
        # Second result
        self.assertEqual(results_cosmo[1][2]['message'], "Hi Cosmo! TNANK for the warm welcome.")
        self.assertEqual(results_cosmo[1][3], 3) # Start index of "Cosmo"
        self.assertEqual(results_cosmo[1][4], 8) # End index of "Cosmo"


    def test_search_day_logs_no_match(self):
        results = self.log_engine.search_day_logs("nonexistentword")
        self.assertEqual(len(results), 0)

    def test_search_day_logs_case_insensitivity(self):
        results_sensitive = self.log_engine.search_day_logs("hello")
        self.assertEqual(len(results_sensitive), 0)

        results_insensitive = self.log_engine.search_day_logs("hello", ignore_case=True)
        self.assertEqual(len(results_insensitive), 1)
        self.assertEqual(results_insensitive[0][2]['message'], "Hello everyone!")


    def test_search_results_to_chart(self):
        # Search for "Hello"
        chart_data = self.log_engine.search_results_to_chart("Hello")
        self.assertEqual(len(chart_data), 1)
        self.assertEqual(chart_data[0]['key'], '')
        
        values = chart_data[0]['values']
        self.assertEqual(len(values), 1) # One month (coarse grouping by default for charts from search_results_to_chart)
        # The key for search_results_to_chart is month start
        self.assertEqual(values[0]['x'], self.expected_timestamp_month_start)
        self.assertEqual(values[0]['y'], 1) # One "Hello"

        # Search for "Cosmo" (in messages)
        chart_data_cosmo = self.log_engine.search_results_to_chart("Cosmo")
        values_cosmo = chart_data_cosmo[0]['values']
        self.assertEqual(len(values_cosmo), 1)
        self.assertEqual(values_cosmo[0]['x'], self.expected_timestamp_month_start)
        self.assertEqual(values_cosmo[0]['y'], 2) # Two messages contain "Cosmo"

    def test_search_results_to_chart_no_match(self):
        chart_data = self.log_engine.search_results_to_chart("nonexistentword")
        self.assertEqual(len(chart_data), 1)
        values = chart_data[0]['values']
        self.assertEqual(len(values), 1) # Still get the month entry
        self.assertEqual(values[0]['x'], self.expected_timestamp_month_start)
        self.assertEqual(values[0]['y'], 0) # Zero count

if __name__ == '__main__':
    unittest.main()
