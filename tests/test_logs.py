import unittest
import os
import sys
import time
import datetime

# Add the parent directory to the Python path to allow importing web.logs
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from web.logs import LogQueryEngine

class TestLogQueryEngine(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sample_log_path = os.path.join(os.path.dirname(__file__), 'test_log_sample.json')
        cls.log_engine = LogQueryEngine(log_file_path=cls.sample_log_path)

    def test_query_logs_simple_match(self):
        results = self.log_engine.query_logs("hello")
        self.assertEqual(len(results), 2) # Two days in sample data
        # Day 1: 1 "hello"
        self.assertEqual(results[0]['y'], 1)
        # Day 2: "HELLO COSMO" does not match "hello" case-sensitively
        self.assertEqual(results[1]['y'], 0)


    def test_query_logs_no_match(self):
        results = self.log_engine.query_logs("nonexistent_pattern")
        self.assertEqual(len(results), 2) # Still returns entries for all days
        self.assertTrue(all(item['y'] == 0 for item in results))

    def test_query_logs_case_insensitivity(self):
        results_sensitive = self.log_engine.query_logs("hello", ignore_case=False)
        self.assertEqual(results_sensitive[0]['y'], 1) # "hello world"
        self.assertEqual(results_sensitive[1]['y'], 0) # "HELLO COSMO" does not match "hello" case-sensitively

        results_insensitive = self.log_engine.query_logs("hello", ignore_case=True)
        self.assertEqual(results_insensitive[0]['y'], 1) # "hello world"
        self.assertEqual(results_insensitive[1]['y'], 1) # "HELLO COSMO"

    def test_query_logs_nick_filter(self):
        results = self.log_engine.query_logs("test", nick="Alice")
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0]['y'], 0) # Alice said "hello world" on day 1
        self.assertEqual(results[1]['y'], 1) # Alice said "another day another test" on day 2

        results_bob = self.log_engine.query_logs("testing", nick="Bob")
        self.assertEqual(len(results_bob), 2)
        self.assertEqual(results_bob[0]['y'], 1) # Bob said "testing" on day 1
        self.assertEqual(results_bob[1]['y'], 0)

    def test_query_logs_cumulative(self):
        results = self.log_engine.query_logs("test", cumulative=True)
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0]['y'], 1) # Day 1: "testing" (1 total)
        self.assertEqual(results[1]['y'], 2) # Day 2: "another day another test" (1) + Day 1 (1) = 2

    def test_query_logs_coarse(self):
        # Coarse groups by month. All sample data is in the same month.
        results = self.log_engine.query_logs("hello", coarse=True, ignore_case=True)
        self.assertEqual(len(results), 1)
        # Day 1: "hello world", Day 2: "HELLO COSMO" -> total 2 in the month
        self.assertEqual(results[0]['y'], 2)

    def test_query_logs_normalize(self):
        # This is tricky to test with small dataset without diving deep into the normalization logic
        # For now, just check if it runs without error and returns the expected structure
        results = self.log_engine.query_logs("hello", normalize=True, normalize_type="trailing_avg_1")
        self.assertEqual(len(results), 2)
        for item in results:
            self.assertIn('x', item)
            self.assertIn('y', item)
            self.assertIsInstance(item['y'], (int, float))
        # A very basic check for normalization effect (expecting values between 0 and 1)
        # self.assertTrue(all(0 <= item['y'] <= 1 for item in results if item['y'] != 0))
        # The above assertion might be too strict depending on how total_window is calculated
        # For "hello" on day 1: 1 match / 2 total lines on day 1 = 0.5
        # For "HELLO COSMO" on day 2: 0 matches for "hello" (case-sensitive) / 2 total lines on day 2 = 0.0
        # if normalize_type is 'trailing_avg_1' and not cumulative
        self.assertAlmostEqual(results[0]['y'], 0.5, places=2) # 1 "hello" / 2 lines on that day
        self.assertAlmostEqual(results[1]['y'], 0.0, places=2) # 0 "hello" / 2 lines on that day

    def test_count_occurrences_simple(self):
        count = self.log_engine.count_occurrences("hello", ignore_case=True)
        self.assertEqual(count, 2) # "hello world" and "HELLO COSMO"
        count_sensitive = self.log_engine.count_occurrences("hello", ignore_case=False)
        self.assertEqual(count_sensitive, 1) # "hello world"

    def test_count_occurrences_nick_filter(self):
        count_alice = self.log_engine.count_occurrences("hello", nick="Alice", ignore_case=True)
        self.assertEqual(count_alice, 1) # Alice: "hello world"
        count_bob = self.log_engine.count_occurrences("HELLO", nick="Bob", ignore_case=True)
        self.assertEqual(count_bob, 0) # Bob didn't say "hello" or "HELLO"

    def test_get_valid_days(self):
        valid_days = self.log_engine.get_valid_days()
        expected_days = [
            (2024, 3, 15), # Corresponds to 1678886400 (Fri Mar 15 2024 13:20:00 GMT+0000) - Note: my sample uses older dates
            (2024, 3, 16)  # Corresponds to 1678972800 (Sat Mar 16 2024 13:20:00 GMT+0000)
        ]
        # Timestamps from sample: 1678886400 (2023-03-15), 1678972800 (2023-03-16)
        # Need to adjust expected_days based on actual parsing by fromtimestamp
        d1 = datetime.datetime.fromtimestamp(1678886400)
        d2 = datetime.datetime.fromtimestamp(1678972800)
        expected_actual_days = sorted(list(set([(d1.year, d1.month, d1.day), (d2.year, d2.month, d2.day)])))
        self.assertEqual(valid_days, expected_actual_days)
        self.assertEqual(len(valid_days), 2)


    def test_get_all_days(self):
        all_days = self.log_engine.get_all_days()
        d1_ts = 1678886400 # 2023-03-15
        d2_ts = 1678972800 # 2023-03-16
        
        start_date = datetime.datetime.fromtimestamp(d1_ts)
        end_date = datetime.datetime.fromtimestamp(d2_ts)
        
        expected_days = []
        current_date = start_date
        while current_date <= end_date:
            expected_days.append((current_date.year, current_date.month, current_date.day))
            current_date += datetime.timedelta(days=1)
            
        self.assertEqual(all_days, expected_days)
        self.assertEqual(len(all_days), (end_date - start_date).days + 1)
        self.assertEqual(len(all_days), 2)


    def test_get_logs_by_day(self):
        logs_by_day = self.log_engine.get_logs_by_day()
        d1 = datetime.datetime.fromtimestamp(1678886400)
        d2 = datetime.datetime.fromtimestamp(1678972800)
        key1 = (d1.year, d1.month, d1.day)
        key2 = (d2.year, d2.month, d2.day)

        self.assertIn(key1, logs_by_day)
        self.assertIn(key2, logs_by_day)
        self.assertEqual(len(logs_by_day[key1]), 2) # Two logs on the first day
        self.assertEqual(len(logs_by_day[key2]), 2) # Two logs on the second day
        self.assertEqual(logs_by_day[key1][0]['message'], "hello world")

    def test_search_day_logs(self):
        results = self.log_engine.search_day_logs("test", ignore_case=True)
        self.assertEqual(len(results), 2) # "testing", "another day another test"
        # Results are ((year, month, day), index, line, m.start(), m.end())
        # Sorted by day descending, then by original log order (index not guaranteed for sorting)
        
        # First result should be from the second day (newer logs first)
        self.assertEqual(results[0][2]['message'], "another day another test")
        self.assertEqual(results[0][3], 20) # start of "test" in "another day another test"
        self.assertEqual(results[0][4], 24) # end of "test"

        # Second result from the first day
        self.assertEqual(results[1][2]['message'], "testing")


    def test_search_results_to_chart(self):
        # This groups results by month for the chart
        chart_data = self.log_engine.search_results_to_chart("hello", ignore_case=True)
        self.assertEqual(len(chart_data), 1) # One series
        self.assertEqual(chart_data[0]['key'], "")
        
        values = chart_data[0]['values']
        # All sample data is in March 2023. So there should be one data point for that month.
        self.assertEqual(len(values), 1) 
        
        # Timestamp for March 1, 2023
        # We need to find the timestamp for the beginning of the month of our sample data
        sample_date = datetime.datetime.fromtimestamp(1678886400) # 2023-03-15
        month_start_ts = time.mktime(datetime.datetime(sample_date.year, sample_date.month, 1).timetuple())

        self.assertEqual(values[0]['x'], month_start_ts)
        self.assertEqual(values[0]['y'], 2) # "hello world" and "HELLO COSMO"

if __name__ == '__main__':
    unittest.main()
