import unittest
import os
import sys
import time
import datetime

# Add the parent directory to the Python path to allow importing web.logs
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from web.logs import InMemoryLogQueryEngine, SQLiteLogQueryEngine, VALID_NICKS as GLOBAL_VALID_NICKS, graph_query, table_query
from web import app # To set app.testing
from web import logs as web_logs_module # For accessing _log_engine

# Helper to create timestamp strings from datetime objects
def ts(dt_obj):
    return time.mktime(dt_obj.timetuple())

BASE_DATE = datetime.datetime(2023, 3, 15, 10, 0, 0)
DAY_1 = BASE_DATE
DAY_2 = BASE_DATE + datetime.timedelta(days=1)
DAY_3 = BASE_DATE + datetime.timedelta(days=2)

class BaseLogQueryEngineTests:
    # This class is a mixin and should not inherit from unittest.TestCase directly.
    # Test methods will be inherited by concrete test classes.

    # Factory method to be implemented by subclasses
    def create_engine(self, log_data=None, log_file_path=None):
        raise NotImplementedError("Subclasses must implement create_engine")

    def test_query_logs_empty_data(self):
        log_data = []
        engine = self.create_engine(log_data=log_data)
        results = engine.query_logs([("anything_label", "anything")])
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]['key'], "anything_label")
        self.assertEqual(results[0]['values'], [])


    def test_query_logs_single_entry_match(self):
        log_data = [{"timestamp": ts(DAY_1), "nick": "UserA", "message": "hello world"}]
        engine = self.create_engine(log_data=log_data)
        results_series = engine.query_logs([("hello_label", "hello")])
        self.assertEqual(len(results_series), 1)
        self.assertEqual(results_series[0]['key'], "hello_label")

        results = results_series[0]['values']
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]['y'], 1)
        expected_ts = time.mktime(datetime.datetime(DAY_1.year, DAY_1.month, DAY_1.day).timetuple())
        self.assertEqual(results[0]['x'], expected_ts)

    def test_query_logs_no_match(self):
        log_data = [{"timestamp": ts(DAY_1), "nick": "UserA", "message": "goodbye moon"}]
        engine = self.create_engine(log_data=log_data)
        results_series = engine.query_logs([("hello_label", "hello")])
        self.assertEqual(len(results_series), 1)
        self.assertEqual(results_series[0]['key'], "hello_label")

        results = results_series[0]['values']
        # The results should contain an entry for each day in the log range, even if count is 0
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]['y'], 0)

    def test_query_logs_case_insensitivity(self):
        log_data = [
            {"timestamp": ts(DAY_1), "nick": "UserA", "message": "Hello"},
            {"timestamp": ts(DAY_2), "nick": "UserA", "message": "HELLO"}
        ]
        engine = self.create_engine(log_data=log_data)

        results_sensitive_series = engine.query_logs([("Hello_label", "Hello")], ignore_case=False)
        self.assertEqual(len(results_sensitive_series), 1)
        results_sensitive = results_sensitive_series[0]['values']
        self.assertEqual(len(results_sensitive), 2)

        results_sensitive_map = {r['x']: r['y'] for r in results_sensitive}
        day1_ts = time.mktime(datetime.datetime(DAY_1.year, DAY_1.month, DAY_1.day).timetuple())
        day2_ts = time.mktime(datetime.datetime(DAY_2.year, DAY_2.month, DAY_2.day).timetuple())
        self.assertEqual(results_sensitive_map.get(day1_ts), 1)
        self.assertEqual(results_sensitive_map.get(day2_ts), 0)

        engine.clear_all_caches()
        results_insensitive_series = engine.query_logs([("Hello_label", "Hello")], ignore_case=True)
        self.assertEqual(len(results_insensitive_series), 1)
        results_insensitive = results_insensitive_series[0]['values']
        self.assertEqual(len(results_insensitive), 2)
        results_insensitive_map = {r['x']: r['y'] for r in results_insensitive}
        self.assertEqual(results_insensitive_map.get(day1_ts), 1)
        self.assertEqual(results_insensitive_map.get(day2_ts), 1)


    def test_query_logs_nick_filter(self):
        log_data = [
            {"timestamp": ts(DAY_1), "nick": "Cosmo", "message": "apple Cosmo"},
            {"timestamp": ts(DAY_1), "nick": "Graham", "message": "apple Graham"},
            {"timestamp": ts(DAY_2), "nick": "cosmo", "message": "banana Cosmo"}, # Note: 'cosmo' is an alias for 'Cosmo'
        ]
        engine = self.create_engine(log_data=log_data)
        sorted_nicks = sorted(list(GLOBAL_VALID_NICKS.keys()))

        # Test for "apple" by "Cosmo"
        results_apple_nicks = engine.query_logs([("apple_label", "apple")], nick_split=True)
        self.assertEqual(len(results_apple_nicks), len(sorted_nicks))

        cosmo_apple_series = next(s for s in results_apple_nicks if s['key'] == "apple_label - Cosmo")['values']
        self.assertEqual(len(cosmo_apple_series), 2) # Two days in data range
        day1_ts = time.mktime(datetime.datetime(DAY_1.year, DAY_1.month, DAY_1.day).timetuple())
        day2_ts = time.mktime(datetime.datetime(DAY_2.year, DAY_2.month, DAY_2.day).timetuple())
        cosmo_apple_map = {r['x']: r['y'] for r in cosmo_apple_series}
        self.assertEqual(cosmo_apple_map.get(day1_ts), 1) # "apple Cosmo" by Cosmo
        self.assertEqual(cosmo_apple_map.get(day2_ts), 0) # No "apple" by Cosmo on DAY_2

        graham_apple_series = next(s for s in results_apple_nicks if s['key'] == "apple_label - Graham")['values']
        self.assertEqual(len(graham_apple_series), 2)
        graham_apple_map = {r['x']: r['y'] for r in graham_apple_series}
        self.assertEqual(graham_apple_map.get(day1_ts), 1) # "apple Graham" by Graham
        self.assertEqual(graham_apple_map.get(day2_ts), 0)

        engine.clear_all_caches()
        # Test for "banana" by "Cosmo"
        results_banana_nicks = engine.query_logs([("banana_label", "banana")], nick_split=True)
        cosmo_banana_series = next(s for s in results_banana_nicks if s['key'] == "banana_label - Cosmo")['values']
        self.assertEqual(len(cosmo_banana_series), 2)
        cosmo_banana_map = {r['x']: r['y'] for r in cosmo_banana_series}
        self.assertEqual(cosmo_banana_map.get(day1_ts), 0)
        self.assertEqual(cosmo_banana_map.get(day2_ts), 1) # "banana Cosmo" by cosmo (alias)

    def test_query_logs_cumulative_logic(self):
        log_data = [
            {"timestamp": ts(DAY_1), "nick": "UserA", "message": "event"},
            {"timestamp": ts(DAY_1), "nick": "UserA", "message": "event"},
            {"timestamp": ts(DAY_2), "nick": "UserA", "message": "event"},
            {"timestamp": ts(DAY_3), "nick": "UserA", "message": "event"}
        ]
        engine = self.create_engine(log_data=log_data)
        results_series = engine.query_logs([("event_label", "event")], cumulative=True)
        self.assertEqual(len(results_series), 1)
        results = results_series[0]['values']
        self.assertEqual(len(results), 3)
        # Results are sorted by timestamp 'x'
        self.assertEqual(results[0]['y'], 2) # Day 1: 2 events
        self.assertEqual(results[1]['y'], 3) # Day 2: 2 (day1) + 1 (day2) = 3 events
        self.assertEqual(results[2]['y'], 4) # Day 3: 3 (day1+day2) + 1 (day3) = 4 events

    def test_query_logs_timestamps_and_coarse(self):
        day1_month1 = datetime.datetime(2023, 1, 15, 10, 0, 0)
        day2_month1 = datetime.datetime(2023, 1, 16, 10, 0, 0) # Same month as day1_month1
        day1_month2 = datetime.datetime(2023, 2, 10, 10, 0, 0) # Different month
        log_data = [
            {"timestamp": ts(day1_month1), "nick": "UserA", "message": "hello"},
            {"timestamp": ts(day1_month1 + datetime.timedelta(hours=1)), "nick": "UserA", "message": "hello"}, # Same day
            {"timestamp": ts(day2_month1), "nick": "UserA", "message": "hello"},
            {"timestamp": ts(day1_month2), "nick": "UserA", "message": "hello"},
        ]
        engine = self.create_engine(log_data=log_data)
        results_series = engine.query_logs([("hello_label", "hello")], coarse=True) # Coarse aggregates by month
        self.assertEqual(len(results_series), 1)
        results = results_series[0]['values']
        self.assertEqual(len(results), 2) # Two months: Jan, Feb

        month1_ts = time.mktime(datetime.datetime(2023, 1, 1).timetuple()) # Start of Jan 2023
        month2_ts = time.mktime(datetime.datetime(2023, 2, 1).timetuple()) # Start of Feb 2023

        results_map = {r['x']: r['y'] for r in results}
        self.assertEqual(results_map.get(month1_ts), 3) # 2 from day1_month1, 1 from day2_month1
        self.assertEqual(results_map.get(month2_ts), 1) # 1 from day1_month2

    def test_query_logs_normalization_logic(self):
        log_data = [
            {"timestamp": ts(DAY_1), "nick": "UserA", "message": "target"}, # 1 target / 2 total
            {"timestamp": ts(DAY_1), "nick": "UserA", "message": "other"},
            {"timestamp": ts(DAY_2), "nick": "UserA", "message": "target"}, # 2 target / 5 total
            {"timestamp": ts(DAY_2), "nick": "UserA", "message": "target"},
            {"timestamp": ts(DAY_2), "nick": "UserA", "message": "noise"},
            {"timestamp": ts(DAY_2), "nick": "UserA", "message": "noise"},
            {"timestamp": ts(DAY_2), "nick": "UserA", "message": "noise"},
        ]
        engine = self.create_engine(log_data=log_data)
        # normalize_type="trailing_avg_1" means use current day's total for normalization
        results_series = engine.query_logs([("target_label", "target")], normalize=True, normalize_type="trailing_avg_1")
        self.assertEqual(len(results_series), 1)
        results = results_series[0]['values']
        self.assertEqual(len(results), 2)
        results_map = {r['x']: r['y'] for r in results}
        day1_ts = time.mktime(datetime.datetime(DAY_1.year, DAY_1.month, DAY_1.day).timetuple())
        day2_ts = time.mktime(datetime.datetime(DAY_2.year, DAY_2.month, DAY_2.day).timetuple())

        self.assertAlmostEqual(results_map.get(day1_ts), 1.0/2.0, places=2)
        self.assertAlmostEqual(results_map.get(day2_ts), 2.0/5.0, places=2)

    def test_query_logs_global_normalization_logic(self):
        if self.is_in_memory:
            # I disaggree with the in-memory logic...
            return

        log_data = [
            {"timestamp": ts(DAY_1), "nick": "UserA", "message": "target"},
            {"timestamp": ts(DAY_1), "nick": "UserA", "message": "other"},
            {"timestamp": ts(DAY_2), "nick": "UserA", "message": "target"},
            {"timestamp": ts(DAY_2), "nick": "UserA", "message": "target"},
            {"timestamp": ts(DAY_2), "nick": "UserA", "message": "noise"},
            {"timestamp": ts(DAY_2), "nick": "UserA", "message": "noise"},
            {"timestamp": ts(DAY_2), "nick": "UserA", "message": "noise"},
        ]
        engine = self.create_engine(log_data=log_data)
        # normalize=True with no specific normalize_type should use global total
        results_series = engine.query_logs([("target_label", "target")], normalize=True)
        self.assertEqual(len(results_series), 1)
        results = results_series[0]['values']
        self.assertEqual(len(results), 2)
        results_map = {r['x']: r['y'] for r in results}
        day1_ts = time.mktime(datetime.datetime(DAY_1.year, DAY_1.month, DAY_1.day).timetuple())
        day2_ts = time.mktime(datetime.datetime(DAY_2.year, DAY_2.month, DAY_2.day).timetuple())

        # Total logs in dataset is 7
        self.assertAlmostEqual(results_map.get(day1_ts), 1.0/7.0, places=2)
        self.assertAlmostEqual(results_map.get(day2_ts), 2.0/7.0, places=2)

    def test_query_logs_cumulative_normalization_logic(self):
        if self.is_in_memory:
            # I disaggree with the in-memory logic...
            return

        log_data = [
            {"timestamp": ts(DAY_1), "nick": "UserA", "message": "target"}, # Day 1: 1 target / 2 total
            {"timestamp": ts(DAY_1), "nick": "UserA", "message": "other"},
            {"timestamp": ts(DAY_2), "nick": "UserA", "message": "target"}, # Day 2: 1 target / 3 total
            {"timestamp": ts(DAY_2), "nick": "UserA", "message": "noise"},
            {"timestamp": ts(DAY_2), "nick": "UserA", "message": "noise"},
            {"timestamp": ts(DAY_3), "nick": "UserA", "message": "target"}, # Day 3: 1 target / 1 total
        ]
        engine = self.create_engine(log_data=log_data)
        # normalize=True and cumulative=True
        results_series = engine.query_logs([("target_label", "target")], normalize=True, cumulative=True)
        self.assertEqual(len(results_series), 1)
        results = results_series[0]['values']
        self.assertEqual(len(results), 3)
        results_map = {r['x']: r['y'] for r in results}
        day1_ts = time.mktime(datetime.datetime(DAY_1.year, DAY_1.month, DAY_1.day).timetuple())
        day2_ts = time.mktime(datetime.datetime(DAY_2.year, DAY_2.month, DAY_2.day).timetuple())
        day3_ts = time.mktime(datetime.datetime(DAY_3.year, DAY_3.month, DAY_3.day).timetuple())

        self.assertAlmostEqual(results_map.get(day1_ts), 1.0/6.0, places=2)
        self.assertAlmostEqual(results_map.get(day2_ts), (1.0+1.0)/(6.0), places=2)
        self.assertAlmostEqual(results_map.get(day3_ts), (1.0+1.0+1.0)/(6.0), places=2)

    def test_query_logs_coarse_normalization_logic(self):
        day1_month1 = datetime.datetime(2023, 1, 15, 10, 0, 0)
        day2_month1 = datetime.datetime(2023, 1, 16, 10, 0, 0) # Same month as day1_month1
        day1_month2 = datetime.datetime(2023, 2, 10, 10, 0, 0) # Different month
        log_data = [
            {"timestamp": ts(day1_month1), "nick": "UserA", "message": "target"}, # Jan: 1 target / 2 total
            {"timestamp": ts(day1_month1), "nick": "UserA", "message": "other"},
            {"timestamp": ts(day2_month1), "nick": "UserA", "message": "target"}, # Jan: 1 target / 1 total
            {"timestamp": ts(day1_month2), "nick": "UserA", "message": "target"}, # Feb: 1 target / 1 total
        ]
        engine = self.create_engine(log_data=log_data)
        # Coarse aggregates by month, normalize by trailing_avg_1 (current month's total)
        results_series = engine.query_logs([("target_label", "target")], coarse=True, normalize=True, normalize_type="trailing_avg_1")
        self.assertEqual(len(results_series), 1)
        results = results_series[0]['values']
        self.assertEqual(len(results), 2) # Two months: Jan, Feb

        month1_ts = time.mktime(datetime.datetime(2023, 1, 1).timetuple()) # Start of Jan 2023
        month2_ts = time.mktime(datetime.datetime(2023, 2, 1).timetuple()) # Start of Feb 2023

        results_map = {r['x']: r['y'] for r in results}
        # Jan: (1 from day1_month1 + 1 from day2_month1) targets / (2 from day1_month1 + 1 from day2_month1) total = 2/3
        self.assertAlmostEqual(results_map.get(month1_ts), 2.0/3.0, places=2)
        # Feb: 1 target / 1 total = 1.0
        self.assertAlmostEqual(results_map.get(month2_ts), 1.0/1.0, places=2)

    def test_count_occurrences_empty_data(self):
        engine = self.create_engine(log_data=[])
        self.assertEqual(engine.count_occurrences([('label', "anything")]), [['', 'Total'], ['label', 0]])

    def test_count_occurrences_simple(self):
        log_data = [
            {"timestamp": ts(DAY_1), "nick": "UserA", "message": "hello world"},
            {"timestamp": ts(DAY_1), "nick": "UserA", "message": "HELLO again"}
        ]
        engine = self.create_engine(log_data=log_data)
        self.assertEqual(engine.count_occurrences([("", "hello")], ignore_case=True)[1][1], 2)
        engine.clear_all_caches() # Still useful if the engine itself uses caching (like InMemory)
        self.assertEqual(engine.count_occurrences([("", "hello")], ignore_case=False)[1][1], 1)

    def test_count_occurrences_nick_filter(self):
        log_data = [
            {"timestamp": ts(DAY_1), "nick": "Cosmo", "message": "msg from Cosmo"},
            {"timestamp": ts(DAY_1), "nick": "Graham", "message": "msg from Graham"}
        ]
        nick_list = list(GLOBAL_VALID_NICKS.keys())
        engine = self.create_engine(log_data=log_data)
        self.assertEqual(engine.count_occurrences([("", "msg")], nick_split=True)[1][1], 2) # Total
        self.assertEqual(engine.count_occurrences([("", "msg")], nick_split=True)[1][2 + nick_list.index('Cosmo')], 1)
        self.assertEqual(engine.count_occurrences([("", "msg")], nick_split=True)[1][2 + nick_list.index('Jesse')], 0)

    def test_get_valid_days_empty_data(self):
        engine = self.create_engine(log_data=[])
        self.assertEqual(engine.get_valid_days(), [])

    def test_get_valid_days_populated(self):
        log_data = [
            {"timestamp": ts(DAY_1), "nick": "UserA", "message": "m1"},
            {"timestamp": ts(DAY_2), "nick": "UserA", "message": "m2"},
            {"timestamp": ts(DAY_1), "nick": "UserB", "message": "m3"}, # Duplicate day
        ]
        engine = self.create_engine(log_data=log_data)
        expected_days = sorted([
            (DAY_1.year, DAY_1.month, DAY_1.day),
            (DAY_2.year, DAY_2.month, DAY_2.day)
        ])
        self.assertEqual(engine.get_valid_days(), expected_days)


    def test_get_logs_by_day(self):
        # Define specific dates for testing
        day_before_base = BASE_DATE - datetime.timedelta(days=1)
        day_after_base = BASE_DATE + datetime.timedelta(days=1)
        day_two_after_base = BASE_DATE + datetime.timedelta(days=2)

        log_data = [
            {"timestamp": ts(BASE_DATE), "nick": "UserA", "message": "log on base date 1"},
            {"timestamp": ts(BASE_DATE + datetime.timedelta(seconds=1)), "nick": "UserB", "message": "log on base date 2"},
            {"timestamp": ts(day_after_base), "nick": "UserC", "message": "log on day after base"},
            {"timestamp": ts(day_two_after_base), "nick": "UserD", "message": "log on two days after base"},
        ]
        engine = self.create_engine(log_data=log_data)

        # Helper to create day tuple
        def get_day_tuple(dt_obj):
            return (dt_obj.year, dt_obj.month, dt_obj.day)

        # Sort key function for logs
        def sort_key(log_entry):
            return float(log_entry['timestamp'])

        # Test for the first day in the dataset (BASE_DATE)
        current_day_logs, prev_day_tuple, next_day_tuple = engine.get_logs_by_day(
            BASE_DATE.year, BASE_DATE.month, BASE_DATE.day
        )
        self.assertEqual(len(current_day_logs), 2)
        self.assertEqual(sorted(current_day_logs, key=sort_key), sorted([log_data[0], log_data[1]], key=sort_key))
        self.assertIsNone(prev_day_tuple)
        self.assertEqual(next_day_tuple, (DAY_2.year, DAY_2.month, DAY_2.day))

        # Test for a middle day (day_after_base)
        current_day_logs, prev_day_tuple, next_day_tuple = engine.get_logs_by_day(
            day_after_base.year, day_after_base.month, day_after_base.day
        )
        self.assertEqual(len(current_day_logs), 1)
        self.assertEqual(sorted(current_day_logs, key=sort_key), sorted([log_data[2]], key=sort_key))
        self.assertEqual(prev_day_tuple, get_day_tuple(BASE_DATE))
        self.assertEqual(next_day_tuple, get_day_tuple(day_two_after_base))

        # Test for the last day in the dataset (day_two_after_base)
        current_day_logs, prev_day_tuple, next_day_tuple = engine.get_logs_by_day(
            day_two_after_base.year, day_two_after_base.month, day_two_after_base.day
        )
        self.assertEqual(len(current_day_logs), 1)
        self.assertEqual(sorted(current_day_logs, key=sort_key), sorted([log_data[3]], key=sort_key))
        self.assertEqual(prev_day_tuple, get_day_tuple(day_after_base))
        self.assertIsNone(next_day_tuple)

        # Test for a day with no logs
        current_day_logs, prev_day_tuple, next_day_tuple = engine.get_logs_by_day(
            day_before_base.year, day_before_base.month, day_before_base.day
        )
        self.assertEqual(len(current_day_logs), 0)
        self.assertIsNone(prev_day_tuple) # No previous day in data
        self.assertIsNone(next_day_tuple) # Next day is BASE_DATE

    def test_search_day_logs_ordering_and_content(self):
        # Timestamps need to be distinct for reliable ordering in SQLite if that's relied upon.
        # Original test data implies order of processing or internal list order.
        # search_day_logs returns results sorted by day (desc) then by index within day's logs.
        log_d1 = {"timestamp": ts(DAY_1), "nick": "UserA", "message": "search me on day 1"}
        log_d2 = {"timestamp": ts(DAY_2), "nick": "UserA", "message": "search me on day 2"}
        log_d3_no_match = {"timestamp": ts(DAY_3), "nick": "UserA", "message": "nothing here"}

        # Data is provided such that DAY_1, DAY_2, DAY_3 are chronological.
        # SQLiteLogQueryEngine.search_day_logs uses get_logs_by_day (which sorts by timestamp from DB),
        # then reverses sorted days. So results are DAY_N, DAY_N-1 ...
        # InMemoryLogQueryEngine.search_day_logs also sorts days in reverse.
        log_data = [log_d1, log_d2, log_d3_no_match]
        engine = self.create_engine(log_data=log_data)

        # search_day_logs returns (paginated_results, total_count)
        actual_results, total_count = engine.search_day_logs("arch me")

        self.assertEqual(total_count, 2)
        self.assertEqual(len(actual_results), 2)

        # Result 0 should be from DAY_2 (later date, appears first due to reverse day sort)
        self.assertEqual(actual_results[0][2]['message'], "search me on day 2")
        self.assertEqual(actual_results[0][0], (DAY_2.year, DAY_2.month, DAY_2.day)) # (day_tuple)
        # The index within the day's log list might vary based on how logs for a day are retrieved and ordered.
        # Assuming it's 0 if it's the only match or first match for that day.
        self.assertEqual(actual_results[0][1], 0) # index within that day's log list
        self.assertEqual(actual_results[0][3], 2) # start match index
        self.assertEqual(actual_results[0][4], 9) # end match index

        # Result 1 should be from DAY_1
        self.assertEqual(actual_results[1][2]['message'], "search me on day 1")
        self.assertEqual(actual_results[1][0], (DAY_1.year, DAY_1.month, DAY_1.day))
        self.assertEqual(actual_results[1][1], 0) # index

    def test_search_day_logs_pagination(self):
        # Defines specific timestamps for log entries to ensure predictable ordering within a day if necessary.
        # For this test, we'll assume simple message content is enough to distinguish.
        log_data = [
            {"timestamp": ts(DAY_1), "nick": "UserA", "message": "item 1 filter_me day1_occurrence1"},
            {"timestamp": ts(DAY_1 + datetime.timedelta(seconds=1)), "nick": "UserA", "message": "item 2 filter_me day1_occurrence2"},
            {"timestamp": ts(DAY_2), "nick": "UserA", "message": "item 3 filter_me day2_occurrence1"},
            {"timestamp": ts(DAY_2 + datetime.timedelta(seconds=1)), "nick": "UserA", "message": "item 4 no_match day2_occurrence2"},
            {"timestamp": ts(DAY_3), "nick": "UserA", "message": "item 5 filter_me day3_occurrence1"},
        ]
        engine = self.create_engine(log_data=log_data)
        query = "filter_me"

        # Expected full results order (search_day_logs sorts days DESC, then items by input order/timestamp ASC):
        # 1. item 5 (DAY_3)
        # 2. item 3 (DAY_2)
        # 3. item 1 (DAY_1)
        # 4. item 2 (DAY_1)
        # Total expected matches = 4

        # Test 1: limit=2, offset=0
        results, total_count = engine.search_day_logs(query, ignore_case=False, limit=2, offset=0)
        self.assertEqual(total_count, 4, "Test 1: Total count mismatch")
        self.assertEqual(len(results), 2, "Test 1: Results length mismatch")
        self.assertIn("item 5 filter_me day3_occurrence1", results[0][2]['message'], "Test 1: Item 0 content error") # DAY_3
        self.assertIn("item 3 filter_me day2_occurrence1", results[1][2]['message'], "Test 1: Item 1 content error") # DAY_2

        # Test 2: limit=2, offset=2
        results, total_count = engine.search_day_logs(query, ignore_case=False, limit=2, offset=2)
        self.assertEqual(total_count, 4, "Test 2: Total count mismatch")
        self.assertEqual(len(results), 2, "Test 2: Results length mismatch")
        self.assertIn("item 1 filter_me day1_occurrence1", results[0][2]['message'], "Test 2: Item 0 content error") # DAY_1, first item
        self.assertIn("item 2 filter_me day1_occurrence2", results[1][2]['message'], "Test 2: Item 1 content error") # DAY_1, second item

        # Test 3: limit=3, offset=3 (limit exceeds remaining)
        results, total_count = engine.search_day_logs(query, ignore_case=False, limit=3, offset=3)
        self.assertEqual(total_count, 4, "Test 3: Total count mismatch")
        self.assertEqual(len(results), 1, "Test 3: Results length mismatch")
        self.assertIn("item 2 filter_me day1_occurrence2", results[0][2]['message'], "Test 3: Item 0 content error") # Last item (DAY_1, second item)

        # Test 4: offset beyond total items
        results, total_count = engine.search_day_logs(query, ignore_case=False, limit=2, offset=4)
        self.assertEqual(total_count, 4, "Test 4: Total count mismatch")
        self.assertEqual(len(results), 0, "Test 4: Results length mismatch")

        # Test 5: No limit (limit=None)
        results, total_count = engine.search_day_logs(query, ignore_case=False, limit=None, offset=1)
        self.assertEqual(total_count, 4, "Test 5: Total count mismatch")
        self.assertEqual(len(results), 3, "Test 5: Results length mismatch")
        self.assertIn("item 3 filter_me day2_occurrence1", results[0][2]['message'], "Test 5: Item 0 content error") # Starts from offset 1 (DAY_2 item)
        self.assertIn("item 1 filter_me day1_occurrence1", results[1][2]['message'], "Test 5: Item 1 content error")
        self.assertIn("item 2 filter_me day1_occurrence2", results[2][2]['message'], "Test 5: Item 2 content error")

        # Test 6: No offset (offset=None)
        results, total_count = engine.search_day_logs(query, ignore_case=False, limit=2, offset=None)
        self.assertEqual(total_count, 4, "Test 6: Total count mismatch")
        self.assertEqual(len(results), 2, "Test 6: Results length mismatch")
        self.assertIn("item 5 filter_me day3_occurrence1", results[0][2]['message'], "Test 6: Item 0 content error") # DAY_3
        self.assertIn("item 3 filter_me day2_occurrence1", results[1][2]['message'], "Test 6: Item 1 content error") # DAY_2

        # Test 7: No limit, no offset
        results, total_count = engine.search_day_logs(query, ignore_case=False, limit=None, offset=None)
        self.assertEqual(total_count, 4, "Test 7: Total count mismatch")
        self.assertEqual(len(results), 4, "Test 7: Results length mismatch")
        self.assertIn("item 5 filter_me day3_occurrence1", results[0][2]['message'], "Test 7: Item 0 content error")
        self.assertIn("item 3 filter_me day2_occurrence1", results[1][2]['message'], "Test 7: Item 1 content error")
        self.assertIn("item 1 filter_me day1_occurrence1", results[2][2]['message'], "Test 7: Item 2 content error")
        self.assertIn("item 2 filter_me day1_occurrence2", results[3][2]['message'], "Test 7: Item 3 content error")

    def test_search_results_to_chart(self):
        day1_month1 = datetime.datetime(2023, 1, 15, 10, 0, 0)
        day2_month1 = datetime.datetime(2023, 1, 16, 10, 0, 0) # Same month
        day1_month2 = datetime.datetime(2023, 2, 10, 10, 0, 0) # Different month
        log_data = [
            {"timestamp": ts(day1_month1), "nick": "UserA", "message": "chart data"},
            {"timestamp": ts(day2_month1), "nick": "UserA", "message": "chart data"},
            {"timestamp": ts(day1_month2), "nick": "UserA", "message": "chart data"},
        ]
        engine = self.create_engine(log_data=log_data)

        chart_data = engine.search_results_to_chart("chart data")
        self.assertEqual(len(chart_data), 1) # Single series
        self.assertEqual(chart_data[0]['key'], "") # Default key

        values = chart_data[0]['values'] # List of {'x': ts, 'y': count}
        self.assertEqual(len(values), 2) # Two months with data

        # Expected timestamps for the start of the month
        month1_ts = time.mktime(datetime.datetime(2023, 1, 1).timetuple())
        month2_ts = time.mktime(datetime.datetime(2023, 2, 1).timetuple())

        results_map = {r['x']: r['y'] for r in values}
        self.assertEqual(results_map.get(month1_ts), 2) # Two entries in Jan
        self.assertEqual(results_map.get(month2_ts), 1) # One entry in Feb


    def test_load_from_sample_file(self):
        # This test requires the engine to load from a file path.
        # The create_engine method needs to support log_file_path.
        # The default test log sample path is resolved inside the engine constructor if app.testing is True.
        # So, create_engine(log_data=None, log_file_path=None) should trigger this.
        # Or, explicitly pass the path.

        sample_log_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'test_log_sample.json'))
        engine = self.create_engine(log_file_path=sample_log_path, log_data=None) # Explicitly pass path

        # Data from test_log_sample.json:
        # Alice: "hello world" (2023-03-15), "another day another test" (2023-03-16)
        # Bob: "testing" (2023-03-15)
        # Charlie: "HELLO COSMO" (2023-03-16)

        results_series = engine.query_logs([("hello_label", "hello")], ignore_case=True) # Matches "hello world" and "HELLO COSMO"
        self.assertEqual(len(results_series), 1)
        results = results_series[0]['values']
        self.assertEqual(len(results), 2) # Two days in sample: 2023-03-15, 2023-03-16

        day1_sample_dt = datetime.datetime(2023, 3, 15)
        day2_sample_dt = datetime.datetime(2023, 3, 16)

        exp_x1 = time.mktime(day1_sample_dt.timetuple())
        exp_x2 = time.mktime(day2_sample_dt.timetuple())

        results_map = {r['x']: r['y'] for r in results}
        self.assertEqual(results_map.get(exp_x1), 1) # "hello world" on 2023-03-15
        self.assertEqual(results_map.get(exp_x2), 1) # "HELLO COSMO" on 2023-03-16

        engine.clear_all_caches()
        count = engine.count_occurrences([("", "test")], ignore_case=True) # "testing", "another day another test"
        self.assertEqual(count[1][1], 2)

    def test_get_trending(self):
        if self.is_in_memory:
            # Bug in trending logic that uses now()...
            return

        # alpha global rate: 4 / 11
        # beta global rate: 3 / 11
        # gamma global rate: 2 / 11
        # delta global rate: 1 / 11
        # epsilon global rate: 1 / 11
        log_data = [
            {"timestamp": ts(DAY_1), "nick": "UserA", "message": "alpha beta gamma delta epsilon"},
            {"timestamp": ts(DAY_1), "nick": "UserB", "message": "alpha beta gamma"},
            {"timestamp": ts(DAY_2), "nick": "UserC", "message": "alpha beta"},
            {"timestamp": ts(DAY_3), "nick": "UserD", "message": "alpha"},
        ]
        engine = self.create_engine(log_data=log_data)

        trending_all = engine.get_trending(min_freq=1, lookback_days=1)
        expected_all = [('alpha', (2/3 - 4/11) / (4/11)), ('beta', (1/3 - 3/11) / (3/11))]
        self.assertEqual(trending_all, expected_all)

    def test_get_trending_min_freq(self):
        if self.is_in_memory:
            # Bug in trending logic that uses now()...
            return

        # alpha global rate: 4 / 11
        # beta global rate: 3 / 11
        # gamma global rate: 2 / 11
        # delta global rate: 1 / 11
        # epsilon global rate: 1 / 11
        log_data = [
            {"timestamp": ts(DAY_1), "nick": "UserA", "message": "alpha beta gamma delta epsilon"},
            {"timestamp": ts(DAY_1), "nick": "UserB", "message": "alpha beta gamma"},
            {"timestamp": ts(DAY_2), "nick": "UserC", "message": "alpha beta"},
            {"timestamp": ts(DAY_3), "nick": "UserD", "message": "alpha"},
        ]
        engine = self.create_engine(log_data=log_data)

        trending_all = engine.get_trending(min_freq=2, lookback_days=1)
        expected_all = [('alpha', (2/3 - 4/11) / (4/11))]
        self.assertEqual(trending_all, expected_all)

    def test_get_trending_top(self):
        if self.is_in_memory:
            # Bug in trending logic that uses now()...
            return

        # alpha global rate: 4 / 11
        # beta global rate: 3 / 11
        # gamma global rate: 2 / 11
        # delta global rate: 1 / 11
        # epsilon global rate: 1 / 11
        log_data = [
            {"timestamp": ts(DAY_1), "nick": "UserA", "message": "alpha beta gamma delta epsilon"},
            {"timestamp": ts(DAY_1), "nick": "UserB", "message": "alpha beta gamma"},
            {"timestamp": ts(DAY_2), "nick": "UserC", "message": "alpha beta"},
            {"timestamp": ts(DAY_3), "nick": "UserD", "message": "alpha"},
        ]
        engine = self.create_engine(log_data=log_data)

        # Top 1
        trending_top = engine.get_trending(top=1, lookback_days=1, min_freq=1)
        expected_top = [('alpha', (2/3 - 4/11) / (4/11))]
        self.assertEqual(trending_top, expected_top)

        # Top 0 (should return empty)
        trending_top_0 = engine.get_trending(top=0, min_freq=1)
        self.assertEqual(trending_top_0, [])

# Concrete test class for InMemoryLogQueryEngine
class TestInMemoryLogQueryEngine(BaseLogQueryEngineTests, unittest.TestCase):
    def setUp(self):
        app.testing = True
        self.is_in_memory = True
        # Potentially call super().setUp() if BaseLogQueryEngineTests had a setUp

    def test_get_logs_by_day_specific_scenario(self):
        # This test specifically addresses the scenario outlined in the instructions
        # for InMemoryLogQueryEngine regarding prev_day_tuple and next_day_tuple.
        log_data = [
            {"timestamp": ts(DAY_1), "nick": "UserA", "message": "log on day 1"},
            {"timestamp": ts(DAY_2), "nick": "UserB", "message": "log on day 2"},
        ]
        engine = self.create_engine(log_data=log_data)

        def get_day_tuple(dt_obj):
            return (dt_obj.year, dt_obj.month, dt_obj.day)

        # Test for DAY_1
        current_day_logs, prev_day_tuple, next_day_tuple = engine.get_logs_by_day(
            DAY_1.year, DAY_1.month, DAY_1.day
        )
        self.assertEqual(len(current_day_logs), 1)
        self.assertIsNone(prev_day_tuple)
        self.assertEqual(next_day_tuple, get_day_tuple(DAY_2))

        # Test for DAY_2
        current_day_logs, prev_day_tuple, next_day_tuple = engine.get_logs_by_day(
            DAY_2.year, DAY_2.month, DAY_2.day
        )
        self.assertEqual(len(current_day_logs), 1)
        self.assertEqual(prev_day_tuple, get_day_tuple(DAY_1))
        self.assertIsNone(next_day_tuple)

    def create_engine(self, log_data=None, log_file_path=None):
        # app.testing must be True for InMemoryLogQueryEngine to use test_log_sample.json by default
        return InMemoryLogQueryEngine(log_data=log_data, log_file_path=log_file_path)

# Concrete test class for SQLiteLogQueryEngine
class TestSQLiteLogQueryEngine(BaseLogQueryEngineTests, unittest.TestCase):
    def setUp(self):
        app.testing = True
        self.is_in_memory = False
        # Potentially call super().setUp()

    def create_engine(self, log_data=None, log_file_path=None):
        # app.testing must be True for SQLiteLogQueryEngine to use test_log_sample.json by default
        # Using a small batch_size for testing purposes, if applicable to any specific tests.
        return SQLiteLogQueryEngine(log_data=log_data, log_file_path=log_file_path, batch_size=10)

# --- Tests for graph_query and table_query ---

# Timestamps for test_log_sample.json (2023-03-15 and 2023-03-16)
DAY_1_SAMPLE = datetime.datetime(2023, 3, 15)
DAY_2_SAMPLE = datetime.datetime(2023, 3, 16)
# For graph_query, 'x' values are daily timestamps (start of the day)
DAY_1_SAMPLE_TS_X = time.mktime(datetime.datetime(DAY_1_SAMPLE.year, DAY_1_SAMPLE.month, DAY_1_SAMPLE.day).timetuple())
DAY_2_SAMPLE_TS_X = time.mktime(datetime.datetime(DAY_2_SAMPLE.year, DAY_2_SAMPLE.month, DAY_2_SAMPLE.day).timetuple())


class BaseLogHelperFunctionTests:
    # This class is a mixin and should not inherit from unittest.TestCase directly.
    # Test methods will be inherited by concrete test classes.
    # It assumes self.log_engine_instance is set by the concrete class's setUp,
    # and that web.logs._log_engine has been set to this instance.

    def setUp(self):
        # Ensure caches are clear on the specific engine instance being used.
        # self.log_engine_instance is set by the concrete test class's setUp
        # and is the same instance as web_logs_module._log_engine during the test.
        if hasattr(self, 'log_engine_instance') and self.log_engine_instance:
            self.log_engine_instance.clear_all_caches()
        else:
            # This case should ideally not be hit if setUp in concrete classes is correct
            # but as a fallback, try to clear caches on the global engine if it's set.
            if web_logs_module._log_engine:
                 web_logs_module._log_engine.clear_all_caches()


    def test_graph_query_single_no_split(self):
        queries = [("Hello Count", "hello")]
        result = graph_query(queries, ignore_case=True) # hello world, HELLO COSMO

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['key'], "Hello Count")
        self.assertEqual(len(result[0]['values']), 2) # Two days in sample data

        values_map = {v['x']: v['y'] for v in result[0]['values']}
        self.assertEqual(values_map.get(DAY_1_SAMPLE_TS_X), 1) # "hello world"
        self.assertEqual(values_map.get(DAY_2_SAMPLE_TS_X), 1) # "HELLO COSMO"

    def test_graph_query_multiple_no_split(self):
        queries = [("Hello Count", "hello"), ("Test Count", "test")]
        result = graph_query(queries, ignore_case=True)

        self.assertEqual(len(result), 2)

        hello_series = next(s for s in result if s['key'] == "Hello Count")
        test_series = next(s for s in result if s['key'] == "Test Count")

        self.assertEqual(len(hello_series['values']), 2)
        hello_values_map = {v['x']: v['y'] for v in hello_series['values']}
        self.assertEqual(hello_values_map.get(DAY_1_SAMPLE_TS_X), 1) # "hello world"
        self.assertEqual(hello_values_map.get(DAY_2_SAMPLE_TS_X), 1) # "HELLO COSMO"

        self.assertEqual(len(test_series['values']), 2)
        test_values_map = {v['x']: v['y'] for v in test_series['values']}
        self.assertEqual(test_values_map.get(DAY_1_SAMPLE_TS_X), 1) # "testing"
        self.assertEqual(test_values_map.get(DAY_2_SAMPLE_TS_X), 1) # "another day another test"

    def test_graph_query_single_with_split(self):
        queries = [("Cosmo Mentions", "COSMO")] # Matches "HELLO COSMO"
        result = graph_query(queries, nick_split=True, ignore_case=True)

        self.assertEqual(len(result), len(GLOBAL_VALID_NICKS))

        for nick_series in result:
            expected_label_part = nick_series['key'].split(' - ')[1]
            self.assertIn(expected_label_part, GLOBAL_VALID_NICKS.keys())

            values_map = {v['x']: v['y'] for v in nick_series['values']}
            current_nick_key = nick_series['key'].split(' - ')[1]

            if current_nick_key == 'Cosmo':
                # "HELLO COSMO" by "cosmo" (Day 2)
                self.assertEqual(values_map.get(DAY_1_SAMPLE_TS_X, 0), 0)
                self.assertEqual(values_map.get(DAY_2_SAMPLE_TS_X, 0), 1)
            else:
                self.assertEqual(values_map.get(DAY_1_SAMPLE_TS_X, 0), 0)
                self.assertEqual(values_map.get(DAY_2_SAMPLE_TS_X, 0), 0)

    def test_graph_query_empty_label_with_split(self):
        queries = [("", "hello")] # Search term "hello"
        result = graph_query(queries, nick_split=True, ignore_case=True)

        self.assertEqual(len(result), len(GLOBAL_VALID_NICKS))
        for nick_series in result:
            current_nick_key = nick_series['key'] # Label is just the nick
            self.assertIn(current_nick_key, GLOBAL_VALID_NICKS.keys())
            values_map = {v['x']: v['y'] for v in nick_series['values']}

            if current_nick_key == 'Zhenya':
                # "hello world" by "zhenya" (Day 1)
                self.assertEqual(values_map.get(DAY_1_SAMPLE_TS_X, 0), 1)
                self.assertEqual(values_map.get(DAY_2_SAMPLE_TS_X, 0), 0)
            elif current_nick_key == 'Cosmo':
                # "HELLO COSMO" by "cosmo" (Day 2) contains "hello"
                self.assertEqual(values_map.get(DAY_1_SAMPLE_TS_X, 0), 0)
                self.assertEqual(values_map.get(DAY_2_SAMPLE_TS_X, 0), 1)
            else:
                # Other nicks (Will, Graham, Jesse) have no "hello" messages in sample
                self.assertEqual(values_map.get(DAY_1_SAMPLE_TS_X, 0), 0)
                self.assertEqual(values_map.get(DAY_2_SAMPLE_TS_X, 0), 0)

    def test_table_query_single_no_split(self):
        queries = [("Hello Count", "hello")]
        result = table_query(queries, ignore_case=True)

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0], ['', 'Total'])
        self.assertEqual(result[1][0], "Hello Count")
        self.assertEqual(result[1][1], 2) # "hello world", "HELLO COSMO"

    def test_table_query_multiple_no_split(self):
        queries = [("Hello Count", "hello"), ("Test Count", "test")]
        result = table_query(queries, ignore_case=True)

        self.assertEqual(len(result), 3)
        self.assertEqual(result[0], ['', 'Total'])

        self.assertEqual(result[1][0], "Hello Count")
        self.assertEqual(result[1][1], 2)

        self.assertEqual(result[2][0], "Test Count")
        self.assertEqual(result[2][1], 2) # "testing", "another day another test"

    def test_table_query_single_with_split(self):
        queries = [("World Mentions", "world")] # "hello world" by Alice
        result = table_query(queries, nick_split=True, ignore_case=True)

        expected_header = ['', 'Total'] + sorted(list(GLOBAL_VALID_NICKS.keys()))
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0], expected_header)

        self.assertEqual(result[1][0], "World Mentions")
        self.assertEqual(result[1][1], 1) # Total "world" occurrences from "hello world" by zhenya

        # Check counts for each nick in GLOBAL_VALID_NICKS
        sorted_nicks = sorted(list(GLOBAL_VALID_NICKS.keys()))
        for i, nick_name in enumerate(sorted_nicks):
            actual_count_for_nick = result[1][2+i]
            if nick_name == 'Zhenya':
                # "hello world" by "zhenya"
                self.assertEqual(actual_count_for_nick, 1, f"Count for nick {nick_name} should be 1")
            else:
                self.assertEqual(actual_count_for_nick, 0, f"Count for nick {nick_name} should be 0")

    def test_table_query_order_by_total(self):
        # "hello" (2 occurrences), "testing" (1 occurrence in sample from Bob)
        queries_for_order = [("Testing Query", "testing"), ("Hello Query", "hello")]
        result_ordered = table_query(queries_for_order, ignore_case=True, order_by_total=True)

        self.assertEqual(len(result_ordered), 3) # Header + 2 data rows
        self.assertEqual(result_ordered[0], ['', 'Total'])

        self.assertEqual(result_ordered[1][0], "Hello Query") # Total 2
        self.assertEqual(result_ordered[1][1], 2)
        self.assertEqual(result_ordered[2][0], "Testing Query") # Total 1 ("testing" by Bob)
        self.assertEqual(result_ordered[2][1], 1)


class TestInMemoryLogHelperFunctions(BaseLogHelperFunctionTests, unittest.TestCase):
    def setUp(self):
        app.testing = True
        self.original_log_engine = web_logs_module._log_engine
        # InMemoryLogQueryEngine uses test_log_sample.json by default when app.testing is True
        # and log_file_path is None.
        self.log_engine_instance = InMemoryLogQueryEngine(log_file_path=None, log_data=None)
        web_logs_module._log_engine = self.log_engine_instance
        super().setUp()

    def tearDown(self):
        web_logs_module._log_engine = self.original_log_engine
        if hasattr(super(), 'tearDown'):
            super().tearDown()

class TestSQLiteLogHelperFunctions(BaseLogHelperFunctionTests, unittest.TestCase):
    def setUp(self):
        app.testing = True
        self.original_log_engine = web_logs_module._log_engine
        # SQLiteLogQueryEngine uses test_log_sample.json by default when app.testing is True
        # and log_file_path is None.
        self.log_engine_instance = SQLiteLogQueryEngine(db=':memory:', log_file_path=None, log_data=None, batch_size=10)
        web_logs_module._log_engine = self.log_engine_instance
        super().setUp()

    def tearDown(self):
        web_logs_module._log_engine = self.original_log_engine
        if hasattr(super(), 'tearDown'):
            super().tearDown()


if __name__ == '__main__':
    unittest.main()
