import unittest
import os
import sys
import time
import datetime

# Add the parent directory to the Python path to allow importing web.logs
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from web.logs import InMemoryLogQueryEngine, SQLiteLogQueryEngine, VALID_NICKS as GLOBAL_VALID_NICKS
from web import app # To set app.testing

# Helper to create timestamp strings from datetime objects
def ts(dt_obj):
    return str(time.mktime(dt_obj.timetuple()))

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
        self.assertEqual(engine.query_logs("anything"), [])

    def test_query_logs_single_entry_match(self):
        log_data = [{"timestamp": ts(DAY_1), "nick": "UserA", "message": "hello world"}]
        engine = self.create_engine(log_data=log_data)
        results = engine.query_logs("hello")
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]['y'], 1)
        expected_ts = time.mktime(datetime.datetime(DAY_1.year, DAY_1.month, DAY_1.day).timetuple())
        self.assertEqual(results[0]['x'], expected_ts)

    def test_query_logs_no_match(self):
        log_data = [{"timestamp": ts(DAY_1), "nick": "UserA", "message": "goodbye moon"}]
        engine = self.create_engine(log_data=log_data)
        results = engine.query_logs("hello")
        # The results should contain an entry for each day in the log range, even if count is 0
        self.assertEqual(len(results), 1) 
        self.assertEqual(results[0]['y'], 0)

    def test_query_logs_case_insensitivity(self):
        log_data = [
            {"timestamp": ts(DAY_1), "nick": "UserA", "message": "Hello"},
            {"timestamp": ts(DAY_2), "nick": "UserA", "message": "HELLO"}
        ]
        engine = self.create_engine(log_data=log_data)
        
        results_sensitive = engine.query_logs("Hello", ignore_case=False)
        self.assertEqual(len(results_sensitive), 2)
        # Assuming DAY_1 is results_sensitive[0] and DAY_2 is results_sensitive[1] after sorting by 'x'
        results_sensitive_map = {r['x']: r['y'] for r in results_sensitive}
        day1_ts = time.mktime(datetime.datetime(DAY_1.year, DAY_1.month, DAY_1.day).timetuple())
        day2_ts = time.mktime(datetime.datetime(DAY_2.year, DAY_2.month, DAY_2.day).timetuple())
        self.assertEqual(results_sensitive_map.get(day1_ts), 1)
        self.assertEqual(results_sensitive_map.get(day2_ts), 0)

        engine.clear_all_caches() 
        results_insensitive = engine.query_logs("Hello", ignore_case=True)
        self.assertEqual(len(results_insensitive), 2)
        results_insensitive_map = {r['x']: r['y'] for r in results_insensitive}
        self.assertEqual(results_insensitive_map.get(day1_ts), 1)
        self.assertEqual(results_insensitive_map.get(day2_ts), 1)


    def test_query_logs_nick_filter(self):
        log_data = [
            {"timestamp": ts(DAY_1), "nick": "Alice", "message": "apple Alice"},
            {"timestamp": ts(DAY_1), "nick": "Bob", "message": "apple Bob"},
            {"timestamp": ts(DAY_2), "nick": "alice", "message": "banana Alice"}, # Note: 'alice' is an alias for 'Alice'
        ]
        engine = self.create_engine(log_data=log_data)
        
        results_alice = engine.query_logs("apple", nick="Alice")
        self.assertEqual(len(results_alice), 2) 
        day1_ts = time.mktime(datetime.datetime(DAY_1.year, DAY_1.month, DAY_1.day).timetuple())
        day2_ts = time.mktime(datetime.datetime(DAY_2.year, DAY_2.month, DAY_2.day).timetuple())
        results_map = {r['x']: r['y'] for r in results_alice}
        self.assertEqual(results_map.get(day1_ts), 1) 
        self.assertEqual(results_map.get(day2_ts), 0) 
        
        engine.clear_all_caches()
        results_bob = engine.query_logs("apple", nick="Bob")
        self.assertEqual(len(results_bob), 2)
        results_map_bob = {r['x']: r['y'] for r in results_bob}
        self.assertEqual(results_map_bob.get(day1_ts), 1)
        self.assertEqual(results_map_bob.get(day2_ts), 0)

        engine.clear_all_caches()
        results_alice_banana = engine.query_logs("banana", nick="Alice")
        self.assertEqual(len(results_alice_banana), 2)
        results_map_ab = {r['x']: r['y'] for r in results_alice_banana}
        self.assertEqual(results_map_ab.get(day1_ts), 0)
        self.assertEqual(results_map_ab.get(day2_ts), 1)

    def test_query_logs_cumulative_logic(self):
        log_data = [
            {"timestamp": ts(DAY_1), "nick": "UserA", "message": "event"},
            {"timestamp": ts(DAY_1), "nick": "UserA", "message": "event"}, 
            {"timestamp": ts(DAY_2), "nick": "UserA", "message": "event"}, 
            {"timestamp": ts(DAY_3), "nick": "UserA", "message": "event"} 
        ]
        engine = self.create_engine(log_data=log_data)
        results = engine.query_logs("event", cumulative=True)
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
        results = engine.query_logs("hello", coarse=True) # Coarse aggregates by month
        
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
        results = engine.query_logs("target", normalize=True, normalize_type="trailing_avg_1")
        self.assertEqual(len(results), 2)
        results_map = {r['x']: r['y'] for r in results}
        day1_ts = time.mktime(datetime.datetime(DAY_1.year, DAY_1.month, DAY_1.day).timetuple())
        day2_ts = time.mktime(datetime.datetime(DAY_2.year, DAY_2.month, DAY_2.day).timetuple())

        self.assertAlmostEqual(results_map.get(day1_ts), 1.0/2.0, places=2) 
        self.assertAlmostEqual(results_map.get(day2_ts), 2.0/5.0, places=2)

    def test_count_occurrences_empty_data(self):
        engine = self.create_engine(log_data=[])
        self.assertEqual(engine.count_occurrences("anything"), 0)

    def test_count_occurrences_simple(self):
        log_data = [
            {"timestamp": ts(DAY_1), "nick": "UserA", "message": "hello world"},
            {"timestamp": ts(DAY_1), "nick": "UserA", "message": "HELLO again"}
        ]
        engine = self.create_engine(log_data=log_data)
        self.assertEqual(engine.count_occurrences("hello", ignore_case=True), 2)
        engine.clear_all_caches() # Still useful if the engine itself uses caching (like InMemory)
        self.assertEqual(engine.count_occurrences("hello", ignore_case=False), 1)

    def test_count_occurrences_nick_filter(self):
        log_data = [
            {"timestamp": ts(DAY_1), "nick": "Alice", "message": "msg from Alice"},
            {"timestamp": ts(DAY_1), "nick": "Bob", "message": "msg from Bob"}
        ]
        # VALID_NICKS['Alice'] = ['alice']
        # VALID_NICKS['Bob'] = ['bob']
        engine = self.create_engine(log_data=log_data)
        self.assertEqual(engine.count_occurrences("msg", nick="Alice"), 1)
        engine.clear_all_caches()
        self.assertEqual(engine.count_occurrences("msg", nick="Charlie"), 0) # Charlie is not in log_data

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

    def test_get_all_days_empty_data(self):
        engine = self.create_engine(log_data=[])
        self.assertEqual(engine.get_all_days(), [])
        
    def test_get_all_days_populated(self):
        log_data = [
            {"timestamp": ts(DAY_1), "nick": "UserA", "message": "m1"},
            {"timestamp": ts(DAY_3), "nick": "UserA", "message": "m3"}, # DAY_2 is missing in data
        ]
        engine = self.create_engine(log_data=log_data)
        expected_days = [
            (DAY_1.year, DAY_1.month, DAY_1.day),
            (DAY_2.year, DAY_2.month, DAY_2.day), 
            (DAY_3.year, DAY_3.month, DAY_3.day),
        ]
        self.assertEqual(engine.get_all_days(), expected_days)

    def test_get_logs_by_day(self):
        log_d1_1 = {"timestamp": ts(DAY_1), "nick": "UserA", "message": "d1m1"}
        log_d1_2 = {"timestamp": ts(DAY_1), "nick": "UserB", "message": "d1m2"} # Same day, different time or data
        log_d2_1 = {"timestamp": ts(DAY_2), "nick": "UserA", "message": "d2m1"}
        # For SQLite, ensure timestamps are distinct enough if they affect order within the day, though get_logs_by_day structure might not show it.
        # The test data is simple enough that string representation of DAY_1 will be the same for log_d1_1 and log_d1_2.
        # The engines should preserve the order of logs as received for a given day if timestamps are identical, or sort by timestamp.
        # The current InMemory implementation preserves order from original list for same-day logs. SQLite sorts by timestamp.
        # Let's make timestamps slightly different to ensure SQLite ordering is tested if it matters.
        log_d1_1_ts = DAY_1
        log_d1_2_ts = DAY_1 + datetime.timedelta(seconds=1)
        log_d1_1 = {"timestamp": ts(log_d1_1_ts), "nick": "UserA", "message": "d1m1"}
        log_d1_2 = {"timestamp": ts(log_d1_2_ts), "nick": "UserB", "message": "d1m2"}

        log_data = [log_d1_1, log_d1_2, log_d2_1] # Original order
        engine = self.create_engine(log_data=log_data)
        
        logs_by_day = engine.get_logs_by_day()
        key_d1 = (DAY_1.year, DAY_1.month, DAY_1.day)
        key_d2 = (DAY_2.year, DAY_2.month, DAY_2.day)
        
        self.assertIn(key_d1, logs_by_day)
        self.assertIn(key_d2, logs_by_day)
        self.assertEqual(len(logs_by_day[key_d1]), 2)
        self.assertEqual(len(logs_by_day[key_d2]), 1)
        # Depending on engine, order within logs_by_day[key_d1] might vary if timestamps were identical.
        # With distinct timestamps, order should be consistent (by timestamp).
        self.assertEqual(logs_by_day[key_d1], [log_d1_1, log_d1_2])
        self.assertEqual(logs_by_day[key_d2], [log_d2_1])

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

        results = engine.search_day_logs("search me")
        self.assertEqual(len(results), 2)
        
        # Result 0 should be from DAY_2 (later date, appears first due to reverse day sort)
        self.assertEqual(results[0][2]['message'], "search me on day 2")
        self.assertEqual(results[0][0], (DAY_2.year, DAY_2.month, DAY_2.day)) # (day_tuple)
        self.assertEqual(results[0][1], 0) # index within that day's log list
        self.assertIsNotNone(results[0][3]) # start match index
        
        # Result 1 should be from DAY_1
        self.assertEqual(results[1][2]['message'], "search me on day 1")
        self.assertEqual(results[1][0], (DAY_1.year, DAY_1.month, DAY_1.day))
        self.assertEqual(results[1][1], 0) # index

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
        
    def test_nick_filtering_various_cases(self):
        # This test modifies GLOBAL_VALID_NICKS. This is problematic for test isolation.
        # For now, we'll keep it but acknowledge this could be an issue.
        # A better solution would be to pass valid_nicks to the engine or mock it.
        original_valid_nicks = GLOBAL_VALID_NICKS.copy()
        
        # Setup specific nicks for this test
        GLOBAL_VALID_NICKS.clear()
        GLOBAL_VALID_NICKS['SpecificUser'] = ['specificuser', 'su']
        GLOBAL_VALID_NICKS['OtherUser'] = ['otheruser']
        GLOBAL_VALID_NICKS['Alice'] = ['alice'] # Make sure default nicks used elsewhere are present if needed
        GLOBAL_VALID_NICKS['Bob'] = ['bob']
        GLOBAL_VALID_NICKS['Charlie'] = ['charlie']


        log_data = [
            {"timestamp": ts(DAY_1), "nick": "SpecificUser", "message": "msg1 by SpecificUser"}, 
            {"timestamp": ts(DAY_1), "nick": "specificuser", "message": "msg2 by specificuser"}, # alias
            {"timestamp": ts(DAY_1), "nick": "OtherUser", "message": "msg3 by OtherUser"},    
            {"timestamp": ts(DAY_1), "nick": "Unknown", "message": "msg4 by Unknown"} # Not in VALID_NICKS for filtering
        ]
        engine = self.create_engine(log_data=log_data)

        # Test filtering for "SpecificUser"
        results = engine.query_logs("msg", nick="SpecificUser")
        self.assertEqual(len(results),1) # Data is only for DAY_1
        self.assertEqual(results[0]['y'], 2) # msg1 and msg2

        engine.clear_all_caches()
        # Test with a nick that has no logs
        GLOBAL_VALID_NICKS['EmptyNick'] = ['emptynick']
        results_empty = engine.query_logs("msg", nick="EmptyNick")
        self.assertEqual(len(results_empty),1)
        self.assertEqual(results_empty[0]['y'], 0)
        del GLOBAL_VALID_NICKS['EmptyNick'] 

        engine.clear_all_caches()
        # Test query without nick filter, expecting "Unknown" to be found if "msg4" is searched
        results_all_for_msg4 = engine.query_logs("msg4 by Unknown") 
        self.assertEqual(results_all_for_msg4[0]['y'], 1) # Found because no nick filter applied
        
        # Restore original GLOBAL_VALID_NICKS
        GLOBAL_VALID_NICKS.clear()
        GLOBAL_VALID_NICKS.update(original_valid_NICKS)

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
        
        results = engine.query_logs("hello", ignore_case=True) # Matches "hello world" and "HELLO COSMO"
        self.assertEqual(len(results), 2) # Two days in sample: 2023-03-15, 2023-03-16
        
        day1_sample_dt = datetime.datetime(2023, 3, 15)
        day2_sample_dt = datetime.datetime(2023, 3, 16)
        
        exp_x1 = time.mktime(day1_sample_dt.timetuple())
        exp_x2 = time.mktime(day2_sample_dt.timetuple())

        results_map = {r['x']: r['y'] for r in results}
        self.assertEqual(results_map.get(exp_x1), 1) # "hello world" on 2023-03-15
        self.assertEqual(results_map.get(exp_x2), 1) # "HELLO COSMO" on 2023-03-16

        engine.clear_all_caches()
        count = engine.count_occurrences("test", ignore_case=True) # "testing", "another day another test"
        self.assertEqual(count, 2)


# Concrete test class for InMemoryLogQueryEngine
class TestInMemoryLogQueryEngine(BaseLogQueryEngineTests, unittest.TestCase):
    def setUp(self):
        app.testing = True
        # Potentially call super().setUp() if BaseLogQueryEngineTests had a setUp

    def create_engine(self, log_data=None, log_file_path=None):
        # app.testing must be True for InMemoryLogQueryEngine to use test_log_sample.json by default
        return InMemoryLogQueryEngine(log_data=log_data, log_file_path=log_file_path)

# Concrete test class for SQLiteLogQueryEngine
class TestSQLiteLogQueryEngine(BaseLogQueryEngineTests, unittest.TestCase):
    def setUp(self):
        app.testing = True
        # Potentially call super().setUp()

    def create_engine(self, log_data=None, log_file_path=None):
        # app.testing must be True for SQLiteLogQueryEngine to use test_log_sample.json by default
        # Using a small batch_size for testing purposes, if applicable to any specific tests.
        return SQLiteLogQueryEngine(log_data=log_data, log_file_path=log_file_path, batch_size=10)


if __name__ == '__main__':
    unittest.main()
