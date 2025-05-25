import unittest
import os
import sys
import time
import datetime

# Add the parent directory to the Python path to allow importing web.logs
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from web.logs import LogQueryEngine, VALID_NICKS as GLOBAL_VALID_NICKS
from web import app # To set app.testing

# Helper to create timestamp strings from datetime objects
def ts(dt_obj):
    return str(time.mktime(dt_obj.timetuple()))

BASE_DATE = datetime.datetime(2023, 3, 15, 10, 0, 0) 
DAY_1 = BASE_DATE
DAY_2 = BASE_DATE + datetime.timedelta(days=1)
DAY_3 = BASE_DATE + datetime.timedelta(days=2)

class TestLogQueryEngine(unittest.TestCase):

    def setUp(self):
        app.testing = True
        # Each test method creates its own LogQueryEngine instance.
        # Caches are per-instance for lru_cache on methods, so no cross-test contamination.
        # clear_all_caches() is called within tests if an instance is reused for multiple queries.

    def test_query_logs_empty_data(self):
        log_data = []
        engine = LogQueryEngine(log_data=log_data)
        self.assertEqual(engine.query_logs("anything"), [])

    def test_query_logs_single_entry_match(self):
        log_data = [{"timestamp": ts(DAY_1), "nick": "UserA", "message": "hello world"}]
        engine = LogQueryEngine(log_data=log_data)
        results = engine.query_logs("hello")
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]['y'], 1)
        # Create the expected timestamp for the beginning of DAY_1
        expected_ts = time.mktime(datetime.datetime(DAY_1.year, DAY_1.month, DAY_1.day).timetuple())
        self.assertEqual(results[0]['x'], expected_ts)

    def test_query_logs_no_match(self):
        log_data = [{"timestamp": ts(DAY_1), "nick": "UserA", "message": "goodbye moon"}]
        engine = LogQueryEngine(log_data=log_data)
        results = engine.query_logs("hello")
        self.assertEqual(len(results), 1) 
        self.assertEqual(results[0]['y'], 0)

    def test_query_logs_case_insensitivity(self):
        log_data = [
            {"timestamp": ts(DAY_1), "nick": "UserA", "message": "Hello"},
            {"timestamp": ts(DAY_2), "nick": "UserA", "message": "HELLO"}
        ]
        engine = LogQueryEngine(log_data=log_data)
        
        results_sensitive = engine.query_logs("Hello", ignore_case=False)
        self.assertEqual(len(results_sensitive), 2)
        self.assertEqual(results_sensitive[0]['y'], 1) 
        self.assertEqual(results_sensitive[1]['y'], 0) 

        engine.clear_all_caches() 
        results_insensitive = engine.query_logs("Hello", ignore_case=True)
        self.assertEqual(len(results_insensitive), 2)
        self.assertEqual(results_insensitive[0]['y'], 1) 
        self.assertEqual(results_insensitive[1]['y'], 1)

    def test_query_logs_nick_filter(self):
        log_data = [
            {"timestamp": ts(DAY_1), "nick": "Alice", "message": "apple Alice"},
            {"timestamp": ts(DAY_1), "nick": "Bob", "message": "apple Bob"},
            {"timestamp": ts(DAY_2), "nick": "alice", "message": "banana Alice"},
        ]
        engine = LogQueryEngine(log_data=log_data)
        
        results_alice = engine.query_logs("apple", nick="Alice")
        # Alice (or alias 'alice') has messages on DAY_1 and DAY_2.
        # So, results_alice will have two points.
        self.assertEqual(len(results_alice), 2) 
        # results_alice[0] corresponds to DAY_1 because log_data is processed in order and then sorted by date.
        # DAY_1: Alice says "apple Alice" (1 match for "apple")
        # DAY_2: alice says "banana Alice" (0 matches for "apple")
        # The order of results from query_logs is sorted by timestamp 'x'.
        # DAY_1_ts will be less than DAY_2_ts.
        day1_ts = time.mktime(datetime.datetime(DAY_1.year, DAY_1.month, DAY_1.day).timetuple())
        day2_ts = time.mktime(datetime.datetime(DAY_2.year, DAY_2.month, DAY_2.day).timetuple())

        # Create a map for easier checking if order is not guaranteed or simply to be robust
        results_map = {r['x']: r['y'] for r in results_alice}
        self.assertEqual(results_map.get(day1_ts), 1) # DAY_1, "apple"
        self.assertEqual(results_map.get(day2_ts), 0) # DAY_2, no "apple"
        
        engine.clear_all_caches()
        results_bob = engine.query_logs("apple", nick="Bob")
        # Bob has messages on DAY_1. The log data also includes DAY_2 (from Alice).
        # The query_logs function will create entries for all days in the full log range.
        self.assertEqual(len(results_bob), 2) # DAY_1 and DAY_2
        
        # DAY_1: Bob says "apple Bob" (1 match for "apple")
        # DAY_2: Bob has no messages (0 matches for "apple")
        day1_ts_bob = time.mktime(datetime.datetime(DAY_1.year, DAY_1.month, DAY_1.day).timetuple())
        day2_ts_bob = time.mktime(datetime.datetime(DAY_2.year, DAY_2.month, DAY_2.day).timetuple())
        
        results_map_bob = {r['x']: r['y'] for r in results_bob}
        self.assertEqual(results_map_bob.get(day1_ts_bob), 1) # DAY_1, "apple"
        self.assertEqual(results_map_bob.get(day2_ts_bob), 0) # DAY_2, no "apple" for Bob

        engine.clear_all_caches()
        results_alice_banana = engine.query_logs("banana", nick="Alice") # Alice is canonical
        self.assertEqual(len(results_alice_banana), 2)
        # For consistency and robustness, use a map for checking results
        day1_ts_ab = time.mktime(datetime.datetime(DAY_1.year, DAY_1.month, DAY_1.day).timetuple())
        day2_ts_ab = time.mktime(datetime.datetime(DAY_2.year, DAY_2.month, DAY_2.day).timetuple())
        results_map_ab = {r['x']: r['y'] for r in results_alice_banana}
        self.assertEqual(results_map_ab.get(day1_ts_ab), 0) # DAY_1, Alice, no "banana"
        self.assertEqual(results_map_ab.get(day2_ts_ab), 1) # DAY_2, (lowercase) alice, "banana"

    def test_query_logs_cumulative_logic(self):
        log_data = [
            {"timestamp": ts(DAY_1), "nick": "UserA", "message": "event"},
            {"timestamp": ts(DAY_1), "nick": "UserA", "message": "event"}, 
            {"timestamp": ts(DAY_2), "nick": "UserA", "message": "event"}, 
            {"timestamp": ts(DAY_3), "nick": "UserA", "message": "event"} 
        ]
        engine = LogQueryEngine(log_data=log_data)
        results = engine.query_logs("event", cumulative=True)
        self.assertEqual(len(results), 3)
        self.assertEqual(results[0]['y'], 2) 
        self.assertEqual(results[1]['y'], 3) 
        self.assertEqual(results[2]['y'], 4)

    def test_query_logs_timestamps_and_coarse(self):
        day1_month1 = datetime.datetime(2023, 1, 15, 10, 0, 0)
        day2_month1 = datetime.datetime(2023, 1, 16, 10, 0, 0)
        day1_month2 = datetime.datetime(2023, 2, 10, 10, 0, 0)
        log_data = [
            {"timestamp": ts(day1_month1), "nick": "UserA", "message": "hello"},
            {"timestamp": ts(day1_month1 + datetime.timedelta(hours=1)), "nick": "UserA", "message": "hello"},
            {"timestamp": ts(day2_month1), "nick": "UserA", "message": "hello"}, 
            {"timestamp": ts(day1_month2), "nick": "UserA", "message": "hello"}, 
        ]
        engine = LogQueryEngine(log_data=log_data)
        results = engine.query_logs("hello", coarse=True)
        
        self.assertEqual(len(results), 2) 
        
        month1_ts = time.mktime(datetime.datetime(2023, 1, 1).timetuple())
        month2_ts = time.mktime(datetime.datetime(2023, 2, 1).timetuple())

        results_map = {r['x']: r['y'] for r in results}
        self.assertEqual(results_map.get(month1_ts), 3)
        self.assertEqual(results_map.get(month2_ts), 1)

    def test_query_logs_normalization_logic(self):
        log_data = [
            {"timestamp": ts(DAY_1), "nick": "UserA", "message": "target"},
            {"timestamp": ts(DAY_1), "nick": "UserA", "message": "other"},
            {"timestamp": ts(DAY_2), "nick": "UserA", "message": "target"},
            {"timestamp": ts(DAY_2), "nick": "UserA", "message": "target"},
            {"timestamp": ts(DAY_2), "nick": "UserA", "message": "noise"},
            {"timestamp": ts(DAY_2), "nick": "UserA", "message": "noise"},
            {"timestamp": ts(DAY_2), "nick": "UserA", "message": "noise"},
        ]
        engine = LogQueryEngine(log_data=log_data)
        results = engine.query_logs("target", normalize=True, normalize_type="trailing_avg_1")
        self.assertEqual(len(results), 2)
        self.assertAlmostEqual(results[0]['y'], 1.0/2.0, places=2) 
        self.assertAlmostEqual(results[1]['y'], 2.0/5.0, places=2)

    def test_count_occurrences_empty_data(self):
        engine = LogQueryEngine(log_data=[])
        self.assertEqual(engine.count_occurrences("anything"), 0)

    def test_count_occurrences_simple(self):
        log_data = [
            {"timestamp": ts(DAY_1), "nick": "UserA", "message": "hello world"},
            {"timestamp": ts(DAY_1), "nick": "UserA", "message": "HELLO again"}
        ]
        engine = LogQueryEngine(log_data=log_data)
        self.assertEqual(engine.count_occurrences("hello", ignore_case=True), 2)
        engine.clear_all_caches()
        self.assertEqual(engine.count_occurrences("hello", ignore_case=False), 1)

    def test_count_occurrences_nick_filter(self):
        log_data = [
            {"timestamp": ts(DAY_1), "nick": "Alice", "message": "msg from Alice"},
            {"timestamp": ts(DAY_1), "nick": "Bob", "message": "msg from Bob"}
        ]
        engine = LogQueryEngine(log_data=log_data)
        self.assertEqual(engine.count_occurrences("msg", nick="Alice"), 1)
        engine.clear_all_caches()
        self.assertEqual(engine.count_occurrences("msg", nick="Charlie"), 0)

    def test_get_valid_days_empty_data(self):
        engine = LogQueryEngine(log_data=[])
        self.assertEqual(engine.get_valid_days(), [])

    def test_get_valid_days_populated(self):
        log_data = [
            {"timestamp": ts(DAY_1), "nick": "UserA", "message": "m1"},
            {"timestamp": ts(DAY_2), "nick": "UserA", "message": "m2"},
        ]
        engine = LogQueryEngine(log_data=log_data)
        expected_days = sorted([
            (DAY_1.year, DAY_1.month, DAY_1.day),
            (DAY_2.year, DAY_2.month, DAY_2.day)
        ])
        self.assertEqual(engine.get_valid_days(), expected_days)

    def test_get_all_days_empty_data(self):
        engine = LogQueryEngine(log_data=[])
        self.assertEqual(engine.get_all_days(), [])
        
    def test_get_all_days_populated(self):
        log_data = [
            {"timestamp": ts(DAY_1), "nick": "UserA", "message": "m1"},
            {"timestamp": ts(DAY_3), "nick": "UserA", "message": "m3"}, # DAY_2 is missing in data
        ]
        engine = LogQueryEngine(log_data=log_data)
        expected_days = [
            (DAY_1.year, DAY_1.month, DAY_1.day),
            (DAY_2.year, DAY_2.month, DAY_2.day), # Should be included
            (DAY_3.year, DAY_3.month, DAY_3.day),
        ]
        self.assertEqual(engine.get_all_days(), expected_days)

    def test_get_logs_by_day(self):
        log_d1_1 = {"timestamp": ts(DAY_1), "nick": "UserA", "message": "d1m1"}
        log_d1_2 = {"timestamp": ts(DAY_1), "nick": "UserB", "message": "d1m2"}
        log_d2_1 = {"timestamp": ts(DAY_2), "nick": "UserA", "message": "d2m1"}
        log_data = [log_d1_1, log_d1_2, log_d2_1]
        engine = LogQueryEngine(log_data=log_data)
        
        logs_by_day = engine.get_logs_by_day()
        key_d1 = (DAY_1.year, DAY_1.month, DAY_1.day)
        key_d2 = (DAY_2.year, DAY_2.month, DAY_2.day)
        
        self.assertIn(key_d1, logs_by_day)
        self.assertIn(key_d2, logs_by_day)
        self.assertEqual(len(logs_by_day[key_d1]), 2)
        self.assertEqual(len(logs_by_day[key_d2]), 1)
        self.assertEqual(logs_by_day[key_d1], [log_d1_1, log_d1_2])
        self.assertEqual(logs_by_day[key_d2], [log_d2_1])

    def test_search_day_logs_ordering_and_content(self):
        log_d1 = {"timestamp": ts(DAY_1), "nick": "UserA", "message": "search me on day 1"}
        log_d2 = {"timestamp": ts(DAY_2), "nick": "UserA", "message": "search me on day 2"}
        log_d3_no_match = {"timestamp": ts(DAY_3), "nick": "UserA", "message": "nothing here"}
        log_data = [log_d1, log_d2, log_d3_no_match] # Data not sorted by time
        engine = LogQueryEngine(log_data=log_data)

        results = engine.search_day_logs("search me")
        self.assertEqual(len(results), 2)
        
        self.assertEqual(results[0][2]['message'], "search me on day 2")
        self.assertEqual(results[0][0], (DAY_2.year, DAY_2.month, DAY_2.day))
        self.assertEqual(results[0][3], 0) 
        
        self.assertEqual(results[1][2]['message'], "search me on day 1")
        self.assertEqual(results[1][0], (DAY_1.year, DAY_1.month, DAY_1.day))

    def test_search_results_to_chart(self):
        day1_month1 = datetime.datetime(2023, 1, 15, 10, 0, 0)
        day2_month1 = datetime.datetime(2023, 1, 16, 10, 0, 0)
        day1_month2 = datetime.datetime(2023, 2, 10, 10, 0, 0)
        log_data = [
            {"timestamp": ts(day1_month1), "nick": "UserA", "message": "chart data"},
            {"timestamp": ts(day2_month1), "nick": "UserA", "message": "chart data"},
            {"timestamp": ts(day1_month2), "nick": "UserA", "message": "chart data"},
        ]
        engine = LogQueryEngine(log_data=log_data)
        
        chart_data = engine.search_results_to_chart("chart data")
        self.assertEqual(len(chart_data), 1)
        self.assertEqual(chart_data[0]['key'], "")
        
        values = chart_data[0]['values']
        self.assertEqual(len(values), 2) 

        month1_ts = time.mktime(datetime.datetime(2023, 1, 1).timetuple())
        month2_ts = time.mktime(datetime.datetime(2023, 2, 1).timetuple())
        
        results_map = {r['x']: r['y'] for r in values}
        self.assertEqual(results_map.get(month1_ts), 2)
        self.assertEqual(results_map.get(month2_ts), 1)
        
    def test_nick_filtering_various_cases(self):
        original_valid_nicks = GLOBAL_VALID_NICKS.copy()
        GLOBAL_VALID_NICKS['SpecificUser'] = ['specificuser', 'su'] # Add a temporary nick for this test
        
        log_data = [
            {"timestamp": ts(DAY_1), "nick": "SpecificUser", "message": "msg1 by SpecificUser"}, 
            {"timestamp": ts(DAY_1), "nick": "specificuser", "message": "msg2 by specificuser"}, 
            {"timestamp": ts(DAY_1), "nick": "OtherUser", "message": "msg3 by OtherUser"},    
            {"timestamp": ts(DAY_1), "nick": "Unknown", "message": "msg4 by Unknown"}       
        ]
        engine = LogQueryEngine(log_data=log_data)

        results = engine.query_logs("msg", nick="SpecificUser")
        self.assertEqual(len(results),1)
        self.assertEqual(results[0]['y'], 2) 

        engine.clear_all_caches()
        GLOBAL_VALID_NICKS['EmptyNick'] = ['emptynick']
        results_empty = engine.query_logs("msg", nick="EmptyNick")
        self.assertEqual(len(results_empty),1)
        self.assertEqual(results_empty[0]['y'], 0)
        del GLOBAL_VALID_NICKS['EmptyNick'] 

        engine.clear_all_caches()
        results_all_for_msg4 = engine.query_logs("msg4 by Unknown") 
        self.assertEqual(results_all_for_msg4[0]['y'], 1)
        
        GLOBAL_VALID_NICKS.clear()
        GLOBAL_VALID_NICKS.update(original_valid_nicks)

    def test_load_from_sample_file(self):
        sample_log_path = os.path.join(os.path.dirname(__file__), 'test_log_sample.json')
        # Ensure app.testing is true, so if log_file_path was None, it would use test_log_sample.json
        # But here we explicitly provide the path.
        engine = LogQueryEngine(log_file_path=sample_log_path)
        
        # From test_log_sample.json:
        # Alice: "hello world" (day1), "another day another test" (day2)
        # Bob: "testing" (day1)
        # Charlie: "HELLO COSMO" (day2)
        
        results = engine.query_logs("hello", ignore_case=True) # Matches "hello world" and "HELLO COSMO"
        self.assertEqual(len(results), 2) 
        # Day 1 (2023-03-15 from 1678886400)
        # Day 2 (2023-03-16 from 1678972800)
        # Assuming DAY_1 from sample is 2023-03-15, DAY_2 is 2023-03-16 for result ordering
        # The actual dates from sample file are March 15 2023, March 16 2023
        # Let's find which result corresponds to which day based on 'x' (timestamp)
        day1_sample_ts = 1678886400.0 
        day2_sample_ts = 1678972800.0
        
        # Create expected x values (start of day timestamps)
        dt1 = datetime.datetime.fromtimestamp(day1_sample_ts)
        exp_x1 = time.mktime(datetime.datetime(dt1.year, dt1.month, dt1.day).timetuple())
        dt2 = datetime.datetime.fromtimestamp(day2_sample_ts)
        exp_x2 = time.mktime(datetime.datetime(dt2.year, dt2.month, dt2.day).timetuple())

        results_map = {r['x']: r['y'] for r in results}
        self.assertEqual(results_map.get(exp_x1), 1) # "hello world"
        self.assertEqual(results_map.get(exp_x2), 1) # "HELLO COSMO"

        engine.clear_all_caches()
        count = engine.count_occurrences("test", ignore_case=True)
        self.assertEqual(count, 2)


if __name__ == '__main__':
    unittest.main()
