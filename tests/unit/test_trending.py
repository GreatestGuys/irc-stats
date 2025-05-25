import unittest
import os
import time # Required for mocking time
from unittest import mock # Required for mocking time

from web import logs  # To access and reconfigure web.logs.log_query_engine
from web.logs import LogQueryEngine
from web.trending import get_trending # The function to test

class TestTrending(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Correct path from tests/unit/test_trending.py to tests/data/sample_log.json
        cls.sample_log_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'sample_log.json')
        
        # Ensure the global log_query_engine in web.logs is using the sample data
        # We create a new instance for our tests
        cls.test_log_engine = LogQueryEngine(log_file_path=cls.sample_log_path)
        
        if not cls.test_log_engine.get_all_log_entries():
            raise ValueError("Sample logs could not be loaded for TestTrending.")

    def test_get_trending_runs_without_error(self):
        # The main goal is to ensure get_trending runs with the refactored LogQueryEngine
        # and doesn't crash. The actual content of trending topics with the tiny sample
        # log might be empty or very short, which is fine.

        # Mock time.time() to make slice_logs behavior predictable.
        # The sample logs are from 2023-03-15 (timestamps 1678886400 to 1678890060)
        # Let's set "now" to be within lookback_days of these logs.
        # For example, 2023-03-15 14:00:00 UTC
        mock_current_time = 1678892400.0 # This is 2023-03-15 14:00:00 UTC

        # Store original log_query_engine and replace it for this test
        original_lqe = logs.log_query_engine
        logs.log_query_engine = self.test_log_engine

        try:
            with mock.patch('time.time', return_value=mock_current_time):
                trending_topics = get_trending(top=5, min_freq=1, lookback_days=1)
                self.assertIsInstance(trending_topics, list, "get_trending should return a list.")
                
                # Based on sample_log.json and mocked time, we can make some assertions.
                # All logs are within 1 day of mock_current_time.
                # Words: "hello" (1), "everyone" (1), "hi" (1), "cosmo" (2 in messages, 2 as nick), 
                # "tnank" (1), "warm" (1), "welcome" (1), "testing" (1), "new" (1), "search" (1),
                # "functionality" (1), "another" (1), "message" (1), "from" (1)
                # Frequencies with min_freq=1:
                # 'hello': 1, 'everyone': 1, 'hi': 1, 'cosmo': 2, 'tnank': 1, 'for': 1, 'the': 2, 
                # 'warm': 1, 'welcome': 1, 'testing': 1, 'new': 1, 'search': 1, 'functionality': 1,
                # 'another': 1, 'message': 1, 'from': 1
                # Since all logs are recent and all logs are "all_logs", diff will be 0.
                # So, trending_topics should be an empty list or list of tuples with diff 0.0
                # Let's check if 'cosmo' is there, as it's the most frequent in the sample.
                # With diff being 0, it's hard to predict the exact order or content if any.
                # The primary check is that it runs and returns a list.
                # If the list is not empty, all diffs should be 0.0
                for topic, diff_value in trending_topics:
                    self.assertAlmostEqual(diff_value, 0.0, 
                                           f"Expected diff to be 0.0 for topic '{topic}' but got {diff_value}")

        except Exception as e:
            self.fail(f"get_trending raised an exception: {e}")
        finally:
            # Restore original log_query_engine
            logs.log_query_engine = original_lqe


if __name__ == '__main__':
    unittest.main()
