import unittest
import os
import time
import datetime # Added for clarity in setting up mocked_current_time
from unittest import mock
from web import logs 
from web.logs import LogQueryEngine
from web.trending import get_trending

class TestTrending(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        current_dir = os.path.dirname(os.path.abspath(__file__))
        sample_log_path = os.path.join(current_dir, "..", "..", "data", "sample_log.json") # Corrected path
        cls.test_log_engine_instance = LogQueryEngine(log_file_path=sample_log_path)
        if not cls.test_log_engine_instance.get_all_log_entries():
            # This check is important. If sample logs arent loaded, tests are meaningless.
            raise ValueError("Sample logs could not be loaded for TestTrendings LogQueryEngine.")

    def test_get_trending_runs_without_error_and_finds_topic(self):
        original_lqe = logs.log_query_engine
        logs.log_query_engine = self.test_log_engine_instance
        
        try:
            # Mock time.time() to a point where sample logs are within the lookback period.
            # Sample logs are dated 2023-03-15.
            # Default lookback_days in get_trending is 7.
            # Mocking time to 2023-03-16 ensures sample data is "recent".
            mocked_current_time = datetime.datetime(2023, 3, 16, 0, 0, 0).timestamp()

            with mock.patch("time.time", return_value=mocked_current_time):
                # Using min_freq=1 to ensure terms from sample data are considered.
                trending_topics = get_trending(top=5, min_freq=1, lookback_days=7)
            
            self.assertIsInstance(trending_topics, list, "get_trending should return a list.")
            
            # With sample data and mocked time, all words effectively have their "all time" frequency
            # equal to their "recent" frequency, because all logs are recent.
            # This means their trend score ( (recent/total_recent - all/total_all) / (all/total_all) ) should be near 0.
            self.assertTrue(len(trending_topics) > 0, "Expected some trending topics with sample data.")

            found_cosmo = False
            for topic, score in trending_topics:
                if topic == "cosmo": # "cosmo" appears in messages and as a nick
                    found_cosmo = True
                    self.assertAlmostEqual(score, 0.0, places=5, 
                                         msg=f"Expected diff for \"{topic}\" to be ~0.0, got {score}")
                    break
            self.assertTrue(found_cosmo, "Expected \"cosmo\" to be a trending topic with a score around 0.0.")

            found_tnank = False
            for topic, score in trending_topics:
                if topic == "tnank":
                    found_tnank = True
                    self.assertAlmostEqual(score, 0.0, places=5,
                                         msg=f"Expected diff for \"{topic}\" to be ~0.0, got {score}")
                    break
            self.assertTrue(found_tnank, "Expected \"tnank\" to be a trending topic with a score around 0.0.")

        except Exception as e:
            self.fail(f"get_trending raised an exception: {e}")
        finally:
            logs.log_query_engine = original_lqe # Restore original

if __name__ == "__main__":
    unittest.main()

