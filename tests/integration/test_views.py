import unittest
import os
from web import app, logs 
from web.logs import LogQueryEngine

class TestViews(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        current_dir = os.path.dirname(os.path.abspath(__file__))
        sample_log_path = os.path.join(current_dir, "..", "data", "sample_log.json")
        logs.log_query_engine = LogQueryEngine(log_file_path=sample_log_path)
        if not logs.log_query_engine.get_all_log_entries():
            raise ValueError("Sample logs could not be loaded for integration tests.")
        app.config["TESTING"] = True 

    def setUp(self):
        self.client = app.test_client()

    def test_home_route(self):
        response = self.client.get("/")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"IRC Log Statistics", response.data)
        self.assertIn(b"tnaks", response.data) 

    def test_query_route_no_params(self):
        response = self.client.get("/query")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Query Logs", response.data)

    def test_query_route_with_params(self):
        response = self.client.get("/query?label=Test&regexp=Hello")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Query Results", response.data)
        self.assertIn(b"Test", response.data) 

    def test_query_route_cumulative(self):
        response = self.client.get("/query?label=Cumul&regexp=Cosmo&cumulative=on")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Query Results", response.data)

    def test_query_route_no_match(self):
        response = self.client.get("/query?regexp=nonexistentqueryterm")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Query Results", response.data) 

    def test_browse_route(self):
        response = self.client.get("/browse")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Browse Logs by Day", response.data)
        self.assertIn(b"15 March 2023", response.data) 

    def test_browse_day_route_valid_date(self):
        response = self.client.get("/browse/2023/3/15") 
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Logs for 2023-03-15", response.data)
        self.assertIn(b"Hello everyone!", response.data) 

    def test_browse_day_route_invalid_date(self):
        response = self.client.get("/browse/2000/1/1")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Logs for 2000-01-01", response.data)
        self.assertIn(b"No logs found for this day.", response.data) 

    def test_search_route_no_params(self):
        response = self.client.get("/search")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Search Logs", response.data)

    def test_search_route_with_query(self):
        response = self.client.get("/search?q=TNANK")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Search Results", response.data)
        self.assertIn(b"TNANK", response.data) 
        self.assertIn(b"Hi Cosmo! TNANK for the warm welcome.", response.data) 

    def test_search_route_no_match(self):
        response = self.client.get("/search?q=nonexistentsearchtermforsure")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Search Results", response.data)
        self.assertIn(b"No results found.", response.data) 

if __name__ == "__main__":
    unittest.main()

