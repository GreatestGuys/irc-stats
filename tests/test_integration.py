import unittest
import os
import sys

# Add the parent directory to the Python path to allow importing web
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from web import app # web/__init__.py creates the Flask app instance

class TestIntegration(unittest.TestCase):

    def setUp(self):
        app.testing = True # Set testing mode
        self.client = app.test_client()
        # The LogQueryEngine in web/logs.py should now automatically use
        # tests/test_log_sample.json due to app.testing = True

    def test_home_route(self):
        response = self.client.get('/')
        self.assertEqual(response.status_code, 200)

    def test_query_route_no_params(self):
        response = self.client.get('/query')
        self.assertEqual(response.status_code, 200)

    def test_query_route_with_params(self):
        response = self.client.get('/query?label=test&regexp=hello')
        self.assertEqual(response.status_code, 200)

    def test_browse_route(self):
        response = self.client.get('/browse')
        self.assertEqual(response.status_code, 200)

    def test_browse_day_route(self):
        # Timestamps in test_log_sample.json:
        # "1678886400" -> 2023-03-15
        # "1678972800" -> 2023-03-16
        response = self.client.get('/browse/2023/03/15')
        self.assertEqual(response.status_code, 200)
        response_next_day = self.client.get('/browse/2023/03/16')
        self.assertEqual(response_next_day.status_code, 200)


    def test_browse_day_route_invalid_date(self):
        # This date does not exist in the sample logs
        response = self.client.get('/browse/2000/01/01')
        self.assertEqual(response.status_code, 200) # Expect graceful handling (empty page)

    def test_search_route_no_params(self):
        response = self.client.get('/search')
        self.assertEqual(response.status_code, 200)

    def test_search_route_with_params(self):
        response = self.client.get('/search?q=test&ignore_case=true')
        self.assertEqual(response.status_code, 200)

if __name__ == '__main__':
    unittest.main()
