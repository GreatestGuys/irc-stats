import unittest
import os
from web import app, logs
from web.logs import LogQueryEngine

class TestViews(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Construct the path to the sample log file
        # This assumes tests are run from the project root directory or a similar context
        # where 'tests/data/sample_log.json' is a valid relative path.
        sample_log_path = os.path.join('tests', 'data', 'sample_log.json')
        
        # Ensure the path is correct if running from a different working directory (e.g. /app)
        if not os.path.exists(sample_log_path):
             # Try a more absolute path from common project root structures
            base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            alt_path = os.path.join(base_dir, 'tests', 'data', 'sample_log.json')
            if os.path.exists(alt_path):
                sample_log_path = alt_path
            else: # Default to the path as given if complex resolution fails (e.g. in /app)
                 sample_log_path = 'tests/data/sample_log.json'


        # Replace the globally available log_query_engine with one using the sample data
        logs.log_query_engine = LogQueryEngine(log_file_path=sample_log_path)
        # Ensure logs were loaded for the test engine
        if not logs.log_query_engine.logs:
            raise FileNotFoundError(f"Sample log file not found or empty at {sample_log_path} for test setup.")


    def setUp(self):
        self.client = app.test_client()
        app.config['TESTING'] = True # Propagate exceptions

    def test_home_route(self):
        response = self.client.get('/')
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"IRC Log Statistics", response.data)

    def test_query_route_no_params(self):
        response = self.client.get('/query')
        self.assertEqual(response.status_code, 200)
        # Check for a common element on the query page, e.g., the form or title
        self.assertIn(b"Query Logs", response.data) 

    def test_query_route_with_params(self):
        response = self.client.get('/query?label=Test&regexp=Hello')
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Query Results", response.data)
        self.assertIn(b"Test", response.data) # Check if label is rendered
        self.assertIn(b"Hello", response.data) # Check if regexp is rendered

    def test_query_route_cumulative(self):
        response = self.client.get('/query?label=TestCumulative&regexp=Cosmo&cumulative=on')
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Query Results", response.data)
        self.assertIn(b"TestCumulative", response.data)
        self.assertIn(b"Cosmo", response.data)
        # Further checks could involve chart data if it's easily verifiable in HTML

    def test_query_route_no_match(self):
        response = self.client.get('/query?regexp=nonexistenttermxyz123')
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Query Results", response.data)
        self.assertIn(b"nonexistenttermxyz123", response.data)
        # Check that the chart data indicates no results (often an empty 'values' array or specific y=0)
        # This might be too detailed; for now, presence of the term and "Query Results" is enough.

    def test_browse_route(self):
        response = self.client.get('/browse')
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Browse Logs by Day", response.data)
        # Check for the date from sample logs
        self.assertIn(b"15 March 2023", response.data)


    def test_browse_day_route_valid_date(self):
        # Date from sample_log.json
        response = self.client.get('/browse/2023/3/15') 
        self.assertEqual(response.status_code, 200)
        # The title in browse_day.html is "<day> <month_name> <year>"
        self.assertIn(b"15 March 2023", response.data) 
        # Check for one of the log messages from sample_log.json
        self.assertIn(b"Hello everyone!", response.data)
        self.assertIn(b"Hi Cosmo! TNANK for the warm welcome.", response.data)

    def test_browse_day_route_invalid_date(self):
        response = self.client.get('/browse/2000/1/1')
        self.assertEqual(response.status_code, 200)
        # The title should still reflect the requested date
        self.assertIn(b"1 January 2000", response.data)
        # Check that no actual log messages appear (e.g., messages from sample_log.json should not be here)
        self.assertNotIn(b"Hello everyone!", response.data)
        self.assertNotIn(b"Hi Cosmo! TNANK for the warm welcome.", response.data)

    def test_search_route_no_params(self):
        response = self.client.get('/search')
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Search Logs", response.data) # Title of the search page

    def test_search_route_with_query(self):
        response = self.client.get('/search?q=TNANK')
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Search Results", response.data)
        self.assertIn(b"TNANK", response.data) # The search term should be present
        # Check for the line containing "TNANK"
        self.assertIn(b"Hi Cosmo! TNANK for the warm welcome.", response.data)

    def test_search_route_no_match(self):
        response = self.client.get('/search?q=nonexistentsearchtermxyz123')
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Search Results", response.data)
        self.assertIn(b"nonexistentsearchtermxyz123", response.data)
        # Check that no results are displayed (e.g., a message or just lack of result entries)
        # The template search_results.html has "No results found."
        self.assertIn(b"No results found.", response.data)

if __name__ == '__main__':
    unittest.main()
