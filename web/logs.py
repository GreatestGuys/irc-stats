#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import datetime
import abc
import functools
import json
import os
import re
import sqlite3
import sqlite_regex
import time

from web import app, APP_STATIC
from flask import Flask, url_for, render_template, g

VALID_NICKS = {
    'Cosmo': ['cosmo', 'cfumo'],
    'Graham': ['graham', 'jorgon'],
    'Jesse': ['jesse', 'je-c'],
    'Will': ['will', 'wyll', 'wyll_'],
    'Zhenya': ['zhenyah', 'zhenya', 'zdog', 'swphantom', 'za', 'zhenya2'],
}

class AbstractLogQueryEngine(abc.ABC):
    def __init__(self, log_file_path=None, log_data=None):
        # This constructor can be used by subclasses to store common initial parameters
        # For now, it doesn't do much, but subclasses should call it via super().
        self.log_file_path = log_file_path
        self.log_data = log_data

    @abc.abstractmethod
    def clear_all_caches(self):
        pass

    @abc.abstractmethod
    def query_logs(self, s,
            cumulative=False, coarse=False, nick=None, ignore_case=False,
            normalize=False, normalize_type=None):
        pass

    @abc.abstractmethod
    def count_occurrences(self, s, ignore_case=False, nick=None):
        pass

    @abc.abstractmethod
    def get_valid_days(self):
        pass

    @abc.abstractmethod
    def get_all_days(self):
        pass

    @abc.abstractmethod
    def get_logs_by_day(self):
        pass

    @abc.abstractmethod
    def search_day_logs(self, s, ignore_case=False):
        pass

    @abc.abstractmethod
    def search_results_to_chart(self, s, ignore_case=False):
        pass

class SQLiteLogQueryEngine(AbstractLogQueryEngine):
    def __init__(self, log_file_path=None, log_data=None, batch_size=1000):
        super().__init__(log_file_path, log_data)
        self.conn = sqlite3.connect(':memory:')
        self.conn.enable_load_extension(True)
        self.batch_size = batch_size

        self.conn.enable_load_extension(True)
        sqlite_regex.load(self.conn)
        self.conn.enable_load_extension(False)

        self._create_table()
        self._load_data(log_file_path, log_data)

    def _create_table(self):
        with self.conn:
            self.conn.execute('''
                CREATE TABLE IF NOT EXISTS logs (
                    timestamp REAL,
                    nick TEXT,
                    message TEXT
                )
            ''')
            self.conn.execute('CREATE INDEX IF NOT EXISTS idx_timestamp ON logs (timestamp)')

    def _load_data(self, log_file_path, log_data):
        if log_data is not None: # Corrected condition
            logs_to_insert = log_data
        else:
            chosen_log_path = None
            if app.testing:
                chosen_log_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'tests', 'test_log_sample.json'))
                if self.log_file_path:
                    chosen_log_path = self.log_file_path
            elif self.log_file_path:
                chosen_log_path = self.log_file_path
            else:
                chosen_log_path = os.path.join(APP_STATIC, 'log.json')

            try:
                with open(chosen_log_path, 'r') as f:
                    logs_to_insert = json.load(f)
            except FileNotFoundError:
                if app.testing and chosen_log_path == os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'tests', 'test_log_sample.json')):
                    raise FileNotFoundError(f"Test log file not found during testing: {chosen_log_path}")
                elif not (app.testing or self.log_file_path):
                    logs_to_insert = [] # Default to empty if default production path is missing
                else:
                    if not app.testing:
                        raise
                    logs_to_insert = [] # Or handle as an error more explicitly if a specific path was given

        if logs_to_insert:
            with self.conn:
                for i in range(0, len(logs_to_insert), self.batch_size):
                    batch = logs_to_insert[i:i + self.batch_size]
                    self.conn.executemany(
                        'INSERT INTO logs (timestamp, nick, message) VALUES (?, ?, ?)',
                        [(item['timestamp'], item['nick'], item['message']) for item in batch]
                    )

    def clear_all_caches(self):
        # For SQLite, lru_cache might not be used in the same way.
        # If methods directly query the DB, this might be a no-op or clear specific caches if implemented.
        pass

    def query_logs(self, s,
            cumulative=False, coarse=False, nick=None, ignore_case=False,
            normalize=False, normalize_type=None):
        # Helper functions (can be static or defined outside if they don't need self)
        def to_datetime_from_ts(timestamp):
            return datetime.datetime.fromtimestamp(float(timestamp))

        def to_timestamp_from_dt(d):
            return time.mktime(
                    datetime.datetime(d.year, d.month, d.day).timetuple())

        def get_key_from_ts(timestamp, coarse_flag):
            d = to_datetime_from_ts(timestamp)
            day = 1 if coarse_flag else d.day # Coarse means aggregate by month (day=1)
            return time.mktime(
                    datetime.datetime(d.year, d.month, day).timetuple())

        def get_value_from_dict(key, m):
            if key not in m:
                m[key] = {'x': key, 'y': 0}
            return m[key]

        # --- SQL Querying Part ---
        sql_params = []
        conditions = []

        # Regex condition
        # Prepend (?i) for case-insensitivity with PCRE-like regex engines (sqlite-regex uses PCRE)
        current_pattern = f"(?i){s}" if ignore_case else s
        conditions.append("regexp(?, message)")
        sql_params.append(current_pattern)

        # Nick filter condition
        if nick:
            if nick in VALID_NICKS:
                nick_aliases = VALID_NICKS[nick]
                if nick_aliases: # Ensure there are aliases to filter by
                    placeholders = ', '.join('?' * len(nick_aliases))
                    # Use lower(nick) in SQL for case-insensitive matching of nicks
                    conditions.append(f"lower(nick) IN ({placeholders})")
                    sql_params.extend([alias.lower() for alias in nick_aliases]) # Ensure aliases are lowercase for comparison
                else: # If nick is in VALID_NICKS but has no aliases, no logs will match
                    return [] # No logs can match this nick
            else: # Unknown nick key
                return [] # No logs can match this nick

        # Construct the full SQL query for matched logs
        matched_sql = "SELECT timestamp, nick, message FROM logs"
        if conditions:
            matched_sql += " WHERE " + " AND ".join(conditions)
        matched_sql += " ORDER BY timestamp" # Ensure consistent order for processing

        cursor = self.conn.cursor()
        try:
            cursor.execute(matched_sql, sql_params)
            matched_db_logs = [{'timestamp': r[0], 'nick': r[1], 'message': r[2]} for r in cursor.fetchall()]
        except sqlite3.OperationalError as e_sqlite:
            if "no such function: regexp" in str(e_sqlite).lower():
                app.logger.error("ERROR: Native regexp function not available in SQLite for query_logs. SQLite regex extension not loaded correctly or missing.")
            else:
                app.logger.error(f"SQL error in query_logs (matched_sql): {e_sqlite} with SQL: {matched_sql} and params: {sql_params}")
            return []

        # --- Fetch all logs for total counts (for normalization) ---
        # This query should only be filtered by nick, not by message regex.
        total_sql_params = []
        total_conditions = []
        if nick:
            if nick in VALID_NICKS:
                nick_aliases = VALID_NICKS[nick]
                if nick_aliases:
                    placeholders = ', '.join('?' * len(nick_aliases))
                    total_conditions.append(f"lower(nick) IN ({placeholders})")
                    total_sql_params.extend([alias.lower() for alias in nick_aliases])
                else:
                    # If nick is in VALID_NICKS but has no aliases, total logs for this nick is 0
                    total_db_logs = []
            else: # Unknown nick key
                total_db_logs = []

        total_sql = "SELECT timestamp, nick, message FROM logs"
        if total_conditions:
            total_sql += " WHERE " + " AND ".join(total_conditions)
        total_sql += " ORDER BY timestamp"

        try:
            cursor.execute(total_sql, total_sql_params)
            total_db_logs = [{'timestamp': r[0], 'nick': r[1], 'message': r[2]} for r in cursor.fetchall()]
        except sqlite3.OperationalError as e_sqlite:
            app.logger.error(f"SQL error in query_logs (total_sql): {e_sqlite} with SQL: {total_sql} and params: {total_sql_params}")
            return []

        if not matched_db_logs and not total_db_logs:
            return []

        # --- Python Processing Part (similar to InMemoryLogQueryEngine) ---
        results = {}  # For messages matching s
        totals = {}   # For all messages (within nick filter)

        # Populate totals first
        for line in total_db_logs:
            key = get_key_from_ts(line['timestamp'], coarse)
            total_value = get_value_from_dict(key, totals)
            total_value['y'] += 1

        # Populate results (matched messages)
        for line in matched_db_logs:
            key = get_key_from_ts(line['timestamp'], coarse)
            value = get_value_from_dict(key, results)
            value['y'] += 1

        smoothed = {}
        total_matched_overall = 0
        total_possible_overall = 0

        # Determine the full date range from the data
        all_days_tuples = self.get_all_days() # This returns (y,m,d)
        if not all_days_tuples:
            # If there are no valid days, but there might be logs (e.g., only logs for a specific nick that doesn't exist)
            # we should still return an empty list.
            return []

        # Generate all relevant keys based on all_days_tuples and coarse flag
        # This ensures that days/months with zero matches are also included.
        all_relevant_keys = sorted(list(set(
            get_key_from_ts(to_timestamp_from_dt(datetime.datetime(d[0],d[1],d[2])), coarse)
            for d in all_days_tuples
        )))

        # Initialize smoothed and totals for all relevant keys to ensure all periods are present
        for key_ts in all_relevant_keys:
            if key_ts not in results:
                results[key_ts] = {'x': key_ts, 'y': 0}
            if key_ts not in totals:
                totals[key_ts] = {'x': key_ts, 'y': 0}

        # Now iterate through sorted keys to apply cumulative and normalization logic
        for key_ts in all_relevant_keys:
            value = results[key_ts]['y'] # Matched count for this period
            total_for_period = totals[key_ts]['y'] # Total count for this period

            total_possible_overall += total_for_period
            total_matched_overall += value

            if cumulative:
                smoothed[key_ts] = {'x': key_ts, 'y': total_matched_overall}
                # For cumulative normalization, totals also need to be cumulative
                totals[key_ts]['y'] = total_possible_overall # Update the 'y' value in the totals dict
            else:
                smoothed[key_ts] = {'x': key_ts, 'y': value}
                # totals[key_ts] already stores total_for_period from the loop over db_logs

        if normalize: # Normalization should only happen if there's data to normalize
            final_smoothed_values = []
            total_window_values = []
            matched_window_values = []

            for key_ts in sorted(smoothed.keys()):
                current_total_for_period = totals[key_ts]['y']
                current_matched_for_period = smoothed[key_ts]['y']

                if cumulative:
                    if current_total_for_period == 0:
                        smoothed[key_ts]['y'] = 0
                    else:
                        smoothed[key_ts]['y'] = current_matched_for_period / current_total_for_period
                else: # Non-cumulative normalization
                    total_window_values.append(current_total_for_period)
                    matched_window_values.append(current_matched_for_period)

                    if str(normalize_type or '').startswith('trailing_avg_'):
                        window_size = int(normalize_type[13:])
                        total_window_values = total_window_values[-window_size:]
                        matched_window_values = matched_window_values[-window_size:]
                    else: # Default window is just the current period
                        total_window_values = total_window_values[-1:]
                        matched_window_values = matched_window_values[-1:]

                    if sum(total_window_values) == 0:
                        smoothed[key_ts]['y'] = 0
                    else:
                        smoothed[key_ts]['y'] = sum(matched_window_values) / sum(total_window_values)

                final_smoothed_values.append(smoothed[key_ts])

            return sorted(final_smoothed_values, key=lambda x_val: x_val['x'])
        else: # No normalization
            return sorted(smoothed.values(), key=lambda x_val: x_val['x'])

    def count_occurrences(self, s, ignore_case=False, nick=None):
        sql_params = []
        conditions = []

        # Validate basic Python regex syntax first
        try:
            re.compile(s)
        except re.error as e_re:
            app.logger.warning(f"Invalid Python regex syntax for pattern '{s}': {e_re}")
            return 0

        # Prepare pattern for SQLite regexp: pattern first, then text.
        # Prepend (?i) for case-insensitivity with PCRE-like regex engines (sqlite-regex uses PCRE)
        current_pattern = f"(?i){s}" if ignore_case else s
        conditions.append("regexp(?, message)")
        sql_params.append(current_pattern)

        if nick:
            if nick in VALID_NICKS:
                nick_aliases = VALID_NICKS[nick]
                if not nick_aliases:
                     return 0 # Should not happen if nick is a valid key and VALID_NICKS is well-formed
                placeholders = ', '.join('?' * len(nick_aliases))
                # Using lower(nick) in SQL for case-insensitive matching.
                conditions.append(f"lower(nick) IN ({placeholders})")
                sql_params.extend([alias.lower() for alias in nick_aliases]) # Ensure aliases are lowercase for comparison
            else: # Unknown nick key
                return 0

        sql_str = "SELECT COUNT(*) FROM logs"
        if conditions:
            sql_str += " WHERE " + " AND ".join(conditions)

        cursor = self.conn.cursor()
        try:
            cursor.execute(sql_str, sql_params)
        except sqlite3.OperationalError as e_sqlite: # Renamed e to e_sqlite for clarity
            if "no such function: regexp" in str(e_sqlite).lower():
                app.logger.error("ERROR: Native regexp function not available in SQLite for count_occurrences. SQLite regex extension not loaded correctly or missing.")
                return 0
            else:
                app.logger.error(f"SQL error in count_occurrences: {e_sqlite} with SQL: {sql_str} and params: {sql_params}")
                return 0

        result = cursor.fetchone()
        return result[0] if result else 0


    def get_valid_days(self):
        """
        Return a list of (year, month, day) tuples where there is at least one
        log entry for that day, using SQL.
        """
        sql = """
            SELECT DISTINCT
                   CAST(strftime('%Y', timestamp, 'unixepoch') AS INTEGER),
                   CAST(strftime('%m', timestamp, 'unixepoch') AS INTEGER),
                   CAST(strftime('%d', timestamp, 'unixepoch') AS INTEGER)
            FROM logs
            ORDER BY 1, 2, 3
        """
        cursor = self.conn.cursor()
        cursor.execute(sql)
        return [tuple(row) for row in cursor.fetchall()]

    def get_all_days(self):
        """
        Return a list of (year, month, day) tuples between the starting and end date
        even if there is no data. Uses get_valid_days to determine range.
        """
        def to_datetime_helper(day_tuple):
            return datetime.datetime(day_tuple[0], day_tuple[1], day_tuple[2])

        valid_days_list = self.get_valid_days()
        if not valid_days_list:
            return []

        current_time = to_datetime_helper(valid_days_list[0])
        end_time = to_datetime_helper(valid_days_list[-1])

        all_days_list = []
        while current_time <= end_time:
            all_days_list.append((current_time.year, current_time.month, current_time.day))
            current_time += datetime.timedelta(days=1)
        return all_days_list

    def get_logs_by_day(self):
        """
        Return a map from (year, month, day) tuples to log lines occurring on that
        day, using SQL.
        """
        sql = """
            SELECT
                   CAST(strftime('%Y', timestamp, 'unixepoch') AS INTEGER) as year,
                   CAST(strftime('%m', timestamp, 'unixepoch') AS INTEGER) as month,
                   CAST(strftime('%d', timestamp, 'unixepoch') AS INTEGER) as day,
                   timestamp,
                   nick,
                   message
            FROM logs
            ORDER BY timestamp
        """
        cursor = self.conn.cursor()
        cursor.execute(sql)

        days = {}
        for row in cursor.fetchall():
            key = (row[0], row[1], row[2])
            if key not in days:
                days[key] = []
            days[key].append({'timestamp': row[3], 'nick': row[4], 'message': row[5]})
        return days

    def search_day_logs(self, s, ignore_case=False):
        """
        Return a list of matching log lines of the form:
                ((year, month, day), index, line)
        This implementation will fetch logs by day and then process them,
        similar to InMemoryLogQueryEngine to keep output structure.
        """
        flags = re.IGNORECASE if ignore_case else 0
        try:
            r = re.compile(s, flags=flags)
        except re.error:
            return []

        results = []
        # get_logs_by_day() for SQLiteLogQueryEngine fetches from DB
        day_logs = self.get_logs_by_day()

        for day in reversed(sorted(day_logs.keys())):
            index = 0
            for line in day_logs[day]: # line is a dict {'timestamp':..., 'nick':..., 'message':...}
                m = r.search(line['message'])
                if m is not None:
                    results.append((day, index, line, m.start(), m.end()))
                index += 1
        return results

    def search_results_to_chart(self, s, ignore_case=False):
        # This method reuses other methods that are already adapted for SQLite.
        results = self.search_day_logs(s, ignore_case) # Uses SQLite version

        def get_key(day_tuple): # day_tuple is (year, month, day)
            # Ensure this is consistent with how keys are generated for 'counts'
            return time.mktime(
                    datetime.datetime(day_tuple[0], day_tuple[1], 1).timetuple())

        counts = {}
        all_days_list = self.get_all_days() # Uses SQLite version
        if not all_days_list:
             return [{'key': '', 'values': []}]

        # Initialize counts for all days in the range (month-level aggregation)
        for day_tuple in all_days_list: # day_tuple is (year, month, day)
            # Key for counts should be the first day of the month of day_tuple
            month_key_ts = get_key(day_tuple)
            if month_key_ts not in counts: # Ensure each month is initialized once
                 counts[month_key_ts] = {'x': month_key_ts, 'y': 0}

        for r_tuple in results: # r_tuple is ((year, month, day), index, line, m.start(), m.end())
            day_of_result = r_tuple[0] # This is (year, month, day)
            month_key_of_result_ts = get_key(day_of_result)
            if month_key_of_result_ts in counts: # Should always be true if all_days_list is comprehensive
                counts[month_key_of_result_ts]['y'] += 1
            # else: # This case should ideally not happen if all_days_list covers the range of results
            #    app.logger.warn(f"Day {day_of_result} from search results not found in all_days_list derived keys.")


        return [{
                'key': '', # Label for the chart series
                'values': sorted(counts.values(), key=lambda val: val['x'])
            }]

    def query_logs(self, s,
            cumulative=False, coarse=False, nick=None, ignore_case=False,
            normalize=False, normalize_type=None):
        """
        Query logs for a given regular expression and return a time series of the
        number of occurrences of lines matching the regular expression per day, using SQL.
        The core filtering is done via SQL, and then processing (aggregation, normalization)
        is done in Python to match the InMemoryLogQueryEngine's complex logic.
        """

        # Helper functions (can be static or defined outside if they don't need self)
        def to_datetime_from_ts(timestamp):
            return datetime.datetime.fromtimestamp(float(timestamp))

        def to_timestamp_from_dt(d):
            return time.mktime(
                    datetime.datetime(d.year, d.month, d.day).timetuple())

        def get_key_from_ts(timestamp, coarse_flag):
            d = to_datetime_from_ts(timestamp)
            day = 1 if coarse_flag else d.day # Coarse means aggregate by month (day=1)
            return time.mktime(
                    datetime.datetime(d.year, d.month, day).timetuple())

        def get_value_from_dict(key, m):
            if key not in m:
                m[key] = {'x': key, 'y': 0}
            return m[key]

        # --- SQL Querying Part ---
        sql_params = []
        base_sql = "SELECT timestamp, nick, message FROM logs WHERE 1=1"

        if nick and nick in VALID_NICKS:
            nick_placeholders = ', '.join(['?'] * len(VALID_NICKS[nick]))
            base_sql += f" AND nick IN ({nick_placeholders})"
            sql_params.extend(VALID_NICKS[nick])

        # For regex, we always use REGEXP_FLAGS for consistency
        # The UDF handles re.IGNORECASE if ignore_case is True
        # base_sql += " AND REGEXP_FLAGS(?, message, ?)" # This was the plan for count_occurrences
        # sql_params.extend([s, re.IGNORECASE if ignore_case else 0])
        # However, for query_logs, the original code iterates all logs and then filters.
        # Let's try to fetch all logs (optionally filtered by nick) and then process.
        # This simplifies the SQL part and keeps the complex logic in Python.
        # OR, filter by regex in SQL too. This is more efficient.

        # Let's stick to filtering by regex in SQL
        # The REGEXP_FLAGS UDF is already defined in __init__ to take (pattern, text, flags)
        # However, it was overwritten in count_occurrences. Let's redefine it properly in __init__
        # For now, assume REGEXP_FLAGS is pattern, text, flags(int)
        # We need to ensure the UDF is robust.
        # Let's re-register the UDF here for clarity or ensure it's correctly done in __init__

        # Re-defining the UDF for safety in this method context, though ideally it's instance-wide
        # def regexp_with_flags(pattern, text, flags_int=0):
        #     try:
        #         return 1 if re.search(pattern, text, flags_int) else 0
        #     except re.error: # Catch invalid regex patterns
        #         return 0
        # self.conn.create_function("REGEXP_FLAGS", 3, regexp_with_flags) # REMOVED - UDFs are in __init__


        # Store data for lines that match the regex
        matched_lines = []
        # Store data for all lines (for normalization)
        all_lines_for_period = []

        # Fetch all logs and then filter in Python (Simpler to port, less efficient)
        # vs Fetch filtered logs from SQL (More efficient, harder to port `r.search` for totals)

        # The original code calculates totals based on ALL lines, not just those matching `s` for `r.search`.
        # So, we need two types of aggregations:
        # 1. Totals per day/month (all messages)
        # 2. Matched per day/month (messages matching `s`)

        cursor = self.conn.cursor()

        # Get all logs within the relevant nick filter (if any)
        # This data will be used for calculating totals and for filtering by regex `s`
        query_all_for_nick = "SELECT timestamp, message, nick FROM logs"
        params_all_for_nick = []
        if nick:
            if nick in VALID_NICKS:
                nick_aliases = VALID_NICKS[nick]
                if nick_aliases:
                    nick_placeholders = ', '.join(['?'] * len(nick_aliases))
                    query_all_for_nick += f" WHERE lower(nick) IN ({nick_placeholders})"
                    params_all_for_nick.extend([alias.lower() for alias in nick_aliases])
                else:
                    # If nick is in VALID_NICKS but has no aliases, no logs will match
                    db_logs = [] # Set db_logs to empty to avoid further processing
                    return []
            else: # Unknown nick key
                db_logs = [] # Set db_logs to empty to avoid further processing
                return []

        cursor.execute(query_all_for_nick, params_all_for_nick)
        db_logs = [{'timestamp': r[0], 'message': r[1], 'nick': r[2]} for r in cursor.fetchall()]

        if not db_logs:
            return []

        # Compile regex s
        try:
            r = re.compile(s, re.IGNORECASE if ignore_case else 0)
        except re.error:
            return [] # Invalid regex pattern

        # --- Python Processing Part (similar to InMemoryLogQueryEngine) ---
        results = {}  # For messages matching s
        totals = {}   # For all messages (within nick filter)

        for line in db_logs:
            key = get_key_from_ts(line['timestamp'], coarse)

            total_value = get_value_from_dict(key, totals)
            total_value['y'] += 1

            if r.search(line['message']) is None:
                continue

            value = get_value_from_dict(key, results)
            value['y'] += 1

        smoothed = {}
        total_matched_overall = 0
        total_possible_overall = 0

        # Determine the full date range from the data
        # min_ts = min(l['timestamp'] for l in db_logs)
        # max_ts = max(l['timestamp'] for l in db_logs)
        # current_dt = to_datetime_from_ts(min_ts)
        # end_dt = to_datetime_from_ts(max_ts)

        # Or, use get_all_days which uses the get_valid_days (from DB)
        # This ensures we cover all days even if some have no logs.
        all_days_tuples = self.get_all_days() # This returns (y,m,d)
        if not all_days_tuples:
            return []

        # Generate all relevant keys based on all_days_tuples and coarse flag
        # This ensures that days/months with zero matches are also included.
        all_relevant_keys = sorted(list(set(
            get_key_from_ts(to_timestamp_from_dt(datetime.datetime(d[0],d[1],d[2])), coarse)
            for d in all_days_tuples
        )))

        last_key_processed = None # To handle coarse grouping correctly over iterations

        for key_ts in all_relevant_keys:
            # key_ts is already the correct key for the period (day or month start)

            # This loop is different from InMemory. We iterate over pre-calculated keys.
            # current_dt = to_datetime_from_ts(key_ts) # Not needed if key_ts is already the key

            value = results.get(key_ts, {'y': 0})['y']
            total_for_period = totals.get(key_ts, {'y': 0})['y']

            total_possible_overall += total_for_period # This is sum of all messages in period
            total_matched_overall += value          # This is sum of matched messages in period

            if cumulative:
                smoothed[key_ts] = {'x': key_ts, 'y': total_matched_overall}
                # For cumulative normalization, totals also need to be cumulative
                totals[key_ts] = {'x': key_ts, 'y': total_possible_overall}
            else:
                smoothed[key_ts] = {'x': key_ts, 'y': value}
                # totals[key_ts] already stores total_for_period from the loop over db_logs
                # but ensure it's there if a key_ts from all_relevant_keys had no logs
                if key_ts not in totals: totals[key_ts] = {'x': key_ts, 'y': 0}


        if normalize and total_matched_overall > 0:
            # Normalization logic - this part is complex and needs careful porting
            # It uses a window which can be 'trailing_avg_X' or just the current period's total.
            total_window_values = []
            matched_window_values = []

            for key_ts in sorted(smoothed.keys()): # Iterate in chronological order
                current_total_for_period = totals.get(key_ts, {'y': 0})['y']
                current_matched_for_period = smoothed[key_ts]['y'] # This is already cumulative if cumulative=True

                if cumulative: # For cumulative, normalization is against cumulative totals
                    if totals[key_ts]['y'] == 0:
                         smoothed[key_ts]['y'] = 0
                    else:
                         smoothed[key_ts]['y'] = total_matched_overall / total_possible_overall # This seems wrong for cumulative per point
                         # The original logic for cumulative normalization:
                         # smoothed[key]['y'] /= totals[key]['y']
                         # where totals[key]['y'] was also cumulative.
                         # So, it should be:
                         smoothed[key_ts]['y'] = smoothed[key_ts]['y'] / totals[key_ts]['y'] if totals[key_ts]['y'] > 0 else 0

                else: # Non-cumulative normalization
                    if str(normalize_type or '').startswith('trailing_avg_'):
                        window_size = int(normalize_type[13:])
                        total_window_values.append(current_total_for_period)
                        matched_window_values.append(current_matched_for_period) # current_matched_for_period is non-cumulative here

                        total_window_values = total_window_values[-window_size:]
                        matched_window_values = matched_window_values[-window_size:]
                    else: # Default window is just the current period
                        total_window_values = [current_total_for_period]
                        matched_window_values = [current_matched_for_period]

                    if sum(total_window_values) == 0:
                        smoothed[key_ts]['y'] = 0
                    else:
                        smoothed[key_ts]['y'] = sum(matched_window_values) / sum(total_window_values)

        return sorted(smoothed.values(), key=lambda x_val: x_val['x'])


class InMemoryLogQueryEngine(AbstractLogQueryEngine):
    def __init__(self, log_file_path=None, log_data=None):
        super().__init__(log_file_path, log_data)
        self.logs = [] # Default to empty list

        if log_data is not None:
            self.logs = log_data
        else:
            # Existing file loading logic
            chosen_log_path = None
            # Assuming 'app' from 'from web import app' is the Flask instance.
            if app.testing:
                chosen_log_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'tests', 'test_log_sample.json'))
                if self.log_file_path: # If a path is explicitly passed during testing, use it
                    chosen_log_path = self.log_file_path
            elif self.log_file_path:
                chosen_log_path = self.log_file_path
            else:
                chosen_log_path = os.path.join(APP_STATIC, 'log.json')

            try:
                with open(chosen_log_path, 'r') as f:
                    self.logs = json.load(f)
            except FileNotFoundError:
                if app.testing and chosen_log_path == os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'tests', 'test_log_sample.json')):
                     raise FileNotFoundError(f"Test log file not found during testing: {chosen_log_path}")
                elif not (app.testing or self.log_file_path):
                    pass # self.logs remains [], default production path missing
                else:
                    if not app.testing:
                        raise # Specific path provided (not default) and not found, and not testing

    def clear_all_caches(self):
        self.query_logs.cache_clear()
        self.count_occurrences.cache_clear()
        self.get_valid_days.cache_clear()
        self.get_all_days.cache_clear()
        self.get_logs_by_day.cache_clear()
        self.search_day_logs.cache_clear()
        self.search_results_to_chart.cache_clear()

    @functools.lru_cache(maxsize=1000)
    def query_logs(self, s,
            cumulative=False, coarse=False, nick=None, ignore_case=False,
            normalize=False, normalize_type=None):
        """
        Query logs for a given regular expression and return a time series of the
        number of occurrences of lines matching the regular expression per day.
        """
        flags = ignore_case and re.IGNORECASE or 0
        try: r = re.compile(s, flags=flags)
        except: return []

        results = {}
        totals = {}

        def to_datetime(timestamp):
            return datetime.datetime.fromtimestamp(float(timestamp))

        def to_timestamp(d):
            return time.mktime(
                    datetime.datetime(d.year, d.month, d.day).timetuple())

        def get_key(timestamp):
            d = to_datetime(timestamp)
            day = coarse and 1 or d.day
            return time.mktime(
                    datetime.datetime(d.year, d.month, day).timetuple())

        def get_value(key, m):
            if key not in m:
                m[key] = {'x': key, 'y': 0}
            return m[key]

        for line in self.logs:
            if nick:
                if nick in VALID_NICKS:
                    if line['nick'].lower() not in VALID_NICKS[nick]:
                        continue
                else: # Unknown nick key
                    continue # Skip if nick is specified but not in VALID_NICKS

            key = get_key(line['timestamp'])
            total_value = get_value(key, totals)
            total_value['y'] += 1

            if r.search(line['message']) == None:
                continue

            value = get_value(key, results)
            value['y'] += 1

        smoothed = {}
        total_matched = 0
        total_possible = 0

        if not self.logs: # Handle empty logs case early
            return []

        current_time = to_datetime(self.logs[0]['timestamp'])
        end_time = to_datetime(self.logs[-1]['timestamp'])
        last_key = None
        while current_time <= end_time:
            key = get_key(to_timestamp(current_time))
            current_time += datetime.timedelta(days=1)
            if key == last_key:
                continue
            last_key = key

            value = key in results and results[key]['y'] or 0
            total_possible += key in totals and totals[key]['y'] or 0
            total_matched += value

            if cumulative:
                smoothed[key] = {'x': key, 'y': total_matched}
                totals[key] = {'x': key, 'y': total_possible}
            else:
                smoothed[key] = {'x': key, 'y': value}

        if normalize and total_matched > 0:
            total_window = []
            matched_window = []
            for key in smoothed:
                total_window.append(key in totals and totals[key]['y'] or 0)
                matched_window.append(smoothed[key]['y'])
                if str(normalize_type or '').startswith('trailing_avg_'):
                    window_size = int(normalize_type[13:])
                    total_window = total_window[0 - window_size:]
                    matched_window = matched_window[0 - window_size:]
                else:
                    total_window = total_window[-1:]
                    matched_window = matched_window[-1:]

                if cumulative:
                    if totals[key]['y'] == 0: # Check specific key in totals
                        smoothed[key]['y'] = 0
                    else:
                        smoothed[key]['y'] /= totals[key]['y']
                else:
                    if sum(total_window) == 0:
                        smoothed[key]['y'] = 0
                    else:
                        smoothed[key]['y'] = (
                            sum(matched_window) / sum(total_window))

        return sorted(smoothed.values(), key=lambda x: x['x'])

    @functools.lru_cache(maxsize=1000)
    def count_occurrences(self, s, ignore_case=False, nick=None):
        flags = ignore_case and re.IGNORECASE or 0
        try: r = re.compile(s, flags=flags)
        except: return 0

        total = 0
        for line in self.logs:
            if nick:
                if nick in VALID_NICKS:
                    if line['nick'].lower() not in VALID_NICKS[nick]:
                        continue
                else: # Unknown nick key
                    continue # Skip if nick is specified but not in VALID_NICKS
            if r.search(line['message']) != None:
                total += 1
        return total

    @functools.lru_cache(maxsize=1)
    def get_valid_days(self):
        """
        Return a list of (year, month, day) tuples where there is at least one
        log entry for that day
        """
        return sorted(self.get_logs_by_day().keys())

    @functools.lru_cache(maxsize=1)
    def get_all_days(self):
        """
        Return a list of (year, month, day) tuples between the starting and end date
        even if there is no data.
        """
        def to_datetime_helper(day_tuple):
            return datetime.datetime.fromtimestamp(float(time.mktime(
                    datetime.datetime(day_tuple[0], day_tuple[1], day_tuple[2]).timetuple())))

        valid_days_list = self.get_valid_days()
        if not valid_days_list:
            return []

        current_time = to_datetime_helper(valid_days_list[0])
        end_time = to_datetime_helper(valid_days_list[-1])

        all_days_list = []
        while current_time <= end_time:
            all_days_list.append((current_time.year, current_time.month, current_time.day))
            current_time += datetime.timedelta(days=1)
        return all_days_list

    @functools.lru_cache(maxsize=1)
    def get_logs_by_day(self):
        """
        Return a map from (year, month, day) tuples to log lines occurring on that
        day.
        """
        days = {}
        for line in self.logs:
            dt = datetime.datetime.fromtimestamp(float(line['timestamp']))
            key = (dt.year, dt.month, dt.day)
            if key not in days:
                days[key] = []
            days[key].append(line)
        return days

    @functools.lru_cache(maxsize=1000)
    def search_day_logs(self, s, ignore_case=False):
        """
        Return a list of matching log lines of the form:
                ((year, month, day), index, line)
        """
        flags = ignore_case and re.IGNORECASE or 0
        try: r = re.compile(s, flags=flags)
        except: return []

        results = []
        day_logs = self.get_logs_by_day()
        for day in reversed(sorted(day_logs.keys())):
            index = 0
            for line in day_logs[day]:
                m = r.search(line['message'])
                if m != None:
                    results.append((day, index, line, m.start(), m.end()))
                index += 1
        return results

    @functools.lru_cache(maxsize=1000)
    def search_results_to_chart(self, s, ignore_case=False):
        results = self.search_day_logs(s, ignore_case)

        def get_key(day):
            return time.mktime(
                    datetime.datetime(day[0], day[1], 1).timetuple())

        counts = {}
        all_days_list = self.get_all_days()
        if not all_days_list: # Handle case where logs might be empty
             return [{'key': '', 'values': []}]

        for day_tuple in all_days_list:
            key = get_key(day_tuple) # Use the tuple directly from get_all_days
            counts[key] = {'x': key, 'y': 0}
        for r_tuple in results: # r_tuple is ((year, month, day), index, line, m.start(), m.end())
            day_of_result = r_tuple[0] # This is (year, month, day)
            counts[get_key(day_of_result)]['y'] += 1

        return [{
                'key': '',
                'values': sorted(counts.values(), key=lambda x: x['x'])
            }]

# Global instance for the application
# The InMemoryLogQueryEngine constructor will now handle choosing the log file based on app.testing
log_engine = InMemoryLogQueryEngine()

# Functions exposed as template globals, using the log_engine instance
@app.template_global()
def graph_query(queries, nick_split=False, **kwargs):
    data = []
    for (label, s) in queries:
        if not nick_split:
            data.append({
                'key': label,
                'values': log_engine.query_logs(s, **kwargs),
            })
        else:
            for nick in sorted(VALID_NICKS.keys()):
                if label == '':
                    nick_label = nick
                else:
                    nick_label = '%s - %s' % (label, nick)
                data.append({
                    'key': nick_label,
                    'values': log_engine.query_logs(s, nick=nick, **kwargs),
                })
    return data

@app.template_global()
def table_query(queries, nick_split=False, order_by_total=False, **kwargs):
    rows = [['', 'Total']]
    if nick_split:
        for nick in sorted(VALID_NICKS.keys()):
            rows[0].append(nick)

    tmp_rows = []
    for (label, s) in queries:
        row = [label]
        row.append(log_engine.count_occurrences(s, **kwargs)) # Uses global log_engine
        if nick_split:
            for nick in rows[0][2:]: # These are canonical nicks from VALID_NICKS
                row.append(log_engine.count_occurrences(s, nick=nick, **kwargs))
        tmp_rows.append(row)

    if order_by_total:
        tmp_rows = sorted(tmp_rows, key=lambda x: x[1], reverse=True)

    rows += tmp_rows
    return rows
