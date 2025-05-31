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
import ijson
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
    def get_logs_by_day(self, year, month, day):
        pass

    @abc.abstractmethod
    def search_day_logs(self, s, ignore_case=False):
        pass

    @abc.abstractmethod
    def search_results_to_chart(self, s, ignore_case=False):
        pass

    @abc.abstractmethod
    def get_trending(self, top=10, min_freq=10, lookback_days=7):
        pass

class SQLiteLogQueryEngine(AbstractLogQueryEngine):
    def __init__(self, log_file_path=None, log_data=None, batch_size=1000, db=':memory:'):
        super().__init__(log_file_path, log_data)
        self.conn = sqlite3.connect(db, check_same_thread=False)
        self.batch_size = batch_size

        self.conn.enable_load_extension(True)
        sqlite_regex.load(self.conn)
        self.conn.enable_load_extension(False)

        self._create_table()
        self._load_data(log_file_path, log_data)
        self._post_insert()

    def _create_table(self):
        queries = [
            'DROP TABLE IF EXISTS logs',
            'DROP TABLE IF EXISTS all_days',
            'DROP TABLE IF EXISTS totals_fine',
            'DROP TABLE IF EXISTS totals_coarse',
            'DROP TABLE IF EXISTS Words',
            'DROP TABLE IF EXISTS valid_nicks',
            'DROP INDEX IF EXISTS logs_idx_date',
            'DROP INDEX IF EXISTS totals_fine_idx_date',
            'DROP INDEX IF EXISTS totals_fine_idx_timestamp',
            'DROP INDEX IF EXISTS totals_coarse_idx_date',
            'DROP INDEX IF EXISTS totals_coarse_idx_timestamp',
            '''
                CREATE TABLE IF NOT EXISTS logs (
                    timestamp INTEGER,
                    year INTEGER,
                    month INTEGER,
                    day INTEGER,
                    timestamp_day INTEGER,
                    nick TEXT,
                    message TEXT
                )
            ''',
            'CREATE INDEX IF NOT EXISTS logs_idx_date ON logs (year, month, day)',
            ]
        with self.conn:
            for query in queries:
                self.conn.execute(query)

    def _post_insert(self):
        queries = [
            f"""
            CREATE TABLE IF NOT EXISTS valid_nicks AS
                {' UNION ALL '.join([f"SELECT '{nick}' AS nick, '{alias}' AS alias" for nick in VALID_NICKS.keys() for alias in VALID_NICKS[nick]])}
            """,
            """
            CREATE TABLE IF NOT EXISTS all_days AS
                WITH RECURSIVE dates(date) AS (
                    SELECT ((SELECT DATE(timestamp_day, 'unixepoch') FROM logs ORDER BY timestamp_day ASC LIMIT 1))
                    UNION ALL
                    SELECT DATE(date, '+1 day')
                    FROM dates
                    WHERE date < ((SELECT DATE(timestamp_day, 'unixepoch') FROM logs ORDER BY timestamp_day DESC LIMIT 1))
                )
                SELECT
                    CAST(strftime('%Y', date) AS INT) as year,
                    CAST(strftime('%m', date) as INT) as month,
                    CAST(strftime('%d', date) as INT) as day,
                    UNIXEPOCH(strftime('%Y-%m-%d', date)) AS timestamp_day
                FROM dates
                GROUP BY 1, 2, 3, 4
            """,
            """
            CREATE TABLE IF NOT EXISTS totals_fine AS
                SELECT
                    nick,
                    year,
                    month,
                    day,
                    date(year ||'-01-01','+'||(month-1)||' month','+'||(day-1)||' day') AS timestamp_day,
                    COUNT(*) AS count
                FROM logs
                GROUP BY 1, 2, 3, 4, 5
            """,
            "CREATE INDEX IF NOT EXISTS totals_fine_idx_date ON totals_fine (year, month, day)",
            "CREATE INDEX IF NOT EXISTS totals_fine_idx_timestamp ON totals_fine (timestamp_day)",
            """
            CREATE TABLE IF NOT EXISTS Words AS
                WITH RECURSIVE _Words (year, month, day, word, remaining) AS (
                    SELECT
                        year, month, day,
                        CASE WHEN logs.message LIKE '% %'
                                THEN SUBSTRING(logs.message, 1, INSTR(logs.message, ' ')-1)
                                ELSE REPLACE(logs.message, ' ', '')
                                END AS word
                        ,
                        CASE WHEN logs.message LIKE '% %'
                                THEN SUBSTRING(logs.message, INSTR(logs.message, ' ')+1)
                                ELSE ''
                                END AS remaining
                    FROM (SELECT * FROM logs ORDER BY timestamp DESC LIMIT 1000) AS logs
                    UNION ALL
                    SELECT
                        year, month, day,
                        CASE WHEN _words.remaining LIKE '% %'
                                THEN SUBSTRING(_words.remaining, 1, INSTR(_words.remaining, ' ')-1)
                                ELSE REPLACE(_words.remaining, ' ', '')
                                END AS word
                        , CASE WHEN _words.remaining LIKE '% %'
                                THEN SUBSTRING(_words.remaining, INSTR(_words.remaining, ' ')+1)
                                ELSE ''
                                END AS remaining
                    FROM _Words AS _words
                    WHERE
                        _words.remaining <> ''
                        AND _words.remaining <> ' '
                        AND _words.remaining IS NOT NULL
                        AND LOWER(_words.remaining) IS NOT NULL
                )
                SELECT
                    year,
                    month,
                    day,
                    LOWER(TRIM(word)) AS word,
                    COUNT(*) AS count
                FROM _Words
                WHERE TRIM(word) <> ''
                GROUP BY 1, 2, 3, 4
            """
        ]
        with self.conn:
            for query in queries:
                self.conn.execute(query)

    def _load_data(self, log_file_path, log_data):
        def load_json_in_batches(file_path):
            with open(file_path, 'rb') as f:
                batch = []
                for record in ijson.items(f, 'item'):
                    batch.append({
                        'timestamp': record['timestamp'],
                        'nick': record['nick'],
                        'message': record['message'],
                    })
                    if len(batch) >= self.batch_size:
                        yield batch
                        batch = []
                if len(batch) > 0:
                    yield batch

        if log_data is not None:
            logs_to_insert = [log_data]
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

            logs_to_insert = load_json_in_batches(chosen_log_path)

        if logs_to_insert:
            with self.conn:
                for batch in logs_to_insert:
                    self.conn.executemany(
                        '''
                        INSERT INTO logs (timestamp, year, month, day, timestamp_day, nick, message)
                        VALUES (
                            ?,
                            CAST(strftime('%Y', CAST(? AS INT), 'unixepoch') AS INT),
                            CAST(strftime('%m', CAST(? AS INT), 'unixepoch') as INT),
                            CAST(strftime('%d', CAST(? AS INT), 'unixepoch') as INT),
                            UNIXEPOCH(strftime('%Y-%m-%d', CAST(? AS INT), 'unixepoch')),
                            ?,
                            ?
                        )
                        ''',
                        [(item['timestamp'], item['timestamp'], item['timestamp'], item['timestamp'], item['timestamp'], item['nick'], item['message']) for item in batch]
                    )

    def clear_all_caches(self):
        self.query_logs.cache_clear()
        self.count_occurrences.cache_clear()
        self.get_valid_days.cache_clear()
        self.get_trending.cache_clear()

    def _prepare_sql_filters(self, s, ignore_case, nick, log_table='logs'):
        sql_params = []
        conditions = []

        try:
            re.compile(s)
            current_pattern = f"(?i){s}" if ignore_case else s
            conditions.append(f"regexp(?, IFNULL({log_table}.message, ''))")
            sql_params.append(current_pattern)
        except re.error as e_re:
            app.logger.warning(f"Invalid Python regex syntax for pattern '{s}': {e_re}")
            return [], [], True

        if nick:
            if nick in VALID_NICKS:
                nick_aliases = VALID_NICKS[nick]
                if nick_aliases:
                    placeholders = ', '.join(['?'] * len(nick_aliases))
                    conditions.append(f"lower(IFNULL({log_table}.nick, '')) IN ({placeholders})")
                    sql_params.extend([alias.lower() for alias in nick_aliases])
                else:
                    return [], [], True
            else:
                return [], [], True

        return sql_params, conditions, False

    @functools.lru_cache(maxsize=1000)
    def query_logs(self, s,
            cumulative=False, coarse=False, nick=None, ignore_case=False,
            normalize=False, normalize_type=None):
        sql_params, conditions, has_error = self._prepare_sql_filters(s, ignore_case, nick)

        def add_cond(cond, param):
            sql_params.append(param)
            return cond

        if has_error:
            return {}

        sql_str = f"""
        WITH
            matches_no_timestamp AS (
                SELECT
                    all_days.year as year,
                    all_days.month as month,
                    {'all_days.day' if not coarse else "1"} as day,
                    SUM(CAST(logs.message IS NOT NULL AND {' AND '.join(conditions)} AS INT)) AS count
                FROM all_days
                LEFT JOIN logs
                ON
                    all_days.year = logs.year
                    AND all_days.month = logs.month
                    AND all_days.day = logs.day
                GROUP BY 1, 2, 3
            ),
            matches AS (
                SELECT
                    year,
                    month,
                    day,
                    date(year ||'-01-01','+'||(month-1)||' month', '+'||(day-1)||' day') AS timestamp_day,
                    IFNULL(count, 0) AS count
                FROM matches_no_timestamp
            )
        """
        if not normalize:
            sql_str += """
            SELECT year, month, day, count
            FROM matches
            ORDER BY 1 ASC, 2 ASC, 3 ASC
            """
        elif str(normalize_type or '').startswith('trailing_avg_'):
            window_size = int(normalize_type[13:])
            lookback_start = f"date(matches.timestamp_day, '-{window_size - 1} day')"
            matches_date = 'matches.timestamp_day'
            totals_date = 'totals.timestamp_day'
            total_window_cond = f'{lookback_start} <= {totals_date} AND {totals_date} <= {matches_date}'
            sql_str += f""",
            totals AS (
                SELECT
                    year,
                    month,
                    {'1' if coarse else 'day'} as day,
                    SUM(count) AS count
                FROM 'totals_fine' AS totals
                {'INNER JOIN valid_nicks ON LOWER(valid_nicks.alias) = LOWER(totals.nick)' if nick else ''}
                WHERE {add_cond('valid_nicks.nick = ?', nick) if nick else 'TRUE'}
                GROUP BY 1, 2, 3
            )
            SELECT
                matches.year,
                matches.month,
                matches.day,
                IFNULL(
                    CAST(SUM(IFNULL(matches.count, 0)) OVER (ROWS {window_size - 1} PRECEDING) AS REAL) /
                    CAST(SUM(IFNULL(totals.count, 0)) OVER (ROWS {window_size - 1} PRECEDING) AS REAL), 0) AS count
            FROM matches
            LEFT OUTER JOIN totals
            ON
                matches.year = totals.year
                AND matches.month = totals.month
                AND matches.day = totals.day
            ORDER BY 1 ASC, 2 ASC, 3 ASC
            """
        else:
            sql_str += """
            SELECT year, month, day, CAST(count AS REAL) / (SELECT COUNT(*) FROM logs) AS count
            FROM matches
            ORDER BY 1 ASC, 2 ASC, 3 ASC
            """

        def get_key(year, month, day):
            return time.mktime(
                    datetime.datetime(year, month, day).timetuple())

        cursor = self.conn.cursor()
        cursor.execute(sql_str, sql_params)
        results = [
            {
                'x': get_key(year, month, day),
                'y': count,
            } for (year, month, day, count) in cursor.fetchall()
            if year is not None and month is not None and day is not None
        ]

        if cumulative:
            total = 0
            for result in results:
                total += result['y']
                result['y'] = total

        return results

    @functools.lru_cache(maxsize=1000)
    def count_occurrences(self, s, ignore_case=False, nick=None):
        sql_params, conditions, has_error = self._prepare_sql_filters(s, ignore_case, nick)

        if has_error:
            return 0

        sql_str = "SELECT COUNT(*) FROM logs WHERE " + " AND ".join(conditions)

        cursor = self.conn.cursor()
        try:
            cursor.execute(sql_str, sql_params)
        except sqlite3.OperationalError as e_sqlite:
            app.logger.error(f"SQL error in count_occurrences: {e_sqlite} with SQL: {sql_str} and params: {sql_params}")
            return 0

        result = cursor.fetchone()
        return result[0] if result else 0

    @functools.lru_cache(maxsize=1)
    def get_valid_days(self):
        """
        Return a list of (year, month, day) tuples where there is at least one
        log entry for that day, using SQL.
        """
        sql = """
            SELECT DISTINCT year, month, day
            FROM logs
            ORDER BY 1, 2, 3
        """
        cursor = self.conn.cursor()
        cursor.execute(sql)
        return [tuple(row) for row in cursor.fetchall()]

    def get_logs_by_day(self, year, month, day):
        """
        Return a tuple: (current_day_logs, prev_day_tuple, next_day_tuple)
        - current_day_logs: A list of log entries for the specified (year, month, day).
        - prev_day_tuple: A (year, month, day) tuple for the previous day with logs, or None.
        - next_day_tuple: A (year, month, day) tuple for the next day with logs, or None.
        """
        # Get all valid days and sort them
        all_valid_days = self.get_valid_days()

        current_day_tuple = (year, month, day)
        current_day_logs = []
        prev_day_tuple = None
        next_day_tuple = None

        try:
            idx = all_valid_days.index(current_day_tuple)
            if idx > 0:
                prev_day_tuple = all_valid_days[idx - 1]
            if idx < len(all_valid_days) - 1:
                next_day_tuple = all_valid_days[idx + 1]
        except ValueError:
            # If the requested day is not in valid_days, it means there are no logs for that day.
            # In this case, current_day_logs will be empty, and prev/next will remain None.
            pass

        sql = """
            SELECT timestamp, nick, message
            FROM logs
            WHERE year = ? AND month = ? AND day = ?
            ORDER BY timestamp
        """
        cursor = self.conn.cursor()
        cursor.execute(sql, (year, month, day))

        for row in cursor.fetchall():
            current_day_logs.append({'timestamp': float(row[0]), 'nick': row[1], 'message': row[2]})

        return (current_day_logs, prev_day_tuple, next_day_tuple)

    def search_day_logs(self, s, ignore_case=False):
        """
        Return a list of matching log lines of the form:
                ((year, month, day), index, {timestamp, nick, message}, match_start, match_end)
        This implementation will fetch logs by day and then process them,
        similar to InMemoryLogQueryEngine to keep output structure.
        """
        sql_params, conditions, has_error = self._prepare_sql_filters(s, ignore_case, None, log_table='this_log')

        if has_error:
            return []

        flags = ignore_case and re.IGNORECASE or 0
        try: r = re.compile(s, flags=flags)
        except: return []

        sql = f"""
            SELECT
                this_log.year,
                this_log.month,
                this_log.day,
                ROW_NUMBER() OVER (PARTITION BY this_log.year, this_log.month, this_log.day ORDER BY this_log.timestamp ASC) - 1 AS day_index,
                this_log.timestamp,
                this_log.nick,
                this_log.message
            FROM logs AS this_log
            WHERE {' AND '.join(conditions)}
            GROUP BY 1, 2, 3, 5, 6, 7
            ORDER BY this_log.timestamp DESC
        """
        cursor = self.conn.cursor()
        cursor.execute(sql, sql_params)

        results = []
        for row in cursor.fetchall():
            date = (row[0], row[1], row[2])
            index = row[3]
            line = {'timestamp': row[4], 'nick': row[5], 'message': row[6]}
            m = r.search(line['message'])
            if m != None:
                results.append((date, index, line, m.start(), m.end()))
        return results

    def search_results_to_chart(self, s, ignore_case=False):
        sql_params, conditions, has_error = self._prepare_sql_filters(s, ignore_case, None)

        if has_error:
            return [{'key': '', 'values': []}]

        sql_str = f"""
            SELECT
                year,
                month,
                COUNT(*) AS count
            FROM logs
            WHERE {' AND '.join(conditions)}
            GROUP BY 1, 2
            ORDER BY 1 ASC, 2 ASC
        """

        cursor = self.conn.cursor()
        try:
            cursor.execute(sql_str, sql_params)
        except sqlite3.OperationalError as e_sqlite:
            app.logger.error(f"SQL error in search_results_to_chart: {e_sqlite} with SQL: {sql_str} and params: {sql_params}")
            return [{'key': '', 'values': []}]

        results = [
            {
                'x': time.mktime(datetime.datetime(year, month, 1).timetuple()),
                'y': count,
            } for (year, month, count) in cursor.fetchall()
        ]

        return [{
            'key': '',
            'values': results
        }]

    @functools.lru_cache(maxsize=1)
    def get_trending(self, top=10, min_freq=10, lookback_days=7):
        """
        Return a list of the top trending terms. The values of the list will be
        tuples of the word along with the relative fractional increase in usage.
        """
        sql_params = []
        def add_cond(cond, param):
            sql_params.append(param)
            return cond

        most_recent_day_sql = "(SELECT printf('%04d-%02d-%02d', year, month, day) FROM logs AS x ORDER BY timestamp DESC LIMIT 1)"
        lookback_cond = f'date(printf("%04d-%02d-%02d", year, month, day)) >= date({most_recent_day_sql}, "-{int(lookback_days)} day")'

        sql_query = f"""
        WITH
            AllWords AS (
                SELECT
                    word,
                    SUM(count) AS freq,
                    CAST(SUM(count) AS REAL) / (SELECT SUM(count) FROM Words) AS rate
                FROM Words
                GROUP BY 1
            ),
            RecentWords AS (
                SELECT
                    word,
                    SUM(count) AS freq,
                    CAST(SUM(count) AS REAL) / (SELECT SUM(count) FROM Words WHERE {lookback_cond}) AS rate
                FROM Words
                WHERE {lookback_cond}
                GROUP BY 1
            )
        SELECT
            RecentWords.word,
            (RecentWords.rate - AllWords.rate) / AllWords.rate AS rate_diff
        FROM RecentWords
        INNER JOIN AllWords ON AllWords.word = RecentWords.word
        WHERE RecentWords.freq >= {int(min_freq)}
        ORDER BY 2 DESC
        LIMIT {int(top)}
        """

        cursor = self.conn.cursor()
        cursor.execute(sql_query, sql_params)
        results = list(cursor.fetchall())
        return results

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
        self.get_trending.cache_clear()

    @functools.lru_cache(maxsize=1)
    def _get_all_logs_by_day(self):
        """
        Return a dictionary mapping (year, month, day) tuples to lists of log entries for that day.
        """
        logs_by_day = {}
        for line in self.logs:
            dt = datetime.datetime.fromtimestamp(float(line['timestamp']))
            day_tuple = (dt.year, dt.month, dt.day)
            if day_tuple not in logs_by_day:
                logs_by_day[day_tuple] = []
            logs_by_day[day_tuple].append(line)
        return logs_by_day

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
                else:
                    continue

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
        return sorted(self._get_all_logs_by_day().keys())

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

    @functools.lru_cache(maxsize=1000)
    def get_logs_by_day(self, year, month, day):
        """
        Return a tuple: (current_day_logs, prev_day_tuple, next_day_tuple)
        - current_day_logs: A list of log entries for the specified (year, month, day).
        - prev_day_tuple: A (year, month, day) tuple for the previous day with logs, or None.
        - next_day_tuple: A (year, month, day) tuple for the next day with logs, or None.
        """
        # Get all valid days and sort them
        all_valid_days = self.get_valid_days()

        current_day_tuple = (year, month, day)
        current_day_logs = []
        prev_day_tuple = None
        next_day_tuple = None

        try:
            idx = all_valid_days.index(current_day_tuple)
            if idx > 0:
                prev_day_tuple = all_valid_days[idx - 1]
            if idx < len(all_valid_days) - 1:
                next_day_tuple = all_valid_days[idx + 1]
        except ValueError:
            # If the requested day is not in valid_days, it means there are no logs for that day.
            # In this case, current_day_logs will be empty, and prev/next will remain None.
            pass

        # Filter logs for the current day
        for line in self.logs:
            dt = datetime.datetime.fromtimestamp(float(line['timestamp']))
            if (dt.year, dt.month, dt.day) == current_day_tuple:
                current_day_logs.append(line)

        return (current_day_logs, prev_day_tuple, next_day_tuple)

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
        day_logs = self._get_all_logs_by_day()
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

    def word_freqs(self, logs, min_freq=0):
        freqs = {}
        for line in logs:
            for word in line['message'].split(' '):
                clean_word = re.sub(r'^[.?!,"\']+|[.?!,"\']+$', '', word).lower()
                if clean_word in freqs:
                    freqs[clean_word] += 1
                else:
                    freqs[clean_word] = 1
        if min_freq > 0:
            new_freqs = {}
            for word in freqs.keys():
                if min_freq <= freqs[word]:
                    new_freqs[word] = freqs[word]
            freqs = new_freqs
        return freqs

    def slice_logs(self, logs, lookback_seconds=7*24*60*60):
        now = time.time()
        sliced_logs = []
        for line in logs:
            if now <= int(float(line['timestamp'])) + lookback_seconds:
                sliced_logs.append(line)
        return sliced_logs

    def to_vector(self, freqs):
        total = sum(freqs.values()) + 1.0
        vector = {}
        for word in freqs:
            vector[word] = freqs[word] / total
        return (vector, total)

    def vector_lookup(self, vector, word):
        (values, total) = vector
        if word in values:
            return values[word]
        else:
            return 1.0 / total

    @functools.lru_cache(maxsize=1)
    def get_trending(self, top=10, min_freq=10, lookback_days=7, **kwargs):
        """
        Return a list of the top trending terms. The values of the list will be
        tuples of the word along with the relative fractional increase in usage.
        """
        logs = self.logs
        recent_logs = self.slice_logs(logs, lookback_seconds=lookback_days * 24 * 60 * 60)

        all_freqs = self.word_freqs(logs)
        all_vector = self.to_vector(all_freqs)
        recent_freqs = self.word_freqs(recent_logs)
        recent_vector = self.to_vector(recent_freqs)

        differences = []
        for word in all_vector[0].keys():
            all_value = self.vector_lookup(all_vector, word)
            recent_value = self.vector_lookup(recent_vector, word)

            if recent_value < min_freq / recent_vector[1]:
                continue

            diff = (recent_value - all_value) / all_value
            differences.append((word, diff))

        return list(reversed(sorted(differences, key=lambda x: x[1])))[0:top]

# Global instance for the application
# The InMemoryLogQueryEngine constructor will now handle choosing the log file based on app.testing
_log_engine = None

def log_engine():
    global _log_engine
    if _log_engine is None:
        _log_engine = SQLiteLogQueryEngine(db=':memory:', batch_size=10)
    return _log_engine

# Functions exposed as template globals, using the log_engine instance
@app.template_global()
def graph_query(queries, nick_split=False, **kwargs):
    data = []
    for (label, s) in queries:
        if not nick_split:
            data.append({
                'key': label,
                'values': log_engine().query_logs(s, **kwargs),
            })
        else:
            for nick in sorted(VALID_NICKS.keys()):
                if label == '':
                    nick_label = nick
                else:
                    nick_label = '%s - %s' % (label, nick)
                data.append({
                    'key': nick_label,
                    'values': log_engine().query_logs(s, nick=nick, **kwargs),
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
        row.append(log_engine().count_occurrences(s, **kwargs)) # Uses global log_engine
        if nick_split:
            for nick in rows[0][2:]: # These are canonical nicks from VALID_NICKS
                row.append(log_engine().count_occurrences(s, nick=nick, **kwargs))
        tmp_rows.append(row)

    if order_by_total:
        tmp_rows = sorted(tmp_rows, key=lambda x: x[1], reverse=True)

    rows += tmp_rows
    return rows
