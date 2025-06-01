#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import datetime
import functools
import os
import re
import sqlite3
import sqlite_regex
import ijson
import time
from collections import defaultdict

from web import app, APP_STATIC # Keep web import for app and APP_STATIC
from .abstract_engine import AbstractLogQueryEngine # Import from local abstract_engine
from .constants import VALID_NICKS # Import VALID_NICKS from .constants

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
                chosen_log_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'tests', 'test_log_sample.json')) # Adjusted path
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
        self._query_logs_single.cache_clear()
        self._count_occurrences.cache_clear()
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
    def _query_logs_single(self, s,
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

    def query_logs(self, queries, nick_split=False, cumulative=False, coarse=False, ignore_case=False, normalize=False, normalize_type=None):
        data = []
        for (label, s_query) in queries:
            if not nick_split:
                series_values = self._query_logs_single(s_query,
                                                        cumulative=cumulative, coarse=coarse, nick=None,
                                                        ignore_case=ignore_case, normalize=normalize,
                                                        normalize_type=normalize_type)
                data.append({
                    'key': label,
                    'values': series_values,
                })
            else:
                for nick_key in sorted(VALID_NICKS.keys()):
                    current_label = f"{label} - {nick_key}" if label else nick_key
                    series_values = self._query_logs_single(s_query,
                                                            cumulative=cumulative, coarse=coarse, nick=nick_key,
                                                            ignore_case=ignore_case, normalize=normalize,
                                                            normalize_type=normalize_type)
                    data.append({
                        'key': current_label,
                        'values': series_values,
                    })
        return data

    def count_occurrences(self, queries, **kwargs):
        return self._count_occurrences(tuple(queries), **kwargs)

    @functools.lru_cache(maxsize=1000)
    def _count_occurrences(self, queries, ignore_case=False, nick_split=False, order_by_total=False):
        sql_params = []
        conditions = []

        def add_query_cond(query):
            pattern = f"(?i){query}" if ignore_case else query
            sql_params.append(pattern)
            return f"regexp(?, IFNULL(message, ''))"

        query_columns = (','.join([
                f'IFNULL(SUM({add_query_cond(query)}), 0) AS q_{i}'
                for i, (label, query) in enumerate(queries)
            ]))

        if not nick_split:
            sql_str = f"SELECT {query_columns} FROM logs"
        else:
            sql_str = f"""
            SELECT valid_nicks.nick, {query_columns}
            FROM logs
            INNER JOIN valid_nicks ON LOWER(logs.nick) = LOWER(valid_nicks.alias)
            GROUP BY 1
            ORDER BY 1 ASC
            """

        cursor = self.conn.cursor()
        try:
            cursor.execute(sql_str, sql_params)
        except sqlite3.OperationalError as e_sqlite:
            app.logger.error(f"SQL error in count_occurrences: {e_sqlite} with SQL: {sql_str} and params: {sql_params}")
            return 0

        results = cursor.fetchall()
        nick_results = defaultdict(lambda: [0] * len(queries))
        for result in results:
            nick_results[result[0]]  = result[1:]

        rows = [['', 'Total']]
        if nick_split:
            for nick in sorted(VALID_NICKS.keys()):
                rows[0].append(nick)

        tmp_rows = []
        for i, (label, s) in enumerate(queries):
            row = [label]
            row.append(sum([r[(1 if nick_split else 0) + i] for r in results])) # Total
            if nick_split:
                for nick in rows[0][2:]:
                    row.append(nick_results[nick][i])
            tmp_rows.append(row)

        if order_by_total:
            tmp_rows = sorted(tmp_rows, key=lambda x: x[1], reverse=True)

        rows += tmp_rows
        return rows

    @functools.lru_cache(maxsize=1)
    def get_valid_days(self):
        sql = """
            SELECT DISTINCT year, month, day
            FROM logs
            ORDER BY 1, 2, 3
        """
        cursor = self.conn.cursor()
        cursor.execute(sql)
        return [tuple(row) for row in cursor.fetchall()]

    def get_logs_by_day(self, year, month, day):
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
        sql_params, conditions, has_error = self._prepare_sql_filters(s, ignore_case, None, log_table='this_log')

        if has_error:
            return []

        flags = ignore_case and re.IGNORECASE or 0
        try: r_compile = re.compile(s, flags=flags) # Renamed to avoid conflict
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
            m = r_compile.search(line['message']) # Use renamed compiled regex
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
        sql_params = []
        def add_cond(cond, param): # This function was defined but not used, keeping for consistency if needed later
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
        cursor.execute(sql_query, sql_params) # sql_params will be empty here as add_cond was not used to populate it
        results = list(cursor.fetchall())
        return results
