#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import datetime
import functools
import json
import os
import re
import time

from web import app, APP_STATIC # Keep web import for app and APP_STATIC
from .abstract_engine import AbstractLogQueryEngine # Import from local abstract_engine
from .constants import VALID_NICKS # Import VALID_NICKS from the logs package __init__

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
                chosen_log_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'tests', 'test_log_sample.json')) # Adjusted path
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
                if app.testing and chosen_log_path == os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'tests', 'test_log_sample.json')): # Adjusted path
                     raise FileNotFoundError(f"Test log file not found during testing: {chosen_log_path}")
                elif not (app.testing or self.log_file_path):
                    pass # self.logs remains [], default production path missing
                else:
                    if not app.testing:
                        raise # Specific path provided (not default) and not found, and not testing

    def clear_all_caches(self):
        self._query_logs_single.cache_clear()
        self._count_occurrences_single.cache_clear()
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
    def _query_logs_single(self, s,
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

        def to_datetime_helper(timestamp): # Renamed to avoid conflict
            return datetime.datetime.fromtimestamp(float(timestamp))

        def to_timestamp(d):
            return time.mktime(
                    datetime.datetime(d.year, d.month, d.day).timetuple())

        def get_key(timestamp):
            d = to_datetime_helper(timestamp)
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

        current_time = to_datetime_helper(self.logs[0]['timestamp'])
        end_time = to_datetime_helper(self.logs[-1]['timestamp'])
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

    def count_occurrences(self, queries, ignore_case=False, nick_split=False, order_by_total=False):
        rows = [['', 'Total']]
        if nick_split:
            for nick in sorted(VALID_NICKS.keys()):
                rows[0].append(nick)

        tmp_rows = []
        for (label, s) in queries:
            row = [label]
            row.append(self._count_occurrences_single(s, ignore_case=ignore_case))
            if nick_split:
                for nick in rows[0][2:]:
                    row.append(self._count_occurrences_single(s, nick=nick, ignore_case=ignore_case))
            tmp_rows.append(row)

        if order_by_total:
            tmp_rows = sorted(tmp_rows, key=lambda x: x[1], reverse=True)

        rows += tmp_rows
        return rows

    @functools.lru_cache(maxsize=1000)
    def _count_occurrences_single(self, s, ignore_case=False, nick=None):
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
        def to_datetime_helper_all_days(day_tuple): # Renamed
            return datetime.datetime.fromtimestamp(float(time.mktime(
                    datetime.datetime(day_tuple[0], day_tuple[1], day_tuple[2]).timetuple())))

        valid_days_list = self.get_valid_days()
        if not valid_days_list:
            return []

        current_time = to_datetime_helper_all_days(valid_days_list[0])
        end_time = to_datetime_helper_all_days(valid_days_list[-1])

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

        for line in self.logs:
            dt = datetime.datetime.fromtimestamp(int(float(line['timestamp'])))
            if (dt.year, dt.month, dt.day) == current_day_tuple:
                current_day_logs.append(line)

        return (current_day_logs, prev_day_tuple, next_day_tuple)

    @functools.lru_cache(maxsize=1000)
    def search_day_logs(self, s, ignore_case=False, limit=None, offset=None):
        """
        Return a tuple: (results, total_count)
        Where `results` is a list of matching log lines of the form:
                ((year, month, day), index, line, match_start, match_end)
        And `total_count` is the total number of matching log lines before pagination.
        """
        flags = ignore_case and re.IGNORECASE or 0
        try: r = re.compile(s, flags=flags)
        except: return ([], 0)

        results = []
        day_logs = self._get_all_logs_by_day()
        for day_tuple_key in reversed(sorted(day_logs.keys())): # Renamed day to day_tuple_key
            index = 0
            for line in day_logs[day_tuple_key]:
                m = r.search(line['message'])
                if m != None:
                    results.append((day_tuple_key, index, line, m.start(), m.end()))
                index += 1

        total_count = len(results)

        if offset is None:
            offset = 0

        if limit is None:
            paginated_results = results[offset:]
        else:
            paginated_results = results[offset:offset + limit]

        return (paginated_results, total_count)

    @functools.lru_cache(maxsize=1000)
    def search_results_to_chart(self, s, ignore_case=False):
        results = self.search_day_logs(s, ignore_case)

        def get_key_chart(day_tuple_chart): # Renamed
            return time.mktime(
                    datetime.datetime(day_tuple_chart[0], day_tuple_chart[1], 1).timetuple())

        counts = {}
        all_days_list = self.get_all_days()
        if not all_days_list:
             return [{'key': '', 'values': []}]

        for day_tuple in all_days_list:
            key = get_key_chart(day_tuple)
            counts[key] = {'x': key, 'y': 0}
        for r_tuple in results:
            day_of_result = r_tuple[0]
            counts[get_key_chart(day_of_result)]['y'] += 1

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
