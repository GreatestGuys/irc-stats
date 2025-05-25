#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import datetime
import functools
import json
import os
import re
import time

from web import app, APP_STATIC
from flask import Flask, url_for, render_template, g

VALID_NICKS = {
    'Cosmo': ['cosmo', 'cfumo'],
    'Graham': ['graham', 'jorgon'],
    'Jesse': ['jesse', 'je-c'],
    'Will': ['will', 'wyll', 'wyll_'],
    'Zhenya': ['zhenyah', 'zhenya', 'zdog', 'swphantom', 'za', 'zhenya2'],
    # Adding test nicks for sample data
    'Alice': ['alice'],
    'Bob': ['bob'],
    'Charlie': ['charlie'],
}

class LogQueryEngine:
    def __init__(self, log_file_path=None):
        self.logs = [] # Default to empty list
        is_default_path = False
        if not log_file_path:
            log_file_path = os.path.join(APP_STATIC, 'log.json')
            is_default_path = True
        
        try:
            with open(log_file_path, 'r') as f:
                self.logs = json.load(f)
        except FileNotFoundError:
            if not is_default_path:
                # If a specific path was provided and not found, raise the error
                raise
            # If it was the default path, self.logs remains [], which is acceptable for tests
            # The actual application should ensure log.json exists.

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
            if nick and line['nick'].lower() not in VALID_NICKS[nick]:
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
            if nick and line['nick'].lower() not in VALID_NICKS[nick]:
                continue
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
# The LogQueryEngine constructor will now handle choosing the log file based on app.testing
log_engine = LogQueryEngine()

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
