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


class LogQueryEngine:
    VALID_NICKS = {
        'Cosmo': ['cosmo', 'cfumo'],
        'Graham': ['graham', 'jorgon'],
        'Jesse': ['jesse', 'je-c'],
        'Will': ['will', 'wyll', 'wyll_'],
        'Zhenya': ['zhenyah', 'zhenya', 'zdog', 'swphantom', 'za', 'zhenya2'],
    }

    def __init__(self, log_file_path=None):
        if log_file_path is None:
            log_file_path = os.path.join(APP_STATIC, 'log.json')
        try:
            with open(log_file_path, 'r') as f:
                self.logs = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            self.logs = []

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
            if nick and line['nick'].lower() not in self.VALID_NICKS[nick]:
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

        # Now hat we have a histogram of occurrences over time it must be smoothed
        # so that there is an entry for each possible key.
        if not self.logs: # handle empty logs
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
                    if totals[key]['y'] == 0:
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

    def count_occurrences(self, s, ignore_case=False, nick=None):
        flags = ignore_case and re.IGNORECASE or 0
        try: r = re.compile(s, flags=flags)
        except: return 0

        total = 0
        for line in self.logs:
            if nick != None and line['nick'].lower() not in self.VALID_NICKS[nick]:
                continue

            if r.search(line['message']) != None:
                total += 1
        return total

    def get_valid_days(self):
        """
        Return a list of (year, month, day) tuples where there is at least one
        log entry for that day
        """
        return sorted(self.get_logs_by_day().keys())

    def get_all_days(self):
        """
        Return a list of (year, month, day) tuples between the starting and end date
        even if there is no data.
        """
        def to_datetime(day):
            return datetime.datetime.fromtimestamp(float(time.mktime(
                    datetime.datetime(day[0], day[1], day[2]).timetuple())))

        days = self.get_valid_days()
        if not days: # handle empty logs
            return []
        current_time = to_datetime(days[0])
        end_time = to_datetime(days[-1])
        days = []
        while current_time <= end_time:
            days.append((current_time.year, current_time.month, current_time.day))
            current_time += datetime.timedelta(days=1)
        return days

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

    def search_results_to_chart(self, s, ignore_case=False):
        results = self.search_day_logs(s, ignore_case)

        def get_key(day):
            return time.mktime(
                    datetime.datetime(day[0], day[1], 1).timetuple())

        counts = {}
        for day in self.get_all_days():
            key = get_key(day)
            counts[key] = {'x': key, 'y': 0}
        for r in results:
            day = r[0]
            counts[get_key(day)]['y'] += 1

        return [{
                'key': '',
                'values': sorted(counts.values(), key=lambda x: x['x'])
            }]

    def get_all_log_entries(self):
        return self.logs

log_query_engine = LogQueryEngine()

@functools.lru_cache(maxsize=1000)
def query_logs(s,
        cumulative=False, coarse=False, nick=None, ignore_case=False,
        normalize=False, normalize_type=None):
    return log_query_engine.query_logs(s, cumulative, coarse, nick,
            ignore_case, normalize, normalize_type)

@app.template_global()
def graph_query(queries, nick_split=False, **kwargs):
    data = []
    for (label, s) in queries:
        if not nick_split:
            data.append({
                'key': label,
                'values': query_logs(s, **kwargs),
            })
        else:
            for nick in sorted(LogQueryEngine.VALID_NICKS.keys()):
                if label == '':
                    nick_label = nick
                else:
                    nick_label = '%s - %s' % (label, nick)
                data.append({
                    'key': nick_label,
                    'values': query_logs(s, nick=nick, **kwargs),
                })
    return data

@app.template_global()
def table_query(queries, nick_split=False, order_by_total=False, **kwargs):
    rows = [['', 'Total']]
    if nick_split:
        for nick in sorted(LogQueryEngine.VALID_NICKS.keys()):
            rows[0].append(nick)

    tmp_rows = []
    for (label, s) in queries:
        row = [label]
        row.append(count_occurrences(s, **kwargs))
        if nick_split:
            for nick in rows[0][2:]:
                row.append(count_occurrences(s, nick=nick, **kwargs))
        tmp_rows.append(row)

    if order_by_total:
        tmp_rows = sorted(tmp_rows, key=lambda x: x[1], reverse=True)

    rows += tmp_rows
    return rows

@app.template_global()
@functools.lru_cache(maxsize=1000)
def count_occurrences(s, ignore_case=False, nick=None):
    return log_query_engine.count_occurrences(s, ignore_case, nick)

@functools.lru_cache(maxsize=1)
def get_valid_days():
    return log_query_engine.get_valid_days()

@functools.lru_cache(maxsize=1)
def get_all_days():
    return log_query_engine.get_all_days()

@functools.lru_cache(maxsize=1)
def get_logs_by_day():
    return log_query_engine.get_logs_by_day()

@functools.lru_cache(maxsize=1000)
def search_day_logs(s, ignore_case=False):
    return log_query_engine.search_day_logs(s, ignore_case)

@functools.lru_cache(maxsize=1000)
def search_results_to_chart(s, ignore_case=False):
    return log_query_engine.search_results_to_chart(s, ignore_case)
