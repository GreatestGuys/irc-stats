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
    'Graham': ['graham'],
    'Jesse': ['jesse', 'je-c'],
    'Will': ['will', 'wyll'],
    'Zhenya': ['zhenyah', 'zhenya', 'zdog', 'swphantom', 'za'],
}

logs = None

@app.before_request
def init_logs():
    # Cache the entire log file since it takes several seconds to parse.
    global logs
    if not logs:
        with open(os.path.join(APP_STATIC, 'log.json'), 'r') as f:
            logs = json.load(f)

@functools.lru_cache(maxsize=1000)
def query_logs(s,
        cumulative=False, coarse=False, nick=None, ignore_case=False,
        normalize=False):
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

    for line in logs:
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

    # Now hat we have a histogram of occurrences over time it must be smoothed
    # so that there is an entry for each possible key.
    current_time = to_datetime(logs[0]['timestamp'])
    end_time = to_datetime(logs[-1]['timestamp'])
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
        for key in smoothed:
            if cumulative:
                if totals[key]['y'] == 0:
                    smoothed[key]['y'] = 0
                else:
                    smoothed[key]['y'] /= totals[key]['y']
            else:
                smoothed[key]['y'] /= key in totals and totals[key]['y'] or 1

    return sorted(smoothed.values(), key=lambda x: x['x'])

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
            for nick in sorted(VALID_NICKS.keys()):
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
        for nick in sorted(VALID_NICKS.keys()):
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
    flags = ignore_case and re.IGNORECASE or 0
    try: r = re.compile(s, flags=flags)
    except: return 0

    total = 0
    for line in logs:
        if nick != None and line['nick'].lower() not in VALID_NICKS[nick]:
            continue

        if r.search(line['message']) != None:
            total += 1
    return total

@functools.lru_cache(maxsize=1)
def get_valid_days():
    """
    Return a list of (year, month, day) tuples where there is at least one
    log entry for that day
    """
    return sorted(get_logs_by_day().keys())

@functools.lru_cache(maxsize=1)
def get_all_days():
    """
    Return a list of (year, month, day) tuples between the starting and end date
    even if there is no data.
    """
    def to_datetime(day):
        return datetime.datetime.fromtimestamp(float(time.mktime(
                datetime.datetime(day[0], day[1], day[2]).timetuple())))

    days = get_valid_days()
    current_time = to_datetime(days[0])
    end_time = to_datetime(days[-1])
    days = []
    while current_time <= end_time:
        days.append((current_time.year, current_time.month, current_time.day))
        current_time += datetime.timedelta(days=1)
    return days


@functools.lru_cache(maxsize=1)
def get_logs_by_day():
    """
    Return a map from (year, month, day) tuples to log lines occurring on that
    day.
    """
    days = {}
    for line in logs:
        dt = datetime.datetime.fromtimestamp(float(line['timestamp']))
        key = (dt.year, dt.month, dt.day)
        if key not in days:
            days[key] = []
        days[key].append(line)
    return days

@functools.lru_cache(maxsize=1000)
def search_day_logs(s, ignore_case=False):
    """
    Return a list of matching log lines of the form:
            ((year, month, day), index, line)
    """
    flags = ignore_case and re.IGNORECASE or 0
    try: r = re.compile(s, flags=flags)
    except: return []

    results = []
    day_logs = get_logs_by_day()
    for day in reversed(sorted(day_logs.keys())):
        index = 0
        for line in day_logs[day]:
            m = r.search(line['message'])
            if m != None:
                results.append((day, index, line, m.start(), m.end()))
            index += 1
    return results

@functools.lru_cache(maxsize=1000)
def search_results_to_chart(s, ignore_case=False):
    results = search_day_logs(s, ignore_case)

    def get_key(day):
        return time.mktime(
                datetime.datetime(day[0], day[1], 1).timetuple())

    counts = {}
    for day in get_all_days():
        key = get_key(day)
        counts[key] = {'x': key, 'y': 0}
    for r in results:
        day = r[0]
        counts[get_key(day)]['y'] += 1

    return [{
            'key': '',
            'values': sorted(counts.values(), key=lambda x: x['x'])
        }]
