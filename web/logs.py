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
    'Jesse': ['jesse'],
    'Will': ['will'],
    'Zhenya': ['zhenya', 'zdog', 'swphantom'],
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
            for nick in VALID_NICKS:
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
@functools.lru_cache(maxsize=1000)
def count_occurrences(s, ignore_case=False):
    flags = ignore_case and re.IGNORECASE or 0
    try: r = re.compile(s, flags=flags)
    except: return 0

    total = 0
    for line in logs:
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
            if r.search(line['message']) != None:
                results.append((day, index, line))
            index += 1
    return results
