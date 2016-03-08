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
def query_logs(s, cumulative=False, coarse=False, nick=None, ignore_case=False):
    """
    Query logs for a given regular expression and return a time series of the
    number of occurrences of lines matching the regular expression per day.
    """

    flags = ignore_case and re.IGNORECASE or 0
    r = re.compile(s, flags=flags)
    results = {}

    total = 0

    def get_key(timestamp):
        d = datetime.datetime.fromtimestamp(float(timestamp))
        day = coarse and 1 or d.day
        return time.mktime(
                datetime.datetime(d.year, d.month, day).timetuple())

    def get_value(key):
        if key not in results:
            results[key] = {'x': key, 'y': 0}
        return results[key]

    for line in logs:
        if nick and line['nick'].lower() not in VALID_NICKS[nick]:
            continue

        if r.search(line['message']) == None:
            continue
        key = get_key(line['timestamp'])
        value = get_value(key)
        total += 1

        if cumulative:
            value['y'] = total
        else:
            value['y'] += 1

    return sorted(results.values(), key=lambda x: x['x'])

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
app.jinja_env.globals['graph_query'] = graph_query

@functools.lru_cache(maxsize=1000)
def count_occurrences(s, ignore_case=False):
    flags = ignore_case and re.IGNORECASE or 0
    r = re.compile(s, flags=flags)
    total = 0
    for line in logs:
        if r.search(line['message']) != None:
            total += 1
    return total
app.jinja_env.globals['count_occurrences'] = count_occurrences
