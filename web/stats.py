#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import datetime
import functools
import json
import os
import re
import time

from flask import Flask, url_for, render_template, g

APP_ROOT = os.path.dirname(os.path.abspath(__file__))
APP_STATIC = os.path.join(APP_ROOT, 'static')

DEBUG = True

logs = None
app = Flask(__name__)
app.config.from_object(__name__)
if 'IRC_STATS_SETTINGS' in os.environ:
    app.config.from_envvar('IRC_STATS_SETTINGS')

@functools.lru_cache(maxsize=1000)
def query_logs(s, cummulative=False, coarse=False):
    """
    Query logs for a given regular expression and return a time series of the
    number of occurrences of lines matching the regular expression per day.
    """

    r = re.compile(s)
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
        if r.search(line['message']) == None:
            continue
        key = get_key(line['timestamp'])
        value = get_value(key)
        total += 1

        if cummulative:
            value['y'] = total
        else:
            value['y'] += 1

    return sorted(results.values(), key=lambda x: x['x'])

def graph_query(queries, **kwargs):
    data = []
    for (label, s) in queries:
        data.append({
            'key': label,
            'values': query_logs(s, **kwargs),
        })
    return data
app.jinja_env.globals['graph_query'] = graph_query

@app.before_request
def misc_stats():
    # Cache the entire log file since it takes several seconds to parse.
    global logs
    global num_tnaks
    if not logs:
        with open(os.path.join(APP_STATIC, 'log.json'), 'r') as f:
            logs = json.load(f)
            num_tnaks = len(list(filter(
                    lambda m: 'tnak' in m['message'].lower(),
                    logs)))

@app.route('/')
def show_entries():
    global num_tnaks
    return render_template('index.html', num_tnaks=num_tnaks)

if __name__ == '__main__':
    port = 'PORT' in os.environ and int(os.environ['PORT']) or 5000
    app.run(host='0.0.0.0', port=port)
