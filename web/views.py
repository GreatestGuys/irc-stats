# -*- coding: utf-8 -*-

from web import app
from flask import Flask, url_for, render_template, g, request
import re
import web.logs
import web.trending

@app.route('/', methods=['GET'])
def home():
    num_tnaks = web.logs.count_occurrences(r'\b[Tt][Nn][Aa][Kk]')
    trending = []
    for trend in web.trending.get_trending():
      trending.append((trend[0], "%.2f" % trend[1]))
    return render_template('index.html',
        num_tnaks=num_tnaks,
        trending=trending)

@app.route('/query', methods=['GET'])
def query(label=None, regexp=None, cumulative=False):
    def to_list(x):
        if x == None:
            return []
        if not isinstance(x, list):
            return [x]
        return x

    label = request.args.getlist('label')
    regexp = request.args.getlist('regexp')

    query = filter(
        lambda x: len(x[1]) > 0,
        zip(to_list(label), to_list(regexp)))

    return render_template(
            'query.html',
            query=list(query))

@app.route('/browse', methods=['GET'])
def browse():
    return render_template('browse.html', valid_days=web.logs.get_valid_days())

@app.route('/browse/<int:year>/<int:month>/<int:day>', methods=['GET'])
def browse_day(year, month, day):
    r = re.compile('(https?://\\S+)', flags=re.IGNORECASE)

    day_logs = web.logs.get_logs_by_day()
    key = (year, month, day)
    prev_day = None
    next_day = None
    lines = []
    if key in day_logs:
        keys = sorted(day_logs.keys())
        lines = day_logs[key]
        index = keys.index(key)
        if index != 0:
            prev_day = keys[index - 1]
        if index < len(keys) - 1:
            next_day = keys[index + 1]

    # Find all the links in each line and mark them so that they can be rendered
    # as hyperlinks.
    for line in lines:
        message = line['message']
        message_parts = []
        last_end = 0
        for link in r.finditer(message):
            start = link.start()
            end = link.end()
            message_parts.append((False, message[last_end : start]))
            message_parts.append((True, message[start : end]))
            last_end = end
        message_parts.append((False, message[last_end :]))
        line['message_parts'] = message_parts

    lines = list(zip(range(0, len(lines)), lines))
    # Insert breaks every time there is a larger than 1 hour break in
    # conversation.
    lines_with_pauses = len(lines) > 0 and [lines[0]] or []
    for i in range(1, len(lines)):
        last_time = int(lines[i - 1][1]['timestamp'])
        this_time = int(lines[i][1]['timestamp'])
        if this_time - last_time > 60 * 60:
            lines_with_pauses.append(None)
        lines_with_pauses.append(lines[i])

    return render_template('browse_day.html',
            lines=lines_with_pauses,
            next_day=next_day,
            prev_day=prev_day,
            year=year, month=month, day=day)

@app.route('/search', methods=['GET'])
def search():
    LINES_PER_PAGE = 25
    lines = []

    start = 0
    end = 0
    query = request.args.get('q')
    page = request.args.get('p', 0, type=int)
    ignore_case = request.args.get('ignore_case', False, type=bool)
    if query:
        lines = web.logs.search_day_logs(query, ignore_case=ignore_case)

    total_lines = len(lines)
    if lines:
        start = min(page * LINES_PER_PAGE, len(lines) - 1)
        end = min((page + 1) * LINES_PER_PAGE, len(lines))
        lines = lines[start:end]

    for i in range(0, len(lines)):
        (day, index, line, match_start, match_end) = lines[i]
        prefix = line['message'][:match_start]
        match = line['message'][match_start:match_end]
        sufix = line['message'][match_end:]
        lines[i] = (day, index, line, prefix, match, sufix)

    next_page = (page + 1) * LINES_PER_PAGE < total_lines and page + 1 or None
    prev_page = None
    if page > 0: prev_page = page - 1

    return render_template('search.html',
            start=(start + 1), end=end,
            lines=lines, total_lines=total_lines,
            next_page=next_page, prev_page=prev_page)
