# -*- coding: utf-8 -*-

from web import app
from flask import Flask, url_for, render_template, g, request
import re
from web.logs import log_engine

@app.route('/', methods=['GET'])
def home():
    num_tnaks = log_engine().count_occurrences([('', r'\b[Tt][Nn][Aa][Kk]')])[1][1]
    trending = []
    for trend in log_engine().get_trending():
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
    valid_days = log_engine().get_valid_days()
    min_year = None
    max_year = None

    if valid_days:
        min_year = min(day[0] for day in valid_days)
        max_year = max(day[0] for day in valid_days)
    else:
        # Default years if no logs are found
        min_year = 2013
        max_year = 2025

    return render_template('browse.html',
                           valid_days=valid_days,
                           min_year=min_year,
                           max_year=max_year)

@app.route('/browse/<int:year>/<int:month>/<int:day>', methods=['GET'])
def browse_day(year, month, day):
    r = re.compile('(https?://\\S+)', flags=re.IGNORECASE)

    lines, prev_day, next_day = log_engine().get_logs_by_day(year, month, day)

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
    histogram = []
    total_lines = 0 # Initialize total_lines

    query = request.args.get('q')
    page = request.args.get('p', 0, type=int)
    # Flask's type=bool for request.args.get should handle "true"/"false" strings.
    # Example: request.args.get('ignore_case', False, type=bool)
    # If 'ignore_case' is "True" or "true", it becomes True.
    # If 'ignore_case' is "False" or "false", it becomes False.
    # If missing, it defaults to False.
    ignore_case = request.args.get('ignore_case', False, type=bool)


    if query:
        offset = page * LINES_PER_PAGE
        # search_day_logs now returns (paginated_lines, total_count_before_pagination)
        lines, total_lines = log_engine().search_day_logs(
            query,
            ignore_case=ignore_case,
            limit=LINES_PER_PAGE,
            offset=offset
        )
        histogram = log_engine().search_results_to_chart(
            query, ignore_case=ignore_case)

    # Process lines for highlighting (operates on the paginated lines received)
    processed_lines = []
    for i in range(len(lines)): # Iterate over the paginated lines
        (day, index, line, match_start, match_end) = lines[i]
        prefix = line['message'][:match_start]
        match = line['message'][match_start:match_end]
        sufix = line['message'][match_end:]
        processed_lines.append((day, index, line, prefix, match, sufix))

    lines = processed_lines # Replace lines with processed_lines

    # Calculate display start and end numbers (1-based for user display)
    start_display = 0
    end_display = 0
    if total_lines > 0: # Only calculate if there are any lines at all
        if lines: # If there are results on the current page
            start_display = (page * LINES_PER_PAGE) + 1
            end_display = (page * LINES_PER_PAGE) + len(lines)
        # If lines is empty but total_lines > 0, it means we are on an empty page (e.g. page beyond last results)
        # start_display and end_display remain 0, which is fine for template logic like "Showing 0 to 0 of X" or similar.

    # Calculate next and previous page numbers
    next_page = page + 1 if (page * LINES_PER_PAGE) + LINES_PER_PAGE < total_lines else None
    prev_page = page - 1 if page > 0 else None

    return render_template('search.html',
            start=start_display, end=end_display, # Use new display variables
            lines=lines, total_lines=total_lines,
            histogram=histogram,
            next_page=next_page, prev_page=prev_page)
