# -*- coding: utf-8 -*-

from web import app
from flask import Flask, url_for, render_template, g, request
import web.logs

@app.route('/', methods=['GET'])
def home():
    num_tnaks = web.logs.count_occurrences(r'\b[Tt][Nn][Aa][Kk]')
    return render_template('index.html', num_tnaks=num_tnaks)

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
def browse(label=None, regexp=None, cumulative=False):
    return render_template('browse.html', valid_days=web.logs.get_valid_days())
