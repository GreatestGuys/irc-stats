#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import calendar

from web import app, APP_STATIC
from flask import Flask, url_for, render_template, g


app.jinja_env.globals['monthrange'] = calendar.monthrange

@app.template_global()
def days_in_month(year, month):
  return calendar.monthrange(year, month)[1]

@app.template_global()
def month_name(month):
  return calendar.month_name[month % 13]
