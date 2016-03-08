import os
from flask import Flask

app = Flask(__name__)

APP_ROOT = os.path.dirname(os.path.abspath(__file__))
APP_STATIC = os.path.join(APP_ROOT, 'static')

DEBUG = True

import web.logs
import web.views

app.config.from_object(__name__)
if 'IRC_STATS_SETTINGS' in os.environ:
    app.config.from_envvar('IRC_STATS_SETTINGS')
