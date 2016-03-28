import datetime
import os
from flask import Flask

app = Flask(__name__)

APP_ROOT = os.path.dirname(os.path.abspath(__file__))
APP_STATIC = os.path.join(APP_ROOT, 'static')

DEBUG = True

import web.dates
import web.logs
import web.utils
import web.views

app.config.from_object(__name__)
if 'IRC_STATS_SETTINGS' in os.environ:
    app.config.from_envvar('IRC_STATS_SETTINGS')

start_time = datetime.datetime.now().strftime('%I:%M%P on %B %d, %Y')
@app.template_global()
def get_start_time():
    return start_time
