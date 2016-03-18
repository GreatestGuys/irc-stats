from flask import request
from web import app, APP_STATIC
from werkzeug import url_encode
import random

MIN_HUE = 60
MAX_HUE = 240
COLOR_TABLE = [x / 255.0 * (MAX_HUE - MIN_HUE) + MIN_HUE for x in range(0, 256)]
random.seed(41)
random.shuffle(COLOR_TABLE)

@app.template_global()
def modify_query(**new_values):
    args = request.args.copy()

    for key, value in new_values.items():
        args[key] = value

    return '{}?{}'.format(request.path, url_encode(args))

@app.template_global()
def color_for_nick(nick):
    hue = COLOR_TABLE[ord(nick[0])]
    saturation = 0.80
    lightness= 0.95
    return 'hsl(%d, %f%%, %f%%)' % (hue, saturation * 100, lightness * 100)
