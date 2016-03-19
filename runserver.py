#!/usr/bin/env python3

import os
from web import app

port = 'PORT' in os.environ and int(os.environ['PORT']) or 5000
app.run(host='0.0.0.0', port=port)
