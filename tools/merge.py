#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Merge multiple JSON IRC logs into a single monolithic JSON log.

import json
import sys

def print_usage():
    sys.stderr.write('usage: merge.py a.json b.json ...\n')
    exit(1)

def print_messages(messages):
    print json.dumps(messages, indent=2, sort_keys=True)

def parse_file(path):
  with open(path, 'r') as f:
    return json.load(f)

if len(sys.argv) == 1:
    print_usage()
files = sys.argv[1:]

messages = []
for f in files:
  messages += parse_file(f)
messages.sort(key=lambda m: m['timestamp'])

print_messages(messages)
