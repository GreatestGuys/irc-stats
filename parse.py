#!/usr/bin/env python
#
# Parse a IRC log file and print a JSON formatted form to STDOUT.
#
# The JSON representation of an IRC log is an array whose values are
# dictionaries. The keys of the dictionary are 'timestamp, 'nick', and
# 'message'.
#
# The entries in the array will be sorted in chronological order.

import datetime
import json
import re
import sys
import time

def parse_generic(f, is_znc_start, is_znc_end, parse_chat):
    messages = []

    in_znc_playback = False
    for line in f.readlines():
        line = line.strip()

        # Will uses an bouncer which pollutes the logs by replaying the last 50
        # lines of messages whenever joining a channel. Filter out this garbage
        # so that it doesn't skew the data set.
        if is_znc_start(line):
            in_znc_playback = True
            continue
        if is_znc_end(line):
            in_znc_playback = False
            continue
        if in_znc_playback:
            continue

        chat = parse_chat(line)
        if chat == None:
            continue

        (timestamp, nick, msg) = chat
        messages.append({
                'timestamp': '%d' % timestamp,
                'nick': nick,
                'message': msg,
            })

    # Sort in chronological order.
    messages.sort(key=lambda m: m['timestamp'])
    return messages

def make_is_regexp(r):
    return lambda line: re.search(r, line) != None

def parse_irssi(f):
    # Python's scoping is so fucked up that it is impossible to modify a
    # variable defined in a parent scope. It is only possible to read variables.
    # The official workaround for this is to make the variable you wish to
    # modify a single element array...
    current_day = [None]
    log_open_re = r'^--- (?:[^ ]+ ){2}[^ ]+ ([^0-9]+) ([^ ]+) (?:[^ ]+ )?([0-9]+)(?:--- .*$)?$'
    chat_re = r'^([0-9]{2}:[0-9]{2}) <.([^ ]+)> (.+)$'

    def parse_chat(line):
        res = re.search(log_open_re, line)
        if res != None:
            current_day[0] = (res.group(1), res.group(2), res.group(3))
            return None

        res = re.search(chat_re, line)
        if res == None:
            return None

        if current_day[0] == None:
            print 'ERROR: Found a chat line but I don\'t know what day it is!'
            exit(1)

        time_of_day = res.group(1)
        nick = res.group(2)
        message = res.group(3)

        full_date_string = '%s %s %s %s' % (current_day[0] + (time_of_day,))
        timestamp = time.mktime(datetime.datetime.strptime(
                    full_date_string, "%b %d %Y %H:%M").timetuple())

        return (timestamp, nick, message)

    return parse_generic(
            f,
            make_is_regexp(r"^[0-9:]{,5}\s+< \*\*\*> Buffer Playback"),
            make_is_regexp(r"^[0-9:]{,5}\s+< \*\*\*> Playback Complete"),
            parse_chat)

def parse_weechat(f):
    def parse_chat(line):
        pass

    return parse_generic(
            f,
            make_is_regexp(r"^[0-9:\-\s]+\s\*\*\*\s+Buffer Playback"),
            make_is_regexp(r"^[0-9:\-\s]+\s\*\*\*\s+Playback Complete"),
            parse_chat)

def print_usage():
    print "usage: parse.py [irssi|weechat]"
    exit(1)

def print_messages(messages):
    print json.dumps(messages, indent=2, sort_keys=True)

if len(sys.argv) != 2:
    print_usage()
log_type = sys.argv[1]

if log_type == 'irssi':
    parser = parse_irssi
elif log_type == 'weechat':
    parser = parse_weechat
else:
    print_usage()

print_messages(parser(sys.stdin))
