#!/usr/bin/env python
# -*- coding: utf-8 -*-
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

def make_is_regexp(s):
    r = re.compile(s)
    return lambda line: r.search(line) != None

def parse_irssi(f):
    # Python's scoping is so fucked up that it is impossible to modify a
    # variable defined in a parent scope. It is only possible to read variables.
    # The official workaround for this is to make the variable you wish to
    # modify a single element array...
    current_day = [None]
    log_open_re = re.compile(
            r'^--- (?:\S+ ){2}\S+ (\D+) (\S+) (?:\S+ )?(\d+)(?:--- .*$)?$')
    chat_re = re.compile(r'^(\d\d:\d\d) <.(\S+)> (.+)$')

    def parse_chat(line):
        res = log_open_re.match(line)
        if res != None:
            current_day[0] = (res.group(1), res.group(2), res.group(3))
            return None

        res = chat_re.match(line)
        if res == None:
            return None

        if current_day[0] == None:
            sys.stderr.write(
                    'ERROR: Found a chat but I don\'t know what day it is!\n')
            exit(1)

        time_of_day = res.group(1)
        nick = res.group(2)
        message = res.group(3)

        full_date_string = '%s %s %s %s' % (current_day[0] + (time_of_day,))
        timestamp = time.mktime(datetime.datetime.strptime(
                full_date_string, '%b %d %Y %H:%M').timetuple())

        return (timestamp, nick, message)

    return parse_generic(
            f,
            make_is_regexp(r'^[0-9:]{,5}\s+< \*\*\*> Buffer Playback'),
            make_is_regexp(r'^[0-9:]{,5}\s+< \*\*\*> Playback Complete'),
            parse_chat)

def parse_weechat(f):
    date_re = '\d+-\d+-\d+ \d+:\d+:\d+'
    chat_re = re.compile(r'(%s)\s[@+]?(\S+)\s+(.+)' % date_re)

    def parse_chat(line):
        res = chat_re.match(line)
        if res == None:
            return None

        full_date_string = res.group(1)
        nick = res.group(2)
        message = res.group(3)

        # Weechat logs are kind of shitty in that it is hard to differentiate
        # nicks from status messages. The only way to filter out status messages
        # is to enumerate every "nick" they come from and skip those lines.
        bad_nicks = [
                'ℹ', '--', '-->', '<--', '←', '→', '⚡', '⚠', '│', '+',
                '▬▬▶', '◀▬▬'
        ]
        if nick in bad_nicks:
            return

        timestamp = time.mktime(datetime.datetime.strptime(
                full_date_string, '%Y-%m-%d %H:%M:%S').timetuple())

        return (timestamp, nick, message)

    return parse_generic(
            f,
            make_is_regexp(r'^%s\s+\*\*\*\s+Buffer Playback' % date_re),
            make_is_regexp(r'^%s\s+\*\*\*\s+Playback Complete' % date_re),
            parse_chat)

def print_usage():
    sys.stderr.write('usage: parse.py [irssi|weechat]\n')
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
