#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import functools
import json
import re
import time
from web.logs import log_engine

def word_freqs(logs, min_freq=0):
    freqs = {}
    for line in logs:
        for word in line['message'].split(' '):
            clean_word = re.sub(r'^[.?!,"\']+|[.?!,"\']+$', '', word).lower()
            if clean_word in freqs:
                freqs[clean_word] += 1
            else:
                freqs[clean_word] = 1
    if min_freq > 0:
        new_freqs = {}
        for word in freqs.keys():
            if min_freq <= freqs[word]:
                new_freqs[word] = freqs[word]
        freqs = new_freqs
    return freqs

def slice_logs(logs, lookback_seconds=7*24*60*60):
    now = time.time()
    sliced_logs = []
    for line in logs:
        if now <= int(line['timestamp']) + lookback_seconds:
            sliced_logs.append(line)
    return sliced_logs

def to_vector(freqs):
    total = sum(freqs.values()) + 1.0
    vector = {}
    for word in freqs:
        vector[word] = freqs[word] / total
    return (vector, total)

def vector_lookup(vector, word):
    (values, total) = vector
    if word in values:
        return values[word]
    else:
        return 1.0 / total

@functools.lru_cache(maxsize=1000)
def get_trending(top=10, min_freq=10, lookback_days=7):
    """
    Return a list of the top trending terms. The values of the list will be
    tuples of the word along with the relative fractional increase in usage.
    """
    logs = log_engine.logs
    recent_logs = slice_logs(logs)

    all_freqs = word_freqs(logs)
    all_vector = to_vector(all_freqs)
    recent_freqs = word_freqs(recent_logs)
    recent_vector = to_vector(recent_freqs)

    differences = []
    for word in all_vector[0].keys():
        all_value = vector_lookup(all_vector, word)
        recent_value = vector_lookup(recent_vector, word)

        if recent_value < min_freq / recent_vector[1]:
            continue

        diff = (recent_value - all_value) / all_value
        differences.append((word, diff))

    return list(reversed(sorted(differences, key=lambda x: x[1])))[0:top]
