#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import functools
import json
import re
import time
import web.logs

def word_freqs(logs, min_freq=0):
    freqs = {}
    for line in logs:
        for word in line['message'].split(' '):
            clean_word = re.sub(r'^[.?!,"\']+|[.?!,"\']+$', '', word).lower()
            if not clean_word: # Added to handle empty strings after re.sub
                continue
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
        try:
            # Ensure timestamp is float before int conversion
            timestamp = float(line['timestamp']) 
            if now <= timestamp + lookback_seconds:
                sliced_logs.append(line)
        except (ValueError, TypeError): # Catch if timestamp is not a valid number
            continue
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
        return 1.0 / total # Avoid division by zero if total is 0 (though sum()+1.0 prevents this)

@functools.lru_cache(maxsize=1000)
def get_trending(top=10, min_freq=10, lookback_days=7):
    """
    Return a list of the top trending terms. The values of the list will be
    tuples of the word along with the relative fractional increase in usage.
    """
    # THE CRITICAL CHANGE:
    logs_data = web.logs.log_query_engine.get_all_log_entries()
    
    if not logs_data: # Robustness: handle empty logs_data
        return []

    lookback_seconds = lookback_days * 24 * 60 * 60 # Calculate lookback_seconds
    recent_logs = slice_logs(logs_data, lookback_seconds)

    if not recent_logs: # If no logs in the recent window, no trends.
        return []

    all_freqs = word_freqs(logs_data)
    all_vector = to_vector(all_freqs)
    recent_freqs = word_freqs(recent_logs)
    recent_vector = to_vector(recent_freqs)

    differences = []
    for word in all_vector[0].keys(): # all_vector[0] is the dictionary of word frequencies
        all_value = vector_lookup(all_vector, word)
        recent_value = vector_lookup(recent_vector, word)

        # Ensure word meets minimum frequency in recent logs
        if recent_freqs.get(word, 0) < min_freq:
            continue
        
        if recent_vector[1] == 0: # Denominator for recent_value; avoid division by zero
             continue


        if all_value == 0: 
            if recent_value > 0: # Word is new and frequent
                diff = float("inf") # Assign a very high trend score (or a large number)
            else: # Both are zero effectively
                diff = 0.0 
        else:
            diff = (recent_value - all_value) / all_value
        
        differences.append((word, diff))

    return list(reversed(sorted(differences, key=lambda x: x[1])))[0:top]
