#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import functools
import json
import re
import time
from web import logs 

def word_freqs(log_entries, min_freq=0): 
    freqs = {}
    for line in log_entries:
        for word in line["message"].split(" "):
            # Robustly clean words, handle potential empty strings from multiple spaces
            clean_word = re.sub(r'^[.?!,"']+|[.?!,"']+$', '', word).lower()
            if not clean_word: # Skip empty strings that can result from re.sub or split
                continue
            freqs[clean_word] = freqs.get(clean_word, 0) + 1
    
    if min_freq > 0:
        # Return a new dict with words meeting min_freq
        return {word_key: count_val for word_key, count_val in freqs.items() if count_val >= min_freq}
    return freqs

def slice_logs(log_entries, lookback_seconds=7*24*60*60): 
    now = time.time() # time.time() is mocked in tests for predictability
    sliced_logs_list = []
    for line in log_entries:
        try:
            # Ensure timestamp is treated as a number before comparison
            timestamp = float(line["timestamp"])
            if now <= timestamp + lookback_seconds:
                sliced_logs_list.append(line)
        except ValueError: # Handle cases where timestamp might not be a valid float
            continue 
    return sliced_logs_list

def to_vector(freqs):
    total_sum = sum(freqs.values())
    # Add 1.0 to total_sum to prevent division by zero if freqs is empty 
    # or all word frequencies are zero (though sum should be >0 if freqs not empty).
    denominator = total_sum + 1.0 
    
    vector_dict = {}
    for word, count in freqs.items():
        vector_dict[word] = float(count) / denominator # Ensure float division
    # Return the denominator as it's used by vector_lookup's original logic
    return (vector_dict, denominator) 

def vector_lookup(vector_tuple, word): 
    (values_dict, total_denominator) = vector_tuple 
    # Original logic implies a smoothing factor or base probability for unknown words
    return values_dict.get(word, 1.0 / total_denominator)

@functools.lru_cache(maxsize=1000) 
def get_trending(top=10, min_freq=10, lookback_days=7):
    all_log_entries = logs.log_query_engine.get_all_log_entries()
    if not all_log_entries: 
        return []

    lookback_seconds = lookback_days * 24 * 60 * 60
    recent_log_entries = slice_logs(all_log_entries, lookback_seconds)

    if not recent_log_entries: # If no logs in the recent window, no trends.
        return []

    all_freqs = word_freqs(all_log_entries)
    all_vector = to_vector(all_freqs) 
    
    recent_freqs = word_freqs(recent_log_entries)
    recent_vector = to_vector(recent_freqs) 

    differences = []
    # Iterate over words that have appeared historically
    for word in all_freqs.keys(): 
        all_value_freq = vector_lookup(all_vector, word)
        recent_value_freq = vector_lookup(recent_vector, word)

        # Ensure word meets minimum frequency in recent logs
        if recent_freqs.get(word, 0) < min_freq:
            continue
        
        # Calculate difference, handling division by zero for new words if any (though iterating all_freqs.keys should prevent this)
        if all_value_freq == 0: 
            if recent_value_freq > 0: # Word is new and frequent
                diff = float("inf") # Assign a very high trend score
            else: # Both are zero effectively
                diff = 0.0 
        else:
            diff = (recent_value_freq - all_value_freq) / all_value_freq
        
        differences.append((word, diff))
    
    # Sort by the trend score in descending order and take the top N
    return list(reversed(sorted(differences, key=lambda x_item_lambda: x_item_lambda[1])))[0:top]
