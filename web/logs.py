#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import datetime
import functools
import json
import os
import re
import time

from web import app, APP_STATIC # app for decorators, APP_STATIC for default path

class LogQueryEngine:
    VALID_NICKS = { # Original VALID_NICKS, now a class attribute
        "Cosmo": ["cosmo", "cfumo"],
        "Graham": ["graham", "jorgon"],
        "Jesse": ["jesse", "je-c"],
        "Will": ["will", "wyll", "wyll_"],
        "Zhenya": ["zhenyah", "zhenya", "zdog", "swphantom", "za", "zhenya2"],
    }

    def __init__(self, log_file_path=None):
        if log_file_path is None:
            log_file_path = os.path.join(APP_STATIC, "log.json")
        
        self.logs = []
        try:
            # This logic is from the original init_logs()
            with open(log_file_path, "r") as f:
                self.logs = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            self.logs = [] 

    def get_all_log_entries(self):
        return self.logs

    # --- Start of methods moved from global scope, original logic preserved ---
    def query_logs_impl(self, s, # Renamed to avoid clash if a global query_logs is kept for delegation
            cumulative=False, coarse=False, nick=None, ignore_case=False,
            normalize=False, normalize_type=None):
        # This is the exact logic from the original global query_logs function
        # just using self.logs instead of global logs.
        if not self.logs: return [] # Added safety for empty logs

        flags = ignore_case and re.IGNORECASE or 0
        try: r = re.compile(s, flags=flags)
        except: return []

        results = {}
        totals = {}

        def to_datetime(timestamp):
            return datetime.datetime.fromtimestamp(float(timestamp))

        def to_timestamp(d):
            return time.mktime(
                    datetime.datetime(d.year, d.month, d.day).timetuple())

        def get_key(timestamp):
            d = to_datetime(timestamp)
            day = coarse and 1 or d.day
            return time.mktime(
                    datetime.datetime(d.year, d.month, day).timetuple())

        def get_value(key, m):
            if key not in m:
                m[key] = {"x": key, "y": 0}
            return m[key]

        for line in self.logs: # Changed from global "logs" to "self.logs"
            if nick and line["nick"].lower() not in self.VALID_NICKS.get(nick, []): # Use self.VALID_NICKS
                continue

            key = get_key(line["timestamp"])
            total_value = get_value(key, totals)
            total_value["y"] += 1

            if r.search(line["message"]) == None:
                continue

            value = get_value(key, results)
            value["y"] += 1

        smoothed = {}
        total_matched = 0
        total_possible = 0
        
        # Ensure self.logs is not empty before trying to access self.logs[0] or self.logs[-1]
        if not self.logs: return []


        current_time = to_datetime(self.logs[0]["timestamp"]) # Changed from global "logs"
        end_time = to_datetime(self.logs[-1]["timestamp"])   # Changed from global "logs"
        last_key = None
        while current_time <= end_time:
            key = get_key(to_timestamp(current_time))
            current_time += datetime.timedelta(days=1)
            if key == last_key:
                continue
            last_key = key

            value = key in results and results[key]["y"] or 0
            total_possible += key in totals and totals[key]["y"] or 0
            total_matched += value

            if cumulative:
                smoothed[key] = {"x": key, "y": total_matched}
                # Original code for cumulative normalization was within the normalize block
                # Here, we just store cumulative values. totals[key] is also made cumulative below for normalization.
                if key in totals: # Ensure key exists if we are to make it cumulative
                    totals[key] = {"x": key, "y": total_possible}
            else:
                smoothed[key] = {"x": key, "y": value}

        if normalize and total_matched > 0:
            total_window = []
            matched_window = []
            # Iterating over sorted keys to maintain chronological order for windowing
            for key_sorted in sorted(smoothed.keys()): 
                s_item = smoothed[key_sorted] # current smoothed item
                # For cumulative, totals[key_sorted] was updated to cumulative total_possible
                # For non-cumulative, totals[key_sorted] is day-specific total
                t_item_y = totals.get(key_sorted, {}).get("y", 0)

                total_window.append(t_item_y)
                matched_window.append(s_item["y"])
                
                norm_type_str = str(normalize_type or "")
                if norm_type_str.startswith("trailing_avg_"):
                    try:
                        window_size = int(norm_type_str[13:])
                        total_window = total_window[-window_size:]
                        matched_window = matched_window[-window_size:]
                    except ValueError: # Default to single point if parse fails
                        total_window = total_window[-1:]
                        matched_window = matched_window[-1:]
                else: # Default to single point normalization
                    total_window = total_window[-1:]
                    matched_window = matched_window[-1:]

                current_total_sum = sum(total_window)
                current_matched_sum = sum(matched_window)

                if cumulative:
                    # For cumulative, s_item["y"] is total_matched up to key_sorted
                    # t_item_y is total_possible up to key_sorted
                    if t_item_y == 0:
                        s_item["y"] = 0.0
                    else:
                        # Ensure float division. Original value was already cumulative sum.
                        s_item["y"] = float(s_item["y"]) / t_item_y 
                else:
                    # For non-cumulative, s_item["y"] is value for that day.
                    # We normalize sum of matched in window by sum of total in window.
                    if current_total_sum == 0:
                        s_item["y"] = 0.0
                    else:
                        s_item["y"] = float(current_matched_sum) / current_total_sum
        
        return sorted(smoothed.values(), key=lambda x_val: x_val["x"])

    def count_occurrences_impl(self, s, ignore_case=False, nick=None):
        # Original logic from global count_occurrences
        if not self.logs: return 0 # Safety for empty logs
        flags = ignore_case and re.IGNORECASE or 0
        try: r = re.compile(s, flags=flags)
        except: return 0

        total = 0
        for line in self.logs: # Changed from global "logs"
            if nick != None and line["nick"].lower() not in self.VALID_NICKS.get(nick, []): # Use self.VALID_NICKS
                continue
            if r.search(line["message"]) != None:
                total += 1
        return total

    def get_logs_by_day_impl(self):
        # Original logic from global get_logs_by_day
        if not self.logs: return {} # Safety for empty logs
        days = {}
        for line in self.logs: # Changed from global "logs"
            dt = datetime.datetime.fromtimestamp(float(line["timestamp"]))
            key = (dt.year, dt.month, dt.day)
            if key not in days:
                days[key] = []
            days[key].append(line)
        return days

    def get_valid_days_impl(self):
        # Original logic from global get_valid_days
        # Depends on the class"s get_logs_by_day_impl
        if not self.logs: return [] # Safety for empty logs
        return sorted(self.get_logs_by_day_impl().keys())

    def get_all_days_impl(self):
        # Original logic from global get_all_days
        # Depends on the class"s get_valid_days_impl
        if not self.logs: return [] # Safety for empty logs
        
        valid_days = self.get_valid_days_impl() # Use internal method
        if not valid_days: return [] # If no valid days, return empty list

        def to_datetime(day): # Inner helper, original scope
            return datetime.datetime.fromtimestamp(float(time.mktime(
                    datetime.datetime(day[0], day[1], day[2]).timetuple())))

        current_time = to_datetime(valid_days[0])
        end_time = to_datetime(valid_days[-1])
        days = []
        while current_time <= end_time:
            days.append((current_time.year, current_time.month, current_time.day))
            current_time += datetime.timedelta(days=1)
        return days
        
    def search_day_logs_impl(self, s, ignore_case=False):
        # Original logic from global search_day_logs
        if not self.logs: return [] # Safety for empty logs
        flags = ignore_case and re.IGNORECASE or 0
        try: r = re.compile(s, flags=flags)
        except: return []

        results = []
        day_logs = self.get_logs_by_day_impl() # Use internal method
        for day in reversed(sorted(day_logs.keys())):
            index = 0
            for line in day_logs[day]:
                m = r.search(line["message"])
                if m != None:
                    results.append((day, index, line, m.start(), m.end()))
                index += 1
        return results

    def search_results_to_chart_impl(self, s, ignore_case=False):
        # Original logic from global search_results_to_chart
        if not self.logs: return [{"key": "", "values": []}] # Safety for empty logs

        results = self.search_day_logs_impl(s, ignore_case) # Use internal method
        all_days_list = self.get_all_days_impl() # Use internal method
        
        if not all_days_list: return [{"key": "", "values": []}]


        def get_key(day): # Inner helper, original scope
             # Original grouped by month (day=1)
            return time.mktime(
                    datetime.datetime(day[0], day[1], 1).timetuple())

        counts = {}
        # Initialize all months in the range with 0 counts
        min_month_key = get_key(all_days_list[0])
        max_month_key = get_key(all_days_list[-1])
        
        current_dt = datetime.datetime.fromtimestamp(min_month_key)
        end_dt = datetime.datetime.fromtimestamp(max_month_key)

        while current_dt <= end_dt:
            key = time.mktime(current_dt.timetuple())
            counts[key] = {"x": key, "y": 0}
            if current_dt.month == 12:
                current_dt = datetime.datetime(current_dt.year + 1, 1, 1)
            else:
                current_dt = datetime.datetime(current_dt.year, current_dt.month + 1, 1)
        
        for r_item in results:
            day = r_item[0]
            month_key = get_key(day) # Get the month key for this result item
            if month_key in counts: # Should always be true due to prefill
                 counts[month_key]["y"] += 1
        
        return [{"key": "", "values": sorted(counts.values(), key=lambda x_val_lambda: x_val_lambda["x"])}]
    # --- End of methods moved from global scope ---

log_query_engine = LogQueryEngine() # Global instance

# --- Delegating global functions, preserving original decorators ---
# Original init_logs() is removed as its logic is in LogQueryEngine.__init__()

@functools.lru_cache(maxsize=1000) # Original decorator
def query_logs(s,
        cumulative=False, coarse=False, nick=None, ignore_case=False,
        normalize=False, normalize_type=None):
    return log_query_engine.query_logs_impl(s, cumulative, coarse, nick, ignore_case, normalize, normalize_type)

@app.template_global() # Original decorator
def graph_query(queries, nick_split=False, **kwargs):
    # This function uses the global query_logs, which is now the delegating one.
    # Its internal logic references LogQueryEngine.VALID_NICKS correctly.
    data = []
    for (label, s_query) in queries: # Renamed s to s_query
        if not nick_split:
            data.append({
                "key": label,
                "values": query_logs(s_query, **kwargs), # Calls the cached global delegator
            })
        else:
            for nick_key in sorted(LogQueryEngine.VALID_NICKS.keys()): # Access class attribute
                if label == "":
                    nick_label = nick_key
                else:
                    nick_label = f"{label} - {nick_key}" # Used f-string
                data.append({
                    "key": nick_label,
                    "values": query_logs(s_query, nick=nick_key, **kwargs), # Calls cached global delegator
                })
    return data

@app.template_global() # Original decorator
def table_query(queries, nick_split=False, order_by_total=False, **kwargs):
    # This function uses the global count_occurrences, which is now the delegating one.
    # Its internal logic references LogQueryEngine.VALID_NICKS correctly.
    rows = [["", "Total"]]
    if nick_split:
        for nick_key_h in sorted(LogQueryEngine.VALID_NICKS.keys()): # Renamed, access class attribute
            rows[0].append(nick_key_h)

    tmp_rows = []
    for (label, s_query) in queries: # Renamed s to s_query
        row = [label]
        row.append(count_occurrences(s_query, **kwargs)) # Calls cached global delegator
        if nick_split:
            for nick_key_c in rows[0][2:]: # Renamed
                row.append(count_occurrences(s_query, nick=nick_key_c, **kwargs)) # Calls cached global delegator
        tmp_rows.append(row)

    if order_by_total:
        tmp_rows = sorted(tmp_rows, key=lambda x_row: x_row[1], reverse=True) # Renamed x to x_row

    rows += tmp_rows
    return rows

@app.template_global() # Original decorator
@functools.lru_cache(maxsize=1000) # Original decorator
def count_occurrences(s, ignore_case=False, nick=None):
    return log_query_engine.count_occurrences_impl(s, ignore_case=ignore_case, nick=nick)

@functools.lru_cache(maxsize=1) # Original decorator
def get_valid_days():
    return log_query_engine.get_valid_days_impl()

@functools.lru_cache(maxsize=1) # Original decorator
def get_all_days():
    return log_query_engine.get_all_days_impl()

@functools.lru_cache(maxsize=1) # Original decorator
def get_logs_by_day():
    return log_query_engine.get_logs_by_day_impl()

@functools.lru_cache(maxsize=1000) # Original decorator
def search_day_logs(s, ignore_case=False):
    return log_query_engine.search_day_logs_impl(s, ignore_case=ignore_case)

@functools.lru_cache(maxsize=1000) # Original decorator
def search_results_to_chart(s, ignore_case=False):
    return log_query_engine.search_results_to_chart_impl(s, ignore_case=ignore_case)


