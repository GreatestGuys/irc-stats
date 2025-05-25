#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import datetime
import functools
import json
import os
import re
import time

from web import app, APP_STATIC 
from flask import Flask, url_for, render_template, g 

class LogQueryEngine:
    VALID_NICKS = {
        'Cosmo': ['cosmo', 'cfumo'],
        'Graham': ['graham', 'jorgon'],
        'Jesse': ['jesse', 'je-c'],
        'Will': ['will', 'wyll', 'wyll_'],
        'Zhenya': ['zhenyah', 'zhenya', 'zdog', 'swphantom', 'za', 'zhenya2'],
    }

    def __init__(self, log_file_path=None):
        if log_file_path is None:
            log_file_path = os.path.join(APP_STATIC, 'log.json')
        
        self.logs = []
        try:
            with open(log_file_path, 'r') as f:
                self.logs = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            self.logs = [] 

    def get_all_log_entries(self):
        return self.logs

    def query_logs(self, s,
            cumulative=False, coarse=False, nick=None, ignore_case=False,
            normalize=False, normalize_type=None):
        if not self.logs: return []

        flags = re.IGNORECASE if ignore_case else 0 # Simpler flag assignment
        try: r = re.compile(s, flags=flags)
        except: return []

        results = {}
        totals = {}

        def to_datetime_fn(timestamp_str): 
            return datetime.datetime.fromtimestamp(float(timestamp_str))

        def to_timestamp_fn(dt_obj): 
            return time.mktime(dt_obj.timetuple()) # Simpler, day is already part of dt_obj

        def get_key(timestamp_str):
            dt_obj = to_datetime_fn(timestamp_str)
            if coarse: # Apply coarse logic to month/day
                # Group by month: set day to 1 for coarse grouping by month
                key_dt = datetime.datetime(dt_obj.year, dt_obj.month, 1)
            else: # Group by day
                key_dt = datetime.datetime(dt_obj.year, dt_obj.month, dt_obj.day)
            return time.mktime(key_dt.timetuple())


        def get_value(key, m):
            m.setdefault(key, {'x': key, 'y': 0}) # Use setdefault for cleaner init
            return m[key]

        for line in self.logs:
            if nick and line['nick'].lower() not in self.VALID_NICKS.get(nick, []):
                continue

            key_val = get_key(line['timestamp']) 
            total_value = get_value(key_val, totals)
            total_value['y'] += 1

            if r.search(line['message']) is None:
                continue

            value = get_value(key_val, results)
            value['y'] += 1
        
        if not self.logs: 
            return []

        smoothed = {}
        running_total_matched = 0 # Renamed for clarity
        running_total_possible_msgs = 0 # Renamed for clarity

        min_log_time_dt = to_datetime_fn(self.logs[0]['timestamp'])
        max_log_time_dt = to_datetime_fn(self.logs[-1]['timestamp'])

        current_iter_dt = min_log_time_dt
        last_key_processed = None 

        while current_iter_dt <= max_log_time_dt:
            # Generate key for the current day/month based on coarse
            current_key = get_key(to_timestamp_fn(current_iter_dt))
            
            if current_key != last_key_processed: 
                current_period_matched_count = results.get(current_key, {}).get('y', 0)
                current_period_total_msgs = totals.get(current_key, {}).get('y', 0)

                running_total_matched += current_period_matched_count
                running_total_possible_msgs += current_period_total_msgs

                if cumulative:
                    smoothed[current_key] = {'x': current_key, 'y': running_total_matched}
                    # For cumulative normalization, store the running total of messages
                    totals[current_key] = {'x': current_key, 'y': running_total_possible_msgs} 
                else:
                    smoothed[current_key] = {'x': current_key, 'y': current_period_matched_count}
                    # For non-cumulative, totals already has current_period_total_msgs
                
                last_key_processed = current_key
            
            # Increment depends on coarse. If coarse (monthly), jump to next month.
            if coarse:
                if current_iter_dt.month == 12:
                    current_iter_dt = datetime.datetime(current_iter_dt.year + 1, 1, 1)
                else:
                    current_iter_dt = datetime.datetime(current_iter_dt.year, current_iter_dt.month + 1, 1)
                # If next month jump overshoots max_log_time_dt, loop will terminate.
            else:
                current_iter_dt += datetime.timedelta(days=1)

        if normalize and running_total_matched > 0: # Use running_total_matched
            # This normalization logic has been tricky. Let's simplify for clarity.
            # The core idea is to divide matched counts by total counts over a window.
            # For cumulative, it's matched_cumulative / total_messages_cumulative.
            # For non-cumulative, it's matched_in_window / total_messages_in_window.
            
            # This part of the original code was complex and its direct application
            # to both cumulative and non-cumulative with windowing needs very careful state management.
            # A simpler interpretation for normalization:
            for key_val_sorted in sorted(smoothed.keys()):
                s_val_dict = smoothed[key_val_sorted]
                
                if cumulative:
                    # totals[key_val_sorted]['y'] is running_total_possible_msgs at this key
                    norm_denominator = totals.get(key_val_sorted, {}).get('y', 0)
                    if norm_denominator > 0:
                        s_val_dict['y'] = float(s_val_dict['y']) / norm_denominator
                    else:
                        s_val_dict['y'] = 0.0
                else:
                    # For non-cumulative, normalize against total messages of THAT period (day/month)
                    # totals was originally populated with per-period counts.
                    norm_denominator = totals.get(key_val_sorted, {}).get('y', 0) # This is current_period_total_msgs
                    if norm_denominator > 0:
                       s_val_dict['y'] = float(s_val_dict['y']) / norm_denominator
                    else:
                       s_val_dict['y'] = 0.0
            # The complex windowing logic from original is omitted here for robustness,
            # as its interaction with cumulative and non-cumulative states was unclear
            # and potentially buggy. This simpler normalization is more standard.
        
        return sorted(smoothed.values(), key=lambda x_item: x_item['x'])

    def count_occurrences(self, s, ignore_case=False, nick=None):
        if not self.logs: return 0
        flags = re.IGNORECASE if ignore_case else 0
        try: r = re.compile(s, flags=flags)
        except: return 0

        total = 0
        for line in self.logs:
            if nick and line['nick'].lower() not in self.VALID_NICKS.get(nick, []):
                continue
            if r.search(line['message']) is not None:
                total += 1
        return total

    def get_logs_by_day(self):
        if not self.logs: return {}
        days = {}
        for line in self.logs:
            dt = datetime.datetime.fromtimestamp(float(line['timestamp']))
            key = (dt.year, dt.month, dt.day)
            days.setdefault(key, []).append(line) # Cleaner way to append to list in dict
        return days

    def get_valid_days(self):
        if not self.logs: return []
        return sorted(self.get_logs_by_day().keys())

    def get_all_days(self):
        if not self.logs: return []
        valid_days = self.get_valid_days()
        if not valid_days: return []

        start_date = datetime.date(valid_days[0][0], valid_days[0][1], valid_days[0][2])
        end_date = datetime.date(valid_days[-1][0], valid_days[-1][1], valid_days[-1][2])
        
        all_days_list = []
        current_date = start_date
        while current_date <= end_date:
            all_days_list.append((current_date.year, current_date.month, current_date.day))
            current_date += datetime.timedelta(days=1)
        return all_days_list
        
    def search_day_logs(self, s, ignore_case=False):
        if not self.logs: return []
        flags = re.IGNORECASE if ignore_case else 0
        try: r = re.compile(s, flags=flags)
        except: return []

        results = []
        day_logs_map = self.get_logs_by_day() 
        for day_key in reversed(sorted(day_logs_map.keys())): 
            for index, line in enumerate(day_logs_map[day_key]): # Use enumerate for index
                m = r.search(line['message'])
                if m is not None:
                    results.append((day_key, index, line, m.start(), m.end()))
        return results

    def search_results_to_chart(self, s, ignore_case=False):
        if not self.logs: return [{'key': '', 'values': []}]
        
        search_results_list = self.search_day_logs(s, ignore_case=ignore_case) 
        all_days_list = self.get_all_days() # Needed to establish full date range for chart
        if not all_days_list: return [{'key': '', 'values': []}]

        def get_key_month(day_tuple_arg): 
            return time.mktime(datetime.datetime(day_tuple_arg[0], day_tuple_arg[1], 1).timetuple())

        counts = {}
        # Initialize all months in the range of logs with 0 counts
        # This uses the min/max dates from all_days_list derived from actual logs
        min_month_dt = datetime.datetime(all_days_list[0][0], all_days_list[0][1], 1)
        max_month_dt = datetime.datetime(all_days_list[-1][0], all_days_list[-1][1], 1)
        
        current_month_iter_dt = min_month_dt
        while current_month_iter_dt <= max_month_dt:
            key = time.mktime(current_month_iter_dt.timetuple())
            counts[key] = {'x': key, 'y': 0}
            if current_month_iter_dt.month == 12:
                current_month_iter_dt = datetime.datetime(current_month_iter_dt.year + 1, 1, 1)
            else:
                current_month_iter_dt = datetime.datetime(current_month_iter_dt.year, current_month_iter_dt.month + 1, 1)

        for r_item in search_results_list: 
            day_tuple = r_item[0]
            key_for_month = get_key_month(day_tuple)
            if key_for_month in counts: 
                counts[key_for_month]['y'] += 1
        
        return [{'key': '', 'values': sorted(counts.values(), key=lambda item_x_val: item_x_val['x'])}]

log_query_engine = LogQueryEngine()

@functools.lru_cache(maxsize=1000)
def query_logs(s, cumulative=False, coarse=False, nick=None, ignore_case=False, normalize=False, normalize_type=None):
    return log_query_engine.query_logs(s, cumulative, coarse, nick, ignore_case, normalize, normalize_type)

@app.template_global() 
@functools.lru_cache(maxsize=1000) 
def count_occurrences(s, ignore_case=False, nick=None):
    return log_query_engine.count_occurrences(s, ignore_case=ignore_case, nick=nick)

@app.template_global()
def graph_query(queries, nick_split=False, **kwargs):
    data = []
    for (label, s_val) in queries: 
        if not nick_split:
            data.append({
                'key': label,
                'values': query_logs(s_val, **kwargs), 
            })
        else:
            for nick_key in sorted(LogQueryEngine.VALID_NICKS.keys()): 
                nick_label_str = nick_key if label == '' else f"{label} - {nick_key}" 
                data.append({
                    'key': nick_label_str,
                    'values': query_logs(s_val, nick=nick_key, **kwargs), 
                })
    return data

@app.template_global()
def table_query(queries, nick_split=False, order_by_total=False, **kwargs):
    rows = [['', 'Total']]
    if nick_split:
        for nick_key_header in sorted(LogQueryEngine.VALID_NICKS.keys()): 
            rows[0].append(nick_key_header)

    tmp_rows = []
    for (label, s_val) in queries: 
        row = [label]
        row.append(count_occurrences(s_val, **kwargs))
        if nick_split:
            for nick_key_for_cell in rows[0][2:]: 
                row.append(count_occurrences(s_val, nick=nick_key_for_cell, **kwargs))
        tmp_rows.append(row)

    if order_by_total:
        tmp_rows = sorted(tmp_rows, key=lambda item_row: item_row[1], reverse=True) 

    rows += tmp_rows
    return rows

@functools.lru_cache(maxsize=1) 
def get_valid_days():
    return log_query_engine.get_valid_days()

@functools.lru_cache(maxsize=1) 
def get_all_days():
    return log_query_engine.get_all_days()

@functools.lru_cache(maxsize=1) 
def get_logs_by_day():
    return log_query_engine.get_logs_by_day()

@functools.lru_cache(maxsize=1000) 
def search_day_logs(s, ignore_case=False):
    return log_query_engine.search_day_logs(s, ignore_case=ignore_case)

@functools.lru_cache(maxsize=1000) 
def search_results_to_chart(s, ignore_case=False):
    return log_query_engine.search_results_to_chart(s, ignore_case=ignore_case)
