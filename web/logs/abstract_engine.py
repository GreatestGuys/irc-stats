#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import abc

class AbstractLogQueryEngine(abc.ABC):
    def __init__(self, log_file_path=None, log_data=None):
        # This constructor can be used by subclasses to store common initial parameters
        # For now, it doesn't do much, but subclasses should call it via super().
        self.log_file_path = log_file_path
        self.log_data = log_data

    @abc.abstractmethod
    def clear_all_caches(self):
        pass

    @abc.abstractmethod
    def query_logs(self, queries, nick_split=False, cumulative=False, coarse=False, ignore_case=False, normalize=False, normalize_type=None): # MODIFIED
        pass

    @abc.abstractmethod
    def count_occurrences(self, queries, ignore_case=False, nick_split=False, order_by_total=False):
        pass

    @abc.abstractmethod
    def get_valid_days(self):
        pass

    @abc.abstractmethod
    def get_logs_by_day(self, year, month, day):
        pass

    @abc.abstractmethod
    def search_day_logs(self, s, ignore_case=False):
        pass

    @abc.abstractmethod
    def search_results_to_chart(self, s, ignore_case=False):
        pass

    @abc.abstractmethod
    def get_trending(self, top=10, min_freq=10, lookback_days=7):
        pass
