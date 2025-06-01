#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from web import app # Needed for @app.template_global()

# Import engine classes from their respective modules within this package
from .abstract_engine import AbstractLogQueryEngine
from .sqlite_engine import SQLiteLogQueryEngine
from .inmemory_engine import InMemoryLogQueryEngine
from .constants import VALID_NICKS # Import VALID_NICKS from constants.py

# Global instance for the application, managed within this package
_log_engine = None

def log_engine():
    global _log_engine
    if _log_engine is None:
        # Use SQLiteLogQueryEngine imported from .sqlite_engine
        _log_engine = SQLiteLogQueryEngine(db=':memory:', batch_size=10)
    return _log_engine

# Functions exposed as template globals, using the log_engine instance
@app.template_global()
def graph_query(queries, nick_split=False, **kwargs):
    return log_engine().query_logs(queries, nick_split=nick_split, **kwargs)

@app.template_global()
def table_query(queries, **kwargs):
    return log_engine().count_occurrences(queries, **kwargs)

__all__ = [
    'AbstractLogQueryEngine',
    'SQLiteLogQueryEngine',
    'InMemoryLogQueryEngine',
    'VALID_NICKS',
    'log_engine',
    'graph_query',
    'table_query',
]
