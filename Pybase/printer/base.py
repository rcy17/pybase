"""
Here defines BasePrinter

@Date: 2020/12/03
"""
from datetime import timedelta

from ..manage_system.result import QueryResult


class BasePrinter(object):
    def _print(self, result: QueryResult, cost: timedelta):
        raise NotImplementedError

    def _error_report(self, msg):
        raise NotImplementedError

    def print(self, result: QueryResult, cost: timedelta):
        if result is None:
            return
        if result.error:
            self._error_report(result.error)
        else:
            self._print(result, cost)
