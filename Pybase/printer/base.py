"""
Here defines BasePrinter

@Date: 2020/12/03
"""

from ..manage_system.result import QueryResult


class BasePrinter(object):
    def _print(self, result: QueryResult):
        raise NotImplementedError

    def _error_report(self, msg):
        raise NotImplementedError

    def print(self, result: QueryResult):
        if result is None:
            return
        if result.error:
            self._error_report(result.error)
        else:
            self._print(result)
