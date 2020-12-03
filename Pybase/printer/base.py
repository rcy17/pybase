"""
Here defines BasePrinter

@Date: 2020/12/03
"""

from ..manage_system.result import QueryResult


class BasePrinter(object):
    def _print(self, result: QueryResult):
        raise NotImplementedError

    def print(self, result: QueryResult):
        if result is None:
            return
        self._print(result)
