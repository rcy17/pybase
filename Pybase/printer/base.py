"""
Here defines BasePrinter

@Date: 2020/12/03
"""
from typing import List
from datetime import timedelta

from ..manage_system.result import QueryResult


class BasePrinter(object):
    def __init__(self):
        self.using_db = None

    def _print(self, result: QueryResult):
        raise NotImplementedError

    def _message_report(self, msg):
        raise NotImplementedError

    def _database_changed(self):
        pass

    def print(self, results: List[QueryResult]):
        for result in results:
            if result is None:
                return
            elif result.database:
                self.using_db = result.database
                self._database_changed()
            elif result.message:
                self._message_report(result.message)
            else:
                self._print(result)
