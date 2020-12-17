"""
It shows a table like mysql

@Date: 2020/12/03
"""
from sys import stderr

from prettytable import PrettyTable

from .base import QueryResult, BasePrinter


class TablePrinter(BasePrinter):
    def _print(self, result: QueryResult):
        table = PrettyTable()
        table.field_names = result.headers
        table.add_rows(result.data)
        print(table.get_string())

    def _error_report(self, msg):
        print(msg, file=stderr)
