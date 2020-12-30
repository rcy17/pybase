"""
It shows a table like mysql

@Date: 2020/12/03
"""
from sys import stderr
from datetime import timedelta

from prettytable import PrettyTable

from .base import QueryResult, BasePrinter


class TablePrinter(BasePrinter):
    def _print(self, result: QueryResult, cost: timedelta):
        table = PrettyTable()
        table.field_names = result.headers
        table.add_rows(result.data)
        if len(result.data):
            print(table.get_string())
            print(f'{len(result.data)} results in {cost.total_seconds():.3f} seconds')
        else:
            print(f'Empty set in {cost.total_seconds():.3f} seconds')

    def _message_report(self, msg):
        print(msg, file=stderr)
