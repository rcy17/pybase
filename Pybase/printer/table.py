"""
It shows a table like mysql

@Date: 2020/12/03
"""
from sys import stderr

from prettytable import PrettyTable

from .base import QueryResult, BasePrinter


class TablePrinter(BasePrinter):
    class MyPrettyTable(PrettyTable):
        def _format_value(self, field, value):
            return 'NULL' if value is None else super()._format_value(field, value)

    def _print(self, result: QueryResult):
        table = self.MyPrettyTable()
        table.field_names = result.headers
        table.add_rows(result.data)
        cost = result.cost
        if len(result.data):
            print(table.get_string())
            print(f'{len(result.data)} results in {cost.total_seconds():.3f} seconds')
        else:
            print(f'Empty set in {cost.total_seconds():.3f} seconds')
        print()

    def _message_report(self, msg):
        print(msg, file=stderr)

    def _database_changed(self):
        print('Database change to', self.using_db)
        print()
