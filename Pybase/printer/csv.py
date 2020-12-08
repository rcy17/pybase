"""
It generates csv output to get a .csv file easily

@Date: 2020/12/03
"""
import csv
from sys import stdout

from .base import QueryResult, BasePrinter


class CSVPrinter(BasePrinter):
    def _print(self, result: QueryResult):
        writer = csv.writer(stdout)
        writer.writerow(result.headers)
        writer.writerows(result.data)
