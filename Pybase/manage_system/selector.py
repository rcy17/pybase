"""
Here defines class Selector to define which column to select

Date: 2021/1/8
"""
from enum import IntEnum, auto


class SelectorType(IntEnum):
    All = auto()
    Field = auto()
    Aggregation = auto()
    Counter = auto()


class Selector:

    def __init__(self, type_, table_name=None, column_name=None, aggregator=None):
        self.type = type_
        self.table_name = table_name
        self.column_name = column_name
        self.aggregator = aggregator
        self.result = None

    def to_string(self, prefix=True):
        base = self.target()
        if self.type == SelectorType.Field:
            return base if prefix else self.column_name
        if self.type == SelectorType.Aggregation:
            return f'{self.aggregator}({base})' if prefix else f'{self.aggregator}({self.column_name})'
        if self.type == SelectorType.Counter:
            return f'COUNT(*)'

    def target(self):
        return f'{self.table_name}.{self.column_name}'

    def select(self, data: tuple):
        function_map = {
            'COUNT': len,
            'MAX': max,
            'MIN': min,
            'SUM': sum,
            'AVG': lambda x: sum(x) / len(x)
        }
        return function_map[self.aggregator](data)
