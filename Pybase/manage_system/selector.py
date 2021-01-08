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
        # self.index = None   # Index of column

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
            'COUNT': lambda x: len(set(x)),
            'MAX': max,
            'MIN': min,
            'SUM': sum,
            'AVG': lambda x: sum(x) / len(x)
        }
        if self.type == SelectorType.Counter:
            return len(data)
        if self.type == SelectorType.Field:
            return data[0]
        if self.type == SelectorType.Aggregation:
            return function_map[self.aggregator](tuple(filter(lambda x: x is not None, data)))
