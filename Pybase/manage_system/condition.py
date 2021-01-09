"""
Here defines Condition class

Date: 2021/1/8
"""
from enum import IntEnum, auto


class ConditionType(IntEnum):
    Compare = auto()
    In = auto()
    Like = auto()
    Null = auto()


class Condition:
    def __init__(self, type_, table_name, column_name, operator=None,
                 target_table=None, target_column=None, value=None):
        self.type: ConditionType = type_
        self.table_name: str = table_name
        self.column_name: str = column_name
        self.operator: str = operator
        self.target_table: str = target_table
        self.target_column: str = target_column
        self.value = value
