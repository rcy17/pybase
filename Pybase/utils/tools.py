"""
Some simple tool function

Date: 2020/11/30
"""
from antlr4 import ParserRuleContext
import re


def to_str(s):
    if isinstance(s, ParserRuleContext):
        s = s.getText()
    return str(s)


def to_int(s):
    return int(to_str(s))


def to_float(s):
    return float(to_str(s))


def compare_to_value(index, op, value):
    if op == '=':
        return lambda x: x[index] == value
    if op == '<':
        return lambda x: x is not None and x[index] < value
    if op == '<=':
        return lambda x: x is not None and x[index] <= value
    if op == '>':
        return lambda x: x is not None and x[index] > value
    if op == '>=':
        return lambda x: x is not None and x[index] >= value
    if op == '<>':
        return lambda x: x[index] != value


def compare_to_attr(index, op, other_index):
    if op == '=':
        return lambda x: x[index] == x[other_index]
    if op == '<':
        return lambda x: x[index] < x[other_index]
    if op == '<=':
        return lambda x: x[index] <= x[other_index]
    if op == '>':
        return lambda x: x[index] > x[other_index]
    if op == '>=':
        return lambda x: x[index] >= x[other_index]
    if op == '<>':
        return lambda x: x[index] != x[other_index]


def in_value_list(index, values):
    return lambda x: x[index] in values


def build_regex_from_sql_like(pattern: str) -> re.Pattern:
    pattern = pattern.replace('%%', '\r').replace('%?', '\n').replace('%_', '\0')
    pattern = re.escape(pattern)
    pattern = pattern.replace('%', '.*').replace(r'\?', '.').replace('_', '.')
    pattern = pattern.replace('\r', '%').replace('\n', r'\?').replace('\0', '_')
    return re.compile('^' + pattern + '$')


def like_pattern(index, pattern):
    pattern = build_regex_from_sql_like(pattern)
    return lambda x: pattern.match(str(x[index]))


def null_check(index, is_null):
    if is_null:
        return lambda x: x[index] is None
    else:
        return lambda x: x[index] is not None
