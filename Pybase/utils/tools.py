"""
Some simple tool function

Date: 2020/11/30
"""
from antlr4 import ParserRuleContext


def to_str(s):
    if isinstance(s, ParserRuleContext):
        s = s.getText()
    return str(s)


def to_int(s):
    return int(to_str(s))
