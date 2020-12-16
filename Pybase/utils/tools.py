"""
Some simple tool function

Date: 2020/11/30
"""
from antlr4 import ParserRuleContext
import struct


def to_str(s):
    if isinstance(s, ParserRuleContext):
        s = s.getText()
    return str(s)


def to_int(s):
    return int(to_str(s))


def float2bytes(f):
    bs = struct.pack("d", f)
    return tuple(bs[i] for i in reversed(range(8)))


def bytes2float(b):
    ba = bytearray()
    for i in b:
        ba.append(i)
    return struct.unpack("!d", ba)[0]


def int2bytes(d):
    bs = struct.pack("q", d)
    return tuple(bs[i] for i in reversed(range(8)))


def bytes2int(b):
    ba = bytearray()
    for i in b:
        ba.append(i)
    return struct.unpack("!q", ba)[0]
