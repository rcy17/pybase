"""
Here define Convert class to encode and decode record

Date: 2020/1/9
"""
import struct
from numbers import Number
from datetime import date

import numpy as np
from dateparser import parse as parse_date


from Pybase.exceptions.run_sql import DataBaseError
from Pybase import settings
from Pybase.record_system.record import Record


class Converter:
    @staticmethod
    def parse_date(value):
        try:
            return parse_date(value).date()
        except (TypeError, AttributeError):
            raise DataBaseError(f"Expect DATE but get {value} instead")

    @staticmethod
    def serialize(value_, type_: str):
        if type_ == "INT":
            if value_ is None:
                value_ = settings.NULL_VALUE
            elif not isinstance(value_, int):
                raise DataBaseError(f"Expect INT but get {value_} instead")
            return struct.pack('<q', value_)
        elif type_ == "FLOAT":
            if value_ is None:
                value_ = settings.NULL_VALUE
            elif not isinstance(value_, Number):
                raise DataBaseError(f"Expect FLOAT but get {value_} instead")
            return struct.pack('<d', value_)
        elif type_ == "DATE":
            if value_ is None:
                day = settings.NULL_VALUE
            else:
                day = parse_date(value_).toordinal()
            return struct.pack('<q', day)
        else:
            raise DataBaseError("Unsupported type.")

    @staticmethod
    def encode(size_list, type_list, total_size, value_list):
        if len(value_list) != len(size_list):
            raise DataBaseError(f'length of value ({len(value_list)}) != length of columns ({len(size_list)})')
        record_data = np.zeros(shape=total_size, dtype=np.uint8)
        pos = 0
        for size_, type_, value_ in zip(size_list, type_list, value_list):
            if type_ == "VARCHAR":
                if value_ is None:
                    length = 1
                    bytes_ = (1, )
                else:
                    if not isinstance(value_, str):
                        raise DataBaseError(f"Expect VARCHAR({size_ - 1}) but get {value_} instead")
                    bytes_ = (0,) + tuple(value_.encode())
                    length = len(bytes_)
                    if length > size_:
                        raise DataBaseError(f"String length {length} exceeds VARCHAR({size_ - 1})")
                record_data[pos: pos + length] = bytes_
                record_data[pos + length: pos + size_] = 0
            else:
                record_data[pos: pos + size_] = list(Converter.serialize(value_, type_))
            pos += size_
        assert pos == total_size
        return record_data

    @staticmethod
    def deserialize(data: np.ndarray, type_):
        if type_ == "VARCHAR":
            value = None if data[0] else data.tobytes()[1:].decode('utf-8')
        elif type_ == "INT":
            value = struct.unpack('<q', data)[0]
        elif type_ == "FLOAT":
            value = struct.unpack('<d', data)[0]
        elif type_ == "DATE":
            value = struct.unpack('<q', data)[0]
            if value > 0:
                value = date.fromordinal(value)
        else:
            raise DataBaseError("Unsupported type.")
        return None if value == settings.NULL_VALUE else value

    @staticmethod
    def decode(size_list, type_list, total_size, record: Record):
        data = record.data
        res = []
        pos = 0
        for size_, type_ in zip(size_list, type_list):
            res.append(Converter.deserialize(data[pos: pos + size_], type_))
            pos += size_
        assert pos == total_size
        return tuple(res)
