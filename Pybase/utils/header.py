"""
Here give some function for header page

Date: 2020/12/1
"""
from json import loads, dumps

import numpy as np

from Pybase import settings
from Pybase.exceptions.record import RecordTooLongError


def header_serialize(header: dict) -> np.ndarray:
    page = np.zeros(settings.PAGE_SIZE, dtype=np.uint8)
    data = dumps(header, ensure_ascii=False).encode('utf-8')
    page[:len(data)] = list(data)
    return page


def header_deserialize(page: np.ndarray) -> dict:
    return loads(page.tobytes().decode('utf-8').rstrip('\0'))


def get_record_capacity(record_length: int) -> int:
    """
    This function will calculate how many records can one page carry
    :param record_length:
    :return: max records in a page
    """
    total_size = settings.PAGE_SIZE - settings.RECORD_PAGE_FIXED_HEADER_SIZE
    # x >> 3 + x * record_length <= total_size
    # x + x * record_length * 8 <= total_size * 8
    # x < total_size * 8 / (1 + record_length * 8)
    x = (total_size << 3) // (1 + (record_length << 3)) + 1
    while (x + 7) >> 3 + x * record_length > total_size:
        x -= 1
    if x <= 0:
        raise RecordTooLongError(f'Record size {record_length} is TOO LONG')
    return x


def get_bitmap_length(record_per_page: int) -> int:
    """Calculate bitmap length"""
    return (record_per_page + 7) >> 3
