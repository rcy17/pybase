"""
Here give some related formulas to calculate size for special request

Data: 2019/10/27
"""
from Pybase import settings
from Pybase.exceptions.record import RecordTooLongError


def get_record_capacity(record_length: int) -> int:
    """
    This function will consider page header
    :param record_length:
    :return: max records in a page
    """
    total_size = settings.PAGE_SIZE - settings.RECORD_PAGE_FIXED_HEADER_SIZE
    # x >> 3 + x * record_length <= total_size
    # x + x * record_length * 8 <= total_size * 8
    # x < total_size * 8 / (1 + record_length * 8)
    x = (total_size << 3) // (1 + (record_length << 8)) + 1
    if x >> 3 + x * record_length > total_size:
        x -= 1
    if x <= 0:
        raise RecordTooLongError(f'Record size {record_length} is TOO LONG')
    return x
