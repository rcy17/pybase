"""
Here provide hash and reverse function for special id

Data: 2019/10/21
"""
from Pybase import settings


def pack_file_page_id(file_id, page_id):
    return file_id | (page_id << settings.FILE_ID_BITS)


def unpack_file_page_id(pair_id):
    """return (file_id, page_id)"""
    return pair_id & ((1 << settings.FILE_ID_BITS) - 1), pair_id >> settings.FILE_ID_BITS
