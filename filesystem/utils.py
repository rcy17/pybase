"""
Utils for file system

Data: 2019/10/21
"""
from . import param


def pack_file_page_id(file_id, page_id):
    return file_id | (page_id << param.FILE_ID_BITS)


def unpack_file_page_id(pair_id):
    return pair_id & (1 << param.FILE_ID_BITS) - 1, pair_id >> param.FILE_ID_BITS
