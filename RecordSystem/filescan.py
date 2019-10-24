"""
Here defines FileScan

Data: 2019/10/24
"""
from .record import Record


class FileScan:
    """
    Class to define method of scan the file
    """
    def __init__(self):
        pass

    def get_next_record(self) -> Record:
        pass

    def close_scan(self):
        pass
