"""
Here defines FileScan

Data: 2019/10/24
"""
import numpy as np

from Pybase import settings
from .filehandle import FileHandle
from .record import Condition, Record
from .rid import RID


class FileScan:
    """
    Class to define method of scan the file
    """
    def __init__(self, handle: FileHandle,  condition: Condition):
        self._condition = condition
        self._handle = handle
        pass

    def records(self) -> Record:
        """
        Now we can use for-loop to finish scan, for example:
        ```
        scanner = FileScan(handle, condition)
        for record in scanner.records():
            pass    # do something with record
        ```
        or like this:
        ```
        records = list(FileScan(handle, condition).records())
        ```
        """
        for page_id in range(1, self._handle.header.page_number):
            page = self._handle.get_page(page_id)
            if page[settings.PAGE_FLAG_OFFSET] != settings.RECORD_PAGE_FLAG:
                continue
            bitmap = self._handle.get_bitmap(page)
            for slot_id in np.where(bitmap == 0):
                record = self._handle.get_record(RID(page_id, slot_id), page)
                if self._condition.is_satisfied(record):
                    yield record
