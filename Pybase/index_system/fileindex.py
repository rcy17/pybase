"""
Here defines FileIndex

Date: 2020/11/05
"""
import numpy as np

from Pybase import settings
from ..record_system.filehandle import FileHandle
from ..record_system.record import Record, Condition
from ..record_system.rid import RID

class FileIndex:
    def __init__(self, handle: FileHandle, root_id) -> None:
        self._root_id = root_id
        self._handle = handle

    def load(self):
        pass

    def build(self, keyList, ridList):
        pass

    def dump(self):
        pass

    def insert(self, key, rid:RID):
        pass

    def remove(self, key, rid:RID):
        pass

