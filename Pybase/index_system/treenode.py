"""
Here defines Treenodes.

Date: 2020/11/05
"""

from Pybase.record_system.filehandle import FileHandle
from abc import abstractmethod
from ..record_system.rid import RID
import numpy as np


class TreeNode:
    def __init__(self):
        self._page_id = None
        self._parent_id = None
        self._child_key: list = []
        self._child_val: list = []
        self._type = -1
        self._handle: FileHandle = None

    def lower_bound(self, key):
        if len(self._child_key) == 0:
            return None
        low, high = 0, len(self._child_key) - 1
        pos = len(self._child_key)
        while low < high:
            mid = int((low + high) / 2)
            if self._child_key[mid] < key:
                low = mid + 1
            else:
                high = mid
        if self._child_key[low] >= key:
            pos = low
        return pos

    def upper_bound(self, key):
        if len(self._child_key) == 0:
            return None
        low, high = 0, len(self._child_key) - 1
        pos = len(self._child_key)
        while low < high:
            mid = int((low + high) / 2)
            if self._child_key[mid] <= key:
                low = mid + 1
            else:
                high = mid
                pos = high
        if self._child_key[low] > key:
            pos = low
        return pos

    def split(self):
        mid = int((len(self._child_key) + 1) / 2)
        half_key = self._child_key[mid:]
        half_rid = self._child_val[mid:]
        self._child_key = self._child_key[:mid]
        self._child_val = self._child_val[:mid]
        return half_key, half_rid, self._child_key[mid - 1]

    def child_values(self) -> list:
        return self._child_val

    def page_id(self):
        return self._page_id

    @abstractmethod
    def page_size(self) -> int:
        return 0

    @abstractmethod
    def insert(self, key, val):
        pass

    @abstractmethod
    def remove(self, key, val):
        pass

    @abstractmethod
    def to_array(self) -> np.ndarray:
        pass

    @abstractmethod
    def search(self, key):
        pass

    @abstractmethod
    def range(self, low, high):
        pass
