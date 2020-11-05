"""
Here defines Treenodes.

Date: 2020/11/05
"""

from typing import Tuple
from ..record_system.rid import RID

class TreeNode:
    def __init__(self) -> None:
        self._parent = None
        self._child_key: list = []
        self._child_rid: list = []
        self._clen:int = 0
    
    def lower_bound(self, key):
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
        low, high = 0, len(self._child_key) - 1
        pos = len(self._child_key)
        while low < high:
            mid = int((low + high) / 2)
            if self._child_key[mid] <= key:
                low = mid + 1
            else:
                high = mid
                pos = high
        if self._child_key[low] >= key:
            pos = low
        return pos
    
    def lower_rid(self, key) -> RID:
        return self._child_rid[self.lower_bound(key)]

    def upper_rid(self, key) -> RID:
        return self._child_rid[self.upper_bound(key)]

    def insert(self, key, rid:RID):
        high = self.upper_bound(key)
        self._child_key.insert(high - 1, key)
        self._child_rid.insert(high - 1, rid)
        self._clen += 1

    def remove(self, key, rid:RID):
        low = self.lower_bound(key)
        high = self.upper_bound(key)
        pos = high
        # DEBUG: Maybe is high + 1
        for i in range(low, high):
            if self._child_rid[pos] == rid:
                pos = i
                break
        if pos == high:
            # Exception Here: Remove non-exists record
            return
        self._child_key.pop(pos)
        self._child_rid.pop(pos)
        self._clen -= 1
    
    def load(self, parent, child_key, child_rid):
        self._parent = parent
        self._child_key = child_key
        self._child_rid = child_rid
    
    def split(self) -> tuple(list, list):
        mid = int((len(self._child_key) + 1) / 2)
        half_key = self._child_key[mid:]
        half_rid = self._child_rid[mid:]
        self._child_key = self._child_key[:mid]
        self._child_rid = self._child_rid[:mid]
        self._clen = mid
        return (half_key, half_rid)
    
    def child_rids(self) -> list:
        return self._child_rid 
    
    def page_size(self) -> int:
        return 8 + self._clen * 24