from Pybase import settings
from .treenode import TreeNode
from .indexhandler import IndexHandler
import numpy as np
import math


class LeafNode(TreeNode):
    def __init__(self, page_id, parent_id, prev_id, next_id, child_keys, child_rids, handle:IndexHandler, keylen:int = 8) -> None:
        super(LeafNode, self).__init__()
        self._page_id = page_id
        self._parent_id = parent_id
        self._prev_id = prev_id
        self._next_id = next_id
        self._child_key = child_keys
        self._child_val = child_rids
        self._type = 1
        self._handle = handle
        self._keylen = keylen

    def insert(self, key, val):
        high = self.upper_bound(key)
        if high is None:
            high = 0
        self._child_key.insert(high, key)
        self._child_val.insert(high, val)

    def remove(self, key, val):
        low = self.lower_bound(key)
        high = self.upper_bound(key)
        pos = high
        # DEBUG: Maybe is high + 1
        for i in range(low, high):
            if self._child_val[pos] == val:
                pos = i
                break
        if pos == high:
            # Exception Here: Remove non-exists record
            return
        self._child_key.pop(pos)
        self._child_val.pop(pos)

    def page_size(self) -> int:
        return 32 + len(self._child_key) * (self._keylen + 16)

    def to_array(self) -> np.ndarray:
        arr = np.zeros(int(settings.PAGE_SIZE/4), np.int32)
        arr[0:5] = [1, self._parent_id, self._prev_id, self._next_id, len(self._child_key)]
        bytesize = math.ceil(self._keylen / 8)
        def transform_data(i:int) -> None:
            res = self._child_key[i]
            for j in range(bytesize):
                arr[5 + (bytesize + 2) * i + j] =  res & 0xFFFFFFFF
                res >>= 32
        for i in range(len(self._child_key)):
            transform_data(i)
            arr[5 + bytesize + (bytesize + 2) * i: 7 + bytesize + (bytesize + 2) * i] = [self._child_val[i].page_id, self._child_val[i].slot_id]
        arr.dtype = np.uint8
        assert arr.size == settings.PAGE_SIZE
        return arr

    def search(self, key):
        pos = self.lower_bound(key)
        if pos == len(self._child_key):
            return None
        if self._child_key[pos] == key:
            return self._child_val[pos]
        else:
            return None

    def range(self, low, high):
        pos_low = self.lower_bound(low)
        pos_high = self.upper_bound(high)
        return self._child_val[pos_low:pos_high]
