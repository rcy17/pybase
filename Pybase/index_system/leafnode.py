from .treenode import TreeNode
from ..record_system.rid import RID
import numpy as np

class LeafNode(TreeNode):
    def __init__(self, page_id, parent_id, prev_id, next_id, child_keys, child_rids) -> None:
        self._page_id = page_id
        self._parent_id = parent_id
        self._prev_id = prev_id
        self._next_id = next_id
        self._child_key = child_keys
        self._child_val = child_rids

    def lower_rid(self, key) -> RID:
        return self._child_val[self.lower_bound(key)]

    def upper_rid(self, key) -> RID:
        return self._child_val[self.upper_bound(key)]

    def insert(self, key, val):
        high = self.upper_bound(key)
        self._child_key.insert(high - 1, key)
        self._child_val.insert(high - 1, val)
        self._clen += 1

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
        self._clen -= 1
    
    def page_size(self) -> int:
        return 32 + self._clen * 24
    
    def to_array(self) -> np.ndarray:
        # DEBUG: Need to be optimized
        data = [0, self._parent_id, self._prev_id, self._next_id]
        for i in range(len(self._child_key)):
            data.append(self._child_key[i])
            data.append(self._child_val[i].page_id())
            data.append(self._child_val[i].slot_id())
        return np.array(data)