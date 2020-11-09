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
        self._type = 1

    def insert(self, key, val):
        high = self.upper_bound(key)
        if high == None:
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
        return 32 + len(self._child_key) * 24
    
    def to_array(self) -> np.ndarray:
        # DEBUG: Need to be optimized
        data = [0, self._parent_id, self._prev_id, self._next_id]
        for i in range(len(self._child_key)):
            data.append(self._child_key[i])
            data.append(self._child_val[i].page_id)
            data.append(self._child_val[i].slot_id)
        return np.array(data)
    
    def search(self, key):
        pos = self.lower_bound(key)
        if self._child_key[pos] == key:
            return self._child_val[pos]
        else:
            return None
    
    def range(self, low, high):
        pos_low = self.lower_bound(low)
        pos_high = self.upper_bound(high)
        for i in range(pos_low, pos_high):
            yield self._child_val[i]