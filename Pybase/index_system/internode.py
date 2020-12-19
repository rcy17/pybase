from Pybase import settings
from .treenode import TreeNode
from ..record_system.rid import RID
from .treenode import TreeNode
from .leafnode import LeafNode
from .indexhandler import IndexHandler
import numpy as np
import math

class InterNode(TreeNode):
    def __init__(self, page_id, parent_id, child_keys, child_nodes, handle:IndexHandler) -> None:
        super(InterNode, self).__init__()
        self._page_id = page_id
        self._parent_id = parent_id
        self._child_key = child_keys
        self._child_val = child_nodes
        self._type = 0
        self._handle = handle

    def insert(self, key, val):
        pos = self.lower_bound(key)
        if pos == len(self._child_key):
            pos -= 1
        if pos is None:
            self._child_key.append(key)
            # DEBUG: Get new Page Here
            new_page_id = self._handle.new_page()
            node = LeafNode(new_page_id, self._page_id, 0, 0, [], [], self._handle)
            self._child_val.append(node)
            node.insert(key, val)
        else:
            node: TreeNode = self._child_val[pos]
            node.insert(key, val)
            # Check need split or not
            if node.page_size() > settings.PAGE_SIZE:
                new_keys, new_vals, mid_val = node.split()
                tmp_val = self._child_key[pos]
                self._child_key[pos] = mid_val
                self._child_key.insert(pos + 1, tmp_val)
                # Allocate new Page
                new_page_id = self._handle.new_page()
                new_node = None
                if node._type == 0:
                    new_node = InterNode(new_page_id, self._page_id, new_keys, new_vals, self._handle)
                elif node._type == 1:
                    node._next_id = new_page_id
                    new_node = LeafNode(new_page_id, self._page_id, node._page_id, node._next_id, new_keys, new_vals, self._handle)
                assert (isinstance(new_node, TreeNode))
                self._child_val.insert(pos + 1, new_node)

    def remove(self, key, val):
        pos = self.lower_bound(key)
        node: TreeNode = self._child_val[pos]
        node.remove(key, val)
        # Check need merge of not
        if len(node._child_key) == 0:
            self._child_key.pop(pos)
            self._child_val.pop(pos)

    def page_size(self):
        return 16 + len(self._child_key) * (8 + 8)

    def to_array(self) -> np.ndarray:
        arr = np.zeros(int(settings.PAGE_SIZE/4), np.int32)
        arr[0:3] = [0, self._parent_id, len(self._child_key)]
        for i in range(len(self._child_key)):
            node: TreeNode = self._child_val[i]
            arr[3 + 2 * i: 5 + 2 * i] = [self._child_key[i], node._page_id]
        arr.dtype = np.uint8
        assert arr.size == settings.PAGE_SIZE
        return arr

    def search(self, key):
        pos = self.lower_bound(key)
        if pos == len(self._child_val):
            pos -= 1
        # DEBUG: Add Exception Here
        return self._child_val[pos].search(key)

    def range(self, low, high):
        pos_low = self.lower_bound(low)
        pos_high = self.upper_bound(high)
        records = []
        for i in range(pos_low, pos_high + 1):
            records += self._child_val[i].range(low, high)
        return records
