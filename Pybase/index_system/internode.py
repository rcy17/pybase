from .treenode import TreeNode
from ..record_system.rid import RID
from .treenode import TreeNode
import numpy as np

class InterNode(TreeNode):
    def __init__(self, page_id, parent_id ,child_keys, child_nodes) -> None:
        self._page_id = page_id
        self._parent_id = parent_id
        self._child_key = child_keys
        self._child_val = child_nodes
    
    def lower_rid(self, key) -> RID:
        # DEBUG:Maybe is upper_bound
        pos = self.lower_bound(key)
        node: TreeNode = self._child_val[pos]
        return node.lower_rid(key)
    
    def upper_rid(self, key) -> RID:
        # DEBUG:Maybe is lower_bound
        pos = self.upper_bound(key)
        node: TreeNode = self._child_val[pos]
        return node.upper_bound(key)
    
    def insert(self, key, val):
        pos = self.lower_bound(key)
        node: TreeNode = self._child_val[pos]
        node.insert(key, val)
        # Check need split or not
    
    def remove(self, key, val):
        pos = self.lower_bound(key)
        node: TreeNode = self._child_val[pos]
        node.remove(key, val)
        # Check need merge of not

    def page_size(self):
        return 16 + self._clen * 16
    
    def to_array(self) -> np.ndarray:
        data = [0, self._parent_id]
        for i in range(len(self._child_key)):
            data.append(self._child_key[i])
            data.append(self._child_val[i].page_id())
        return np.array(data)
    
    
    