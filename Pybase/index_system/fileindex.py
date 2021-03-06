"""
Here defines FileIndex

Date: 2020/11/05
"""
import numpy as np

from Pybase import settings
from .indexhandler import IndexHandler
from ..record_system.rid import RID
from .treenode import TreeNode
from .leafnode import LeafNode
from .internode import InterNode


class FileIndex:
    def __init__(self, handle: IndexHandler, root_id) -> None:
        self._root_id = root_id
        self._handle = handle
        self._root = InterNode(root_id, root_id, [], [], self._handle)
        self._is_modified = False

    @property
    def root_id(self):
        return self._root_id

    @property
    def is_modified(self):
        return self._is_modified

    @property
    def handler(self):
        return self._handle

    def build_node(self, page_id) -> TreeNode:
        self._is_modified = True
        data = self._handle.get_page(page_id)
        data.dtype = np.int64
        node = None
        parent_id = data[1]
        if data[0] == 1:
            prev_id = data[2]
            next_id = data[3]
            child_len = data[4]
            child_keys = [data[3 * i + 5] for i in range(child_len)]
            child_rids = [RID(int(data[3 * i + 6]), int(data[3 * i + 7])) for i in range(child_len)]
            assert len(child_keys) == len(child_rids)
            node = LeafNode(page_id, parent_id, prev_id, next_id, child_keys, child_rids, self._handle)
        elif data[0] == 0:
            child_len = data[2]
            child_keys = [data[2 * i + 3] for i in range(child_len)]
            child_nodes = [self.build_node(data[2 * i + 4]) for i in range(child_len)]
            assert len(child_keys) == len(child_nodes)
            node = InterNode(page_id, parent_id, child_keys, child_nodes, self._handle)
        return node

    def load(self):
        """
        Tree Node Page Structure:
        Node Type = 0 is InterNode
        InterNode:
        | Node Type | Parent PID | Child Key 1 | Child PID 1 | ... |

        Node Type = 1 is LeafNode
        | Node Type | Parent PID | Prev PID | Next PID | Key 1 | RID 1 | ... |
        """
        data = self._handle.get_page(self._root_id)
        data.dtype = np.int64
        node_type = data[0]
        parent_id = data[1]
        assert (node_type == 0)
        assert (parent_id == self._root_id)
        self._root = self.build_node(self._root_id)

    def build(self, key_list: list, rid_list: list):
        self._is_modified = True
        assert (len(key_list) == len(rid_list))
        for i in range(len(key_list)):
            self.insert(key_list[i], rid_list[i])

    def dump(self):
        q = [self._root]
        while len(q) > 0:
            node = q.pop(0)
            page_id = node.page_id()
            if isinstance(node, InterNode):
                for i in node.child_values():
                    q.append(i)
            self._handle.put_page(page_id, node.to_array())

    def insert(self, key, rid: RID):
        self._is_modified = True
        self._root.insert(key, rid)
        # Check if root need change
        if self._root.page_size() > settings.PAGE_SIZE:
            new_root_id = self._handle.new_page()
            new_root = InterNode(new_root_id, new_root_id, [], [], self._handle)
            self._root._parent_id = new_root_id
            max_key = self._root._child_key[len(self._root._child_key) - 1]
            new_keys, new_values, mid_key = self._root.split()
            old_node = self._root
            # DEBUG:
            new_page_id = self._handle.new_page()
            new_node = InterNode(new_page_id, new_root_id, new_keys, new_values, self._handle)
            self._root = new_root
            self._root_id = new_root_id
            self._root._child_key = [mid_key, max_key]
            self._root._child_val = [old_node, new_node]

    def remove(self, key, rid: RID):
        self._is_modified = True
        self._root.remove(key, rid)

    def search(self, key):
        return self._root.search(key)

    def range(self, low, high):
        return self._root.range(low, high)
