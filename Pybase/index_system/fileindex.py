"""
Here defines FileIndex

Date: 2020/11/05
"""
import numpy as np

from Pybase import settings
from ..record_system.filehandle import FileHandle
from ..record_system.record import Record, Condition
from ..record_system.rid import RID
from .treenode import TreeNode
from .leafnode import LeafNode
from .internode import InterNode
from queue import Queue

class FileIndex:
    def __init__(self, handle: FileHandle, root_id) -> None:
        self._root_id = root_id
        self._handle = handle
        self._root = InterNode(root_id, root_id, [], [])

    def buildNode(self, page_id) -> TreeNode:
        data = self._handle.get_page(page_id)
        parent_id = data[1]
        node = None
        if data[0] == 1:
            prev_id = data[2]
            next_id = data[3]
            child_keys = [data[i] for i in range(int((len(data) - 4) / 3))]
            child_rids = [RID(data[i + 1], data[i + 2]) for i in range(int((len(data) - 4) / 3))]
            assert(len(child_keys) == len(child_rids))
            node = LeafNode(page_id, parent_id, prev_id, next_id, child_keys, child_rids)
        else:
            child_keys = [data[i] for i in range(int((len(data) - 2) / 2))]
            child_nodes = [self.buildNode(data[i + 1]) for i in range(int((len(data) - 2) / 2))]
            assert(len(child_keys) == len(child_nodes))
            node = InterNode(page_id, parent_id, child_keys, child_nodes)
        return node

    def load(self):
        '''
        Tree Node Page Structure:
        Node Type = 0 is InterNode
        InterNode:
        | Node Type | Parent PID | Child Key 1 | Child PID 1 | ... |

        Node Type = 1 is LeafNode
        | Node Type | Parent PID | Prev PID | Next PID | Key 1 | RID 1 | ... |
        '''
        data = self._handle.get_page(self._root_id)
        node_type = data[0]
        parent_id = data[1]
        clen = data[2]
        assert(node_type == 0)
        assert(parent_id == self._root_id)
        assert(len(data) == 2*clen + 3)
        self._root = self.buildNode(self._root_id)


    def build(self, keyList:list, ridList:list):
        assert(len(keyList) == len(ridList))
        for i in range(len(keyList)):
            self.insert(keyList[i], ridList[i])
        

    def dump(self):
        q = [self._root]
        while len(q) > 0:
            node = q.pop(0)
            page_id = node.page_id()
            if isinstance(node, InterNode):
                for i in node.child_vals():
                    q.append(i)
            # self._handle.put_page(page_id, node.to_array())
            print(page_id, node.to_array())
        

    def insert(self, key, rid:RID):
        self._root.insert(key, rid)
        # Check if root need change
        if self._root.page_size() > settings.PAGE_SIZE:
            # split
            
            pass

    def remove(self, key, rid:RID):
        self._root.remove(key, rid)
    
    def search(self, key):
        return self._root.search(key)

