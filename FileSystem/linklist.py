"""
Here defines LinkList

Data: 2019/10/21
"""
import numpy as np


class LinkList:
    def __init__(self, capacity, list_number):
        self._capacity = capacity
        self._list_number = list_number
        self._next = np.arange(capacity + list_number)
        self._last = np.arange(capacity + list_number)

    def _link(self, last_node, next_node):
        self._last[next_node] = last_node
        self._next[last_node] = next_node

    def remove(self, index):
        if self._last[index] == index:
            return
        self._link(self._last[index], self._next[index])
        self._last[index] = index
        self._next[index] = index

    def append(self, list_id, index):
        self.remove(index)
        head = list_id + self._capacity
        self._link(self._last[head], index)
        self._link(index, head)

    def insert_first(self, list_id, index):
        self.remove(index)
        head = list_id + self._capacity
        self._link(head, index)
        self._link(index, self._next[head])

    def get_first(self, list_id):
        return self._next[list_id + self._capacity]

    def get_next(self, index):
        return self._next[index]

    def is_head(self, index):
        return index >= self._capacity

    def is_alone(self, index):
        # this function won't be used in system
        return self._next[index] == index

