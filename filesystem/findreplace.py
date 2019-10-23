"""
Here defines FindReplace, we use LRU algorithm

Date: 2019/10/21
"""
from .linklist import LinkList


class FindReplace:
    def __init__(self, capacity):
        self._capacity = capacity
        self.list = LinkList(capacity, 1)
        for i in range(capacity):
            self.list.insert_first(0, i)

    def find(self):
        """
        Here we use LRU to get an index for new data
        :return: next usable index
        """
        index = self.list.get_first(0)
        self.list.remove(index)
        self.list.append(0, index)
        return index

    def free(self, index):
        """
        Here we free an index from buffer,
        so move it to the first to find
        :param index:
        :return:
        """
        self.list.insert_first(0, index)

    def access(self, index):
        """
        We mark index when we access it by move it to last
        :param index:
        :return:
        """
        self.list.append(0, index)



