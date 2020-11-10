"""
This is entry file for this project

Data: 2019/10/21
"""
import numpy as np
from Pybase.record_system.manager import RecordManager
from Pybase.record_system.rid import RID
from Pybase.record_system.record import Condition
from Pybase.record_system.filescan import FileScan
from Pybase.index_system.fileindex import FileIndex
from Pybase.index_system.leafnode import LeafNode
from random import randint

def insert_test(file):
    indexer = FileIndex(file, 1)
    # for i in range(5):
    indexer.insert(5, RID(0, 5))
    indexer.insert(1, RID(0, 1))
    indexer.insert(2, RID(0, 2))
    indexer.insert(4, RID(0, 4))
    indexer.insert(3, RID(0, 3))
    for i in indexer._root.child_values()[0].child_values():
        print(i)
    for i in range(7):
        print("Search %d:"%i, indexer._root.search(i))
    # print(indexer._root.search(0))
    indexer.dump()


def load_test(file):
    indexer = FileIndex(file, 1)
    indexer.load()
    for i in indexer._root.child_values()[0].child_values():
        print(i)
    for i in range(7):
        print("Search %d:"%i, indexer._root.search(i))


def main():
    # now just do some test
    manager = RecordManager()
    manager.create_file('1.db', 32)
    file = manager.open_file('1.db')
    # insert_test(file)
    load_test(file)
    manager.close_file(file)


if __name__ == '__main__':
    main()
