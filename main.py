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

def main():
    # now just do some test
    manager = RecordManager()
    manager.create_file('1.db', 32)
    file = manager.open_file('1.db')
    indexer = FileIndex(file, 0)
    for i in range(5):
        indexer.insert(i, RID(0, i))
    for i in indexer._root.child_vals()[0].child_vals():
        print(i)
    val = indexer._root.search(2)
    print(val)
    indexer.dump()
    manager.close_file(file)


if __name__ == '__main__':
    main()
