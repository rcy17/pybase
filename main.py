"""
This is entry file for this project

Data: 2019/10/21
"""
import numpy as np
from numpy.lib.function_base import select
from Pybase.record_system.manager import RecordManager
from Pybase.record_system.rid import RID
from Pybase.record_system.record import Condition
from Pybase.record_system.filescan import FileScan
from Pybase.index_system.fileindex import FileIndex
from Pybase.index_system.leafnode import LeafNode
from random import randint

def insert_test(file, page_id):
    print("Init Test...")
    indexer = FileIndex(file, page_id)
    for i in range(4096):
        indexer.insert(i, RID(0, i))
    for i in range(4090, 4100):
        res = indexer.search(i)
        print("Search %d:"%i, None if res == None else res.slot_id )
    print("Range [100, 110]:")
    for i in indexer.range(100, 110):
        print(i)
    indexer.dump()
    print("Test End")


def load_test(file, page_id):
    print("Load Test...")
    indexer = FileIndex(file, page_id)
    indexer.load()
    for i in range(4090, 4100):
        res = indexer.search(i)
        print("Search %d:"%i, None if res == None else res.slot_id )
    print("Test End")


def main():
    # now just do some test
    manager = RecordManager()
    manager.create_file('1.db', 32)
    file = manager.open_file('1.db')
    print()
    page_id = file.new_page()
    insert_test(file, page_id)
    load_test(file, page_id)
    manager.close_file(file)

    file = manager.open_file('1.db')
    load_test(file, page_id)


if __name__ == '__main__':
    main()
