from Pybase.record_system.rid import RID
from Pybase.file_system import FileManager
from Pybase.index_system.indexhandler import IndexHandler
from Pybase.index_system.fileindex import FileIndex


def insert_test(handler, page_id):
    print("Init Test...")
    indexer = FileIndex(handler, page_id)
    for i in range(4096):
        indexer.insert(i, RID(0, i))
    for i in range(4090, 4100):
        res = indexer.search(i)
        print("Search %d:" % i, None if res == None else res.slot_id)
    print("Range [100, 110]:")
    for i in indexer.range(100, 110):
        print(i)
    indexer.dump()
    print("Test End")


def load_test(handler, page_id):
    print("Load Test...")
    indexer = FileIndex(handler, page_id)
    indexer.load()
    for i in range(4090, 4100):
        res = indexer.search(i)
        print("Search %d:" % i, None if res == None else res.slot_id)
    print("Test End")


def test1():
    # now just do some test
    manager = FileManager()
    handler = IndexHandler(manager)
    print("Test start.")
    page_id = handler.new_page()
    print(page_id)
    insert_test(handler, page_id)
    load_test(handler, page_id)

def test2():
    manager = FileManager()
    handler = IndexHandler(manager)
    page_id = 0
    load_test(handler, page_id)


if __name__ == "__main__":
    test2()
