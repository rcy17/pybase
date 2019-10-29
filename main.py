"""
This is entry file for this project

Data: 2019/10/21
"""
import numpy as np
from Pybase.record_system.manager import RecordManager
from Pybase.record_system.rid import RID
from Pybase.record_system.record import Condition
from Pybase.record_system.filescan import FileScan


def main():
    # now just do some test
    manager = RecordManager()
    # manager.create_file('1.db', 32)
    file = manager.open_file('1.db')
    data = np.random.randint(0,  255, 32, np.uint8)
    # print(file.insert_record(data))
    scanner = FileScan(file)
    for record in scanner:
        print(record.data.tobytes().hex())
    manager.close_file(file)


if __name__ == '__main__':
    main()
