"""
This is entry file for this project

Data: 2019/10/21
"""
from Pybase.record_system.manager import RecordManager
from Pybase.record_system.rid import RID
import numpy as np


def main():
    # now just do some test
    manager = RecordManager()
    manager.create_file('1.db', 20)
    file = manager.open_file('1.db')
    data = np.full(20, 127, np.uint8)
    file.insert_record(data)
    print(file.get_record(RID(1, 1)))


if __name__ == '__main__':
    main()
