"""
Here defines RecordManager

Data: 2019/10/24
"""
from Pybase.file_system import FileManager
from .filehandle import FileHandle
from .header_pb2 import HeaderInfo


class RecordManager:
    """
    Class to manage records of database
    """
    def __init__(self):
        self._FM = FileManager()
        pass

    def create_file(self, filename, record_size):
        self._FM.create_file(filename)
        file = self._FM.open_file(filename)
        header = HeaderInfo()

        header_data = b''
        self._FM.write_page(file, 0, header_data)

    def destroy_file(self, filename):
        pass

    def open_file(self, filename):
        handle = FileHandle(self, self._FM.open_file(filename))
        return handle




