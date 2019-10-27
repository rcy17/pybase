"""
Here defines RecordManager

Data: 2019/10/24
"""
from FileSystem import FileManager
from .filehandle import FileHandle


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
        header_data = b''
        self._FM.write_page(file, 0, header_data)

    def destroy_file(self, filename):
        pass

    def open_file(self, filename):
        handle = FileHandle(self, self._FM.open_file(filename))
        return handle




