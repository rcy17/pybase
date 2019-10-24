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
        self.FM = FileManager()
        pass

    def create_file(self, record_size):
        pass

    def destroy_file(self, filename):
        pass

    def open_file(self, filename):
        handle = FileHandle(self, self.FM.open_file(filename))
        return handle




