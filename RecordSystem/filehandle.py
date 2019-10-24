"""
Here defines FileHandle

Data: 2019/10/24
"""
from FileSystem.filemanager import FileManager
from .record import Record
from .rid import RID


class FileHandle:
    """
    Class to handle a file as records
    """
    def __init__(self, manger, file_id):
        self._file_id = file_id
        self._manger = manger
        self._opened = True
        pass

    def get_record(self, rid: RID) -> Record:
        pass

    def insert_record(self, data: bytes) -> RID:
        pass

    def delete_record(self, rid: RID):
        pass

    def update_record(self, record: Record):
        pass

    def force_pages(self, pages=-1):
        pass

