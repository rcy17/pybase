"""
Here defines FileHandle

Data: 2019/10/24
"""
from Pybase import settings
from .manager import FileManager
from .header_pb2 import HeaderInfo
from .record import Record
from .rid import RID


class FileHandle:
    """
    Class to handle a file as records
    """
    def __init__(self, manger: FileManager, file_id, filename):
        self._file_id = file_id
        self._manger = manger
        self._opened = True
        self._file_name = filename
        header_page = manger.get_page(file_id, 0)
        self._header = HeaderInfo.fromString(header_page)
        self._header_modified = False

    @property
    def filename(self):
        return self._file_name

    @property
    def header(self):
        return self._header

    @property
    def header_modified(self):
        return self._header_modified

    def modify_header(self):
        data = self._header.SerializeToString()
        data += b'\0' * (settings.PAGE_SIZE - len(data))
        self._manger.put_page(self._file_id, 0, data)

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

    def append_page(self):
        pass

    def set_header_page(self):
        pass

