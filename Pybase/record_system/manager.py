"""
Here defines RecordManager

Data: 2019/10/24
"""
from Pybase.file_system import FileManager
from Pybase import settings
from Pybase.utils.formula import get_record_capacity
from Pybase.exceptions.record import PageOverflowError, RecordFileReopenError
from .filehandle import FileHandle
from .header_pb2 import HeaderInfo


class RecordManager:
    """
    Class to manage records of database
    """
    def __init__(self):
        self._FM = FileManager()
        self.opened_files = set()

    def create_file(self, filename, record_size):
        # create file
        self._FM.create_file(filename)

        # open the file to add header page
        file = self._FM.open_file(filename)
        header = HeaderInfo()
        header.record_per_page = get_record_capacity(record_size)
        header.page_number = 1
        header.record_number = 0
        header.first_insert_page = 0
        if header.ByteSize() > settings.PAGE_SIZE:
            raise PageOverflowError(f'Header page with {header.ByteSize()} bytes is overflow')
        header_data = header.SerializeToString()
        self._FM.write_page(file, 0, header_data)

        # close the file
        self._FM.close_file(file)

    def remove_file(self, filename):
        self._FM.remove_file(filename)

    def open_file(self, filename):
        if filename in self.opened_files:
            raise RecordFileReopenError(f'File {filename} is already opened')
        file = self._FM.open_file(filename)
        handle = FileHandle(self._FM, file, filename)
        self.opened_files.add(filename)
        return handle

    def close_file(self, handle: FileHandle):
        if handle.header_modified:
            handle.modify_header()
        self.opened_files.remove(handle.filename)




