"""
Here defines RecordManager

Data: 2019/10/24
"""
from Pybase.file_system import FileManager
from Pybase.utils.formula import get_record_capacity
from Pybase.utils.header import header_serialize
from Pybase.exceptions.record import RecordFileOperationError
from .filehandle import FileHandle
from Pybase.utils.header_pb2 import HeaderInfo


class RecordManager:
    """
    Class to manage records of database
    """
    def __init__(self):
        self._FM = FileManager()
        self.opened_files = {}

    @property
    def file_manager(self) -> FileManager:
        return self._FM

    def create_file(self, filename, record_length, append=False):
        # create file
        if append:
            self._FM.touch_file(filename)
        else:
            self._FM.create_file(filename)

        # open the file to add header page
        file = self._FM.open_file(filename)
        header = HeaderInfo()
        header.record_length = record_length
        header.record_per_page = get_record_capacity(record_length)
        header.page_number = 1
        header.record_number = 0
        header.next_vacancy_page = 0
        header.filename = filename
        self._FM.new_page(file, header_serialize(header))

        # close the file
        self._FM.close_file(file)

    def remove_file(self, filename):
        self._FM.remove_file(filename)

    def open_file(self, filename):
        if filename in self.opened_files:
            raise RecordFileOperationError(f'File {filename} is already opened')
        file = self._FM.open_file(filename)
        handle = FileHandle(self, file, filename)
        self.opened_files[filename] = handle
        return handle

    def close_file(self, filename):
        if filename not in self.opened_files:
            raise RecordFileOperationError(f'File {filename} is not opened')
        handle = self.opened_files[filename]
        if handle.header_modified:
            handle.modify_header()
        self.opened_files.pop(handle.filename)
        self._FM.close_file(handle.file_id)
        handle.is_opened = False




