"""
Here defines RecordManager

Data: 2019/10/24
"""
from Pybase.file_system import FileManager
from Pybase.utils.formula import get_record_capacity
from Pybase.utils.header import header_serialize
from Pybase.exceptions.record import RecordFileReopenError
from .filehandle import FileHandle
from Pybase.utils.header_pb2 import HeaderInfo


class RecordManager:
    """
    Class to manage records of database
    """
    def __init__(self):
        self._FM = FileManager()
        self.opened_files = set()

    def create_file(self, filename, record_length):
        # create file
        self._FM.create_file(filename)

        # open the file to add header page
        file = self._FM.open_file(filename)
        header = HeaderInfo()
        header.record_length = record_length
        header.record_per_page = get_record_capacity(record_length)
        header.page_number = 1
        header.record_number = 0
        header.next_vacancy_page = 0
        self._FM.new_page(file, header_serialize(header))

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
        self._FM.close_file(handle.file_id)




