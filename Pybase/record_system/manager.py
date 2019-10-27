"""
Here defines RecordManager

Data: 2019/10/24
"""
from Pybase.file_system import FileManager
from Pybase import settings
from Pybase.utils.formula import get_record_capacity
from Pybase.exceptions.record import PageOverflow
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
            raise PageOverflow(f'Header page with {header.ByteSize()} bytes is overflow')
        header_data = header.SerializeToString()
        self._FM.write_page(file, 0, header_data)

        # close the file

    def destroy_file(self, filename):
        pass

    def open_file(self, filename):
        handle = FileHandle(self, self._FM.open_file(filename))
        return handle




