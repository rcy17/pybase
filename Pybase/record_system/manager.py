"""
Here defines RecordManager

Data: 2019/10/24
"""
from Pybase.file_system import FileManager
from Pybase.utils.header import get_record_capacity, header_serialize, get_bitmap_length
from Pybase.exceptions.record import RecordFileOperationError
from .filehandle import FileHandle


class RecordManager:
    """
    Class to manage records of database
    """

    def __init__(self, manager: FileManager):
        self._FM = manager
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
        record_per_page = get_record_capacity(record_length)
        bitmap_length = get_bitmap_length(record_per_page)
        header = {
            'record_length': record_length,
            'record_per_page': record_per_page,
            'page_number': 1,
            'record_number': 0,
            'next_vacancy_page': 0,
            'filename': str(filename),
            'bitmap_length': bitmap_length,
        }
        self._FM.new_page(file, header_serialize(header))

        # close the file
        self._FM.close_file(file)

    def remove_file(self, filename):
        self._FM.remove_file(filename)

    def open_file(self, filename):
        if filename in self.opened_files:
            # raise RecordFileOperationError(f'File {filename} is already opened')
            return self.opened_files[filename]
        file = self._FM.open_file(filename)
        handle = FileHandle(self, file, filename)
        self.opened_files[filename] = handle
        return handle

    def replace_file(self, source, dest):
        """Replace dest with source"""
        if source in self.opened_files:
            self.close_file(source)
        if dest in self.opened_files:
            self.close_file(dest)
        self.remove_file(dest)
        self._FM.move_file(source, dest)

    def close_file(self, filename):
        if filename not in self.opened_files:
            # raise RecordFileOperationError(f'File {filename} is not opened')
            return False
        handle = self.opened_files[filename]
        if handle.header_modified:
            handle.modify_header()
        self.opened_files.pop(handle.filename)
        self._FM.close_file(handle.file_id)
        handle.is_opened = False
        return True
