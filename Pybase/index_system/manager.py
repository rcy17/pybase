"""
"""

from Pybase import settings
from Pybase.file_system.manager import FileManager
from .indexhandler import IndexHandler
from .fileindex import FileIndex


class IndexManager:
    def __init__(self, manager: FileManager, home_dir="./") -> None:
        self._FM = manager
        self._open_indexes = {}
        self._home_dir = home_dir

    @property
    def file_manager(self) -> FileManager:
        return self._FM

    def create_index(self, dbname, tbname, colname) -> FileIndex:
        handle = IndexHandler(self._FM, dbname, self._home_dir)
        fileindex = FileIndex(handle, handle.new_page())
        fileindex.dump()
        self._open_indexes[(tbname, colname)] = fileindex
        return fileindex

    def open_index(self, dbname, tbname, colname, root_id) -> FileIndex:
        if (tbname, colname) in self._open_indexes:
            return self._open_indexes.get((tbname, colname))
        handle = IndexHandler(self._FM, dbname, self._home_dir)
        fileindex = FileIndex(handle, root_id)
        fileindex.load()
        self._open_indexes[(tbname, colname)] = fileindex
        return fileindex

    def close_index(self, tbname, colname):
        if (tbname, colname) not in self._open_indexes:
            return None
        fileindex: FileIndex = self._open_indexes.pop((tbname, colname))
        handle: IndexHandler = fileindex._handle
        if not handle._is_modified:
            self._FM.close_file(handle._file_id)
            return None
        else:
            fileindex.dump()
            self._FM.close_file(handle._file_id)
            return fileindex._root_id

    def shutdown(self):
        for table, column in tuple(self._open_indexes):
            self.close_index(table, column)
