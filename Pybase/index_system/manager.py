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
    
    def create_index(self, dbname, colname) -> FileIndex:
        handle = IndexHandler(self._FM, dbname, self._home_dir)
        fileindex = FileIndex(handle, handle.new_page())
        fileindex.dump()
        self._open_indexes[(dbname, colname)] = fileindex
        return fileindex

    def remove_all(self, dbname):
        self._FM.remove_file(dbname + settings.INDEX_FILE_SUFFIX)
    
    def open_index(self, dbname, colname, root_id) -> FileIndex:
        if self._open_indexes.get((dbname, colname)) is not None:
            return self._open_indexes.get((dbname, colname))
        handle = IndexHandler(self._FM, dbname)
        fileindex = FileIndex(handle, root_id)
        fileindex.load()
        self._open_indexes[(dbname, colname)] = fileindex
        return fileindex
    
    def close_index(self, dbname, colname):
        if self._open_indexes.get((dbname, colname)) == None:
            return None
        fileindex: FileIndex = self._open_indexes.get((dbname, colname))
        handle: IndexHandler = fileindex._handle
        self._open_indexes.pop((dbname, colname))
        if not handle._is_modified:
            self._FM.close_file(handle._file_id)
            return None
        else:
            fileindex.dump()
            self._FM.close_file(handle._file_id)
            return fileindex._root_id