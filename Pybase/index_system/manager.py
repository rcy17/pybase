"""
"""

from Pybase import settings
from numpy.lib.function_base import select
from Pybase.file_system.filemanager import FileManager
from .indexhandler import IndexHandler
from .fileindex import FileIndex

class IndexManager:
    def __init__(self) -> None:
        self._FM = FileManager()
        self._open_indexes = {}

    @property
    def file_manager(self) -> FileManager:
        return self._FM
    
    def create_index(self, dbname, colname, keylen) -> FileIndex:
        handle = IndexHandler(self._FM, dbname)
        fileindex = FileIndex(handle, handle.new_page(), keylen)
        fileindex.dump()
        self._open_indexes[(dbname, colname)] = fileindex
        return fileindex

    def remove_all(self, dbname):
        self._FM.remove_file(dbname + settings.INDEX_FILE_SUFFIX)
    
    def open_index(self, dbname, colname, root_id, keylen:int = 8) -> FileIndex:
        if self._open_indexes.get((dbname, colname)) is not None:
            return self._open_indexes.get((dbname, colname))
        handle = IndexHandler(self._FM, dbname)
        fileindex = FileIndex(handle, root_id, keylen)
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