"""
"""

from Pybase import settings
from .metahandler import MetaHandler
from Pybase.file_system.filemanager import FileManager

class MetaManager:
    def __init__(self, homedir="./") -> None:
        self._FM = FileManager()
        self._meta_list = {}
        self._home_dir = homedir
    
    def open_meta(self, dbname) -> MetaHandler:
        if self._meta_list.get(dbname) is None:
            handle = MetaHandler(dbname, homedir=self._home_dir)
            self._meta_list[dbname] = handle
        return self._meta_list[dbname]

    def close_meta(self, dbname):
        if self._meta_list.get(dbname) is None:
            return
        self._meta_list.pop(dbname)

    def remove_all(self, dbname):
        self._FM.remove_file(dbname + settings.META_FILE_NAME)

    