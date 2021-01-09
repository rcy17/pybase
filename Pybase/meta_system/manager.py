"""
"""

from Pybase.exceptions.run_sql import DataBaseError
from Pybase import settings
from .metahandler import MetaHandler
from Pybase.file_system.manager import FileManager


class MetaManager:
    def __init__(self, manager: FileManager, homedir="./") -> None:
        self._FM = manager
        self._meta_list = {}
        self._home_dir = homedir

    def open_meta(self, dbname) -> MetaHandler:
        if dbname not in self._meta_list:
            handle = MetaHandler(dbname, homedir=self._home_dir)
            self._meta_list[dbname] = handle
        return self._meta_list[dbname]

    def close_meta(self, dbname):
        if self._meta_list.get(dbname) is None:
            raise DataBaseError("Never open this database")
        meta_handler: MetaHandler = self._meta_list[dbname]
        meta_handler.close()
        self._meta_list.pop(dbname)

    def remove_all(self, dbname):
        self._FM.remove_file(dbname + settings.META_FILE_NAME)
