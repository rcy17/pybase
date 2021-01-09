"""
"""

from typing import Dict
from Pybase.file_system.manager import FileManager
from .indexhandler import IndexHandler
from .fileindex import FileIndex


class IndexManager:
    def __init__(self, manager: FileManager, home_dir="./") -> None:
        self._FM = manager
        self._opened_index_handlers: Dict[str, IndexHandler] = {}
        self.opened_file_indexes: Dict[tuple, FileIndex] = {}
        self._home_dir = home_dir

    @property
    def file_manager(self) -> FileManager:
        return self._FM
    
    def get_handler(self, db_name):
        if db_name in self._opened_index_handlers:
            return self._opened_index_handlers[db_name]
        handler = IndexHandler(self._FM, db_name, self._home_dir)
        self._opened_index_handlers[db_name] = handler
        return handler

    def close_handler(self, db_name):
        if db_name not in self._opened_index_handlers:
            return False
        handler = self._opened_index_handlers.pop(db_name)
        for key, file_index in tuple(self.opened_file_indexes.items()):
            if file_index.handler is not handler:
                continue
            self.close_index(*key)
        handler.close()
        return True

    def create_index(self, db_name, table_name) -> FileIndex:
        handler = self.get_handler(db_name)
        file_index = FileIndex(handler, handler.new_page())
        file_index.dump()
        self.opened_file_indexes[table_name, file_index.root_id] = file_index
        return file_index

    def open_index(self, db_name, table_name, root_id) -> FileIndex:
        if (table_name, root_id) in self.opened_file_indexes:
            return self.opened_file_indexes[table_name, root_id]
        handler = self.get_handler(db_name)
        file_index = FileIndex(handler, root_id)
        file_index.load()
        self.opened_file_indexes[table_name, root_id] = file_index
        return file_index

    def close_index(self, table_name, root_id):
        if (table_name, root_id) not in self.opened_file_indexes:
            return None
        file_index = self.opened_file_indexes.pop((table_name, root_id))
        if file_index.is_modified:
            file_index.dump()

    def shutdown(self):
        for database in tuple(self._opened_index_handlers):
            self.close_handler(database)
