"""
Here defines the index handler

Data: 2020/12/01
"""

import numpy as np

from Pybase import settings
from Pybase.file_system import FileManager


class IndexHandler:
    """
    Class to treat a file as index
    """

    def __init__(self, manager: FileManager, dbname, home_dir) -> None:
        self._manager = manager
        self._is_modified = False
        path = home_dir / dbname / (dbname + settings.INDEX_FILE_SUFFIX)
        if not self._manager.exists_file(path):
            self._manager.create_file(path)
        self._file_id = self._manager.open_file(path)

    def get_page(self, page_id) -> np.ndarray:
        return self._manager.get_page(self._file_id, page_id)

    def put_page(self, page_id, data: np.ndarray):
        self._is_modified = True
        return self._manager.put_page(self._file_id, page_id, data)

    def new_page(self) -> int:
        return self._manager.new_page(self._file_id, np.zeros(settings.PAGE_SIZE, dtype=np.uint8))

    def close(self):
        self._manager.close_file(self._file_id)
