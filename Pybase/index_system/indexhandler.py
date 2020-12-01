"""
Here defines the index handler

Data: 2020/12/01
"""

import numpy as np

from Pybase import settings
from Pybase.file_system import FileManager

INDEX_FILE = ".INDEX"

class IndexHandler:
    '''
    Class to treat a file as index
    '''
    def __init__(self, manager: FileManager, dirhead="./") -> None:
        self._manager = manager
        if self._manager.exists_file(dirhead + INDEX_FILE):
            self._file_id = self._manager.open_file(dirhead + INDEX_FILE)
        else:
            self._manager.create_file(dirhead + INDEX_FILE)
            self._file_id = self._manager.open_file(dirhead + INDEX_FILE)

    def get_page(self, page_id) -> np.ndarray:
        return self._manager.get_page(self._file_id, page_id)
    
    def put_page(self, page_id, data: np.ndarray):
        return self._manager.put_page(self._file_id, page_id, data)

    def new_page(self) -> int:
        return self._manager.new_page(self._file_id, np.zeros(settings.PAGE_SIZE, dtype=np.uint8))
        


    