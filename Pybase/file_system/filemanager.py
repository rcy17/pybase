"""
Here defines FileManager

Data: 2019/10/21
"""
import os

import numpy as np

from Pybase import settings
from Pybase.utils.id_hash import pack_file_page_id, unpack_file_page_id
from Pybase.exceptions.file import OpenFileFailed, ReadFileFailed
from .findreplace import FindReplace


class FileManager:
    try:
        FILE_OPEN_MODE = os.O_RDWR | os.O_BINARY
    except AttributeError as exception:
        FILE_OPEN_MODE = os.O_RDWR

    def __init__(self):
        self.opened_files = {}
        self.page_buffer = np.zeros((settings.CACHE_CAPACITY, settings.PAGE_SIZE), dtype=np.uint8)
        self.dirty = np.zeros(settings.CACHE_CAPACITY, dtype=np.bool)
        self.index_to_file_page = np.full(settings.CACHE_CAPACITY, settings.ID_DEFAULT_VALUE)
        self.replace = FindReplace(settings.CACHE_CAPACITY)
        self.id_to_index = {}
        self.file_size = {}
        self.last = -1

    def __del__(self):
        self.shutdown()

    def _access(self, index):
        if index == self.last:
            return
        self.replace.access(index)
        self.last = index

    def mark_dirty(self, index):
        self.dirty[index] = True
        self._access(index)

    def _write_back(self, index):
        if self.dirty[index]:
            self.write_page(*unpack_file_page_id(self.index_to_file_page[index]), self.page_buffer[index])
        self._release(index)

    def _release(self, index):
        self.dirty[index] = False
        self.replace.free(index)
        self.id_to_index.pop(self.index_to_file_page[index])
        self.index_to_file_page[index] = -1

    @staticmethod
    def create_file(filename):
        open(filename, 'w').close()

    @staticmethod
    def remove_file(filename):
        os.remove(filename)

    def open_file(self, filename):
        file_id = os.open(filename, FileManager.FILE_OPEN_MODE)
        if file_id != -1:
            self.opened_files[file_id] = filename
        else:
            raise OpenFileFailed("Can't open file " + filename)
        return file_id

    def close_file(self, file_id):
        # notice that in shutdown file_id already popped
        self.opened_files.pop(file_id, None)
        os.close(file_id)

    @staticmethod
    def read_page(file_id, page_id):
        """
        Read page for the given file_id and page_id
        *This function is not recommended to call directly*
        :settings file_id:
        :settings page_id:
        :return: data: bytes
        """
        offset = page_id << settings.PAGE_SIZE_BITS
        os.lseek(file_id, offset, os.SEEK_SET)
        data = os.read(file_id, settings.PAGE_SIZE)
        if not data:
            raise ReadFileFailed(f"Can't read page {page_id} from file {file_id}")
        return data

    @staticmethod
    def write_page(file_id, page_id, data: np.ndarray):
        """
        Write the data to the given file_id and page_id
        Don't call this function explicitly unless creating a new page

        :settings file_id:
        :settings page_id:
        :settings data:
        :return:
        """
        offset = page_id << settings.PAGE_SIZE_BITS
        os.lseek(file_id, offset, os.SEEK_SET)
        os.write(file_id, data.tobytes())

    @staticmethod
    def new_page(file_id, data: np.ndarray) -> int:
        """
        Append new page for the file and return page_id
        :param file_id:
        :param data: new page's data
        :return: new page's id
        """
        pos = os.lseek(file_id, 0, os.SEEK_END)
        os.write(file_id, data.tobytes())
        return pos >> settings.PAGE_SIZE_BITS

    def put_page(self, file_id, page_id, data: np.ndarray):
        index = self.id_to_index.get(pack_file_page_id(file_id, page_id))
        if index is None:
            self.get_page(file_id, page_id)
            # then assert can't be None
            index = self.id_to_index.get(pack_file_page_id(file_id, page_id))
        self.page_buffer[index] = data
        self.dirty[index] = True
        self.replace.access(index)

    def get_page(self, file_id, page_id) -> np.ndarray:
        pair_id = pack_file_page_id(file_id, page_id)
        index = self.id_to_index.get(pair_id)

        # if index is not None, then just past to
        if index is not None:
            self._access(index)
            return self.page_buffer[index]

        # else we should get a position in cache
        index = self.replace.find()
        last_id = self.index_to_file_page[index]

        # if this position is occupied, we should remove it first
        if last_id != settings.ID_DEFAULT_VALUE:
            self._write_back(index)

        # now save the new page info
        self.id_to_index[pair_id] = index
        self.index_to_file_page[index] = pair_id
        data = self.read_page(file_id, page_id)
        data = np.frombuffer(data, np.uint8, settings.PAGE_SIZE)
        self.page_buffer[index] = data
        return data.copy()

    def release_cache(self):
        for index in np.nditer(np.where(self.dirty)[0]):
            self._write_back(index)
        self.page_buffer.fill(0)
        self.dirty.fill(False)
        self.index_to_file_page.fill(-1)
        self.id_to_index.clear()
        self.last = -1

    def shutdown(self):
        self.release_cache()
        while self.opened_files:
            self.close_file(self.opened_files.popitem()[0])
