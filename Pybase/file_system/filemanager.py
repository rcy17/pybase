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
        self.file_cache_pages = {}
        self.file_id_to_name = {}
        self.file_name_to_id = {}
        self.page_buffer = np.zeros((settings.CACHE_CAPACITY, settings.PAGE_SIZE), dtype=np.uint8)
        self.dirty = np.zeros(settings.CACHE_CAPACITY, dtype=np.bool)
        self.index_to_file_page = np.full(settings.CACHE_CAPACITY, settings.ID_DEFAULT_VALUE, dtype=np.int64)
        self.replace = FindReplace(settings.CACHE_CAPACITY)
        self.file_page_to_index = {}
        # self.file_size = {}
        self.last = settings.ID_DEFAULT_VALUE

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
        file_page = self.index_to_file_page[index]
        self.file_cache_pages[unpack_file_page_id(file_page)[0]].remove(index)
        self.file_page_to_index.pop(file_page)
        self.index_to_file_page[index] = settings.ID_DEFAULT_VALUE

    @staticmethod
    def create_file(filename):
        open(filename, 'w').close()

    @staticmethod
    def touch_file(filename):
        open(filename, 'a').close()

    @staticmethod
    def remove_file(filename):
        os.remove(filename)

    def open_file(self, filename):
        if filename in self.file_name_to_id:
            return self.file_name_to_id[filename]
        file_id = os.open(filename, FileManager.FILE_OPEN_MODE)
        if file_id == settings.ID_DEFAULT_VALUE:
            raise OpenFileFailed("Can't open file " + filename)
        self.file_cache_pages[file_id] = set()
        self.file_name_to_id[filename] = file_id
        self.file_id_to_name[file_id] = filename
        return file_id

    def close_file(self, file_id):
        # notice that in shutdown file_id already popped
        pages = self.file_cache_pages.pop(file_id, {})
        for index in pages:
            # remove index information
            file_page = self.index_to_file_page[index]
            self.index_to_file_page[index] = settings.ID_DEFAULT_VALUE
            self.file_page_to_index.pop(file_page)
            # remove from replace
            self.replace.free(index)
            # write back
            if self.dirty[index]:
                self.write_page(*unpack_file_page_id(file_page), self.page_buffer[index])
                self.dirty[index] = False
        os.close(file_id)
        filename = self.file_id_to_name.pop(file_id)
        self.file_name_to_id.pop(filename)

    @staticmethod
    def read_page(file_id, page_id) -> bytes:
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
        file_page = pack_file_page_id(file_id, page_id)
        index = self.file_page_to_index.get(file_page)
        if index is None:
            self.get_page(file_id, page_id)
            # then assert can't be None
            index = self.file_page_to_index.get(file_page)
        self.page_buffer[index] = data
        self.dirty[index] = True
        self.replace.access(index)

    def _get_page(self, file_id, page_id) -> np.ndarray:
        file_page = pack_file_page_id(file_id, page_id)
        index = self.file_page_to_index.get(file_page)

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
        self.file_page_to_index[file_page] = index
        self.file_cache_pages[file_id].add(index)
        self.index_to_file_page[index] = file_page
        data = self.read_page(file_id, page_id)
        data = np.frombuffer(data, np.uint8, settings.PAGE_SIZE)
        self.page_buffer[index] = data
        return self.page_buffer[index]

    def get_page_reference(self, file_id, page_id) -> np.ndarray:
        page = self._get_page(file_id, page_id)
        self.mark_dirty(self.file_page_to_index[pack_file_page_id(file_id, page_id)])
        return page

    def get_page(self, file_id, page_id) -> np.ndarray:
        return self._get_page(file_id, page_id).copy()

    def release_cache(self):
        for index in np.where(self.dirty)[0]:
            self._write_back(index)
        self.page_buffer.fill(0)
        self.dirty.fill(False)
        self.index_to_file_page.fill(settings.ID_DEFAULT_VALUE)
        self.file_page_to_index.clear()
        self.last = settings.ID_DEFAULT_VALUE

    def shutdown(self):
        self.release_cache()
        # Notice that close_file will change file_cache_pages
        # So we can't just use for-in loop
        while self.file_cache_pages:
            self.close_file(self.file_cache_pages.popitem()[0])
