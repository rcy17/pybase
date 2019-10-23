"""
Here defines FileManager

Data: 2019/10/21
"""
import os

import numpy as np

from .utils import pack_file_page_id, unpack_file_page_id
from .findreplace import FindReplace
from . import param, exception


class FileManager:
    def __init__(self):
        self.opened_files = {}
        self.page_buffer = np.zeros((param.CACHE_CAPACITY, param.PAGE_SIZE), dtype=np.uint8)
        self.dirty = np.zeros(param.CACHE_CAPACITY, dtype=np.bool)
        self.index_to_file_page = np.full(param.CACHE_CAPACITY, param.ID_DEFAULT_VALUE)
        self.replace = FindReplace(param.CACHE_CAPACITY)
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
            self.write_page(*unpack_file_page_id(index), self.page_buffer[index].tobytes())
        self._release(index)

    def _release(self, index):
        self.dirty[index] = False
        self.replace.free(index)
        self.id_to_index.pop(self.index_to_file_page[index])
        self.index_to_file_page[index] = -1

    @staticmethod
    def create_file(filename):
        open(filename, 'w').close()

    def open_file(self, filename):
        file_id = os.open(filename, os.O_RDWR | os.O_BINARY)
        if file_id != -1:
            self.opened_files[file_id] = filename
        else:
            raise exception.OpenFileFailed("Can't open file " + filename)
        return file_id

    def close_file(self, file_id):
        self.opened_files.pop(file_id)
        os.close(file_id)

    @staticmethod
    def read_page(file_id, page_id):
        """
        Read page for the given file_id and page_id
        *This function is not recommended to call directly*
        :param file_id:
        :param page_id:
        :return: data: bytes
        """
        offset = page_id << param.PAGE_SIZE_BITS
        os.lseek(file_id, offset, os.SEEK_SET)
        data = os.read(file_id, param.PAGE_SIZE)
        return data

    @staticmethod
    def write_page(file_id, page_id, data: bytes):
        """
        Write the data to the given file_id and page_id
        :param file_id:
        :param page_id:
        :param data:
        :return:
        """
        offset = page_id << param.PAGE_SIZE_BITS
        os.lseek(file_id, offset, os.SEEK_SET)
        os.write(file_id, data)

    def put_page(self, file_id, page_id, data: bytes):
        index = self.id_to_index[pack_file_page_id(file_id, page_id)]
        self.page_buffer[index] = np.frombuffer(data, dtype=np.uint8)
        self.dirty[index] = True
        self.replace.access(index)

    def get_page(self, file_id, page_id):
        pair_id = pack_file_page_id(file_id, page_id)
        index = self.id_to_index.get(pair_id)

        # if index is not None, then just past to
        if index is not None:
            self._access(index)
            return self.page_buffer[index].tobytes()

        # else we should get a position in cache
        index = self.replace.find()
        last_id = self.index_to_file_page[index]

        # if this position is occupied, we should remove it first
        if last_id != param.ID_DEFAULT_VALUE:
            self._write_back(index)

        # now save the new page info
        self.id_to_index[pair_id] = index
        self.index_to_file_page[index] = pair_id
        data = self.read_page(file_id, page_id)
        if not data:
            raise exception.ReadFileFailed(f"Can't read page {page_id} from file {self.opened_files[file_id]}")
        self.page_buffer[index] = np.frombuffer(data, dtype=np.uint8)
        return data

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
