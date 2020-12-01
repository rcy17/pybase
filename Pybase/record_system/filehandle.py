"""
Here defines FileHandle

Data: 2019/10/24
"""
import numpy as np

from Pybase import settings
# from .manager import FileManager, RecordManager
from Pybase.utils.header import header_deserialize, header_serialize
from .record import Record
from .rid import RID


class FileHandle:
    """
    Class to handle a file as records
    """

    def __init__(self, manger, file_id, filename):
        self.is_opened = True
        self._file_id = file_id
        self._manger = manger
        self._opened = True
        self._file_name = filename
        header_page = manger.file_manager.get_page(file_id, settings.HEADER_PAGE_ID)
        self._header = header_deserialize(header_page)
        self._header_modified = False

    def __del__(self):
        if self.is_opened:
            self._manger.close_file(self)

    @property
    def filename(self):
        return self._file_name

    @property
    def header(self):
        return self._header

    @property
    def header_modified(self):
        return self._header_modified

    @property
    def file_id(self):
        return self._file_id

    def get_bitmap(self, data: np.ndarray):
        offset = settings.RECORD_PAGE_FIXED_HEADER_SIZE
        return np.unpackbits(data[offset: offset + (self.header['record_per_page'] >> 3)])

    def _get_next_vacancy(self, data: np.ndarray) -> int:
        offset = settings.RECORD_PAGE_NEXT_OFFSET
        return int.from_bytes(data[offset: offset + 4].tobytes(), 'big')

    def _set_next_vacancy(self, data: np.ndarray, page_id: int):
        offset = settings.RECORD_PAGE_NEXT_OFFSET
        data[offset: offset + 4] = np.frombuffer(page_id.to_bytes(4, 'big'), dtype=np.uint8)

    def _get_record_offset(self, slot_id):
        header = self.header
        return settings.RECORD_PAGE_FIXED_HEADER_SIZE + (header['record_length'] >> 3) + header[
            'record_length'] * slot_id

    def get_page(self, page_id) -> np.ndarray:
        return self._manger.file_manager.get_page(self._file_id, page_id)

    def put_page(self, page_id, data: np.ndarray) -> np.ndarray:
        return self._manger.file_manager.put_page(self._file_id, page_id, data)

    def new_page(self) -> int:
        return self._manger.file_manager.new_page(self._file_id, np.zeros(settings.PAGE_SIZE, dtype=np.uint8))

    def modify_header(self):
        self._manger.file_manager.put_page(self._file_id, settings.HEADER_PAGE_ID, header_serialize(self._header))

    def get_record(self, rid: RID, data=None) -> Record:
        header = self.header
        slot_id = rid.slot_id
        assert rid.page_id < header['page_number']
        assert slot_id < header['record_per_page']
        if data is None:
            data = self._manger.file_manager.get_page(self._file_id, rid.page_id)
        offset = self._get_record_offset(slot_id)
        record = Record(rid, data[offset: offset + header['record_length']])
        return record

    def insert_record(self, data: np.ndarray) -> RID:
        header = self.header
        page_id = header.next_vacancy_page
        if page_id == settings.HEADER_PAGE_ID:
            self.append_record_page()
            page_id = header.next_vacancy_page
            assert page_id != settings.HEADER_PAGE_ID
        page = self._manger.file_manager.get_page(self._file_id, page_id)
        record_length = header['record_length']
        assert len(data) == record_length
        bitmap = self.get_bitmap(page)

        # get first valid slot
        valid_slots = np.where(bitmap)[0]
        slot_id = valid_slots[0]

        # insert record
        offset = self._get_record_offset(slot_id)
        page[offset: offset + record_length] = data
        bitmap[slot_id] = False

        # set new bitmap
        offset = settings.RECORD_PAGE_FIXED_HEADER_SIZE
        page[offset: offset + (header['record_per_page'] >> 3)] = np.packbits(bitmap)

        # update header
        header['record_number'] += 1
        self._header_modified = True

        # check if this is the last valid slot
        if len(valid_slots) == 1:
            header.next_vacancy_page = self._get_next_vacancy(page)
            # make self-loop
            self._set_next_vacancy(page, page_id)

        # finish insert and return the rid
        self._manger.file_manager.put_page(self._file_id, page_id, page)
        return RID(page_id, slot_id)

    def delete_record(self, rid: RID):
        header = self.header
        page_id = rid.page_id
        slot_id = rid.slot_id
        page = self._manger.file_manager.get_page(self._file_id, page_id)
        bitmap = self.get_bitmap(page)

        # just use lazy deletion
        assert bitmap[slot_id] is False
        bitmap[slot_id] = True
        header['record_number'] -= 1
        self._header_modified = True

        # check to update first_vacancy_page
        if self._get_next_vacancy(page) == page_id:
            self._set_next_vacancy(page, header.next_vacancy_page)
            header.next_vacancy_page = page_id

        # finish deletion
        self._manger.file_manager.put_page(self._file_id, page_id, page)

    def update_record(self, record: Record):
        header = self.header
        rid = record.rid
        data = self._manger.file_manager.get_page(rid.page_id, rid.slot_id)
        offset = self._get_record_offset(rid.slot_id)
        data[offset: offset + header['record_length']] = record.data
        self._manger.file_manager.put_page(self._file_id, rid.page_id, data)

    def force_pages(self, pages=-1):
        pass

    def append_record_page(self):
        header = self.header
        # use link list method
        next_page = header.next_vacancy_page
        data = np.full(settings.PAGE_SIZE, -1, dtype=np.uint8)
        data[settings.PAGE_FLAG_OFFSET] = settings.RECORD_PAGE_FLAG
        self._set_next_vacancy(data, next_page)

        # write data and update header
        page_id = self._manger.file_manager.new_page(self._file_id, data)
        header['page_number'] += 1
        header.next_vacancy_page = page_id
        self._header_modified = True
