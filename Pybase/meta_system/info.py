from Pybase.exceptions.run_sql import DataBaseError
from Pybase.record_system.record import Record
from Pybase.exceptions.meta import TableExistenceError, ColumnExistenceError
from Pybase.utils.tools import int2bytes, bytes2int, float2bytes, bytes2float

import numpy as np


class ColumnInfo:
    def __init__(self, type, name, size, default=None, is_index=False, root_id=None, foreign=None) -> None:
        self._type = type
        self._name = name
        self._size = size
        self._default = default
        self._is_index = is_index
        self._root_id = root_id
        self._foreign = foreign

    def get_size(self) -> int:
        if self._type == "INT":
            return 8
        elif self._type == "DATE":
            return 8
        elif self._type == "FLOAT":
            return 8
        elif self._type == "VARCHAR":
            return self._size

    def get_description(self):
        """
        Get description of column
        :return: (name, type, null, key, default, extra)
        """
        return (
            self._name,
            f'{self._type}{("(%d)" % self._size) if self._size else ""}',
            "NO",
            "MUL" if self._foreign else "",
            self._default,
            "",
        )


class TableInfo:
    def __init__(self, name, colList, orderList=None) -> None:
        self._name = name
        self._colMap = {col._name: col for col in colList}
        self.primary = []
        self.foreign = {}
        self.indexes = {}
        if orderList is None:
            self._colindex = {col._name: i for i, col in enumerate(colList)}
        else:
            self._colindex = {col._name: i for i, col in zip(colList, orderList)}
    
    @property
    def name(self):
        return self._name

    def insert_column(self, column: ColumnInfo, colindex: int):
        if column._name not in self._colMap:
            self._colMap[column._name] = column
            self._colindex[column._name] = colindex
        else:
            raise ColumnExistenceError(f"Column {column._name} should not exists.")

    def remove_column(self, colname):
        if colname not in self._colMap:
            raise ColumnExistenceError(f"Column {colname} should exists.")
        else:
            self._colindex.pop(colname)
            self._colMap.pop(colname)

    def get_size(self) -> int:
        return sum([col.get_size() for col in self._colMap.values()])

    def set_primary(self, cols):
        self.primary = cols

    def set_foriegn(self, col, foreign_col):
        self.foreign[col] = foreign_col

    def get_size_list(self):
        return [col.get_size() for col in self._colMap.values()]

    def get_type_list(self):
        return [col._type for col in self._colMap.values()]

    def build_record(self, value_list: list) -> np.ndarray:
        size_list = self.get_size_list()
        type_list = self.get_type_list()
        size_total = self.get_size()
        assert len(value_list) == len(size_list)
        record_data = np.zeros(shape=(size_total), dtype=np.uint8)
        pos = 0
        for i in range(len(size_list)):
            size_ = size_list[i]
            type_ = type_list[i]
            value_ = value_list[i]
            if type_ == "VARCHAR":
                l = len(value_)
                if l > size_:
                    raise DataBaseError("Varchar length exceeds.")
                for i in range(l):
                    record_data[pos + i] = ord(value_[i])
                pos += size_
            else:
                ba = None
                if type_ == "INT":
                    ba = int2bytes(int(value_))
                elif type_ == "FLOAT":
                    ba = float2bytes(float(value_))
                elif type_ == "DATE":
                    ba = tuple(ord(value_[i]) for i in range(8))
                else:
                    raise DataBaseError("Unsupported type.")
                for i in range(size_):
                    record_data[pos + i] = ba[i]
                pos += size_
        assert pos == size_total
        return record_data

    def load_record(self, record: Record):
        data = record.data
        size_list = self.get_size_list()
        type_list = self.get_type_list()
        size_total = self.get_size()
        res = []
        pos = 0
        for i in range(len(size_list)):
            size_ = size_list[i]
            type_ = type_list[i]
            if type_ == "VARCHAR":
                val = ""
                for i in range(size_):
                    t = data[pos + i]
                    if t == 0:
                        break
                    val += chr(t)
                res.append(val)
            elif type_ == "INT":
                ba = data[pos: pos + size_].tolist()
                res.append(bytes2int(ba))
            elif type_ == "FLOAT":
                ba = data[pos: pos + size_].tolist()
                res.append(bytes2float(ba))
            elif type_ == "DATE":
                val = ""
                for i in range(size_):
                    t = data[pos + i]
                    val += chr(t)
                res.append(val)
            else:
                raise DataBaseError("Unsupported type.")
            pos += size_
        assert pos == size_total
        return res

    def get_col_index(self, colname):
        if colname in self._colindex:
            return self._colindex[colname]
        else:
            return None
    
    def get_value(self, colname, value):
        col:ColumnInfo = self._colMap[colname]
        if col._type == "VARCHAR":
            return value
        elif col._type == "INT":
            return int(value)
        elif col._type == "FLOAT":
            return float(value)
        elif col._type == "DATE":
            return value
    
    def get_header(self):
        return tuple(self._name + '.' + colname for colname in self._colMap.keys())

    def exists_index(self, colname):
        return colname in self.indexes

    def create_index(self, colname, root_id):
        assert not self.exists_index(colname)
        self.indexes[colname] = root_id
    
    def drop_index(self, colname):
        self.indexes.pop(colname)


class DbInfo:
    def __init__(self, name, tbList) -> None:
        self._name = name
        self._tbMap = {tb._name: tb for tb in tbList}
        self._index_map = {}

    def insert_table(self, table: TableInfo):
        if table._name not in self._tbMap:
            self._tbMap[table._name] = table
        else:
            raise TableExistenceError(f"Table {table._name} should not exists.")

    def insert_column(self, tbname, column: ColumnInfo, colindex: int):
        if tbname not in self._tbMap:
            raise TableExistenceError(f"Table {tbname} should exists.")
        else:
            table: TableInfo = self._tbMap[tbname]
            table.insert_column(column, colindex)

    def remove_table(self, tbname):
        if tbname not in self._tbMap:
            raise TableExistenceError(f"Table {tbname} should exists.")
        else:
            self._tbMap.pop(tbname)

    def remove_column(self, tbname, colname):
        if tbname not in self._tbMap:
            raise TableExistenceError(f"Table {tbname} should exists.")
        else:
            table: TableInfo = self._tbMap[tbname]
            table.remove_column(colname)
    
    def create_index(self, index_name, tbname, colname):
        if index_name in self._index_map:
            raise DataBaseError("Index name already exists.")
        self._index_map[index_name] = (tbname, colname)
    
    def drop_index(self, index_name):
        if index_name not in self._index_map:
            raise DataBaseError("Index name not exists.")
        self._index_map.pop(index_name)
    
    def get_index_info(self, index_name):
        if index_name not in self._index_map:
            raise DataBaseError("Index name not exists.")
        return self._index_map[index_name]
