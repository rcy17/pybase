from Pybase import settings
from Pybase.exceptions.run_sql import DataBaseError
from Pybase.record_system.record import Record
from Pybase.exceptions.meta import TableExistenceError, ColumnExistenceError
from Pybase.utils.tools import int2bytes, bytes2int, float2bytes, bytes2float
from .converter import Converter


class ColumnInfo:
    def __init__(self, type, name, size, default=None) -> None:
        self._type = type
        self._name = name
        self._size = size
        self._default = default

    @property
    def type(self):
        return self._type

    def get_size(self) -> int:
        if self._type == "INT":
            return 8
        elif self._type == "DATE":
            return 8
        elif self._type == "FLOAT":
            return 8
        elif self._type == "VARCHAR":
            return self._size + 1

    def get_description(self):
        """
        Get description of column
        :return: (name, type, null, key, default, extra)
        """
        return (
            self._name,
            f'{self._type}{("(%d)" % self._size) if self._size else ""}',
            "NO",
            "",
            self._default,
            "",
        )


class TableInfo:
    def __init__(self, name, colList, orderList=None) -> None:
        self._name = name
        self._colMap = {col._name: col for col in colList}
        self.primary = None
        self.foreign = {}
        self.indexes = {}
        self.size_list = tuple(map(ColumnInfo.get_size, self._colMap.values()))
        self.type_list = tuple(map(lambda x: x.type, self._colMap.values()))
        self.total_size = sum(self.size_list)
        if orderList is None:
            self._colindex = {col._name: i for i, col in enumerate(colList)}
        else:
            self._colindex = {col._name: i for i, col in zip(colList, orderList)}
    
    @property
    def name(self):
        return self._name

    def insert_column(self, column: ColumnInfo):
        if column._name not in self._colMap:
            self._colMap[column._name] = column
            self._colindex[column._name] = len(self._colMap) - 1
        else:
            raise ColumnExistenceError(f"Column {column._name} should not exists.")

    def remove_column(self, colname):
        if colname not in self._colMap:
            raise ColumnExistenceError(f"Column {colname} should exists.")
        else:
            self._colindex.pop(colname)
            self._colMap.pop(colname)

    def set_primary(self, primary):
        self.primary = primary
    
    def add_foreign(self, col, foreign):
        self.foreign[col] = foreign
    
    def remove_foreign(self, col):
        if col in self.foreign:
            self.foreign.pop(col)

    def build_record(self, value_list: list):
        return Converter.encode(self.size_list, self.type_list, self.total_size, value_list)

    def load_record(self, record: Record):
        return Converter.decode(self.size_list, self.type_list, self.total_size, record)

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

    def insert_column(self, tbname, column: ColumnInfo):
        if tbname not in self._tbMap:
            raise TableExistenceError(f"Table {tbname} should exists.")
        else:
            table: TableInfo = self._tbMap[tbname]
            table.insert_column(column)

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
