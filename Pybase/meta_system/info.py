from datetime import date

from Pybase.exceptions.run_sql import DataBaseError
from Pybase.record_system.record import Record
from Pybase.exceptions.meta import TableExistenceError, ColumnExistenceError
from .converter import Converter

ACCEPT_TYPE = {
    'INT': (int,),
    'FLOAT': (int, float),
    'VARCHAR': (str,),
    'DATE': (date, str),
}


class ColumnInfo:
    def __init__(self, type, name, size, default=None) -> None:
        self._type = type
        self._name = name
        self._size = size
        self._default = default

    @property
    def type(self):
        return self._type

    @property
    def name(self):
        return self._name

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
    def __init__(self, name, columns, orders=None) -> None:
        self._name = name
        self.column_map = {col.name: col for col in columns}
        self.primary = None
        self.foreign = {}
        self.indexes = {}
        self.size_list = tuple(map(ColumnInfo.get_size, self.column_map.values()))
        self.type_list = tuple(map(lambda x: x.type, self.column_map.values()))
        self.total_size = sum(self.size_list)
        if orders is None:
            self._colindex = {col.name: i for i, col in enumerate(columns)}
        else:
            self._colindex = {col.name: i for i, col in zip(columns, orders)}
    
    @property
    def name(self):
        return self._name

    def insert_column(self, column: ColumnInfo):
        if column.name not in self.column_map:
            self.column_map[column.name] = column
            self._colindex[column.name] = len(self.column_map) - 1
        else:
            raise ColumnExistenceError(f"Column {column.name} should not exists.")

    def remove_column(self, column_name):
        if column_name not in self.column_map:
            raise ColumnExistenceError(f"Column {column_name} should exists.")
        else:
            self._colindex.pop(column_name)
            self.column_map.pop(column_name)

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

    def get_col_index(self, column_name):
        if column_name in self._colindex:
            return self._colindex[column_name]
        else:
            return None

    def check_value_map(self, value_map: dict):
        for column_name, value in value_map.items():
            column: ColumnInfo = self.column_map.get(column_name)
            if column is None:
                raise DataBaseError(f'Field {column_name} is unknown')
            if type(value) not in ACCEPT_TYPE[column.type]:
                raise DataBaseError(f'Field {column_name} expects {column.type} bug get {value} instead')
            if column.type == 'DATE':
                value_map[column_name] = Converter.parse_date(value)

    def get_header(self):
        return tuple(self._name + '.' + column_name for column_name in self.column_map.keys())

    def exists_index(self, column_name):
        return column_name in self.indexes

    def create_index(self, column_name, root_id):
        assert not self.exists_index(column_name)
        self.indexes[column_name] = root_id
    
    def drop_index(self, column_name):
        self.indexes.pop(column_name)


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

    def remove_column(self, tbname, column_name):
        if tbname not in self._tbMap:
            raise TableExistenceError(f"Table {tbname} should exists.")
        else:
            table: TableInfo = self._tbMap[tbname]
            table.remove_column(column_name)
    
    def create_index(self, index_name, tbname, column_name):
        if index_name in self._index_map:
            raise DataBaseError("Index name already exists.")
        self._index_map[index_name] = (tbname, column_name)
    
    def drop_index(self, index_name):
        if index_name not in self._index_map:
            raise DataBaseError("Index name not exists.")
        self._index_map.pop(index_name)
    
    def get_index_info(self, index_name):
        if index_name not in self._index_map:
            raise DataBaseError("Index name not exists.")
        return self._index_map[index_name]
