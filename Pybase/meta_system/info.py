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

    @property
    def default(self):
        return self._default

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
    def __init__(self, name, columns: list) -> None:
        self._name = name
        self.columns = columns
        self.column_map = {}
        self.primary = None
        self.foreign = {}
        self.indexes = {}
        self.size_list = []
        self.type_list = []
        self.total_size = 0
        self._colindex = {}
        self.update_params()
    
    @property
    def name(self):
        return self._name

    def update_params(self):
        self.column_map = {col.name: col for col in self.columns}
        self.size_list = tuple(map(ColumnInfo.get_size, self.column_map.values()))
        self.type_list = tuple(map(lambda x: x.type, self.column_map.values()))
        self.total_size = sum(self.size_list)
        self._colindex = {col.name: i for i, col in enumerate(self.columns)}

    def insert_column(self, column: ColumnInfo):
        if column.name in self.column_map:
            raise ColumnExistenceError(f"Column {column.name} should not exists.")
        self.columns.append(column)
        self.update_params()

    def remove_column(self, column_name):
        if column_name not in self.column_map:
            raise ColumnExistenceError(f"Column {column_name} should exists.")
        self.columns = [column for column in self.columns if column.name != column_name]
        self.update_params()

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
        return self._colindex.get(column_name)

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
    def __init__(self, name, tables) -> None:
        self._name = name
        self._tbMap = {tb.name: tb for tb in tables}
        self._index_map = {}

    def insert_table(self, table: TableInfo):
        if table.name in self._tbMap:
            raise TableExistenceError(f"Table {table.name} should not exists.")
        self._tbMap[table.name] = table

    def insert_column(self, database_name, column: ColumnInfo):
        if database_name not in self._tbMap:
            raise TableExistenceError(f"Table {database_name} should exists.")
        table: TableInfo = self._tbMap[database_name]
        table.insert_column(column)

    def remove_table(self, database_name):
        if database_name not in self._tbMap:
            raise TableExistenceError(f"Table {database_name} should exists.")
        self._tbMap.pop(database_name)

    def remove_column(self, database_name, column_name):
        if database_name not in self._tbMap:
            raise TableExistenceError(f"Table {database_name} should exists.")
        table: TableInfo = self._tbMap[database_name]
        table.remove_column(column_name)
    
    def create_index(self, index_name, database_name, column_name):
        if index_name in self._index_map:
            raise DataBaseError("Index name already exists.")
        self._index_map[index_name] = (database_name, column_name)
    
    def drop_index(self, index_name):
        if index_name not in self._index_map:
            raise DataBaseError("Index name not exists.")
        self._index_map.pop(index_name)
    
    def get_index_info(self, index_name):
        if index_name not in self._index_map:
            raise DataBaseError("Index name not exists.")
        return self._index_map[index_name]
