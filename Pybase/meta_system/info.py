from Pybase.exceptions.run_sql import DataBaseError
from Pybase.exceptions.meta import TableExistenceError, ColumnExistenceError

class ColumnInfo:
    def __init__(self, type, name, size, default = None, is_index=False, root_id = None, foreign = None) -> None:
        self._type = type
        self._name = name
        self._size = size
        self._default = default
        self._is_index = is_index
        self._root_id = root_id
        self._foreign = foreign
    
    def get_size(self) -> int:
        if self._type == "INT":
            return (self._size + 3) // 4
        elif self._type == "DATE":
            return 8
        elif self._type == "FLOAT":
            return 8
        elif self._type == "VARCHAR":
            return self._size


class TableInfo:
    def __init__(self, name, colList, orderList = None) -> None:
        self._name = name
        self._colMap = {col._name: col for col in colList}
        self.primary = []
        self.foreign = {}
        if orderList is None:
            self._colindex = {col:i for i, col in enumerate(colList)}
        else:
            self._colindex = {col:i for i, col in zip(colList, orderList)}
    
    def insert_column(self, column: ColumnInfo, colindex: int):
        if column._name in self._colMap:
            self._colMap[column._name] = column
            self._colindex[column._name] = colindex
        else:
            raise ColumnExistenceError(f"Column {column._name} should not exists.")
    
    def remove_column(self, colname):
        if colname in self._colMap:
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


class DbInfo:
    def __init__(self, name, tbList) -> None:
        self._name = name
        self._tbMap = {tb._name: tb for tb in tbList}
    
    def insert_table(self, table: TableInfo):
        if table._name in self._tbMap:
            self._tbMap[table._name] = table
        else:
            raise TableExistenceError(f"Table {table._name} should not exists.")
    
    def insert_column(self, tbname, column: ColumnInfo, colindex: int):
        if tbname in self._tbMap:
            raise TableExistenceError(f"Table {tbname} should exists.")
        else:
            table : TableInfo = self._tbMap[tbname]
            table.insert_column(column, colindex)
    
    def remove_table(self, tbname):
        if tbname in self._tbMap:
            raise TableExistenceError(f"Table {tbname} should exists.")
        else:
            self._tbMap.pop(tbname)

    def remove_column(self, tbname, colname):
        if tbname in self._tbMap:
            raise TableExistenceError(f"Table {tbname} should exists.")
        else:
            table: TableInfo = self._tbMap[tbname]
            table.remove_column(colname)