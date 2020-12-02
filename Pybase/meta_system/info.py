class ColumnInfo:
    def __init__(self, type, name, size, is_index=False, root_id = None, foreign = None) -> None:
        self._type = type
        self._name = name
        self._size = size
        self._is_index = is_index
        self._root_id = root_id
        self._foreign = foreign


class TableInfo:
    def __init__(self, name, pid, colList, orderList = None) -> None:
        self._name = name
        self._pid = pid
        self._colMap = {col._name: col for col in colList}
        if orderList is None:
            self._colindex = {col:i for i, col in enumerate(colList)}
        else:
            self._colindex = {col:i for i, col in zip(colList, orderList)}
    
    def insert_column(self, column: ColumnInfo, colindex: int):
        if self._colMap.get(column._name) is None:
            self._colMap[column._name] = column
            self._colindex[column._name] = colindex
        else:
            # Error
            pass
    
    def remove_column(self, colname):
        if self._colMap.get(colname) is None:
            # Error
            pass
        else:
            self._colindex.pop(colname)
            self._colMap.pop(colname)


class DbInfo:
    def __init__(self, name, tbList) -> None:
        self._name = name
        self._tbMap = {tb._name: tb for tb in tbList}
    
    def insert_table(self, table: TableInfo):
        if self._tbMap.get(table._name) is None:
            self._tbMap[table._name] = table
        else:
            # Error
            pass
    
    def insert_column(self, tbname, column: ColumnInfo, colindex: int):
        if self._tbMap.get(tbname) is None:
            # Error
            pass
        else:
            self._tbMap[tbname].insert_column(column, colindex)
    
    def remove_table(self, tbname):
        if self._tbMap.get(tbname) is None:
            # Error
            pass
        else:
            self._tbMap.pop(tbname)

    def remove_column(self, tbname, colname):
        if self._tbMap.get(tbname) is None:
            # Error
            pass
        else:
            table = self._tbMap.get(tbname)
            table.remove_column(colname)