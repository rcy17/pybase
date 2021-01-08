"""
"""

from math import inf
import pickle
import os
from pickle import dump

from numpy.lib.npyio import load

from Pybase import settings
from .info import TableInfo, ColumnInfo, DbInfo
from Pybase.exceptions.meta import TableExistenceError, ColumnExistenceError


META_FILE = ".meta"

class MetaHandler:
    def __init__(self, dbname=".", homedir="./") -> None:
        self._db_info = None
        self._db_name = dbname
        self._home_dir = homedir
        if os.path.exists(self._home_dir / self._db_name / (self._db_name + META_FILE)):
            self._load()
        else:
            self._db_info = DbInfo(dbname, [])
            self._dump()
    
    def add_table(self, table: TableInfo):
        self._db_info.insert_table(table)
        self._dump()
    
    def add_col(self, tbname, column: ColumnInfo):
        self._db_info.insert_column(tbname, column)
        self._dump()
    
    def drop_table(self, tbname):
        self._db_info.remove_table(tbname)
        self._dump()
    
    def drop_column(self, tbname, colname):
        self._db_info.remove_column(tbname, colname)
        self._dump()

    def get_column(self, tbname, colname) -> ColumnInfo:
        if tbname not in self._db_info._tbMap:
            return None
        table:TableInfo = self._db_info._tbMap[tbname]
        return table._colMap[colname]
    
    def get_column_index(self, tbname, colname) -> int:
        if tbname not in self._db_info._tbMap:
            return None
        table:TableInfo = self._db_info._tbMap[tbname]
        return table._colindex[colname]
    
    def get_table(self, tbname) -> TableInfo:
        # print(tbname)
        return self._db_info._tbMap[tbname]
    
    def update_index_root(self, tbname, colname, new_root):
        if self._db_info._tbMap.get(tbname) is None:
            raise TableExistenceError(f"Table {tbname} should exists.")
        table: TableInfo = self._db_info._tbMap[tbname]
        if table._colMap.get(colname) is None:
            raise ColumnExistenceError(f"Column {colname} should exists.")
        column:ColumnInfo = table._colMap[colname]
        assert column._is_index == True
        column._root_id = new_root
    
    def exists_index(self, index_name):
        return index_name in self._db_info._index_map

    def create_index(self, index_name, tbname, colname):
        self._db_info.create_index(index_name, tbname, colname)
        self._dump()
    
    def drop_index(self, index_name):
        self._db_info.drop_index(index_name)
        self._dump()
    
    def get_index_info(self, index_name):
        return self._db_info.get_index_info(index_name)
    
    def set_primary(self, tbname, primary):
        table: TableInfo = self._db_info._tbMap[tbname]
        table.set_primary(primary)
        self._dump()
    
    def add_foreign(self, tbname, col, foreign):
        table: TableInfo = self._db_info._tbMap[tbname]
        table.add_foreign(col, foreign)
        self._dump()
    
    def remove_foreign(self, tbname, col):
        table: TableInfo = self._db_info._tbMap[tbname]
        table.remove_foreign(col)
        self._dump()

    def close(self):
        self._dump()

    def _dump(self):
        outfile = open(self._home_dir / self._db_name / (self._db_name + META_FILE), "wb")
        pickle.dump(self._db_info, outfile)
        outfile.close()
    
    def _load(self):
        infile = open(self._home_dir / self._db_name / (self._db_name + META_FILE), 'rb')
        self._db_info = pickle.load(infile)
        infile.close()
    
    