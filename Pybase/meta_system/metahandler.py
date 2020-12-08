"""
"""

from math import inf
import pickle
import os
from pickle import dump

from numpy.lib.npyio import load

from Pybase import settings
from .info import TableInfo, ColumnInfo, DbInfo


META_FILE = ".meta"

class MetaHandler:
    def __init__(self, dbname="./") -> None:
        self._db_info = None
        self._db_name = dbname
        if os.path.exists(dbname + META_FILE):
            self._load()
        else:
            self._db_info = DbInfo(dbname, [])
            self._dump()
    
    def add_table(self, table: TableInfo):
        self._db_info.insert_table(table)
        self._dump()
    
    def add_col(self, tbname, column: ColumnInfo, colindex: int):
        self._db_info.insert_column(tbname, column, colindex)
        self._dump()
    
    def drop_table(self, tbname):
        self._db_info.remove_table(tbname)
        self._dump()
    
    def drop_column(self, tbname, colname):
        self._db_info.remove_column(tbname, colname)
        self._dump()

    def get_column(self, tbname, colname):
        table = self._db_info._tbMap.get(tbname)
        if table is None:
            return None
        else:
            return table._colMap.get(colname)
    
    def get_column_index(self, tbname, colname):
        table = self._db_info._tbMap.get(tbname)
        if table is None:
            return None
        else:
            return table._colIndex.get(colname)
    
    def get_table(self, tbname):
        return self._db_info._tbMap.get(tbname)
    
    def get_table_pid(self, tbname):
        if self._db_info._tbMap.get(tbname) is None:
            return None
        return self._db_info._tbMap.get(tbname)._pid
    
    def update_index_root(self, tbname, colname, new_root):
        if self._db_info._tbMap.get(tbname) is None:
            # Error
            pass
        table: TableInfo = self._db_info._tbMap[tbname]
        if table._colMap.get(colname) is None:
            # Error
            pass
        column = table._colMap[colname]
        assert column._is_index == True
        column._root_id = new_root

    def _dump(self):
        outfile = open(self._db_name + META_FILE, "w")
        pickle.dump(self._db_info, outfile)
        outfile.close()
    
    def _load(self):
        infile = open(self._db_name + META_FILE, 'r')
        self._db_info = pickle.load(infile)
        infile.close()
    
    