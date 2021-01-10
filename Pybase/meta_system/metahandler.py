"""
"""

from Pybase.exceptions.run_sql import DataBaseError
from math import inf
import pickle
import os
from collections import defaultdict
from pickle import dump

from numpy.lib.npyio import load

from Pybase import settings
from .info import TableInfo, ColumnInfo, DbInfo
from Pybase.exceptions.meta import TableExistenceError, ColumnExistenceError

META_FILE = ".meta"


class MetaHandler:
    def __init__(self, dbname, homedir) -> None:
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
        table: TableInfo = self._db_info._tbMap[tbname]
        return table.column_map[colname]

    def get_column_index(self, tbname, colname) -> int:
        if tbname not in self._db_info._tbMap:
            return None
        table: TableInfo = self._db_info._tbMap[tbname]
        return table._colindex[colname]

    def get_table(self, tbname) -> TableInfo:
        # print(tbname)
        if tbname not in self._db_info._tbMap:
            raise DataBaseError(f"There is not table named {tbname}")
        return self._db_info._tbMap[tbname]

    def update_index_root(self, tbname, colname, new_root):
        if self._db_info._tbMap.get(tbname) is None:
            raise TableExistenceError(f"Table {tbname} should exists.")
        table: TableInfo = self._db_info._tbMap[tbname]
        if table.column_map.get(colname) is None:
            raise ColumnExistenceError(f"Column {colname} should exists.")
        column: ColumnInfo = table.column_map[colname]
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
        table: TableInfo = self.get_table(tbname)
        table.set_primary(primary)
        self._dump()

    def drop_primary(self, tbname):
        table: TableInfo = self.get_table(tbname)
        table.primary = None
        self._dump()

    def add_foreign(self, tbname, col, foreign):
        table: TableInfo = self.get_table(tbname)
        table.add_foreign(col, foreign)
        self._dump()

    def remove_foreign(self, tbname, col):
        table: TableInfo = self.get_table(tbname)
        table.remove_foreign(col)
        self._dump()

    def add_unique(self, table: TableInfo, column, unique):
        table.add_unique(column, unique)
        self._dump()

    def rename_table(self, old_name, new_name):
        if old_name not in self._db_info._tbMap:
            raise DataBaseError(f"Table {old_name} not in database.")
        tbInfo = self._db_info._tbMap[old_name]
        self._db_info._tbMap.pop(old_name)
        self._db_info._tbMap[new_name] = tbInfo
        for index_name in self._db_info._index_map:
            pair = self._db_info._index_map[index_name]
            if pair[0] == old_name:
                self._db_info._index_map[index_name] = (new_name, pair[1])
        self._dump()

    def rename_index(self, old_index, new_index):
        self._db_info._index_map[new_index] = self._db_info._index_map[old_index]
        self._db_info._index_map.pop(old_index)
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

    def build_column_to_table_map(self, table_names):
        column_to_table = defaultdict(list)
        for table in table_names:
            table_info = self.get_table(table)
            for column in table_info.column_map:
                column_to_table[column].append(table)
        return column_to_table
