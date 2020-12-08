"""
Here defines SystemManger class

Date: 2020/11/30
"""
from Pybase.meta_system.manager import MetaManager
from Pybase.index_system.manager import IndexManager
from Pybase.file_system.filemanager import FileManager
from pathlib import Path

from antlr4 import FileStream, CommonTokenStream

from Pybase.sql_parser.SQLLexer import SQLLexer
from Pybase.sql_parser.SQLParser import SQLParser
from Pybase.sql_parser.SQLVisitor import SQLVisitor
from Pybase.record_system.manager import RecordManager
# from Pybase.index_system.
from Pybase.exceptions.run_sql import DataBaseError
from Pybase.settings import (INDEX_FILE_SUFFIX, TABLE_FILE_SUFFIX, META_FILE_NAME)
from Pybase.meta_system.info import ColumnInfo, TableInfo, DbInfo


class SystemManger:
    """Class to manage the whole system"""

    def __init__(self, visitor: SQLVisitor, base_path: Path):
        self._FM = FileManager()
        self._RM = RecordManager(self._FM)
        self._IM = IndexManager(self._FM)
        self._MM = MetaManager(base_path)
        self._base_path = base_path
        base_path.mkdir(exist_ok=True, parents=True)
        self.dbs = {path.name for path in base_path.iterdir()}
        self.using_db = None
        self.visitor = visitor
        self.visitor.manager = self

    def get_db_path(self, db_name):
        return self._base_path / db_name

    def get_table_path(self, table_name):
        assert self.using_db is not None
        return self._base_path / self.using_db / table_name

    def execute(self, filename):
        input_stream = FileStream(filename, encoding='utf-8')
        lexer = SQLLexer(input_stream)
        tokens = CommonTokenStream(lexer)
        parser = SQLParser(tokens)
        tree = parser.program()
        try:
            return self.visitor.visit(tree)
        except DataBaseError as e:
            print(e)

    def create_db(self, name):
        if name in self.dbs:
            raise DataBaseError(f"Can't create existing database {name}")
        db_path = self.get_db_path(name)
        assert not db_path.exists()
        db_path.mkdir(parents=True)
        self.dbs.add(name)

    def drop_db(self, name):
        if name not in self.dbs:
            raise DataBaseError(f"Can't drop not existing database {name}")
        db_path = self.get_db_path(name)
        assert db_path.exists()
        for each in db_path.iterdir():
            if each.suffix == TABLE_FILE_SUFFIX and str(each) in self._RM.opened_files:
                self._RM.close_file(str(each))
            if each.suffix == INDEX_FILE_SUFFIX:
                pass
            each.unlink()
        db_path.rmdir()
        self.dbs.remove(name)
        if self.using_db == name:
            self.using_db = None

    def use_db(self, name):
        if name not in self.dbs:
            raise DataBaseError(f"Can't use not existing database {name}")
        self.using_db = name

    def show_tables(self):
        if self.using_db is None:
            raise DataBaseError(f"No using database to show tables")
        return [file.stem for file in (self._base_path / self.using_db).iterdir() if file.suffix == '.table']

    def create_table(self, tb_info: TableInfo):
        if self.using_db is None:
            raise DataBaseError(f"No using database to create table")
        meta_handle = self._MM.open_meta(self.using_db)
        meta_handle.add_table(tb_info)
        pass

    def drop_table(self, tbname):
        pass

    def add_column(self, tbname, colinfo: ColumnInfo):

        pass

    def drop_column(self, tbname, colname):
        pass

    def create_index(self, tbname, colname):
        # Remember to get the size of colname
        pass

    def insert_record(self, tbname, value):
        '''
        value is a dict like:
        {key: 1, value: 2}
        '''
        # Remember to get the order in Record from meta
        pass
