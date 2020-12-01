"""
Here defines SystemManger class

Date: 2020/11/30
"""
from pathlib import Path

from antlr4 import FileStream, CommonTokenStream

from Pybase.sql_parser.SQLLexer import SQLLexer
from Pybase.sql_parser.SQLParser import SQLParser
from Pybase.record_system.manager import RecordManager
# from Pybase.index_system.
from Pybase.exceptions.run_sql import DateBaseError
from Pybase.settings import (INDEX_FILE_SUFFIX, TABLE_FILE_SUFFIX, META_FILE_NAME)


class SystemManger:
    """Class to manage the whole system"""
    def __init__(self, visitor, base_path: Path):
        self._RM = RecordManager()
        self._FM = self._RM.file_manager
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
        input_stream = FileStream(filename)
        lexer = SQLLexer(input_stream)
        tokens = CommonTokenStream(lexer)
        parser = SQLParser(tokens)
        tree = parser.program()
        try:
            self.visitor.visit(tree)
        except DateBaseError as e:
            print(e)

    def create_db(self, name):
        if name in self.dbs:
            raise DateBaseError(f"Can't create existing database {name}")
        db_path = self.get_db_path(name)
        assert not db_path.exists()
        db_path.mkdir(parents=True)
        self.dbs.add(name)

    def drop_db(self, name):
        if name not in self.dbs:
            raise DateBaseError(f"Can't drop not existing database {name}")
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
            raise DateBaseError(f"Can't use not existing database {name}")
        self.using_db = name

    def show_tables(self):
        if self.using_db is None:
            raise DateBaseError(f"No using database to show tables")
        for file in (self._base_path / self.using_db).iterdir():
            if file.suffix == '.table':
                print(file.stem)
