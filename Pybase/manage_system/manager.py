"""
Here defines SystemManger class

Date: 2020/11/30
"""
from Pybase.record_system.record import Record
from pathlib import Path

from antlr4 import InputStream, CommonTokenStream
from antlr4.error.Errors import ParseCancellationException
from antlr4.error.ErrorStrategy import BailErrorStrategy

from Pybase import settings
from Pybase.manage_system.result import QueryResult
from Pybase.record_system.filescan import FileScan
from Pybase.sql_parser.SQLLexer import SQLLexer
from Pybase.sql_parser.SQLParser import SQLParser
from Pybase.sql_parser.SQLVisitor import SQLVisitor
from Pybase.file_system.manager import FileManager
from Pybase.record_system.manager import RecordManager
from Pybase.index_system.manager import IndexManager
from Pybase.meta_system.manager import MetaManager
from Pybase.exceptions.run_sql import DataBaseError
from Pybase.settings import (INDEX_FILE_SUFFIX, TABLE_FILE_SUFFIX, META_FILE_NAME)
from Pybase.meta_system.info import ColumnInfo, TableInfo, DbInfo
from Pybase.printer.table import TablePrinter

from .join import nested_loops_join


class SystemManger:
    """Class to manage the whole system"""

    def __init__(self, visitor: SQLVisitor, base_path: Path):
        self._FM = FileManager()
        self._RM = RecordManager(self._FM)
        self._IM = IndexManager(self._FM, base_path)
        self._MM = MetaManager(base_path)
        self._printer = TablePrinter()
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

    def get_table_name(self, table_name):
        assert self.using_db is not None
        return str(self.get_table_path(table_name)) + settings.TABLE_FILE_SUFFIX

    def execute(self, sql):
        class Strategy(BailErrorStrategy):
            def recover(self, recognizer, e):
                recognizer._errHandler.reportError(recognizer, e)
                super().recover(recognizer, e)

        input_stream = InputStream(sql)
        lexer = SQLLexer(input_stream)
        tokens = CommonTokenStream(lexer)
        parser = SQLParser(tokens)
        parser._errHandler = Strategy()
        try:
            tree = parser.program()
        except ParseCancellationException:
            return
        try:
            return self.visitor.visit(tree)
        except DataBaseError as e:
            return QueryResult(None, None, str(e))

    def create_db(self, name):
        if name in self.dbs:
            raise DataBaseError(f"Can't create existing database {name}")
        db_path = self.get_db_path(name)
        assert not db_path.exists()
        db_path.mkdir(parents=True)
        self.dbs.add(name)

    def drop_db(self, name):
        if name not in self.dbs:
            raise DataBaseError(f"Can't drop non-existing database {name}")
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
        '''
        # DEBUG INFO
        for i in tb_info._colindex.keys():
            print(i._name, ":", tb_info._colindex[i])
        '''
        record_length = tb_info.get_size()
        self._RM.create_file(self.get_table_name(tb_info._name), record_length)

    def drop_table(self, tbname):
        if self.using_db is None:
            raise DataBaseError(f"No using database to create table")
        meta_handle = self._MM.open_meta(self.using_db)
        meta_handle.drop_table(tbname)
        self._RM.remove_file(self.get_table_name(tbname))

    def describe_table(self, tbname):
        if self.using_db is None:
            raise DataBaseError(f"No using database to create table")
        meta_handle = self._MM.open_meta(self.using_db)
        tbInfo = meta_handle.get_table(tbname)
        desc = f"Table {tbInfo._name} (\n"
        for col in tbInfo._colMap.values():
            desc += f"\t{col._name} {col._type} {col._size}\n"
        desc += ")\n"
        desc += f"Size:{tbInfo.get_size()}\n"
        print(desc)
        header = ('Field', 'Type', 'Null', 'Key', 'Default', 'Extra')
        data = tuple((column.get_description()) for column in tbInfo._colMap.values())
        return QueryResult(header, data)


    def add_column(self, tbname, colinfo: ColumnInfo):
        pass

    def drop_column(self, tbname, colname):
        pass

    def create_index(self, tbname, colname):
        # Remember to get the size of colname
        pass

    def insert_record(self, tbname, value_list: list):
        # Remember to get the order in Record from meta
        if self.using_db is None:
            raise DataBaseError(f"No using database to insert record")
        meta_handle = self._MM.open_meta(self.using_db)
        tbInfo = meta_handle.get_table(tbname)
        record_handle = self._RM.open_file(self.get_table_name(tbname))
        # Build a record
        data = tbInfo.build_record(value_list)
        rid = record_handle.insert_record(data)
        # Insert to indexes
        # Other
        self._RM.close_file(self.get_table_name(tbname))

    def scan_record(self, tbname):
        if self.using_db is None:
            raise DataBaseError(f"No using database to scan")
        meta_handle = self._MM.open_meta(self.using_db)
        tbInfo = meta_handle.get_table(tbname)
        record_handle = self._RM.open_file(self.get_table_name(tbname))
        scanner = FileScan(record_handle)
        for record in scanner:
            print(tbInfo.load_record(record))
        self._RM.close_file(self.get_table_name(tbname))

    def print_results(self, result: QueryResult):
        self._printer.print(result)

    def cond_scan(self, tbname, conditions: tuple):
        '''
        condition is a list like:
        (tbname, colname, operator, value)
        or
        (tbname, colname, operator, )
        '''
        if self.using_db is None:
            raise DataBaseError(f"No using database to scan.")
        func_list = []
        meta_handle = self._MM.open_meta(self.using_db)

        def build_cond_func(condition):
            if condition[0] is None:
                condition[0] = tbname
            if condition[0] != tbname:
                return None
            tbInfo = meta_handle.get_table(condition[0])
            cond_index = tbInfo.get_col_index(condition[1])
            if cond_index is None:
                return None
            if len(condition) == 4:
                cond_func = eval(f"lambda x:x[{cond_index}] {condition[2]} {condition[3]}")
                return cond_func
            elif len(condition) == 5:
                if condition[3] != tbname:
                    return None
                cond_index_2 = tbInfo.get_col_index(condition[4])
                cond_func = eval(f"lambda x:x[{cond_index}] {condition[2]} x[{cond_index_2}]")
                return cond_func
            else:
                return None

        for condition in conditions:
            cond_func = build_cond_func(condition)
            if cond_func is not None:
                func_list.append(build_cond_func(condition))
        meta_handle = self._MM.open_meta(self.using_db)
        tbInfo = meta_handle.get_table(tbname)
        record_handle = self._RM.open_file(self.get_table_name(tbname))
        scanner = FileScan(record_handle)
        results = []
        for record in scanner:
            values = tbInfo.load_record(record)
            is_satisfied = True
            for cond_func in func_list:
                if not cond_func(values):
                    is_satisfied = False
                    break
            if is_satisfied:
                results.append(values)
        self._RM.close_file(self.get_table_name(tbname))
        headers = tbInfo.get_header()
        return QueryResult(headers, results)

    def cond_join(self, results_map: dict, conditions) -> QueryResult:
        if self.using_db is None:
            raise DataBaseError(f"No using database to scan.")
        join_pair_map = {}

        def build_join_pair(condition):
            if len(condition) != 5:
                return None
            if condition[0] == condition[3]:
                return None
            assert condition[2] == '=='
            if condition[0] < condition[3]:
                return (condition[0], condition[3]), (condition[1], condition[4])
            else:
                return (condition[3], condition[0]), (condition[4], condition[1])

        for condition in conditions:
            if build_join_pair(condition) is None:
                continue
            join_pair_key, join_pair_col = build_join_pair(condition)
            if join_pair_key in join_pair_map:
                join_pair_map[join_pair_key][0].append(join_pair_col[0])
                join_pair_map[join_pair_key][1].append(join_pair_col[1])
            else:
                join_pair_map[join_pair_key] = ([join_pair_col[0]], [join_pair_col[1]])
        # assert len(join_pair_map) > 0
        # 
        union_set = {key: key for key in results_map.keys()}

        def union_set_find(x):
            if x != union_set[x]:
                union_set[x] = union_set_find(union_set[x])
            return union_set[x]

        def union_set_union(x, y):
            x = union_set_find(x)
            y = union_set_find(y)
            union_set[x] = y

        results = None
        for join_pair in join_pair_map:
            outer: QueryResult = results_map[join_pair[0]]
            inner: QueryResult = results_map[join_pair[1]]
            outer_joined = tuple(join_pair[0] + "." + col for col in join_pair_map[join_pair][0])
            inner_joined = tuple(join_pair[1] + "." + col for col in join_pair_map[join_pair][1])
            new_result = nested_loops_join(outer, inner, outer_joined, inner_joined)
            union_set_union(join_pair[0], join_pair[1])
            new_key = union_set_find(join_pair[0])
            results_map[new_key] = new_result
            results = new_result
        return results
<<<<<<< HEAD
    
    def delete_records(self, tbname, conditions: tuple):
        if self.using_db is None:
            raise DataBaseError(f"No using database to scan.")
        func_list = []
        meta_handle = self._MM.open_meta(self.using_db)
        def build_cond_func(condition):
            if condition[0] is None:
                condition[0] = tbname
            if condition[0] != tbname:
                return None
            tbInfo = meta_handle.get_table(condition[0])
            cond_index = tbInfo.get_col_index(condition[1])
            if cond_index is None:
                return None
            if len(condition) == 4:
                cond_func = eval(f"lambda x:x[{cond_index}] {condition[2]} {condition[3]}")
                return cond_func
            elif len(condition) == 5:
                if condition[3] != tbname:
                    return None
                cond_index_2 = tbInfo.get_col_index(condition[4])
                cond_func = eval(f"lambda x:x[{cond_index}] {condition[2]} x[{cond_index_2}]")
                return cond_func
            else:
                return None
        for condition in conditions:
            cond_func = build_cond_func(condition)
            if cond_func is not None:
                func_list.append(build_cond_func(condition))
        meta_handle = self._MM.open_meta(self.using_db)
        tbInfo = meta_handle.get_table(tbname)
        record_handle = self._RM.open_file(self.get_table_name(tbname))
        scanner = FileScan(record_handle)
        results = []
        for record in scanner:
            values = tbInfo.load_record(record)
            is_satisfied = True
            for cond_func in func_list:
                if not cond_func(values):
                    is_satisfied = False
                    break
            if is_satisfied:
                results.append(record.rid)
        for rid in results:
            record_handle.delete_record(rid)
        self._RM.close_file(self.get_table_name(tbname))
    
    def update_records(self, tbname, conditions: tuple, set_value_map: list):
        if self.using_db is None:
            raise DataBaseError(f"No using database to scan.")
        func_list = []
        meta_handle = self._MM.open_meta(self.using_db)
        def build_cond_func(condition):
            if condition[0] is None:
                condition[0] = tbname
            if condition[0] != tbname:
                return None
            tbInfo = meta_handle.get_table(condition[0])
            cond_index = tbInfo.get_col_index(condition[1])
            if cond_index is None:
                return None
            if len(condition) == 4:
                cond_func = eval(f"lambda x:x[{cond_index}] {condition[2]} {condition[3]}")
                return cond_func
            elif len(condition) == 5:
                if condition[3] != tbname:
                    return None
                cond_index_2 = tbInfo.get_col_index(condition[4])
                cond_func = eval(f"lambda x:x[{cond_index}] {condition[2]} x[{cond_index_2}]")
                return cond_func
            else:
                return None
        for condition in conditions:
            cond_func = build_cond_func(condition)
            if cond_func is not None:
                func_list.append(build_cond_func(condition))
        meta_handle = self._MM.open_meta(self.using_db)
        tbInfo = meta_handle.get_table(tbname)
        record_handle = self._RM.open_file(self.get_table_name(tbname))
        scanner = FileScan(record_handle)
        results = []
        for record in scanner:
            values = tbInfo.load_record(record)
            is_satisfied = True
            for cond_func in func_list:
                if not cond_func(values):
                    is_satisfied = False
                    break
            if is_satisfied:
                results.append((record, values))
        for record_and_values in results:
            record = record_and_values[0]
            values = record_and_values[1]
            for pair in set_value_map:
                colname = pair[0]
                value = pair[1]
                real = tbInfo.get_value(colname, value)
                index = tbInfo.get_col_index(colname)
                values[index] = real
            record.data = tbInfo.build_record(values)
            record_handle.update_record(record)
        self._RM.close_file(self.get_table_name(tbname))
=======
>>>>>>> 9df662bdd2885ab15ae9304cfb149618eba40259
