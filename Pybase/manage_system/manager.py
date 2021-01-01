"""
Here defines SystemManger class

Date: 2020/11/30
"""
from numpy.core.arrayprint import printoptions
from numpy.core.numeric import flatnonzero
from numpy.lib.function_base import insert
from Pybase.meta_system.metahandler import MetaHandler
from Pybase.record_system.rid import RID
from Pybase.index_system.fileindex import FileIndex
from datetime import datetime
from pathlib import Path

from antlr4 import InputStream, CommonTokenStream
from antlr4.error.Errors import ParseCancellationException
from antlr4.error.ErrorStrategy import BailErrorStrategy
from antlr4.error.ErrorListener import ErrorListener

from Pybase import settings
from Pybase.manage_system.result import QueryResult
from Pybase.record_system.filescan import FileScan
from Pybase.sql_parser import SQLLexer, SQLParser, SQLVisitor
from Pybase.file_system.manager import FileManager
from Pybase.record_system.manager import RecordManager
from Pybase.index_system.manager import IndexManager
from Pybase.meta_system.manager import MetaManager
from Pybase.exceptions.run_sql import DataBaseError
from Pybase.exceptions.base import Error
from Pybase.settings import (INDEX_FILE_SUFFIX, TABLE_FILE_SUFFIX, META_FILE_NAME)
from Pybase.meta_system.info import ColumnInfo, TableInfo, DbInfo
from Pybase.printer.table import TablePrinter

from .join import nested_loops_join


class SystemManger:
    """Class to manage the whole system"""

    def __init__(self, visitor, base_path: Path):
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
        self.bar = None             # Only for file input

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

        class StringErrorListener(ErrorListener):
            def syntaxError(self, recognizer, offendingSymbol, line, column, msg, e):
                raise ParseCancellationException("line " + str(line) + ":" + str(column) + " " + msg)
        self.visitor.time_cost()
        input_stream = InputStream(sql)
        lexer = SQLLexer(input_stream)
        lexer.removeErrorListeners()
        lexer.addErrorListener(StringErrorListener())
        tokens = CommonTokenStream(lexer)
        parser = SQLParser(tokens)
        parser.removeErrorListeners()
        parser.addErrorListener(StringErrorListener())
        # parser._errHandler = Strategy()
        try:
            tree = parser.program()
        except ParseCancellationException as e:
            return [QueryResult(None, None, str(e), cost=self.visitor.time_cost())]
        try:
            return self.visitor.visit(tree)
        except Error as e:
            return [QueryResult(message=str(e), cost=self.visitor.time_cost())]

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
            return QueryResult(change_db='None')

    def use_db(self, name):
        if name not in self.dbs:
            raise DataBaseError(f"Can't use not existing database {name}")
        self.using_db = name
        return QueryResult(change_db=name)

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
        desc += f"Indexes:{tbInfo.indexes.__str__()}\n"
        header = ('Field', 'Type', 'Null', 'Key', 'Default', 'Extra')
        data = tuple((column.get_description()) for column in tbInfo._colMap.values())
        return QueryResult(header, data)

    def add_foreign(self, tbname, col, foreign):
        meta_handle = self._MM.open_meta(self.using_db)
        meta_handle.add_foreign(tbname, col, foreign)
        if not meta_handle.exists_index(foreign[0] + "." + foreign[1]):
            self.create_index(foreign[0] + "." + foreign[1], foreign[0], foreign[1])
    
    def remove_foreign(self, tbname, col):
        meta_handle = self._MM.open_meta(self.using_db)
        meta_handle.remove_foreign(tbname, col)
        foreign = meta_handle.get_table(tbname).foreign[col]
        self.drop_index(foreign[0] + "." + foreign[1])

    def set_primary(self, tbname, primary):
        meta_handle = self._MM.open_meta(self.using_db)
        meta_handle.set_primary(tbname, primary)
        if primary is None:
            return
        for col in primary:
            if not meta_handle.exists_index(tbname + "." + col):
                self.create_index(tbname + "." + col, tbname, col)

    def add_column(self, tbname, colinfo: ColumnInfo):
        pass

    def drop_column(self, tbname, colname):
        pass

    def create_index(self, index_name, tbname, colname):
        if self.using_db is None:
            raise DataBaseError(f"No using database to create index")
        meta_handle = self._MM.open_meta(self.using_db)
        tbInfo = meta_handle.get_table(tbname)
        if tbInfo.exists_index(colname):
            raise DataBaseError(f"Indexes already exists.")
        index = self._IM.create_index(self.using_db, tbname, colname)
        tbInfo.create_index(colname, index.root_id)
        col_id = tbInfo.get_col_index(colname)
        if col_id is None:
            raise DataBaseError(f"Column not exists.")
        record_handle = self._RM.open_file(self.get_table_name(tbname))
        scanner = FileScan(record_handle)
        for record in scanner:
            data = tbInfo.load_record(record)
            key = data[col_id]
            index.insert(key, record.rid)
        meta_handle.create_index(index_name, tbname, colname)
        self._IM.close_index(tbname, colname)
        self._RM.close_file(self.get_table_name(tbname))
    
    def drop_index(self, index_name):
        if self.using_db is None:
            raise DataBaseError(f"No using database to create index")
        meta_handle = self._MM.open_meta(self.using_db)
        tbname, colname = meta_handle.get_index_info(index_name)
        tbInfo = meta_handle.get_table(tbname)
        if not tbInfo.exists_index(colname):
            raise DataBaseError(f"Indexes already exists.")
        tbInfo.drop_index(colname)
        meta_handle.drop_index(index_name)
        self._MM.close_meta(self.using_db)

    def insert_record(self, tbname, value_list: list):
        # Remember to get the order in Record from meta
        if self.using_db is None:
            raise DataBaseError(f"No using database to insert record")
        meta_handle = self._MM.open_meta(self.using_db)
        tbInfo = meta_handle.get_table(tbname)
        record_handle = self._RM.open_file(self.get_table_name(tbname))
        # Build a record
        data = tbInfo.build_record(value_list)
        values = tbInfo.load_record(Record(RID(0,0), data))
        rid = record_handle.insert_record(data)
        # TODO:Check constraints
        if not self.check_insert_constraints(tbname, values):
            raise DataBaseError("This record can not be inserted.")
        # Handle indexes
        self.handle_insert_indexes(tbInfo, self.using_db, values, rid)
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
        from datetime import timedelta
        self._printer.print(result, timedelta(0))

    def build_cond_func(self, tbname, conditions, meta_handle:MetaHandler) -> list:
        func_list = []
        def build_cond_func(condition):
            if condition[0] is None:
                condition = (tbname, *condition[1:])
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
        
        return func_list

    def cond_scan(self, tbname, conditions: tuple) -> QueryResult:
        '''
        condition is a list like:
        (tbname, colname, operator, value)
        or
        (tbname, colname, operator, )
        '''
        if self.using_db is None:
            raise DataBaseError(f"No using database to scan.")
        meta_handle = self._MM.open_meta(self.using_db)
        tbInfo = meta_handle.get_table(tbname)
        records = self.search_records(tbname, conditions)
        headers = tbInfo.get_header()
        results = tuple(tbInfo.load_record(record) for record in records)
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
    
    def delete_records(self, tbname, conditions: tuple):
        if self.using_db is None:
            raise DataBaseError(f"No using database to scan.")
        meta_handle = self._MM.open_meta(self.using_db)
        tbInfo = meta_handle.get_table(tbname)
        # TODO:
        records = self.search_records_indexes(tbname, conditions)
        record_handle = self._RM.open_file(self.get_table_name(tbname))
        for record in records:
            rid:RID = record.rid
            values = tbInfo.load_record(record.data)
            # TODO:Check Constraint
            if not self.check_insert_constraints(tbname, values):
                raise DataBaseError("This record can not be inserted.")
            record_handle.delete_record(rid)
            # Handle Index
            self.handle_remove_indexes(tbInfo, self.using_db, values, rid)
        self._RM.close_file(self.get_table_name(tbname))
        return QueryResult('deleted_items', (len(records), ))
    
    def update_records(self, tbname, conditions: tuple, set_value_map: dict):
        if self.using_db is None:
            raise DataBaseError(f"No using database to scan.")
        meta_handle = self._MM.open_meta(self.using_db)
        tbInfo = meta_handle.get_table(tbname)
        # TODO:
        records = self.search_records_indexes(tbname, conditions)
        record_handle = self._RM.open_file(self.get_table_name(tbname))
        for record in records:
            old_values = tbInfo.load_record(record)
            new_values = old_values
            # Modify values
            for pair in set_value_map.items():
                colname = pair[0]
                value = pair[1]
                real = tbInfo.get_value(colname, value)
                index = tbInfo.get_col_index(colname)
                new_values[index] = real
            # TODO:Check Constraint
            if not self.check_insert_constraints(tbname, old_values):
                raise DataBaseError("This record can not be deleted.")
            if not self.check_insert_constraints(tbname, new_values):
                raise DataBaseError("This record can not be inserted.")

            # Handle indexes
            self.handle_remove_indexes(tbInfo, self.using_db, old_values, record.rid)
            
            record.update_data(tbInfo.build_record(new_values))
            record_handle.update_record(record)
            # Handle indexes
            self.handle_insert_indexes(tbInfo, self.using_db, new_values, record.rid)
        self._RM.close_file(self.get_table_name(tbname))
        return QueryResult('updated_items', (len(records), ))


    def index_filter(self, tbname, conditions) -> QueryResult:
        if self.using_db is None:
            raise DataBaseError(f"No using database to scan.")
        cond_index_map = {}
        meta_handle = self._MM.open_meta(self.using_db)
        def build_cond_index(condition):
            if condition[0] is None:
                condition = (tbname, *condition[1:])
            if condition[0] != tbname:
                return None
            tbInfo = meta_handle.get_table(condition[0])
            cond_index = tbInfo.get_col_index(condition[1])
            if cond_index is None:
                return None
            if len(condition) == 4:
                if condition[1] in tbInfo.indexes:
                    if condition[1] not in cond_index_map:
                        cond_index_map[condition[1]] = [-1000000000,1000000000]
                    if condition[2] == "==":
                        lower = cond_index_map[condition[1]][0]
                        upper = cond_index_map[condition[1]][1]
                        val = int(condition[3])
                        if lower < val:
                            cond_index_map[condition[1]][0] = val
                        if upper > val:
                            cond_index_map[condition[1]][1] = val
                    elif condition[2] == "<":
                        upper = cond_index_map[condition[1]][1]
                        val = int(condition[3]) + 1
                        if upper > val:
                            cond_index_map[condition[1]][1] = val
                    elif condition[2] == ">":
                        lower = cond_index_map[condition[1]][0]
                        val = int(condition[3]) + 1
                        if lower < val:
                            cond_index_map[condition[1]][0] = val
                    elif condition[2] == "<=":
                        upper = cond_index_map[condition[1]][1]
                        val = int(condition[3])
                        if upper > val:
                            cond_index_map[condition[1]][1] = val
                    elif condition[2] == ">=":
                        lower = cond_index_map[condition[1]][0]
                        val = int(condition[3]) + 1
                        if lower < val:
                            cond_index_map[condition[1]][0] = val
                    else:
                        raise DataBaseError("No such operate.")
        for condition in conditions:
            build_cond_index(condition)
        tbInfo = meta_handle.get_table(tbname)
        results = None
        for colname in cond_index_map:
            lower = cond_index_map[colname][0]
            upper = cond_index_map[colname][1]
            index: FileIndex = self._IM.open_index(self.using_db, tbname, colname, tbInfo.indexes[colname])
            if results is None:
                results = set(index.range(lower, upper))
            else:
                results = results & (index.range(lower, upper))
        return results
    
    def cond_scan_index(self, tbname, conditions: tuple) -> QueryResult:
        if self.using_db is None:
            raise DataBaseError(f"No using database to scan.")
        meta_handle = self._MM.open_meta(self.using_db)
        records = self.search_records_indexes(tbname, conditions)
        tbInfo = meta_handle.get_table(tbname)
        headers = tbInfo.get_header()
        results = tuple(tbInfo.load_record(record) for record in records)
        return QueryResult(headers, results)    
    
    
    def search_records(self, tbname, conditions: tuple):
        if self.using_db is None:
            raise DataBaseError(f"No using database to scan.")
        meta_handle = self._MM.open_meta(self.using_db)
        func_list = self.build_cond_func(tbname, conditions, meta_handle)
        tbInfo = meta_handle.get_table(tbname)
        record_handle = self._RM.open_file(self.get_table_name(tbname))
        results = []
        scanner = FileScan(record_handle)
        for record in scanner:
            values = tbInfo.load_record(record)
            is_satisfied = True
            for cond_func in func_list:
                if not cond_func(values):
                    is_satisfied = False
                    break
            if is_satisfied:
                results.append(record)
        self._RM.close_file(self.get_table_name(tbname))
        return results
    
    def search_records_indexes(self, tbname, conditions: tuple):
        if self.using_db is None:
            raise DataBaseError(f"No using database to scan.")
        meta_handle = self._MM.open_meta(self.using_db)
        func_list = self.build_cond_func(tbname, conditions, meta_handle)
        tbInfo = meta_handle.get_table(tbname)
        index_filter_rids = self.index_filter(tbname, conditions)
        record_handle = self._RM.open_file(self.get_table_name(tbname))
        results = []
        if index_filter_rids is None:
            scanner = FileScan(record_handle)
            for record in scanner:
                values = tbInfo.load_record(record)
                is_satisfied = True
                for cond_func in func_list:
                    if not cond_func(values):
                        is_satisfied = False
                        break
                if is_satisfied:
                    results.append(record)
        else:
            for rid in index_filter_rids:
                record = record_handle.get_record(rid)
                values = tbInfo.load_record(record)
                is_satisfied = True
                for cond_func in func_list:
                    if not cond_func(values):
                        is_satisfied = False
                        break
                if is_satisfied:
                    results.append(record)
        self._RM.close_file(self.get_table_name(tbname))
        return results

    def check_primary(self, tbname, values):
        meta_handle = self._MM.open_meta(self.using_db)
        tbInfo: TableInfo = meta_handle.get_table(tbname)
        if tbInfo.primary is None:
            return True
        results = None
        for col in tbInfo.primary:
            val = values[tbInfo.get_col_index(col)]
            index: FileIndex = self._IM.open_index(self.using_db, tbname, col, tbInfo.indexes[col])
            if results is None:
                results = set(index.range(val, val))
            else:
                results = results & (index.range(val, val))
        return len(results) == 0
    
    def check_foreign(self, tbname, values):
        meta_handle = self._MM.open_meta(self.using_db)
        tbInfo: TableInfo = meta_handle.get_table(tbname)
        if len(tbInfo.foreign) == 0:
            return True
        results = None
        for col in tbInfo.foreign:
            val = values[tbInfo.get_col_index(col)]
            foreign_tbname = tbInfo.foreign[col][0]
            foreign_colname = tbInfo.foreign[col][1]
            foreign_tbInfo:TableInfo = meta_handle.get_table(foreign_tbname)
            root_id = foreign_tbInfo.indexes[foreign_tbname]
            index: FileIndex = self._IM.open_index(self.using_db, foreign_tbname, foreign_colname, root_id)
            if results is None:
                results = set(index.range(val, val))
            else:
                results = results & (index.range(val, val))
        return len(results) > 0

    def check_insert_constraints(self, tbname, values):
        # PRIMARY CONSTRAINT
        if not self.check_primary(tbname, values):
            return False
        # UNIQUE CONSTRAINT

        # FOREIGN CONSTRAINT
        if not self.check_foreign(tbname, values):
            return False
        return True
    
    def check_remove_constraints(self, tbname, values):
        # FOERIGN CONSTRAINT

        return True

    def handle_insert_indexes(self, tbInfo:TableInfo, dbname, data, rid:RID):
        tbname = tbInfo.name
        for colname in tbInfo.indexes:
            index:FileIndex = self._IM.open_index(dbname, tbname, colname, tbInfo.indexes[colname])
            col_id = tbInfo.get_col_index(colname)
            index.insert(data[col_id], rid)
            self._IM.close_index(tbname, colname)

    def handle_remove_indexes(self, tbInfo:TableInfo, dbname, data, rid:RID):
        tbname = tbInfo.name
        for colname in tbInfo.indexes:
            index:FileIndex = self._IM.open_index(dbname, tbname, colname, tbInfo.indexes[colname])
            col_id = tbInfo.get_col_index(colname)
            index.remove(data[col_id], rid)
            self._IM.close_index(tbname, colname)