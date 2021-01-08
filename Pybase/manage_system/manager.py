"""
Here defines SystemManger class

Date: 2020/11/30
"""
from pathlib import Path
import re

from antlr4 import InputStream, CommonTokenStream
from antlr4.error.Errors import ParseCancellationException
from antlr4.error.ErrorStrategy import BailErrorStrategy
from antlr4.error.ErrorListener import ErrorListener

from Pybase import settings
from Pybase.meta_system import MetaHandler, MetaManager, ColumnInfo, TableInfo, DbInfo
from Pybase.record_system import RID, FileScan, RecordManager, Record
from Pybase.index_system import FileIndex, IndexManager
from Pybase.sql_parser import SQLLexer, SQLParser
from Pybase.file_system import FileManager
from Pybase.exceptions.run_sql import DataBaseError
from Pybase.exceptions.base import Error
from Pybase.printer.table import TablePrinter
from .condition import ConditionType, Condition
from .result import QueryResult

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
        self.bar = None  # Only for file input

    def get_db_path(self, db_name):
        return self._base_path / db_name

    def get_table_path(self, table_name):
        assert self.using_db is not None
        return self._base_path / self.using_db / table_name

    def get_table_name(self, table_name):
        assert self.using_db is not None
        return str(self.get_table_path(table_name)) + settings.TABLE_FILE_SUFFIX

    def execute(self, sql):
        # class Strategy(BailErrorStrategy):
        #     def recover(self, recognizer, e):
        #         recognizer._errHandler.reportError(recognizer, e)
        #         super().recover(recognizer, e)

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
            if each.suffix == settings.TABLE_FILE_SUFFIX and str(each) in self._RM.opened_files:
                self._RM.close_file(str(each))
            if each.suffix == settings.INDEX_FILE_SUFFIX:
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

    def drop_table(self, table_name):
        if self.using_db is None:
            raise DataBaseError(f"No using database to create table")
        meta_handle = self._MM.open_meta(self.using_db)
        meta_handle.drop_table(table_name)
        self._RM.remove_file(self.get_table_name(table_name))

    def describe_table(self, table_name):
        if self.using_db is None:
            raise DataBaseError(f"No using database to create table")
        meta_handle = self._MM.open_meta(self.using_db)
        table_info = meta_handle.get_table(table_name)
        desc = f"Table {table_info._name} (\n"
        for col in table_info._colMap.values():
            desc += f"\t{col._name} {col._type} {col._size}\n"
        desc += ")\n"
        desc += f"Size:{table_info.get_size()}\n"
        desc += f"Indexes:{table_info.indexes.__str__()}\n"
        header = ('Field', 'Type', 'Null', 'Key', 'Default', 'Extra')
        data = tuple((column.get_description()) for column in table_info._colMap.values())
        return QueryResult(header, data)

    def add_foreign(self, table_name, col, foreign):
        meta_handle = self._MM.open_meta(self.using_db)
        meta_handle.add_foreign(table_name, col, foreign)
        if not meta_handle.exists_index(foreign[0] + "." + foreign[1]):
            self.create_index(foreign[0] + "." + foreign[1], foreign[0], foreign[1])

    def remove_foreign(self, table_name, col):
        meta_handle = self._MM.open_meta(self.using_db)
        meta_handle.remove_foreign(table_name, col)
        foreign = meta_handle.get_table(table_name).foreign[col]
        self.drop_index(foreign[0] + "." + foreign[1])

    def set_primary(self, tbname, primary):
        meta_handle = self._MM.open_meta(self.using_db)
        meta_handle.set_primary(table_name, primary)
        if primary is None:
            return
        for col in primary:
            if not meta_handle.exists_index(tbname + "." + col):
                self.create_index(tbname + "." + col, tbname, col)
    
    def drop_primary(self, tbname):
        meta_handle = self._MM.open_meta(self.using_db)
        primary = meta_handle.get_table(tbname).primary
        meta_handle.drop_column(tbname)
        for col in primary:
            if meta_handle.exists_index(tbname + "." + col):
                self.drop_index(tbname + "." + col)

    def add_column(self, table_name, column_info: ColumnInfo):
        if self.using_db is None:
            raise DataBaseError(f"No using database to add column")
        meta_handle = self._MM.open_meta(self.using_db)
        table_info = meta_handle.get_table(table_name)
        if table_info.get_col_index(column_info._name) is not None:
            raise DataBaseError(f"Column already exists.")
        old_table_info = table_info
        meta_handle.add_col(table_name, column_info)

        record_handle = self._RM.open_file(self.get_table_name(table_name))
        new_record_handle = self._RM.open_file(self.get_table_name(table_name + ".copy"))
        scanner = FileScan(record_handle)
        for record in scanner:
            value_list = old_table_info.load_record(record)
            if column_info._default is not None:
                value_list.append(column_info._default)
            else:
                value_list.append(settings.NULL_VALUE)
            data = table_info.build_record(value_list)
            new_record_handle.insert_record(data)
        self._RM.close_file(self.get_table_name(table_name))
        self._RM.close_file(self.get_table_name(table_name + ".copy"))
        # Rename
        self._RM.replace_file(self.get_table_name(table_name + ".copy"), self.get_table_name(table_name))

    def drop_column(self, table_name, column_name):
        if self.using_db is None:
            raise DataBaseError(f"No using database to drop column")
        meta_handle = self._MM.open_meta(self.using_db)
        table_info = meta_handle.get_table(table_name)
        if table_info.get_col_index(column_name) is not None:
            raise DataBaseError(f"Column already exists.")
        index = table_info.get_col_index(column_name)
        old_table_info = table_info
        meta_handle.drop_column(table_name, column_name)
        record_handle = self._RM.open_file(self.get_table_name(table_name))
        new_record_handle = self._RM.open_file(self.get_table_name(table_name + ".copy"))
        scanner = FileScan(record_handle)
        for record in scanner:
            value_list = old_table_info.load_record(record)
            value_list.pop(index)
            data = table_info.build_record(value_list)
            new_record_handle.insert_record(data)
        self._RM.close_file(self.get_table_name(table_name))
        self._RM.close_file(self.get_table_name(table_name + ".copy"))
        # Rename
        self._RM.replace_file(self.get_table_name(table_name + ".copy"), self.get_table_name(table_name))

    def create_index(self, index_name, table_name, column_name):
        if self.using_db is None:
            raise DataBaseError(f"No using database to create index")
        meta_handle = self._MM.open_meta(self.using_db)
        table_info = meta_handle.get_table(table_name)
        if table_info.exists_index(column_name):
            raise DataBaseError(f"Indexes already exists.")
        index = self._IM.create_index(self.using_db, table_name, column_name)
        table_info.create_index(column_name, index.root_id)
        col_id = table_info.get_col_index(column_name)
        if col_id is None:
            raise DataBaseError(f"Column not exists.")
        record_handle = self._RM.open_file(self.get_table_name(table_name))
        scanner = FileScan(record_handle)
        for record in scanner:
            data = table_info.load_record(record)
            key = data[col_id]
            index.insert(key, record.rid)
        meta_handle.create_index(index_name, table_name, column_name)
        self._IM.close_index(table_name, column_name)
        self._RM.close_file(self.get_table_name(table_name))

    def drop_index(self, index_name):
        if self.using_db is None:
            raise DataBaseError(f"No using database to create index")
        meta_handle = self._MM.open_meta(self.using_db)
        table_name, column_name = meta_handle.get_index_info(index_name)
        table_info = meta_handle.get_table(table_name)
        if not table_info.exists_index(column_name):
            raise DataBaseError(f"Indexes already exists.")
        table_info.drop_index(column_name)
        meta_handle.drop_index(index_name)
        self._MM.close_meta(self.using_db)
    
    def rename_index(self, old_index, new_index):
        if self.using_db is None:
            raise DataBaseError(f"No using database to create index")
        meta_handler = self._MM.open_meta(self.using_db)
        meta_handler.rename_index(old_index, new_index)

    def rename_column(self, tbname, oldname, newname):
        if self.using_db is None:
            raise DataBaseError(f"No using database to create index")
        meta_handler = self._MM.open_meta(self.using_db)
        meta_handler.rename_col(tbname, oldname, newname)
        # Primary
        # Foreign
    

    def insert_record(self, table_name, value_list: list):
        # Remember to get the order in Record from meta
        if self.using_db is None:
            raise DataBaseError(f"No using database to insert record")
        meta_handle = self._MM.open_meta(self.using_db)
        table_info = meta_handle.get_table(table_name)
        # Build a record
        data = table_info.build_record(value_list)
        values = table_info.load_record(Record(RID(0, 0), data))
        # TODO:Check constraints
        if not self.check_insert_constraints(table_name, values):
            raise DataBaseError("This record can not be inserted.")
        record_handle = self._RM.open_file(self.get_table_name(table_name))
        rid = record_handle.insert_record(data)
        # Handle indexes
        self.handle_insert_indexes(table_info, self.using_db, values, rid)
        # Other
        self._RM.close_file(self.get_table_name(table_name))

    def print_results(self, result: QueryResult):
        self._printer.print([result])

    @staticmethod
    def build_regex_from_sql_like(pattern: str):
        pattern = pattern.replace('%%', '\r').replace('%?', '\n').replace('%_', '\0')
        pattern = re.escape(pattern)
        pattern = pattern.replace('%', '.*').replace(r'\?', '.').replace('_', '.')
        pattern = pattern.replace('\r', '%').replace('\n', r'\?').replace('\0', '_')
        return re.compile('^' + pattern + '$')

    def build_conditions_func(self, table_name, conditions, meta_handle: MetaHandler) -> list:
        def build_condition_func(condition: Condition):
            if condition.table_name != table_name:
                return None
            cond_index = table_info.get_col_index(condition.column_name)
            if cond_index is None:
                return None
            if condition.type == ConditionType.Compare:
                if condition.target_column:
                    if condition.target_table != table_name:
                        return None
                    cond_index_2 = table_info.get_col_index(condition.target_column)
                    return eval(f"lambda x:x[{cond_index}] {condition.operator} x[{cond_index_2}]")
                else:
                    return eval(f"lambda x:x[{cond_index}] {condition.operator} {condition.value}")
            elif condition.type == ConditionType.In:
                return lambda x: x[cond_index] in condition.value
            elif condition.type == ConditionType.Like:
                pattern = self.build_regex_from_sql_like(condition.value)
                return lambda x: pattern.match(x)

        table_info = meta_handle.get_table(table_name)
        func_list = [func for func in (build_condition_func(condition) for condition in conditions) if func]
        return func_list

    def cond_scan(self, table_name, conditions: tuple) -> QueryResult:
        """
        condition is a list like:
        (table_name, column_name, operator, table_name, column_name)
        or
        (table_name, column_name, operator, value)
        """
        if self.using_db is None:
            raise DataBaseError(f"No using database to scan.")
        meta_handle = self._MM.open_meta(self.using_db)
        table_info = meta_handle.get_table(table_name)
        records = self.search_records(table_name, conditions)
        headers = table_info.get_header()
        results = tuple(table_info.load_record(record) for record in records)
        return QueryResult(headers, results)

    def cond_join(self, results_map: dict, conditions) -> QueryResult:
        if self.using_db is None:
            raise DataBaseError(f"No using database to scan.")
        join_pair_map = {}

        def build_join_pair(condition: Condition):
            if condition.target_table and condition.table_name != condition.target_table:
                assert condition.operator == '=='
                pair = (condition.table_name, condition.column_name), (condition.target_table, condition.target_column)
                return sorted(pair)
            return None, None

        for join_pair_key, join_pair_col in map(build_join_pair, conditions):
            if join_pair_col is None:
                continue
            if join_pair_key in join_pair_map:
                join_pair_map[join_pair_key][0].append(join_pair_col[0])
                join_pair_map[join_pair_key][1].append(join_pair_col[1])
            else:
                join_pair_map[join_pair_key] = ([join_pair_col[0]], [join_pair_col[1]])

        if not join_pair_map:
            raise DataBaseError('Join tables need join condition')
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

    def select_records(self, table_names: tuple, conditions: tuple):
        if len(table_names) > 1 and any(condition.table_name is None for condition in conditions):
            raise DataBaseError('Filed without table name is forbidden when join on tables ')
        for condition in conditions:
            if condition.table_name is None:
                # If there is still any condition with None table name, we can ensure that there is only one table
                condition.table_name = table_names[0]
        result_map = {table_name: self.cond_scan_index(table_name, conditions) for table_name in table_names}
        return result_map[table_names[0]] if len(table_names) == 1 else self.cond_join(result_map, conditions)

    def delete_records(self, table_name, conditions: tuple):
        if self.using_db is None:
            raise DataBaseError(f"No using database to scan.")
        meta_handle = self._MM.open_meta(self.using_db)
        table_info = meta_handle.get_table(table_name)
        # TODO:
        records = self.search_records_indexes(table_name, conditions)
        record_handle = self._RM.open_file(self.get_table_name(table_name))
        for record in records:
            rid: RID = record.rid
            values = table_info.load_record(record.data)
            # TODO:Check Constraint
            if not self.check_insert_constraints(table_name, values):
                self._RM.close_file(self.get_table_name(table_name))
                raise DataBaseError("This record can not be inserted.")
            record_handle.delete_record(rid)
            # Handle Index
            self.handle_remove_indexes(table_info, self.using_db, values, rid)
        self._RM.close_file(self.get_table_name(table_name))
        return QueryResult('deleted_items', (len(records),))

    def update_records(self, table_name, conditions: tuple, set_value_map: dict):
        if self.using_db is None:
            raise DataBaseError(f"No using database to scan.")
        meta_handle = self._MM.open_meta(self.using_db)
        table_info = meta_handle.get_table(table_name)
        # TODO:
        records = self.search_records_indexes(table_name, conditions)
        record_handle = self._RM.open_file(self.get_table_name(table_name))
        for record in records:
            old_values = table_info.load_record(record)
            new_values = old_values
            # Modify values
            for pair in set_value_map.items():
                column_name = pair[0]
                value = pair[1]
                real = table_info.get_value(column_name, value)
                index = table_info.get_col_index(column_name)
                new_values[index] = real
            # TODO:Check Constraint
            if not self.check_insert_constraints(table_name, old_values):
                raise DataBaseError("This record can not be deleted.")
            if not self.check_insert_constraints(table_name, new_values):
                raise DataBaseError("This record can not be inserted.")

            # Handle indexes
            self.handle_remove_indexes(table_info, self.using_db, old_values, record.rid)

            record.update_data(table_info.build_record(new_values))
            record_handle.update_record(record)
            # Handle indexes
            self.handle_insert_indexes(table_info, self.using_db, new_values, record.rid)
        self._RM.close_file(self.get_table_name(table_name))
        return QueryResult('updated_items', (len(records),))

    def index_filter(self, table_name, conditions) -> set:
        if self.using_db is None:
            raise DataBaseError(f"No using database to scan.")
        cond_index_map = {}
        meta_handle = self._MM.open_meta(self.using_db)

        def build_cond_index(condition: Condition):
            if condition.type != ConditionType.Compare or condition.table_name != table_name:
                return None
            cond_index = table_info.get_col_index(condition.column_name)
            if cond_index and condition.value is not None:
                operator = condition.operator
                column = condition.column_name
                lower, upper = cond_index_map.get(column, (-1 << 32, 1 << 32))
                value = int(condition.value)
                if operator == "==":
                    lower = max(lower, value)
                    upper = min(upper, value)
                elif operator == "<":
                    upper = min(upper, value - 1)
                elif operator == ">":
                    lower = max(lower, value + 1)
                elif operator == "<=":
                    upper = min(upper, value)
                elif operator == ">=":
                    lower = max(lower, value)
                cond_index_map[column] = lower, upper

        table_info = meta_handle.get_table(table_name)
        tuple(map(build_cond_index, conditions))
        results = None
        for column_name in cond_index_map:
            _lower, _upper = cond_index_map[column_name]
            index = self._IM.open_index(self.using_db, table_name, column_name, table_info.indexes[column_name])
            if results is None:
                results = set(index.range(_lower, _upper))
            else:
                results &= set(index.range(_lower, _upper))
        return results

    def cond_scan_index(self, table_name, conditions: tuple) -> QueryResult:
        if self.using_db is None:
            raise DataBaseError(f"No using database to scan.")
        meta_handle = self._MM.open_meta(self.using_db)
        records = self.search_records_indexes(table_name, conditions)
        table_info = meta_handle.get_table(table_name)
        headers = table_info.get_header()
        results = tuple(table_info.load_record(record) for record in records)
        return QueryResult(headers, results)

    def search_records(self, table_name, conditions: tuple):
        if self.using_db is None:
            raise DataBaseError(f"No using database to scan.")
        meta_handle = self._MM.open_meta(self.using_db)
        func_list = self.build_conditions_func(table_name, conditions, meta_handle)
        table_info = meta_handle.get_table(table_name)
        record_handle = self._RM.open_file(self.get_table_name(table_name))
        results = []
        scanner = FileScan(record_handle)
        for record in scanner:
            values = table_info.load_record(record)
            is_satisfied = True
            for cond_func in func_list:
                if not cond_func(values):
                    is_satisfied = False
                    break
            if is_satisfied:
                results.append(record)
        self._RM.close_file(self.get_table_name(table_name))
        return results

    def search_records_indexes(self, table_name, conditions: tuple):
        if self.using_db is None:
            raise DataBaseError(f"No using database to scan.")
        meta_handle = self._MM.open_meta(self.using_db)
        func_list = self.build_conditions_func(table_name, conditions, meta_handle)
        table_info = meta_handle.get_table(table_name)
        index_filter_rids = self.index_filter(table_name, conditions)
        record_handle = self._RM.open_file(self.get_table_name(table_name))
        iterator = map(record_handle.get_record, index_filter_rids) if index_filter_rids else FileScan(record_handle)
        results = []
        for record in iterator:
            values = table_info.load_record(record)
            if all(map(lambda func: func(values), func_list)):
                results.append(record)
        self._RM.close_file(self.get_table_name(table_name))
        return results

    def check_primary(self, table_name, values):
        meta_handle = self._MM.open_meta(self.using_db)
        table_info: TableInfo = meta_handle.get_table(table_name)
        if not table_info.primary:
            return True
        results = None
        for col in table_info.primary:
            val = values[table_info.get_col_index(col)]
            index: FileIndex = self._IM.open_index(self.using_db, table_name, col, table_info.indexes[col])
            if results is None:
                results = set(index.range(val, val))
            else:
                results = results & set(index.range(val, val))
            self._IM.close_index(table_name, col)
        return len(results) == 0

    def check_foreign(self, table_name, values):
        meta_handle = self._MM.open_meta(self.using_db)
        table_info: TableInfo = meta_handle.get_table(table_name)
        if len(table_info.foreign) == 0:
            return True
        results = None
        for col in table_info.foreign:
            val = values[table_info.get_col_index(col)]
            foreign_table_name = table_info.foreign[col][0]
            foreign_column_name = table_info.foreign[col][1]
            foreign_table_info: TableInfo = meta_handle.get_table(foreign_table_name)
            root_id = foreign_table_info.indexes[foreign_table_name]
            index: FileIndex = self._IM.open_index(self.using_db, foreign_table_name, foreign_column_name, root_id)
            if results is None:
                results = set(index.range(val, val))
            else:
                results = results & set(index.range(val, val))
            self._IM.close_index(table_name, col)
        return len(results) > 0

    def check_insert_constraints(self, table_name, values):
        # PRIMARY CONSTRAINT
        if not self.check_primary(table_name, values):
            return False
        # UNIQUE CONSTRAINT

        # FOREIGN CONSTRAINT
        if not self.check_foreign(table_name, values):
            return False
        return True

    def check_remove_constraints(self, table_name, values):
        # FOERIGN CONSTRAINT

        return True

    def handle_insert_indexes(self, table_info: TableInfo, dbname, data, rid: RID):
        table_name = table_info.name
        for column_name in table_info.indexes:
            index: FileIndex = self._IM.open_index(dbname, table_name, column_name, table_info.indexes[column_name])
            col_id = table_info.get_col_index(column_name)
            if data[col_id] is not None:
                index.insert(data[col_id], rid)
            else:
                index.insert(settings.NULL_VALUE, rid)
            self._IM.close_index(table_name, column_name)

    def handle_remove_indexes(self, table_info: TableInfo, dbname, data, rid: RID):
        table_name = table_info.name
        for column_name in table_info.indexes:
            index: FileIndex = self._IM.open_index(dbname, table_name, column_name, table_info.indexes[column_name])
            col_id = table_info.get_col_index(column_name)
            if data[col_id] is not None:
                index.remove(data[col_id], rid)
            else:
                index.remove(settings.NULL_VALUE, rid)
            self._IM.close_index(table_name, column_name)
