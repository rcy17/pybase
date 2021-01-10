"""
Here defines SystemManger class

Date: 2020/11/30
"""
from typing import Tuple
from pathlib import Path
from collections import defaultdict

from antlr4 import InputStream, CommonTokenStream
from antlr4.error.Errors import ParseCancellationException
from antlr4.error.ErrorListener import ErrorListener
from copy import deepcopy

from Pybase import settings
from Pybase.meta_system import MetaHandler, MetaManager, ColumnInfo, TableInfo, DbInfo
from Pybase.record_system import RID, FileScan, RecordManager, Record
from Pybase.index_system import FileIndex, IndexManager
from Pybase.sql_parser import SQLLexer, SQLParser
from Pybase.file_system import FileManager
from Pybase.exceptions.run_sql import DataBaseError, ConstraintError
from Pybase.exceptions.base import Error
from Pybase.printer.table import TablePrinter
from Pybase.meta_system.converter import Converter
from Pybase.utils.tools import compare_to_value, compare_to_attr, like_pattern, in_value_list, null_check
from .condition import ConditionType, Condition
from .result import QueryResult
from .selector import Selector, SelectorType
from .join import nested_loops_join


class SystemManger:
    """Class to manage the whole system"""

    def __init__(self, visitor, base_path: Path):
        self._FM = FileManager()
        self._RM = RecordManager(self._FM)
        self._IM = IndexManager(self._FM, base_path)
        self._MM = MetaManager(self._FM, base_path)
        self._base_path = base_path
        base_path.mkdir(exist_ok=True, parents=True)
        self.dbs = {path.name for path in base_path.iterdir()}
        self.using_db = None
        self.visitor = visitor
        self.visitor.manager = self
        self.bar = None  # Only for file input

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._MM.shutdown()
        self._IM.shutdown()
        self._RM.shutdown()
        self._FM.shutdown()

    def get_db_path(self, db_name):
        return self._base_path / db_name

    def _get_table_name(self, table_name):
        assert self.using_db is not None
        return self._base_path / self.using_db / table_name

    def get_table_path(self, table_name):
        assert self.using_db is not None
        return str(self._get_table_name(table_name)) + settings.TABLE_FILE_SUFFIX

    def execute(self, sql):
        # class Strategy(BailErrorStrategy):
        #     def recover(self, recognizer, e):
        #         recognizer._errHandler.reportError(recognizer, e)
        #         super().recover(recognizer, e)

        class StringErrorListener(ErrorListener):
            def syntaxError(self, recognizer, offending_symbol, line, column, msg, e):
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
        self._IM.close_handler(name)
        self._MM.close_meta(name)
        for each in db_path.iterdir():
            if each.suffix == settings.TABLE_FILE_SUFFIX:
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
        record_length = tb_info.total_size
        self._RM.create_file(self.get_table_path(tb_info._name), record_length)

    def drop_table(self, table_name):
        if self.using_db is None:
            raise DataBaseError(f"No using database to create table")
        meta_handle = self._MM.open_meta(self.using_db)
        meta_handle.drop_table(table_name)
        self._RM.remove_file(self.get_table_path(table_name))

    def get_table_info(self, table_name, error="execute sql"):
        if self.using_db is None:
            raise DataBaseError(f"No using database to {error}")
        meta_handle = self._MM.open_meta(self.using_db)
        table_info = meta_handle.get_table(table_name)
        return meta_handle, table_info

    def describe_table(self, table_name):
        _, table_info = self.get_table_info(table_name, "create table")
        desc = f"Table {table_info._name} (\n"
        for col in table_info.column_map.values():
            desc += f"\t{col._name} {col._type} {col._size}\n"
        desc += ")\n"
        desc += f"Size:{table_info.total_size}\n"
        desc += f"Indexes:{table_info.indexes.__str__()}\n"
        header = ('Field', 'Type', 'Null', 'Key', 'Default', 'Extra')
        data = tuple((column.get_description()) for column in table_info.column_map.values())
        return QueryResult(header, data)

    def add_foreign(self, table_name, col, foreign, foreign_name = None):
        meta_handle = self._MM.open_meta(self.using_db)
        meta_handle.add_foreign(table_name, col, foreign)
        if foreign_name is None:
            if not meta_handle.exists_index(foreign[0] + "." + foreign[1]):
                self.create_index(foreign[0] + "." + foreign[1], foreign[0], foreign[1])
        else:
            if not meta_handle.exists_index(foreign_name):
                self.create_index(foreign_name, foreign[0], foreign[1])

    def remove_foreign(self, table_name, col, foreign_name = None):
        if foreign_name is None:
            meta_handle = self._MM.open_meta(self.using_db)
            meta_handle.remove_foreign(table_name, col)
            foreign = meta_handle.get_table(table_name).foreign[col]
            self.drop_index(foreign[0] + "." + foreign[1])
        else:
            self.drop_index(foreign_name)

    def set_primary(self, table_name, primary):
        meta_handle = self._MM.open_meta(self.using_db)
        meta_handle.set_primary(table_name, primary)
        if primary is None:
            return
        for col in primary:
            if not meta_handle.exists_index(table_name + "." + col):
                self.create_index(table_name + "." + col, table_name, col)

    def drop_primary(self, table_name):
        meta_handle = self._MM.open_meta(self.using_db)
        primary = meta_handle.get_table(table_name).primary
        for col in primary:
            if meta_handle.exists_index(table_name + "." + col):
                self.drop_index(table_name + "." + col)

    def add_column(self, table_name, column_info: ColumnInfo):
        meta_handle, table_info = self.get_table_info(table_name, "add column")
        if table_info.get_col_index(column_info.name) is not None:
            raise DataBaseError(f"Column already exists.")
        old_table_info = deepcopy(table_info)
        meta_handle.add_col(table_name, column_info)

        self._RM.create_file(self.get_table_path(table_name + ".copy"), table_info.total_size)
        record_handle = self._RM.open_file(self.get_table_path(table_name))
        new_record_handle = self._RM.open_file(self.get_table_path(table_name + ".copy"))
        scanner = FileScan(record_handle)
        for record in scanner:
            value_list = list(old_table_info.load_record(record))
            if column_info.default is not None:
                value_list.append(column_info.default)
            else:
                value_list.append(None)
            data = table_info.build_record(value_list)
            new_record_handle.insert_record(data)
        self._RM.close_file(self.get_table_path(table_name))
        self._RM.close_file(self.get_table_path(table_name + ".copy"))
        # Rename
        self._RM.replace_file(self.get_table_path(table_name + ".copy"), self.get_table_path(table_name))

    def drop_column(self, table_name, column_name):
        meta_handle, table_info = self.get_table_info(table_name, "drop column")
        if table_info.get_col_index(column_name) is None:
            raise DataBaseError(f"Column not exists.")
        index = table_info.get_col_index(column_name)
        old_table_info = deepcopy(table_info)
        meta_handle.drop_column(table_name, column_name)

        self._RM.create_file(self.get_table_path(table_name + ".copy"), table_info.total_size)
        record_handle = self._RM.open_file(self.get_table_path(table_name))
        new_record_handle = self._RM.open_file(self.get_table_path(table_name + ".copy"))
        scanner = FileScan(record_handle)
        for record in scanner:
            value_list = list(old_table_info.load_record(record))
            value_list.pop(index)
            data = table_info.build_record(value_list)
            new_record_handle.insert_record(data)
        self._RM.close_file(self.get_table_path(table_name))
        self._RM.close_file(self.get_table_path(table_name + ".copy"))
        # Rename
        self._RM.replace_file(self.get_table_path(table_name + ".copy"), self.get_table_path(table_name))

    def create_index(self, index_name, table_name, column_name):
        meta_handle, table_info = self.get_table_info(table_name, "create index")
        if meta_handle.exists_index(index_name):
            raise DataBaseError(f"Indexes {index_name} not exists.")
        if table_info.exists_index(column_name):
            meta_handle.create_index(index_name, table_name, column_name)
            return 
        index = self._IM.create_index(self.using_db, table_name)
        table_info.create_index(column_name, index.root_id)
        col_id = table_info.get_col_index(column_name)
        if col_id is None:
            raise DataBaseError(f"Column not exists.")
        record_handle = self._RM.open_file(self.get_table_path(table_name))
        scanner = FileScan(record_handle)
        for record in scanner:
            data = table_info.load_record(record)
            key = data[col_id]
            index.insert(key, record.rid)
        meta_handle.create_index(index_name, table_name, column_name)

    def drop_index(self, index_name):
        if self.using_db is None:
            raise DataBaseError(f"No using database to create index")
        meta_handle = self._MM.open_meta(self.using_db)
        table_name, column_name = meta_handle.get_index_info(index_name)
        table_info = meta_handle.get_table(table_name)
        if not meta_handle.exists_index(index_name):
            raise DataBaseError(f"Indexes {index_name} not exists.")
        table_info.drop_index(column_name)
        meta_handle.drop_index(index_name)
        self._MM.close_meta(self.using_db)

    def rename_index(self, old_index, new_index):
        if self.using_db is None:
            raise DataBaseError(f"No using database to remove index")
        meta_handler = self._MM.open_meta(self.using_db)
        meta_handler.rename_index(old_index, new_index)

    def rename_table(self, old_name, new_name):
        if self.using_db is None:
            raise DataBaseError(f"No using database to rename table")
        meta_handle:TableInfo = self._MM.open_meta(self.using_db)
        meta_handle.rename_table(old_name, new_name)
        self._RM.rename_file(self.get_table_path(old_name), self.get_table_path(new_name))

    def insert_record(self, table_name, value_list: list):
        # Remember to get the order in Record from meta
        meta_handle, table_info = self.get_table_info(table_name, "insert record")
        # Build a record
        data = table_info.build_record(value_list)
        values = table_info.load_record(Record(RID(0, 0), data))
        # TODO:Check constraints
        self.check_insert_constraints(table_name, values)
        record_handle = self._RM.open_file(self.get_table_path(table_name))
        rid = record_handle.insert_record(data)
        # Handle indexes
        self.handle_insert_indexes(table_info, self.using_db, values, rid)

    @staticmethod
    def print_results(result: QueryResult):
        TablePrinter().print([result])

    @staticmethod
    def build_conditions_func(table_name, conditions, meta_handle: MetaHandler) -> list:
        def build_condition_func(condition: Condition):
            if condition.table_name and condition.table_name != table_name:
                return None
            cond_index = table_info.get_col_index(condition.column_name)
            if cond_index is None:
                raise DataBaseError(f'Field {condition.column_name} for table {table_name} is unknown')
            type_ = table_info.type_list[cond_index]
            if condition.type == ConditionType.Compare:
                if condition.target_column:
                    if condition.target_table != table_name:
                        return None
                    cond_index_2 = table_info.get_col_index(condition.target_column)
                    return compare_to_attr(cond_index, condition.operator, cond_index_2)
                else:
                    value = condition.value
                    if type_ in ('INT', 'FLOAT'):
                        if not isinstance(value, (int, float)):
                            raise DataBaseError(f"Expect {type_} but get '{value}' instead")
                    elif type_ == 'DATE':
                        value = Converter.parse_date(value)
                    elif type_ == 'VARCHAR':
                        if not isinstance(value, str):
                            raise DataBaseError(f'Expect VARCHAR but get {value} instead')
                    return compare_to_value(cond_index, condition.operator, value)
            elif condition.type == ConditionType.In:
                values = condition.value
                if type_ == 'DATE':
                    values = tuple(map(Converter.parse_date, values))
                return in_value_list(cond_index, values)
            elif condition.type == ConditionType.Like:
                if type_ != 'VARCHAR':
                    raise DataBaseError(f'Like operator expects VARCHAR but get {condition.column_name}:{type_}')
                return like_pattern(cond_index, condition.value)
            elif condition.type == ConditionType.Null:
                assert isinstance(condition.value, bool)
                return null_check(cond_index, condition.value)

        table_info = meta_handle.get_table(table_name)
        func_list = [func for func in (build_condition_func(condition) for condition in conditions) if func]
        return func_list

    @staticmethod
    def result_to_value(result: QueryResult, is_in):
        if len(result.headers) > 1:
            raise DataBaseError('Recursive select must return one column')
        value = sum(result.data, ())
        if not is_in:
            if len(result.data) != 1:
                raise DataBaseError(f'One value of {result.headers[0]} expected but got {len(result.data)}')
            value, = value
        return value

    def cond_join(self, results_map: dict, conditions) -> QueryResult:
        if self.using_db is None:
            raise DataBaseError(f"No using database to scan.")
        join_pair_map = {}

        def build_join_pair(condition: Condition):
            if condition.target_table and condition.table_name != condition.target_table:
                if condition.operator != '=':
                    raise DataBaseError('Comparison between different tables must be "="')
                pair = (condition.table_name, condition.column_name), (condition.target_table, condition.target_column)
                return tuple(zip(*sorted(pair)))
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

    def select_records(self, selectors: Tuple[Selector], table_names: Tuple[str, ...],
                       conditions: Tuple[Condition], group_by: Tuple[str, str]) -> QueryResult:
        def get_selected_data(column_to_data):
            column_to_data['*.*'] = next(iter(column_to_data.values()))
            return tuple(map(lambda selector: selector.select(column_to_data[selector.target()]), selectors))

        def set_table_name(item, table_name_attr, column_name_attr):
            _table = getattr(item, table_name_attr)
            _column = getattr(item, column_name_attr)
            if _column is None:
                return
            if _table is None:
                tables = column_to_table[_column]
                if len(tables) > 1:
                    raise DataBaseError(f'Field {_column} is ambiguous when joining on tables ')
                if not tables:
                    raise DataBaseError(f'Field {_column} is unknown')
                setattr(item, table_name_attr, tables[0])

        if self.using_db is None:
            raise DataBaseError(f"No using database to select.")
        group_table, group_column = group_by

        meta = self._MM.open_meta(self.using_db)
        column_to_table = meta.build_column_to_table_map(table_names)

        for each in conditions + selectors:
            if isinstance(each, Condition):
                set_table_name(each, 'target_table', 'target_column')
            set_table_name(each, 'table_name', 'column_name')

        group_table = group_table or table_names[0]
        group_by = group_table + '.' + group_column
        types = set(selector.type for selector in selectors)
        if not group_column and SelectorType.Field in types and len(types) > 1:
            raise DataBaseError("Select without group by shouldn't contain both field and aggregations")
        if not selectors and not group_column and len(table_names) == 1 and selectors[0].type == SelectorType.Counter:
            # COUNT(*) can has a shortcut from table.header['record_number']
            file = self._RM.open_file(self.get_table_path(table_names[0]))
            data = (file.header['record_number'],)
            headers = (selectors[0].to_string(False),)
            return QueryResult(headers, data)

        result_map = {table_name: self.cond_scan_index(table_name, conditions) for table_name in table_names}
        result = result_map[table_names[0]] if len(table_names) == 1 else self.cond_join(result_map, conditions)
        prefix = len(table_names) > 1
        if group_column:
            def make_row(group):
                _data_map = {_header: _data for _header, _data in zip(result.headers, zip(*group))}
                return get_selected_data(_data_map)

            index = result.get_header_index(group_by)
            groups = defaultdict(list)
            for row in result.data:
                groups[row[index]].append(row)
            if selectors[0].type == SelectorType.All:
                assert len(selectors) == 1
                data = tuple(group[0] for group in groups.values())
                return QueryResult(result.headers, data)
            data = tuple(map(make_row, groups.values()))
        else:
            if selectors[0].type == SelectorType.All:
                assert len(selectors) == 1
                return result
            if SelectorType.Field in types:  # No aggregation
                def take_columns(_row):
                    return tuple(_row[each] for each in indexes)

                headers = tuple(selector.target() for selector in selectors)
                indexes = tuple(result.get_header_index(header) for header in headers)
                data = tuple(map(take_columns, result.data))
            else:
                # Only aggregations
                if not result.data:
                    data = (None,) * len(result.headers)
                else:
                    data_map = {_header: _data for _header, _data in zip(result.headers, zip(*result.data))}
                    data = get_selected_data(data_map),
        # Reset headers regarding to prefix
        headers = tuple(selector.to_string(prefix) for selector in selectors)
        return QueryResult(headers, data)

    def delete_records(self, table_name, conditions: tuple):
        meta_handle, table_info = self.get_table_info(table_name, "delete")
        records, data = self.search_records_indexes(table_name, conditions)
        record_handle = self._RM.open_file(self.get_table_path(table_name))
        for record, values in zip(records, data):
            rid: RID = record.rid
            # TODO:Check Constraint
            self.check_remove_constraints(table_name, values)
            record_handle.delete_record(rid)
            # Handle Index
            self.handle_remove_indexes(table_info, self.using_db, values, rid)
        return QueryResult('deleted_items', (len(records),))

    def update_records(self, table_name, conditions: tuple, value_map: dict):
        meta_handle, table_info = self.get_table_info(table_name, "update")
        # TODO:
        records, values = self.search_records_indexes(table_name, conditions)
        record_handle = self._RM.open_file(self.get_table_path(table_name))
        table_info.check_value_map(value_map)
        for record, old_values in zip(records, values):
            new_values = list(old_values)
            # Modify values
            for column_name, value in value_map.items():
                index = table_info.get_col_index(column_name)
                new_values[index] = value
            # TODO:Check Constraint
            self.check_remove_constraints(table_name, old_values)
            self.check_insert_constraints(table_name, new_values, record.rid)

            # Handle indexes
            self.handle_remove_indexes(table_info, self.using_db, old_values, record.rid)

            record.update_data(table_info.build_record(new_values))
            record_handle.update_record(record)
            # Handle indexes
            self.handle_insert_indexes(table_info, self.using_db, new_values, record.rid)
        return QueryResult('updated_items', (len(records),))

    def index_filter(self, table_name, conditions) -> set:
        cond_index_map = {}
        meta_handle, table_info = self.get_table_info(table_name, "scan")

        def build_cond_index(condition: Condition):
            if condition.type != ConditionType.Compare or (condition.table_name and condition.table_name != table_name):
                return None
            cond_index = table_info.get_col_index(condition.column_name)
            if cond_index is not None and condition.value is not None and table_info.exists_index(
                    condition.column_name):
                operator = condition.operator
                column = condition.column_name
                lower, upper = cond_index_map.get(column, (-1 << 31 + 1, 1 << 31))
                value = int(condition.value)
                if operator == "=":
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
                else:
                    return None
                cond_index_map[column] = lower, upper

        tuple(map(build_cond_index, conditions))
        results = None
        for column_name in cond_index_map:
            _lower, _upper = cond_index_map[column_name]
            index = self._IM.open_index(self.using_db, table_name, table_info.indexes[column_name])
            if results is None:
                results = set(index.range(_lower, _upper))
            else:
                results &= set(index.range(_lower, _upper))
        return results

    def cond_scan_index(self, table_name, conditions: tuple) -> QueryResult:
        meta_handle, table_info = self.get_table_info(table_name, "scan")
        _, data = self.search_records_indexes(table_name, conditions)
        headers = table_info.get_header()
        return QueryResult(headers, data)

    def search_records_indexes(self, table_name, conditions: tuple):
        meta_handle, table_info = self.get_table_info(table_name, "scan")
        func_list = self.build_conditions_func(table_name, conditions, meta_handle)
        index_filter_rids = self.index_filter(table_name, conditions)
        record_handle = self._RM.open_file(self.get_table_path(table_name))
        iterator = map(record_handle.get_record, index_filter_rids) \
            if index_filter_rids is not None else FileScan(record_handle)
        records = []
        data = []
        for record in iterator:
            values = table_info.load_record(record)
            if all(map(lambda func: func(values), func_list)):
                records.append(record)
                data.append(values)
        return records, data

    def check_primary(self, table_name, values, this: RID = None):
        """
        Check primary key constraint if values are inserted
        Return False if nothing in wrong else primary key (keys, values)

        Parameter "this" means an updating is going and not check itself
        """
        meta_handle, table_info = self.get_table_info(table_name, "check primary")
        if not table_info.primary:
            return False
        results = None
        primary_key = {column_name: values[table_info.get_col_index(column_name)] for column_name in table_info.primary}
        for col, value in primary_key.items():
            index: FileIndex = self._IM.open_index(self.using_db, table_name, table_info.indexes[col])
            if results is None:
                results = set(index.range(value, value))
            else:
                results = results & set(index.range(value, value))
        if len(results) > 1:
            print(len(results))
            assert len(results) <= 1
        if this in results:
            return False
        return results and (tuple(primary_key.keys()), tuple(primary_key.values()))

    def check_foreign(self, table_name, values):
        """
        Check primary key constraint if values are inserted
        Return False if nothing in wrong else foreign key values
        """
        meta_handle, table_info = self.get_table_info(table_name, "check foreign")
        if len(table_info.foreign) == 0:
            return False
        for col in table_info.foreign:
            val = values[table_info.get_col_index(col)]
            foreign_table_name = table_info.foreign[col][0]
            foreign_column_name = table_info.foreign[col][1]
            foreign_table_info: TableInfo = meta_handle.get_table(foreign_table_name)
            root_id = foreign_table_info.indexes[foreign_column_name]
            index: FileIndex = self._IM.open_index(self.using_db, foreign_table_name, root_id)
            results = set(index.range(val, val))
            if len(results) == 0:
                return col, val
        return False

    def check_insert_constraints(self, table_name, values, this=None):
        # PRIMARY CONSTRAINT
        duplicated = self.check_primary(table_name, values, this)
        if duplicated:
            raise ConstraintError(f'Duplicated primary keys {duplicated[0]}: {duplicated[1]}, failed to insert')
        # UNIQUE CONSTRAINT

        # FOREIGN CONSTRAINT
        missing = self.check_foreign(table_name, values)
        if missing:
            raise ConstraintError(f"Missing foreign keys {missing[0]}: {missing[1]}, failed to insert")
        return True

    def check_remove_constraints(self, table_name, values):
        # FOERIGN CONSTRAINT

        return True

    def handle_insert_indexes(self, table_info: TableInfo, dbname, data, rid: RID):
        table_name = table_info.name
        for column_name in table_info.indexes:
            index: FileIndex = self._IM.open_index(dbname, table_name, table_info.indexes[column_name])
            col_id = table_info.get_col_index(column_name)
            if data[col_id] is not None:
                index.insert(data[col_id], rid)
            else:
                index.insert(settings.NULL_VALUE, rid)

    def handle_remove_indexes(self, table_info: TableInfo, dbname, data, rid: RID):
        table_name = table_info.name
        for column_name in table_info.indexes:
            index: FileIndex = self._IM.open_index(dbname, table_name, table_info.indexes[column_name])
            col_id = table_info.get_col_index(column_name)
            if data[col_id] is not None:
                index.remove(data[col_id], rid)
            else:
                index.remove(settings.NULL_VALUE, rid)
