"""
Define FileExecutor to load file

Date: 2020/12/29
"""
from pathlib import Path
import csv
from tqdm import tqdm

from Pybase.manage_system.manager import SystemManger
from Pybase.exceptions.file import ExcutorFileError
from Pybase.exceptions.run_sql import ConstraintError
from Pybase.exceptions.base import Error
from Pybase.manage_system.result import QueryResult


class FileExecutor:
    def __init__(self, bar):
        self.iterate = tqdm if bar else (lambda x: x)

    @staticmethod
    def _insert(manager, table, iterator, preprocess):
        def decoder(pair):
            value_, type_ = pair
            if type_ == 'INT':
                return int(value_) if value_ else None
            if type_ == 'FLOAT':
                return float(value_) if value_ else None
            if type_ == 'VARCHAR':
                return value_
            if type == 'DATE':
                return value_ if value_ else None

        inserted = 0
        _, table_info = manager.get_table_info(table, None)
        for row in iterator:
            row = preprocess(row)
            if row[-1] == '':
                row = row[:-1]
            row = tuple(map(decoder, zip(row, table_info.type_list)))
            try:
                manager.insert_record(table, row)
                inserted += 1
            except ConstraintError as e:
                # According to TA, we shouldn't abort insert if constraint failed
                print('[WARNING]', e)
        return inserted

    def exec_csv(self, manager: SystemManger, path: Path, database: str, table: str):
        if not table:
            table = path.stem.upper()
        manager.use_db(database)
        inserted = self._insert(manager, table, self.iterate(csv.reader(open(path, encoding='utf-8'))), lambda x: x)
        return [QueryResult('inserted_items', (inserted,), cost=manager.visitor.time_cost())]

    def exec_tbl(self, manager: SystemManger, path: Path, database: str, table: str):
        if not table:
            table = path.stem.upper()
        manager.use_db(database)
        inserted = self._insert(manager, table, self.iterate(open(path, encoding='utf-8')),
                                lambda x: x.rstrip('\n').split('|'))
        return [QueryResult('inserted_items', (inserted,), cost=manager.visitor.time_cost())]

    def exec_sql(self, manager: SystemManger, path: Path, database: str, table: str):
        if database:
            manager.use_db(database)
        return manager.execute(open(path, encoding='utf-8').read())

    def execute(self, manager: SystemManger, path: Path, database: str, table: str):
        manager.visitor.time_cost()
        suffix = path.suffix.lstrip('.')
        func = getattr(self, 'exec_' + suffix)
        if not func:
            return [QueryResult(message="Unsupported format " + suffix, cost=manager.visitor.time_cost())]
        try:
            return func(manager, path, database, table)
        except Error as e:
            return [QueryResult(message=str(e), cost=manager.visitor.time_cost())]
