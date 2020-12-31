"""
Define FileExecutor to load file

Date: 2020/12/29
"""
from pathlib import Path
import csv
from tqdm import tqdm

from Pybase.manage_system.manager import SystemManger
from Pybase.exceptions.file import ExcutorFileError
from Pybase.exceptions.base import Error
from Pybase.manage_system.result import QueryResult


class FileExecutor:
    def __init__(self, bar):
        self.iterate = tqdm if bar else (lambda x: x)

    def insert_check(self, manager: SystemManger):
        if not manager.using_db:
            raise ExcutorFileError('No chosen database to insert')

    def exec_csv(self, manager: SystemManger, path: Path, table: str):
        if not table:
            table = path.stem.upper()
        self.insert_check(manager)
        inserted = 0
        for row in self.iterate(csv.reader(open(path, encoding='utf-8'))):
            if row[-1] == '':
                row = row[:-1]
            manager.insert_record(table, row)
            inserted += 1
        return [QueryResult('inserted_items', (inserted, ), cost=manager.visitor.time_cost())]

    def exec_tbl(self, manager: SystemManger, path: Path, table: str):
        if not table:
            table = path.stem.upper()
        self.insert_check(manager)
        inserted = 0
        for line in self.iterate(open(path, encoding='utf-8')):
            row = line.rstrip('\n').split('|')
            if row[-1] == '':
                row = row[:-1]
            manager.insert_record(table, row)
            inserted += 1
        return [QueryResult('inserted_items', (inserted, ), cost=manager.visitor.time_cost())]

    def exec_sql(self, manager: SystemManger, path: Path, table: str):
        return manager.execute(open(path, encoding='utf-8').read())

    def execute(self, manager: SystemManger, path: Path, table: str):
        manager.visitor.time_cost()
        suffix = path.suffix.lstrip('.')
        func = getattr(self, 'exec_' + suffix)
        if not func:
            return [QueryResult(message="Unsupported format " + suffix, cost=manager.visitor.time_cost())]
        try:
            return func(manager, path, table)
        except Error as e:
            return [QueryResult(message=str(e), cost=manager.visitor.time_cost())]
