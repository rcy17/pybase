"""
Define FileExecutor to load file

Date: 2020/12/29
"""
from pathlib import Path
import csv
from tqdm import tqdm

from Pybase.manage_system.manager import SystemManger
from Pybase.exceptions.file import ExcutorFileError


class FileExecutor:
    def __init__(self, bar):
        self.iterate = tqdm if bar else (lambda x: x)

    def insert_check(self, manager: SystemManger):
        if not manager.using_db:
            raise ExcutorFileError('No chosen database to insert')
        if not manager.target_table:
            raise ExcutorFileError('No chosen table to insert')

    def exec_csv(self, manager: SystemManger, path: Path):
        self.insert_check(manager)
        for row in self.iterate(csv.reader(open(path, encoding='utf-8'))):
            if row[-1] == '':
                row = row[:-1]
            manager.insert_record(manager.target_table, row)

    def exec_tbl(self, manager: SystemManger, path: Path):
        if not manager.target_table:
            manager.target_table = path.stem.upper()
        self.insert_check(manager)
        for line in self.iterate(open(path, encoding='utf-8')):
            row = line.rstrip('\n').split('|')
            if row[-1] == '':
                row = row[:-1]
            manager.insert_record(manager.target_table, row)

    def exec_sql(self, manager: SystemManger, path: Path):
        manager.execute(open(path, encoding='utf-8').read())

    def execute(self, manager: SystemManger, path: Path):
        suffix = path.suffix.lstrip('.')
        func = getattr(self, 'exec_' + suffix)
        if not func:
            raise ExcutorFileError("Unsupported format " + suffix)
        func(manager, path)
