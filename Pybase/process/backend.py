"""
Here defines backend's process

Date: 2020/12/30
"""
from multiprocessing.connection import Connection
from pathlib import Path

from Pybase.manage_system.manager import SystemManger
from Pybase.manage_system.visitor import SystemVisitor
from Pybase.utils.file_executor import FileExecutor


def backend(args, connection: Connection):
    visitor = SystemVisitor()
    bath_path = Path(args.base)
    with SystemManger(visitor, bath_path) as manager:
        if args.database:
            manager.use_db(args.database)
        manager.bar = args.bar
        while True:
            sql = connection.recv()
            if isinstance(sql, str):
                result = manager.execute(sql)
            elif isinstance(sql, tuple):
                file, database, table = sql
                executor = FileExecutor(args.bar)
                result = executor.execute(manager, file, database, table)
            elif sql is None:
                break
            else:
                print('Unknown message', sql)
                break
            connection.send(result)
