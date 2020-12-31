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
    manager = SystemManger(visitor, bath_path)
    if args.database:
        manager.use_db(args.database)
    manager.target_table = args.table
    manager.bar = args.bar
    if args.file:
        executor = FileExecutor(args.bar)
        connection.send(executor.execute(manager, args.file))
    else:
        while True:
            sql = connection.recv()
            if isinstance(sql, str):
                result = manager.execute(sql)
            elif isinstance(sql, tuple):
                file, table, bar = sql
            else:
                print('Unknown message', sql)
                break
            connection.send(result)
