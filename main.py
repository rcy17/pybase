"""
This is entry file for this project

Data: 2020/10/30
"""
import os
import stat
import sys
from pathlib import Path

from Pybase.manage_system.manager import SystemManger
from Pybase.manage_system.visitor import SystemVisitor
from Pybase.printer import TablePrinter, CSVPrinter


def main():
    visitor = SystemVisitor()
    bath_path = Path('data')
    manager = SystemManger(visitor, bath_path)
    printer = TablePrinter()
    sql = ''
    if len(sys.argv) < 2:
        # python main.py
        mode = os.fstat(0).st_mode
        while True:
            if not stat.S_ISREG(mode):
                # if stdin is redirected, do not print
                prefix = f'pybase({manager.using_db})'
                print(('-'.rjust(len(prefix)) if sql else prefix) + '> ', end='')
            try:
                sql += input().strip()
            except (KeyboardInterrupt, EOFError):
                break
            if sql.lower() in ('quit', 'exit', '.quit', '.exit'):
                break
            if sql and sql[-1] == ';':
                printer.print(manager.execute(sql))
                sql = ''
    else:
        # python main.py <in.sql>
        manager.execute(open(sys.argv[1], encoding='utf-8').read())


if __name__ == '__main__':
    main()
