"""
A simple cmd entry for Pybase

Data: 2020/12/27
"""
import os
import stat
import sys
from pathlib import Path
from argparse import ArgumentParser, Namespace

from Pybase.manage_system.manager import SystemManger
from Pybase.manage_system.visitor import SystemVisitor
from Pybase.printer import TablePrinter, CSVPrinter

NAME_TO_PRINTER = {
    'table': TablePrinter,
    'csv': CSVPrinter,
}


def get_parser() -> ArgumentParser:
    parser = ArgumentParser()
    parser.add_argument('-p', '--printer', type=str, choices=NAME_TO_PRINTER, default='table',
                        help='choose printer for query result')
    parser.add_argument('-b', '--base', type=Path, default='data',
                        help='base directory of all databases')
    parser.add_argument('-f', '--file', type=Path,
                        help='input file, support some different formats')
    parser.add_argument('-t', '--table', type=str, help='table to insert rows from given file')
    parser.add_argument('database', nargs='?', type=str, help='database name')
    return parser


class FileExecutor:
    @staticmethod
    def exec_csv(manager: SystemManger, path: Path):
        pass

    @staticmethod
    def exec_tbl(manager: SystemManger, path: Path):
        pass

    @staticmethod
    def exec_sql(manager: SystemManger, path: Path):
        manager.execute(open(sys.argv[1], encoding='utf-8').read())


def main(args: Namespace):
    visitor = SystemVisitor()
    bath_path = Path(args.base)
    manager = SystemManger(visitor, bath_path)
    printer = NAME_TO_PRINTER[args.printer]()
    if args.database:
        manager.use_db(args.database)
    if args.table:
        manager.target_table = args.table
    sql = ''
    if args.file:
        suffix = args.file.suffix.lstrip('.')
        func = getattr(FileExecutor, 'exec_' + suffix)
        if func:
            func(manager, args.file)
        else:
            raise NotImplementedError("unsupported format " + suffix)
    else:
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


if __name__ == '__main__':
    main(get_parser().parse_args())
