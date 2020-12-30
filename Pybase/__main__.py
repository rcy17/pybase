"""
A simple cmd entry for Pybase

Data: 2020/12/27
"""
import os
import stat
from pathlib import Path
from argparse import ArgumentParser, Namespace
from datetime import datetime
from multiprocessing import Pipe, Process

from Pybase.process.backend import backend
from Pybase.printer import TablePrinter, CSVPrinter
from Pybase.utils.file_executor import FileExecutor

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
    parser.add_argument('--bar', action='store_true', help='show progress bar when insert from file')
    parser.add_argument('database', nargs='?', type=str, help='database name')
    return parser


def main(args: Namespace):
    printer = NAME_TO_PRINTER[args.printer]()
    parent_conn, child_conn = Pipe()
    p = Process(target=backend, args=(args, child_conn))
    p.start()
    if args.file:
        p.join()
    else:
        sql = ''
        mode = os.fstat(0).st_mode
        while True:
            if not stat.S_ISREG(mode):
                # if stdin is redirected, do not print
                # prefix = f'pybase({manager.using_db})'
                prefix = f'pybase'
                print(('-'.rjust(len(prefix)) if sql else prefix) + '> ', end='')
            try:
                sql += input().strip()
            except (KeyboardInterrupt, EOFError):
                break
            if sql.lower() in ('quit', 'exit', '.quit', '.exit'):
                break
            if sql and sql[-1] == ';':
                parent_conn.send(sql)
                start = datetime.now()
                result = parent_conn.recv()
                stop = datetime.now()
                printer.print(result, stop - start)
                sql = ''
        p.terminate()


if __name__ == '__main__':
    main(get_parser().parse_args())
