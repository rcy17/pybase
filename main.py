"""
This is entry file for this project

Data: 2020/10/30
"""
import os
import stat
import sys
import tempfile
from pathlib import Path

from Pybase.manage_system.manager import SystemManger
from Pybase.manage_system.visitor import SystemVisitor


def main():
    visitor = SystemVisitor()
    bath_path = Path('data')
    manager = SystemManger(visitor, bath_path)
    if len(sys.argv) < 2:
        # python main.py
        mode = os.fstat(0).st_mode
        while True:
            if not stat.S_ISREG(mode):
                # if stdin is redirected, do not print
                print('pybase> ', end='')
            try:
                line = input()
            except (KeyboardInterrupt, EOFError):
                break
            if line in ('quit', 'exit', '.quit', '.exit'):
                break
            file = tempfile.NamedTemporaryFile('w', delete=False, encoding='utf-8')
            file.write(line)
            file.close()
            manager.execute(file.name)
            os.unlink(file.name)
    else:
        # python main.py <in.sql>
        manager.execute(sys.argv[1])


if __name__ == '__main__':
    main()
