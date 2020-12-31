"""
Worker is a thread to interactive with SQL processing

Date: 2020/12/31
"""
from datetime import timedelta

from PyQt5.QtCore import QThread, pyqtSignal
from PyQt5.QtGui import QStandardItem, QStandardItemModel


class ReadOnlyItem(QStandardItem):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.setEditable(False)


class Worker(QThread):
    executed = pyqtSignal(timedelta)
    finished = pyqtSignal(QStandardItemModel)

    def run(self) -> None:
        pass
