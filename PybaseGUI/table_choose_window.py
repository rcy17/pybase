"""
Here defines Window to choose table when load a data file

Date: 2020/12/31
"""
from typing import Union, Tuple
from pathlib import Path
from multiprocessing.connection import Connection
from datetime import timedelta, datetime
from enum import Enum
from queue import Queue

from PyQt5.QtWidgets import QMainWindow, QMessageBox, QFileDialog, QDialog
from PyQt5.QtGui import QStandardItemModel, QKeyEvent, QDragEnterEvent, QDropEvent, QTextDocument
from PyQt5.QtCore import QModelIndex, QTimer, Qt, QMutex, QWaitCondition, pyqtSlot, pyqtSignal

from .ui.table_choose_window import Ui_TableChooseWindow


class TableChooseWindow(QDialog, Ui_TableChooseWindow):
    def __init__(self, using_db: str, table: str, model: QStandardItemModel, *args, obj=None, **kwargs):
        super(QDialog, self).__init__(*args, **kwargs)
        self.setupUi(self)
        self.using_db = using_db
        self.table = table
        self.model = model


    # def accept(self) -> None:
    #     pass

    # def reject(self) -> None:
    #     pass
