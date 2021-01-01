"""
Here defines Window to choose table when load a data file

Date: 2020/12/31
"""
from typing import Union, Tuple, Dict, Set
from pathlib import Path
from multiprocessing.connection import Connection
from datetime import timedelta, datetime
from enum import Enum
from queue import Queue

from PyQt5.QtWidgets import QMainWindow, QMessageBox, QFileDialog, QDialog
from PyQt5.QtGui import QStandardItemModel, QKeyEvent, QDragEnterEvent, QDropEvent, QTextDocument
from PyQt5.QtCore import QModelIndex, QTimer, Qt, QMutex, QWaitCondition, pyqtSlot, pyqtSignal

from .ui.table_choose_window import Ui_TableChooseWindow
from .worker import ReadOnlyItem


class TableChooseWindow(QDialog, Ui_TableChooseWindow):
    def __init__(self, using_db: str, tables: Dict[str, Set[str]], *args, obj=None, **kwargs):
        super(QDialog, self).__init__(*args, **kwargs)
        self.setupUi(self)
        self.using_db = using_db
        self.tables = tables
        self.combo_db.setModel(QStandardItemModel())
        self.combo_table.setModel(QStandardItemModel())
        model: QStandardItemModel = self.combo_db.model()
        for db in self.tables:
            model.appendRow(ReadOnlyItem(db))
        if using_db in self.tables:
            self.on_combo_db_currentTextChanged(using_db)

    @pyqtSlot(str)
    def on_combo_db_currentTextChanged(self, text):
        print(text)

    # def accept(self) -> None:
    #     pass

    # def reject(self) -> None:
    #     pass
