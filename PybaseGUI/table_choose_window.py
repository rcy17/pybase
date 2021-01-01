"""
Here defines Window to choose table when load a data file

Date: 2020/12/31
"""
from typing import Dict, Set

from PyQt5.QtWidgets import QDialog, QDialogButtonBox
from PyQt5.QtCore import pyqtSlot

from .ui.table_choose_window import Ui_TableChooseWindow


class TableChooseWindow(QDialog, Ui_TableChooseWindow):
    def __init__(self, using_db: str, tables: Dict[str, Set[str]], table, *args, obj=None, **kwargs):
        super(QDialog, self).__init__(*args, **kwargs)
        self.setupUi(self)
        self.tables = tables
        self.buttonBox.button(QDialogButtonBox.Ok).setEnabled(False)
        for db in self.tables:
            self.combo_db.addItem(db, db)
        index = self.combo_db.findData(using_db)
        if index != -1:
            self.combo_db.setCurrentIndex(index)
        index = self.combo_table.findData(table)
        if index != -1:
            self.combo_table.setCurrentIndex(index)

    @pyqtSlot(str)
    def on_combo_db_currentTextChanged(self, text):
        self.combo_table.clear()
        self.combo_table.addItems(self.tables[text])

    @pyqtSlot(str)
    def on_combo_table_currentTextChanged(self, text):
        self.buttonBox.button(QDialogButtonBox.Ok).setEnabled(bool(text))
