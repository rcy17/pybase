"""
Here defines MainWindow for GUI

Date: 2020/12/30
"""
from pathlib import Path
from multiprocessing.connection import Connection
from datetime import timedelta, datetime

from PyQt5.QtWidgets import QMainWindow, QMessageBox
from PyQt5.QtGui import QStandardItem, QStandardItemModel, QKeyEvent
from PyQt5.QtCore import QModelIndex, QTimer, Qt

from .ui.main_window import Ui_MainWindow
from Pybase.manage_system.result import QueryResult


class ReadOnlyItem(QStandardItem):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.setEditable(False)


class MainWindow(QMainWindow, Ui_MainWindow):
    def __init__(self, connection: Connection, base: Path, *args, obj=None, **kwargs):
        super(MainWindow, self).__init__(*args, **kwargs)
        self.setupUi(self)
        self.showMaximized()
        self.connection = connection
        self.base_path = base
        self.tree_file.setModel(QStandardItemModel())
        self.table_result.setModel(QStandardItemModel())
        self.load_tree_file()
        self.timer = QTimer(self)
        self.timer.setInterval(10)
        self.timer.timeout.connect(self.check_result)
        self.last_start = None
        self.last_stop = None

    def load_tree_file(self):
        model: QStandardItemModel = self.tree_file.model()
        root = model.invisibleRootItem()
        for database in self.base_path.iterdir():
            if database.is_file():
                continue
            db = ReadOnlyItem(database.name)
            root.appendRow(db)
            table = ReadOnlyItem('table')
            schema = ReadOnlyItem('schema')
            db.appendRow(table)
            db.appendRow(schema)
            for file in database.glob('*.table'):
                table.appendRow(ReadOnlyItem(file.stem))
                schema.appendRow(ReadOnlyItem(file.stem))

    def show_result(self, result: QueryResult, cost: timedelta):
        if result is None:
            return
        if result.message:
            QMessageBox.critical(self, 'error', result.message)
            return
        model: QStandardItemModel = self.table_result.model()
        model.clear()
        model.setHorizontalHeaderLabels(result.headers)
        for row in result.data:
            model.appendRow(ReadOnlyItem(str(item)) for item in row)
        self.label_result.setText(f'用时{cost.total_seconds():.2f}秒，共{len(result.data)}个结果')

    def run_sql(self, sql):
        if self.timer.isActive():
            QMessageBox.critical(self, 'error', '当前有正在运行中的SQL')
            return
        print('run sql:', sql)
        self.connection.send(sql)
        self.timer.start()
        self.last_start = datetime.now()

    def check_result(self):
        if self.connection.poll():
            # self.last_stop = datetime.now()
            cost = datetime.now() - self.last_start
            self.timer.stop()
            result = self.connection.recv()
            self.show_result(result, cost)

    def on_tree_file_doubleClicked(self, index: QModelIndex):
        if index.parent().data() == 'table':
            sql = f'USE {index.parent().parent().data()};SELECT * FROM {index.data()};'
            self.run_sql(sql)
            print('show table', index.data())
        elif index.parent().data() == 'schema':
            sql = f'USE {index.parent().parent().data()};DESC {index.data()};'
            self.run_sql(sql)
            print('show schema', index.data())

    def keyPressEvent(self, event: QKeyEvent) -> None:
        if event.key() == Qt.Key_Return and event.modifiers() == Qt.ControlModifier:
            # This is ctrl + enter
            if self.focusWidget() == self.text_code:
                self.run_sql(self.text_code.toPlainText())
