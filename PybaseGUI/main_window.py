"""
Here defines MainWindow for GUI

Date: 2020/12/30
"""
from typing import Union, Tuple
from pathlib import Path
from multiprocessing.connection import Connection
from datetime import timedelta, datetime
from enum import Enum
from queue import Queue

from PyQt5.QtWidgets import QMainWindow, QMessageBox
from PyQt5.QtGui import QStandardItemModel, QKeyEvent
from PyQt5.QtCore import QModelIndex, QTimer, Qt, QMutex, QWaitCondition, pyqtSlot

from .ui.main_window import Ui_MainWindow
from .worker import ReadOnlyItem, Worker


class Status(Enum):
    Waiting = '空闲'
    Running = '运行中'
    Building = '加载中'
    Rendering = '渲染中'


class MainWindow(QMainWindow, Ui_MainWindow):
    def __init__(self, connection: Connection, base: Path, *args, obj=None, **kwargs):
        super(MainWindow, self).__init__(*args, **kwargs)
        self.setupUi(self)
        self.showMaximized()
        self.connection = connection
        self.base_path = base
        self.tree_file.setModel(QStandardItemModel())
        self.load_tree_file()

        self.timer = QTimer(self)
        self.timer.setInterval(100)
        self.timer.timeout.connect(self.check_result)

        self.mutex = QMutex()
        self.wait_condition = QWaitCondition()
        self.queue = Queue(1)
        self.worker = Worker(connection, self.mutex, self.wait_condition, self.queue)
        self.worker.executed.connect(self.worker_execute)
        self.worker.finished.connect(self.worker_finish)
        self.worker.start()

        self.last_start = None
        self.status = None
        self.cost = None
        self.results = []
        self.tasks = []

        self.set_status(Status.Waiting)
        self.set_result_report()

    @pyqtSlot()
    def worker_execute(self):
        self.set_status(Status.Building)

    @pyqtSlot(tuple)
    def worker_finish(self, args: Tuple[Union[QStandardItemModel, None, str], timedelta]):
        result, cost = args
        self.set_status(Status.Rendering)
        self.show_result(result, cost)
        self.timer.stop()
        self.set_status(Status.Waiting)

    def set_status(self, status: Status, cost=None):
        self.status = status
        msg = '状态：' + status.value
        if cost:
            msg += '，已进行%.1fs' % cost.total_seconds()
        self.statusbar.showMessage(msg)

    def set_result_report(self, size: int = 0, cost: timedelta = timedelta(0)):
        self.label_result.setText(f'用时{cost.total_seconds():.2f}秒，共{size}个结果')

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

    def show_result(self, result: Union[QStandardItemModel, None, str], cost: timedelta):
        if result is None:
            self.set_result_report(0, cost)
            return
        if isinstance(result, str):
            self.set_result_report(0, cost)
            QMessageBox.critical(self, 'error', result)
            return
        self.results.append((result, cost))
        self.table_result.setModel(result)
        self.table_result.resizeColumnsToContents()
        self.set_result_report(result.rowCount(), cost)

    def run_sql(self, sql):
        if self.status != Status.Waiting:
            QMessageBox.critical(self, 'error', '请等待当前SQL运行、渲染完成后再执行新的SQL')
            return
        self.mutex.lock()
        self.queue.put(sql)
        self.wait_condition.wakeAll()
        self.mutex.unlock()
        self.timer.start()
        self.last_start = datetime.now()
        self.set_status(Status.Running)

    def check_result(self):
        cost = datetime.now() - self.last_start
        self.set_status(self.status, cost)

    def on_tree_file_doubleClicked(self, index: QModelIndex):
        if index.parent().data() == 'table':
            sql = f'USE {index.parent().parent().data()};SELECT * FROM {index.data()};'
            self.run_sql(sql)
        elif index.parent().data() == 'schema':
            sql = f'USE {index.parent().parent().data()};DESC {index.data()};'
            self.run_sql(sql)

    def keyPressEvent(self, event: QKeyEvent) -> None:
        if event.key() == Qt.Key_Return and event.modifiers() == Qt.ControlModifier:  # ctrl + enter
            if self.focusWidget() == self.text_code:
                self.run_sql(self.text_code.toPlainText())

    def on_button_clear_clicked(self):
        self.table_result.model().clear()
        self.set_result_report()
