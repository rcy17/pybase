"""
Worker is a thread to interactive with SQL processing

Date: 2020/12/31
"""
from typing import Tuple, Union, List
from datetime import datetime, timedelta
from multiprocessing.connection import Connection
from queue import Queue

from PyQt5.QtCore import QThread, pyqtSignal, QMutex, QWaitCondition
from PyQt5.QtGui import QStandardItem, QStandardItemModel

from Pybase.manage_system.result import QueryResult


class ReadOnlyItem(QStandardItem):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.setEditable(False)


class Worker(QThread):
    executed = pyqtSignal()
    finished = pyqtSignal(tuple)

    def __init__(self, connection: Connection, mutex: QMutex, wait_condition: QWaitCondition, queue: Queue):
        super(Worker, self).__init__()
        self.connection = connection
        self.mutex = mutex
        self.wait_condition = wait_condition
        self.queue = queue

    @staticmethod
    def make_models(results: List[QueryResult]) -> Tuple[Tuple[Union[str, QStandardItemModel], timedelta]]:
        def make_model(result) -> Union[str, QStandardItemModel]:
            if result is None:
                return ''
            if result.message:
                return result.message
            if result.database:
                return '#' + result.database
            model = QStandardItemModel()
            model.setHorizontalHeaderLabels(result.headers)
            for row in result.data:
                model.appendRow(ReadOnlyItem(str(item)) for item in row)
            return model
        return tuple((make_model(each), each.cost) for each in results)

    def work(self, sql) -> List[QueryResult]:
        self.connection.send(sql)
        return self.connection.recv()

    def wait_task(self):
        self.mutex.lock()
        self.wait_condition.wait(self.mutex)
        sql = self.queue.get()
        self.mutex.unlock()
        return sql

    def run(self) -> None:
        while True:
            task = self.wait_task()
            results = self.work(task)
            self.executed.emit()
            models = self.make_models(results)
            self.finished.emit(models)
