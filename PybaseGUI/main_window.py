"""
Here defines MainWindow for GUI

Date: 2020/12/30
"""
from typing import Union, Tuple, Dict, Set
from pathlib import Path
from multiprocessing.connection import Connection
from datetime import timedelta, datetime
from enum import Enum
from queue import Queue

from PyQt5.QtWidgets import QMainWindow, QMessageBox, QFileDialog
from PyQt5.QtGui import QStandardItemModel, QKeyEvent, QDragEnterEvent, QDropEvent, QTextDocument
from PyQt5.QtCore import QModelIndex, QTimer, Qt, QMutex, QWaitCondition, pyqtSlot, pyqtSignal

from .ui.main_window import Ui_MainWindow
from .table_choose_window import TableChooseWindow
from .worker import ReadOnlyItem, Worker
from .syntax_highlighter import sql as SQLHighlighter


class Status(Enum):
    Waiting = '空闲'
    Running = '运行中'
    Building = '加载中'
    Rendering = '渲染中'


class MainWindow(QMainWindow, Ui_MainWindow):
    page_changed = pyqtSignal(int)
    sql_chosen = pyqtSignal(Path)
    data_chosen = pyqtSignal(Path)
    table_chosen = pyqtSignal(Path)
    sql_finished = pyqtSignal()

    def __init__(self, connection: Connection, base: Path, *args, obj=None, **kwargs):
        super(MainWindow, self).__init__(*args, **kwargs)
        self.setupUi(self)
        self.connection = connection
        self.base_path = base
        self.last_start = None
        self.status = None
        self.cost = None
        self.using_db = None
        self.current_page = -1
        self.results = []
        self.tasks = []
        self.tables: Dict[str, Set[str]] = {}

        self.tree_file.setModel(QStandardItemModel())
        self.update_tree_file()

        self.timer = QTimer(self)
        self.timer.setInterval(100)
        self.timer.timeout.connect(self.update_cost)

        self.mutex = QMutex()
        self.wait_condition = QWaitCondition()
        self.queue = Queue(1)
        self.worker = Worker(connection, self.mutex, self.wait_condition, self.queue)
        self.worker.executed.connect(self.worker_execute)
        self.worker.finished.connect(self.worker_finish)
        self.worker.start()

        self.document = QTextDocument(self)
        self.processing_highlight = False
        self.document.setDefaultStyleSheet(SQLHighlighter.style())
        self.text_code.setDocument(self.document)

        self.set_status(Status.Waiting)
        self.set_result_report()
        self.change_page()
        self.update_buttons()
        self.change_db(None)

        self.page_changed.connect(self.change_page)
        self.page_changed.connect(self.show_result)
        self.page_changed.connect(self.update_buttons)
        self.sql_finished.connect(self.update_tree_file)

    @pyqtSlot()
    def worker_execute(self):
        self.set_status(Status.Building)

    @pyqtSlot(tuple)
    def worker_finish(self, args: Tuple[Tuple[Union[QStandardItemModel, str], timedelta]]):
        self.set_status(Status.Rendering)
        error = None
        for model, cost in args:
            if not model:
                continue
            elif isinstance(model, QStandardItemModel):
                self.results.append((model, cost))
            elif model.startswith('#'):
                self.change_db(model[1:])
            else:
                error = model
        if self.results:
            self.page_changed.emit(-1)
        self.timer.stop()
        if error:
            self.error_report('执行错误', error)
        self.set_status(Status.Waiting)
        self.sql_finished.emit()

    def set_status(self, status: Status, cost=None):
        self.status = status
        msg = '状态：' + status.value
        if cost:
            msg += '，已进行%.1fs' % cost.total_seconds()
        self.statusbar.showMessage(msg)

    def show_result(self, page: int):
        result, cost = self.results[page]
        self.table_result.setModel(result)
        self.table_result.resizeColumnsToContents()
        self.set_result_report(result.rowCount(), cost)

    def change_page(self, page: int = -1):
        pages = len(self.results)
        if page < 0:
            page += pages
        self.current_page = page
        self.label_page.setText(f'{self.current_page + 1}/{pages}')

    def update_buttons(self, page: int = -1):
        self.button_next.setEnabled(page < len(self.results) - 1)
        self.button_last.setEnabled(bool(page and self.results))
        self.button_clear.setEnabled(bool(self.results))

        self.action_next.setEnabled(self.button_next.isEnabled())
        self.action_last.setEnabled(self.button_last.isEnabled())
        self.action_clear.setEnabled(self.button_clear.isEnabled())

    def change_db(self, database):
        self.using_db = database
        self.label_db.setText('当前数据库: %s' % database)

    def set_result_report(self, size: int = 0, cost: timedelta = timedelta(0)):
        self.label_result.setText(f'用时{cost.total_seconds():.2f}秒，共{size}个结果')

    def update_tree_file(self):
        model: QStandardItemModel = self.tree_file.model()
        root = model.invisibleRootItem()
        old_databases = set(self.tables)  # remember current databases in GUI
        for database in self.base_path.iterdir():
            if database.is_file():
                continue
            db_name = database.name
            db = None
            for row in range(root.rowCount()):
                if root.child(row, 0).text() == db_name:
                    db = root.child(row, 0)
                    break
            if db is None:
                db = ReadOnlyItem(db_name)
                db.appendRow(ReadOnlyItem('table'))
                db.appendRow(ReadOnlyItem('schema'))
                self.tables[db_name] = set()
                root.appendRow(db)
                old_tables = set()
            else:
                old_databases.remove(db_name)  # ensured this database still exists
                old_tables = set(self.tables[db_name])  # remember current tables in GUI
            table = db.child(0, 0)
            schema = db.child(1, 0)
            for file in database.glob('*.table'):
                table_name = file.stem
                if table_name in self.tables[db_name]:
                    old_tables.remove(table_name)  # ensured this table still exists
                    continue
                self.tables[db_name].add(table_name)
                table.appendRow(ReadOnlyItem(table_name))
                schema.appendRow(ReadOnlyItem(table_name))
            for row in reversed(range(table.rowCount())):
                if table.child(row, 0).text() in old_tables:    # Remove redundant tables in GUI
                    self.tables[db_name].remove(table.child(row, 0).text())
                    table.removeRow(row)
                    schema.removeRow(row)
        for row in reversed(range(root.rowCount())):  # remove redundant databases in GUI
            if root.child(row, 0).text() in old_databases:
                self.tables.pop(root.child(row, 0).text())
                root.removeRow(row)

    def run_sql(self, sql):
        if self.status != Status.Waiting:
            self.error_report('系统忙碌', '请等待当前SQL运行、渲染完成后再执行新的SQL')
            return
        self.mutex.lock()
        self.queue.put(sql)
        self.wait_condition.wakeAll()
        self.mutex.unlock()

        self.timer.start()
        self.last_start = datetime.now()
        self.set_status(Status.Running)

    def update_cost(self):
        cost = datetime.now() - self.last_start
        self.set_status(self.status, cost)

    def load_sql(self, path):
        try:
            self.document.setPlainText(open(path, encoding='utf-8').read())
        except Exception as e:
            self.error_report(type(e).__name__, str(e))

    def load_data(self, path):
        table = path.stem.upper()
        window = TableChooseWindow(self.using_db, self.tables, table, parent=self)
        if window.exec():
            self.run_sql((path, table))

    def error_report(self, title, content):
        QMessageBox.critical(self, title, content)

    @pyqtSlot(QModelIndex)
    def on_tree_file_doubleClicked(self, index: QModelIndex):
        if index.parent().data() == 'table':
            sql = f'USE {index.parent().parent().data()};SELECT * FROM {index.data()};'
            self.run_sql(sql)
        elif index.parent().data() == 'schema':
            sql = f'USE {index.parent().parent().data()};DESC {index.data()};'
            self.run_sql(sql)

    @pyqtSlot()
    def on_text_code_textChanged(self):
        if self.processing_highlight:
            self.processing_highlight = False
        else:
            self.processing_highlight = True
            sql = self.text_code.toPlainText()
            highlighted = SQLHighlighter.highlight(sql)
            cursor = self.text_code.textCursor()
            cursor_position = cursor.position()
            self.document.setHtml(highlighted)
            cursor.setPosition(cursor_position)
            self.text_code.setTextCursor(cursor)

    def keyPressEvent(self, event: QKeyEvent) -> None:
        if event.key() == Qt.Key_Return:  # enter
            if event.modifiers() in (Qt.ControlModifier, Qt.AltModifier):  # alt + enter or ctrl + enter
                self.run_sql(self.text_code.toPlainText())

    @pyqtSlot()
    def on_button_clear_clicked(self):
        if self.table_result.model():
            self.table_result.model().clear()
        self.results.clear()
        self.change_page()
        self.update_buttons()
        self.set_result_report()

    @pyqtSlot()
    def on_button_next_clicked(self):
        if self.button_next.isEnabled():
            self.page_changed.emit(self.current_page + 1)

    @pyqtSlot()
    def on_button_last_clicked(self):
        if self.button_last.isEnabled():
            self.page_changed.emit(self.current_page - 1)

    @pyqtSlot()
    def open_action(self):
        file, _ = QFileDialog.getOpenFileName(self, '选择sql文件', '.', 'SQL (*.sql);;All files (*)')
        if file:
            self.load_sql(Path(file))

    @pyqtSlot()
    def insert_action(self):
        file, _ = QFileDialog.getOpenFileName(self, '选择tbl文件', '.', 'TBL (*.tbl);;All files (*)')
        if file:
            self.load_data(Path(file))

    @pyqtSlot()
    def exit_action(self):
        self.close()

    @pyqtSlot()
    def next_action(self):
        self.button_next.clicked.emit()

    @pyqtSlot()
    def last_action(self):
        self.button_last.clicked.emit()

    @pyqtSlot()
    def clear_action(self):
        self.button_clear.clicked.emit()

    def dragEnterEvent(self, e: QDragEnterEvent) -> None:
        if e.mimeData().hasUrls():
            urls = e.mimeData().urls()
            if urls and len(urls) == 1 and urls[0].path().endswith(('.sql', '.tbl')):
                e.accept()

    def dropEvent(self, e: QDropEvent) -> None:
        path = Path(e.mimeData().urls()[0].path())
        if path.suffix == '.sql':
            self.load_sql(path)
        elif path.suffix == '.tbl':
            self.load_data(path)
