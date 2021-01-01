"""
Here defines TableModel to show data
"""
from datetime import date

from PyQt5.QtCore import QAbstractTableModel, QModelIndex, Qt

from Pybase.manage_system.result import QueryResult


class TableModel(QAbstractTableModel):
    TYPE_TO_DEFAULT = {
        str: '',
        int: -1,
        float: -1,
        date: date.fromtimestamp(0),
    }

    def __init__(self, result: QueryResult):
        super(TableModel, self).__init__()
        self._headers = result.headers
        self._data = list(result.data)

    def rowCount(self, parent=None, *args, **kwargs):
        return 0 if parent and parent.isValid() else len(self._data)

    def columnCount(self, parent=None, *args, **kwargs):
        return 0 if parent and parent.isValid() else len(self._headers)

    def data(self, index: QModelIndex, role=None):
        if role == Qt.DisplayRole:
            return str(self._data[index.row()][index.column()])

    def headerData(self, section: int, orientation: Qt.Orientation, role: int = None):
        if role == Qt.DisplayRole:
            return str(section) if orientation == Qt.Vertical else self._headers[section]

    def sort(self, column: int, order=0):
        if not self._data:
            return
        for each in self._data[0]:
            if each is not None:
                type_ = type(each)
                break
        else:
            return
        self._data.sort(key=lambda x: self.TYPE_TO_DEFAULT[type_] if x[column] is None else x[column],
                        reverse=bool(order))
        self.layoutChanged.emit()
