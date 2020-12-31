# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'main_window.ui'
#
# Created by: PyQt5 UI code generator 5.9.2
#
# WARNING! All changes made in this file will be lost!

from PyQt5 import QtCore, QtGui, QtWidgets

class Ui_MainWindow(object):
    def setupUi(self, MainWindow):
        MainWindow.setObjectName("MainWindow")
        MainWindow.resize(465, 369)
        self.centralwidget = QtWidgets.QWidget(MainWindow)
        self.centralwidget.setObjectName("centralwidget")
        self.horizontalLayout = QtWidgets.QHBoxLayout(self.centralwidget)
        self.horizontalLayout.setObjectName("horizontalLayout")
        self.verticalLayout = QtWidgets.QVBoxLayout()
        self.verticalLayout.setObjectName("verticalLayout")
        self.tree_file = QtWidgets.QTreeView(self.centralwidget)
        sizePolicy = QtWidgets.QSizePolicy(QtWidgets.QSizePolicy.Preferred, QtWidgets.QSizePolicy.Expanding)
        sizePolicy.setHorizontalStretch(0)
        sizePolicy.setVerticalStretch(0)
        sizePolicy.setHeightForWidth(self.tree_file.sizePolicy().hasHeightForWidth())
        self.tree_file.setSizePolicy(sizePolicy)
        self.tree_file.setObjectName("tree_file")
        self.tree_file.header().setVisible(False)
        self.verticalLayout.addWidget(self.tree_file)
        self.label_db = QtWidgets.QLabel(self.centralwidget)
        sizePolicy = QtWidgets.QSizePolicy(QtWidgets.QSizePolicy.Expanding, QtWidgets.QSizePolicy.Preferred)
        sizePolicy.setHorizontalStretch(0)
        sizePolicy.setVerticalStretch(0)
        sizePolicy.setHeightForWidth(self.label_db.sizePolicy().hasHeightForWidth())
        self.label_db.setSizePolicy(sizePolicy)
        self.label_db.setObjectName("label_db")
        self.verticalLayout.addWidget(self.label_db)
        self.horizontalLayout.addLayout(self.verticalLayout)
        self.frame = QtWidgets.QFrame(self.centralwidget)
        sizePolicy = QtWidgets.QSizePolicy(QtWidgets.QSizePolicy.MinimumExpanding, QtWidgets.QSizePolicy.Preferred)
        sizePolicy.setHorizontalStretch(1)
        sizePolicy.setVerticalStretch(0)
        sizePolicy.setHeightForWidth(self.frame.sizePolicy().hasHeightForWidth())
        self.frame.setSizePolicy(sizePolicy)
        self.frame.setFrameShape(QtWidgets.QFrame.StyledPanel)
        self.frame.setFrameShadow(QtWidgets.QFrame.Raised)
        self.frame.setObjectName("frame")
        self.gridLayout = QtWidgets.QGridLayout(self.frame)
        self.gridLayout.setObjectName("gridLayout")
        self.text_code = QtWidgets.QTextBrowser(self.frame)
        sizePolicy = QtWidgets.QSizePolicy(QtWidgets.QSizePolicy.MinimumExpanding, QtWidgets.QSizePolicy.Preferred)
        sizePolicy.setHorizontalStretch(0)
        sizePolicy.setVerticalStretch(0)
        sizePolicy.setHeightForWidth(self.text_code.sizePolicy().hasHeightForWidth())
        self.text_code.setSizePolicy(sizePolicy)
        self.text_code.setReadOnly(False)
        self.text_code.setObjectName("text_code")
        self.gridLayout.addWidget(self.text_code, 0, 0, 1, 5)
        self.label_page = QtWidgets.QLabel(self.frame)
        sizePolicy = QtWidgets.QSizePolicy(QtWidgets.QSizePolicy.Minimum, QtWidgets.QSizePolicy.Preferred)
        sizePolicy.setHorizontalStretch(0)
        sizePolicy.setVerticalStretch(0)
        sizePolicy.setHeightForWidth(self.label_page.sizePolicy().hasHeightForWidth())
        self.label_page.setSizePolicy(sizePolicy)
        self.label_page.setAlignment(QtCore.Qt.AlignRight|QtCore.Qt.AlignTrailing|QtCore.Qt.AlignVCenter)
        self.label_page.setObjectName("label_page")
        self.gridLayout.addWidget(self.label_page, 2, 1, 1, 1)
        self.button_next = QtWidgets.QPushButton(self.frame)
        sizePolicy = QtWidgets.QSizePolicy(QtWidgets.QSizePolicy.Maximum, QtWidgets.QSizePolicy.Fixed)
        sizePolicy.setHorizontalStretch(0)
        sizePolicy.setVerticalStretch(0)
        sizePolicy.setHeightForWidth(self.button_next.sizePolicy().hasHeightForWidth())
        self.button_next.setSizePolicy(sizePolicy)
        self.button_next.setObjectName("button_next")
        self.gridLayout.addWidget(self.button_next, 2, 3, 1, 1)
        self.label_result = QtWidgets.QLabel(self.frame)
        self.label_result.setObjectName("label_result")
        self.gridLayout.addWidget(self.label_result, 2, 0, 1, 1)
        self.button_last = QtWidgets.QPushButton(self.frame)
        sizePolicy = QtWidgets.QSizePolicy(QtWidgets.QSizePolicy.Maximum, QtWidgets.QSizePolicy.Fixed)
        sizePolicy.setHorizontalStretch(0)
        sizePolicy.setVerticalStretch(0)
        sizePolicy.setHeightForWidth(self.button_last.sizePolicy().hasHeightForWidth())
        self.button_last.setSizePolicy(sizePolicy)
        self.button_last.setObjectName("button_last")
        self.gridLayout.addWidget(self.button_last, 2, 2, 1, 1)
        self.table_result = QtWidgets.QTableView(self.frame)
        sizePolicy = QtWidgets.QSizePolicy(QtWidgets.QSizePolicy.MinimumExpanding, QtWidgets.QSizePolicy.Expanding)
        sizePolicy.setHorizontalStretch(0)
        sizePolicy.setVerticalStretch(0)
        sizePolicy.setHeightForWidth(self.table_result.sizePolicy().hasHeightForWidth())
        self.table_result.setSizePolicy(sizePolicy)
        self.table_result.setSortingEnabled(True)
        self.table_result.setObjectName("table_result")
        self.gridLayout.addWidget(self.table_result, 1, 0, 1, 5)
        self.button_clear = QtWidgets.QPushButton(self.frame)
        sizePolicy = QtWidgets.QSizePolicy(QtWidgets.QSizePolicy.Maximum, QtWidgets.QSizePolicy.Fixed)
        sizePolicy.setHorizontalStretch(0)
        sizePolicy.setVerticalStretch(0)
        sizePolicy.setHeightForWidth(self.button_clear.sizePolicy().hasHeightForWidth())
        self.button_clear.setSizePolicy(sizePolicy)
        self.button_clear.setObjectName("button_clear")
        self.gridLayout.addWidget(self.button_clear, 2, 4, 1, 1)
        self.horizontalLayout.addWidget(self.frame)
        MainWindow.setCentralWidget(self.centralwidget)
        self.statusbar = QtWidgets.QStatusBar(MainWindow)
        self.statusbar.setObjectName("statusbar")
        MainWindow.setStatusBar(self.statusbar)
        self.menubar = QtWidgets.QMenuBar(MainWindow)
        self.menubar.setGeometry(QtCore.QRect(0, 0, 465, 14))
        self.menubar.setObjectName("menubar")
        self.menuasd = QtWidgets.QMenu(self.menubar)
        self.menuasd.setObjectName("menuasd")
        self.menu = QtWidgets.QMenu(self.menubar)
        self.menu.setObjectName("menu")
        MainWindow.setMenuBar(self.menubar)
        self.action_last = QtWidgets.QAction(MainWindow)
        self.action_last.setObjectName("action_last")
        self.action_next = QtWidgets.QAction(MainWindow)
        self.action_next.setObjectName("action_next")
        self.action_clear = QtWidgets.QAction(MainWindow)
        self.action_clear.setObjectName("action_clear")
        self.action_open = QtWidgets.QAction(MainWindow)
        self.action_open.setObjectName("action_open")
        self.action_exit = QtWidgets.QAction(MainWindow)
        self.action_exit.setObjectName("action_exit")
        self.menuasd.addAction(self.action_open)
        self.menuasd.addSeparator()
        self.menuasd.addAction(self.action_exit)
        self.menu.addAction(self.action_last)
        self.menu.addAction(self.action_next)
        self.menu.addAction(self.action_clear)
        self.menubar.addAction(self.menuasd.menuAction())
        self.menubar.addAction(self.menu.menuAction())

        self.retranslateUi(MainWindow)
        self.action_clear.triggered.connect(MainWindow.clear_action)
        self.action_next.triggered.connect(MainWindow.next_action)
        self.action_last.triggered.connect(MainWindow.last_action)
        self.action_open.triggered.connect(MainWindow.open_action)
        self.action_exit.triggered.connect(MainWindow.exit_action)
        QtCore.QMetaObject.connectSlotsByName(MainWindow)

    def retranslateUi(self, MainWindow):
        _translate = QtCore.QCoreApplication.translate
        MainWindow.setWindowTitle(_translate("MainWindow", "Pybase"))
        self.label_db.setText(_translate("MainWindow", "当前数据库:None"))
        self.text_code.setPlaceholderText(_translate("MainWindow", "(请在此处输入SQL语句)"))
        self.label_page.setText(_translate("MainWindow", "0/0"))
        self.button_next.setText(_translate("MainWindow", "下一个"))
        self.label_result.setText(_translate("MainWindow", "用时0.00秒，共0个结果"))
        self.button_last.setText(_translate("MainWindow", "上一个"))
        self.button_clear.setText(_translate("MainWindow", "清空"))
        self.menuasd.setTitle(_translate("MainWindow", "文件"))
        self.menu.setTitle(_translate("MainWindow", "操作"))
        self.action_last.setText(_translate("MainWindow", "上一个(&Last)"))
        self.action_last.setShortcut(_translate("MainWindow", "Alt+L"))
        self.action_next.setText(_translate("MainWindow", "下一个(&Next)"))
        self.action_next.setShortcut(_translate("MainWindow", "Alt+N"))
        self.action_clear.setText(_translate("MainWindow", "清空(&Clear)"))
        self.action_clear.setShortcut(_translate("MainWindow", "Alt+C"))
        self.action_open.setText(_translate("MainWindow", "导入(&Open)"))
        self.action_open.setShortcut(_translate("MainWindow", "Alt+O"))
        self.action_exit.setText(_translate("MainWindow", "退出(&Exit)"))
        self.action_exit.setShortcut(_translate("MainWindow", "Alt+E"))

