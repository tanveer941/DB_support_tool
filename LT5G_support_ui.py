# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'LT5G_oracle.ui'
#
# Created: Thu Nov 02 15:18:36 2017
#      by: PyQt4 UI code generator 4.9.6
#
# WARNING! All changes made in this file will be lost!

from PyQt4 import QtCore, QtGui

try:
    _fromUtf8 = QtCore.QString.fromUtf8
except AttributeError:
    def _fromUtf8(s):
        return s

try:
    _encoding = QtGui.QApplication.UnicodeUTF8
    def _translate(context, text, disambig):
        return QtGui.QApplication.translate(context, text, disambig, _encoding)
except AttributeError:
    def _translate(context, text, disambig):
        return QtGui.QApplication.translate(context, text, disambig)

class Ui_Dialog(object):
    def setupUi(self, Dialog):
        Dialog.setObjectName(_fromUtf8("Dialog"))
        Dialog.resize(490, 211)
        self.horizontalLayout_6 = QtGui.QHBoxLayout(Dialog)
        self.horizontalLayout_6.setObjectName(_fromUtf8("horizontalLayout_6"))
        self.tabWidget = QtGui.QTabWidget(Dialog)
        self.tabWidget.setObjectName(_fromUtf8("tabWidget"))
        self.tab = QtGui.QWidget()
        self.tab.setObjectName(_fromUtf8("tab"))
        self.horizontalLayout_5 = QtGui.QHBoxLayout(self.tab)
        self.horizontalLayout_5.setObjectName(_fromUtf8("horizontalLayout_5"))
        self.verticalLayout_2 = QtGui.QVBoxLayout()
        self.verticalLayout_2.setObjectName(_fromUtf8("verticalLayout_2"))
        self.horizontalLayout = QtGui.QHBoxLayout()
        self.horizontalLayout.setObjectName(_fromUtf8("horizontalLayout"))
        self.radioButton = QtGui.QRadioButton(self.tab)
        self.radioButton.setObjectName(_fromUtf8("radioButton"))
        self.horizontalLayout.addWidget(self.radioButton)
        self.lineEdit = QtGui.QLineEdit(self.tab)
        self.lineEdit.setObjectName(_fromUtf8("lineEdit"))
        self.horizontalLayout.addWidget(self.lineEdit)
        self.verticalLayout_2.addLayout(self.horizontalLayout)
        self.horizontalLayout_2 = QtGui.QHBoxLayout()
        self.horizontalLayout_2.setObjectName(_fromUtf8("horizontalLayout_2"))
        self.radioButton_2 = QtGui.QRadioButton(self.tab)
        self.radioButton_2.setObjectName(_fromUtf8("radioButton_2"))
        self.horizontalLayout_2.addWidget(self.radioButton_2)
        self.lineEdit_2 = QtGui.QLineEdit(self.tab)
        self.lineEdit_2.setObjectName(_fromUtf8("lineEdit_2"))
        self.horizontalLayout_2.addWidget(self.lineEdit_2)
        self.verticalLayout_2.addLayout(self.horizontalLayout_2)
        self.horizontalLayout_3 = QtGui.QHBoxLayout()
        self.horizontalLayout_3.setObjectName(_fromUtf8("horizontalLayout_3"))
        self.label = QtGui.QLabel(self.tab)
        self.label.setObjectName(_fromUtf8("label"))
        self.horizontalLayout_3.addWidget(self.label)
        self.lineEdit_3 = QtGui.QLineEdit(self.tab)
        self.lineEdit_3.setObjectName(_fromUtf8("lineEdit_3"))
        self.horizontalLayout_3.addWidget(self.lineEdit_3)
        self.toolButton = QtGui.QToolButton(self.tab)
        self.toolButton.setObjectName(_fromUtf8("toolButton"))
        self.horizontalLayout_3.addWidget(self.toolButton)
        self.verticalLayout_2.addLayout(self.horizontalLayout_3)
        self.horizontalLayout_4 = QtGui.QHBoxLayout()
        self.horizontalLayout_4.setObjectName(_fromUtf8("horizontalLayout_4"))
        spacerItem = QtGui.QSpacerItem(378, 20, QtGui.QSizePolicy.Expanding,
                                       QtGui.QSizePolicy.Minimum)
        self.horizontalLayout_4.addItem(spacerItem)
        self.pushButton = QtGui.QPushButton(self.tab)
        self.pushButton.setObjectName(_fromUtf8("pushButton"))
        self.horizontalLayout_4.addWidget(self.pushButton)
        self.verticalLayout_2.addLayout(self.horizontalLayout_4)
        self.horizontalLayout_5.addLayout(self.verticalLayout_2)
        self.tabWidget.addTab(self.tab, _fromUtf8(""))
        self.tab_2 = QtGui.QWidget()
        self.tab_2.setObjectName(_fromUtf8("tab_2"))
        self.gridLayout = QtGui.QGridLayout(self.tab_2)
        self.gridLayout.setObjectName(_fromUtf8("gridLayout"))
        self.verticalLayout = QtGui.QVBoxLayout()
        self.verticalLayout.setObjectName(_fromUtf8("verticalLayout"))
        self.horizontalLayout_7 = QtGui.QHBoxLayout()
        self.horizontalLayout_7.setObjectName(_fromUtf8("horizontalLayout_7"))

        self.label_2 = QtGui.QLabel(self.tab_2)
        self.label_2.setObjectName(_fromUtf8("label_2"))
        self.horizontalLayout_7.addWidget(self.label_2)
        self.lineEdit_4 = QtGui.QLineEdit(self.tab_2)
        self.lineEdit_4.setObjectName(_fromUtf8("lineEdit_4"))
        self.horizontalLayout_7.addWidget(self.lineEdit_4)
        self.toolButton_2 = QtGui.QToolButton(self.tab_2)
        self.toolButton_2.setObjectName(_fromUtf8("toolButton_2"))
        self.horizontalLayout_7.addWidget(self.toolButton_2)
        self.verticalLayout.addLayout(self.horizontalLayout_7)
        self.horizontalLayout_8 = QtGui.QHBoxLayout()
        self.horizontalLayout_8.setObjectName(_fromUtf8("horizontalLayout_8"))
        spacerItem1 = QtGui.QSpacerItem(200, 20, QtGui.QSizePolicy.Expanding,
                                        QtGui.QSizePolicy.Minimum)
        self.horizontalLayout_8.addItem(spacerItem1)
        self.pushButton_2 = QtGui.QPushButton(self.tab_2)
        self.pushButton_2.setObjectName(_fromUtf8("pushButton_2"))
        self.horizontalLayout_8.addWidget(self.pushButton_2)
        self.verticalLayout.addLayout(self.horizontalLayout_8)
        self.gridLayout.addLayout(self.verticalLayout, 0, 0, 1, 1)
        self.tabWidget.addTab(self.tab_2, _fromUtf8(""))
        self.horizontalLayout_6.addWidget(self.tabWidget)

        self.retranslateUi(Dialog)
        self.tabWidget.setCurrentIndex(1)
        QtCore.QMetaObject.connectSlotsByName(Dialog)

    def retranslateUi(self, Dialog):
        Dialog.setWindowTitle(_translate("Dialog", "LT5G support tool", None))
        self.radioButton.setText(_translate("Dialog", "Configuration ID : ", None))
        self.radioButton_2.setText(_translate("Dialog", "Ticket ID : ", None))
        self.label.setText(_translate("Dialog", "Folder to save :", None))
        self.toolButton.setText(_translate("Dialog", "...", None))
        self.pushButton.setText(_translate("Dialog", "Retrieve", None))
        self.pushButton_2.setText(_translate("Dialog", "Sync", None))
        self.tabWidget.setTabText(self.tabWidget.indexOf(self.tab), _translate("Dialog", "Oracle to JSON", None))
        self.label_2.setText(_translate("Dialog", "Select JSON file : ", None))
        self.toolButton_2.setText(_translate("Dialog", "...", None))
        self.tabWidget.setTabText(self.tabWidget.indexOf(self.tab_2), _translate("Dialog", "JSON to Oracle", None))
        # self.tabWidget.setTabEnabled(1, False)


# if __name__ == "__main__":
#     import sys
#     app = QtGui.QApplication(sys.argv)
#     Dialog = QtGui.QDialog()
#     ui = Ui_Dialog()
#     ui.setupUi(Dialog)
#     Dialog.show()
#     sys.exit(app.exec_())

