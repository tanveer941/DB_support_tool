import json

from PyQt4 import QtGui, QtCore
from PyQt4.QtCore import QSettings
from PyQt4.QtGui import QDialog, QApplication, QMessageBox, QFileDialog
import sys
from LT5G_support_ui import Ui_Dialog
from client_lt_support import build_label_config, get_confid_frm_tckid, get_lbld_op_data


DEFAULT_LOC = "D:/"
LABEL_CONFIG = "LabelSchema.json"
LABEL_OUTPUT = "LabelData.json"

class LabelConfEvents(QDialog, Ui_Dialog):

    def __init__(self):

        super(QDialog, self).__init__()
        self.setupUi()
        # LT5GReadJSONUpdate()

    def setupUi(self):

        # try:
        super(LabelConfEvents, self).setupUi(self)
        self.lineEdit.setDisabled(True)
        self.lineEdit_2.setDisabled(True)
        self.radioButton.toggled.connect(self.conf_radio_clickd)
        self.radioButton_2.toggled.connect(self.tickt_radio_clickd)
        self.toolButton.clicked.connect(self.browse_folder_path)
        self.lineEdit_3.setText(DEFAULT_LOC)
        self.pushButton.clicked.connect(self.generate_label_config)


    def conf_radio_clickd(self):
        if self.radioButton.isChecked():
            self.lineEdit.setEnabled(True)
        if not self.radioButton_2.isChecked():
            self.lineEdit_2.setDisabled(True)

    def tickt_radio_clickd(self):
        if self.radioButton_2.isChecked():
            self.lineEdit_2.setEnabled(True)
        if not self.radioButton.isChecked():
            self.lineEdit.setDisabled(True)

    def browse_folder_path(self):
        self.folder_loc = QFileDialog.getExistingDirectory\
            (None, "Please select a Directory", directory=DEFAULT_LOC)
        self.lineEdit_3.setText(self.folder_loc)

    def generate_label_config(self):

        conf_id = str(self.lineEdit.text())
        ticket_id = self.lineEdit_2.text()
        folder_loc = self.lineEdit_3.text()
        if self.radioButton.isChecked() and conf_id != '':
            print "vvv"
            lbl_conf_data = build_label_config(conf_id)
            if lbl_conf_data is None:
                return QMessageBox.information(self, "Information",
                                               "Not a valid configuration ID",
                                               QtGui.QMessageBox.Ok)
            with open(folder_loc + r"\\" + LABEL_CONFIG, 'w+') as outfile:
                json.dump(lbl_conf_data, outfile, indent=4)
            # print "lbl_conf_data ::> ", lbl_conf_data

            QMessageBox.information(self, "Information",
                                    "Label configuration data generated",
                                    QtGui.QMessageBox.Ok)
        elif self.radioButton_2.isChecked() and ticket_id != '':
            # query check to see if the configuration ID exists
            data = get_confid_frm_tckid(int(ticket_id))
            # print colun_names
            print "data ::> ", data
            if data:
                confid = data[0]['LABEL_DESC_VERS']
            else:
                return QMessageBox.information(self, "Information",
                                               "No configuration ID for the entered ticket ID."
                                               " Enter a valid one.",
                                               QtGui.QMessageBox.Ok)
            # Generate the input label configuration
            print "Creating configuration JSON >>>>>>>> "
            lbl_conf_data = build_label_config(confid)
            with open(folder_loc + r"\\" + LABEL_CONFIG, 'w+') as outfile:
                json.dump(lbl_conf_data, outfile, indent=4)
            # Generate the labeled output
            lbl_output_dt = get_lbld_op_data(int(ticket_id), confid)
            # print "lbl_output_dt ::> ", lbl_output_dt

            try:
                if lbl_output_dt['code'] == 12:
                    return QMessageBox.information(self, "Information",
                                                   lbl_output_dt['message'],
                                                   QtGui.QMessageBox.Ok)
            except KeyError:
                pass
            print "Creating Output JSON >>>>>>>> "
            with open(folder_loc + r"\\" + str(ticket_id)+'_'+LABEL_OUTPUT, 'w+') as outfile:
                json.dump(lbl_output_dt, outfile, indent=4)
            QMessageBox.information(self, "Information",
                                    "Label configuration data and labeled output data generated",
                                    QtGui.QMessageBox.Ok)

        else:
            return QMessageBox.information(self, "Information",
                                           "Enter either configuration ID or ticket ID!!",
                                           QtGui.QMessageBox.Ok)




# if __name__ == '__main__':
#     app = QApplication(sys.argv)
#     window = LabelConfEvents()
#     window.show()
#     sys.exit(app.exec_())