
from PyQt4.QtGui import QDialog
from login_ui import Login_Dialog


class LoginEvents(QDialog, Login_Dialog):


    def __init__(self, parent):

        super(QDialog, self).__init__(parent)

        self.setupUi()

    def setupUi(self):
        super(LoginEvents, self).setupUi(self)

        self.show()