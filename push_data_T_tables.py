r"""
LabelData.json is a sample labeled data JSON in the current directory of this repository
Execute this file to launch the UI.
To sync the labeled data to oracle stay on 'JSON to Oracle' tab.
Browse for the file in you system, the file is internally validated.
    - The FolderName should be the ticket ID of the recording (2018080708035211893)
    - It will return the project, function, TAN and recording name of the ticket ID
    - Hit the 'Sync' button to sync the data to their respective tables

To generate Label schema and Labeled data file
    - Browse to the 'Oracle to JSON' tab
    - Enter the config id by selecting the radio button, it will only generate the label schema file
    - Enter the ticket id for the recording, it will generate both the label schema and labeled data
"""
import json
import sys
import re
from PyQt4 import QtGui
from LT5G_support_events import LabelConfEvents
from client_lt_support import get_prjfn_tan_rec_for_tid, get_clmn_names_for_tables,\
add_generic_data_prepared_lt5, get_mts_usr_details
from PyQt4.QtGui import QDialog, QApplication, QMessageBox
from login_events import LoginEvents

UID = 'ALGO_DB_USER'
PWD = 'read'
# TBL_PRFX = 'ADMS_TESTSTAGE'
TBL_PRFX = 'ADMSADMIN'

PORTION = 1200

CD_LABEL = 'CD'
LDROI_LABEL = 'LDROI'
LDSS_LABEL = 'LDSS'


class LT5GReadJSONUpdate(LabelConfEvents):

    def __init__(self):

        super(QDialog, self).__init__()
        self.setupUi()
        # self.json_file_path = json_file_path

    def setupUi(self):
        # try:
        super(LT5GReadJSONUpdate, self).setupUi()
        self.toolButton_2.clicked.connect(self.browse_for_json_files)
        self.pushButton_2.setDisabled(True)
        self.pushButton_2.clicked.connect(self.sync_data)
        # self.label_data is assigned on browsing the file from the tool button


    def sync_data(self):

        print "call sync!!"
        self.validate_usr_credentials(self)


    def validate_usr_credentials(self, parent):
        print "..."

        self.login_obj = LoginEvents(parent)
        self.login_obj.pb_login.clicked.connect(self.push_data_to_db)
        self.login_obj.pb_cancel.clicked.connect(self.login_obj.close)

    def push_data_to_db(self):
        """
        uidj8936   SMR1EKO6PB
        Only on successful login with MTS credentials should this function be called
        :return:
        """

        # usr_id = self.login_obj.ln_usrname.text()
        # usr_pwd = self.login_obj.ln_pswrd.text()
        # uidq4214  --  HP6PXVG8B4
        # uidq4210  --  GZF2XBFZSW
        # uid42828  --  ZSY1Y9SM0Y
        usr_id = 'uid42828'
        usr_pwd = 'ZSY1Y9SM0Y'
        usr_data_dict = get_mts_usr_details(str(usr_id))

        if not bool(usr_data_dict):
         return QMessageBox.information(self, "Information",
                                                "Username and password entered are not entered",
                                                QtGui.QMessageBox.Ok)
        if usr_data_dict['WINDOWS_USERNAME'] == str(usr_id) and usr_data_dict[
            'PW'] == str(usr_pwd):
            self.mks_uid = usr_data_dict['PK']
            self.login_obj.close()
            # Choose from the below two lines to determine which data to push, ldss or ldroi
            self.insert_data_T_tables_ldroi()
            self.insert_data_T_tables_ldss()
            return QMessageBox.information(self, "Information",
                                           "Data successfully pushed!",
                                           QtGui.QMessageBox.Ok)
        else:
            return QMessageBox.information(self, "Information",
                                    "Username and password entered are wrong",
                                    QtGui.QMessageBox.Ok)


    def browse_for_json_files(self):
        # print "call...
        json_file_path = QtGui.QFileDialog.getOpenFileName(
            self, 'Select BPL to import', r'D:/',
            '*.json')
        self.lineEdit_4.setText(json_file_path)
        self.label_data = self.read_json(json_file_path)
        self.ticket_id = self.label_data['Sequence'][0]["DeviceCommonData"][
            "FolderName"]
        print "self.ticket_id :: ", self.ticket_id
        try:
            data = get_prjfn_tan_rec_for_tid(int(self.ticket_id))
        except ValueError:
            return QMessageBox.information(self, "Information",
                                           "Invalid ticket ID. Should be a number."
                                           "Check the JSON file.",
                                           QtGui.QMessageBox.Ok)

        # self.prj_fn, self.r_name, self.tan
        #
        self.prj_fn, self.r_name, self.tan = data
        if self.prj_fn is None:
            return QMessageBox.information(self, "Information",
                                           "Project, function or Tan info does not "
                                           "exist for the ticket ID",
                                           QtGui.QMessageBox.Ok)
        else:
            self.pushButton_2.setEnabled(True)
            print "self.prj_fn, self.r_name, self.tan :: ", self.prj_fn, self.r_name, self.tan

    def read_json(self, json_file_path):
        with open(json_file_path) as outfile:
            lbl_output_data = json.load(outfile)
            return lbl_output_data

    def parse_json_data_ldroi(self, json_data):
        # Rec File name picked from JSON, For now details taken from database
        rec_file_name = json_data['Sequence'][0]["DeviceCommonData"]["Name"]
        # For now TAN is picked from CD tables and not JSON,
        # To pick it from JSON uncomment below line
        tan = None
        # tan = json_data['Sequence'][0]["DeviceCommonData"]["TAN"]
        # print "rec_file_name :: ", rec_file_name, tan
        mandtry_colmn_names = ['''"RecIdFileName"''', '''"TAN"''']
        colmn_data = [rec_file_name, tan]
        return dict(zip(mandtry_colmn_names, colmn_data))


    def gen_ordinates_on_shptyp(self, ordinate_data, attr_dict, colun_names):

        if ordinate_data:
            x_ordinate_lst = ordinate_data["x"]
            y_ordinate_lst = ordinate_data["y"]
            # To fill ROI_QUANTATY_OF_COORDINATES column in the table
            # ordinate_qntity = len(x_ordinate_lst)
            shape_typ = ordinate_data["type"]
            if shape_typ in ['Line']:
                ordinate_qntity = 2
            elif shape_typ in ['Box']:
                ordinate_qntity = 4
            else:
                ordinate_qntity = 4
            if '''"ROI_QUANTATY_OF_COORDINATES"''' in colun_names:
                # print "enter this space ......."
                attr_dict.update(
                    {'''"ROI_QUANTATY_OF_COORDINATES"''': ordinate_qntity})
                for ech_ord in range(0, ordinate_qntity):
                    # x1 = x4 , x2 = x3, y1=y2, y3=y4, keep y1 as first coordinate and y3 as second coordinate
                    ord_idx_x = ech_ord
                    ord_idx_y = ech_ord
                    # Conditions for Y-coordinates
                    if ech_ord == 2 or ech_ord == 3:
                        ord_idx_y = 1
                    if ech_ord == 0 or ech_ord == 1:
                        ord_idx_y = 0
                    # Conditions for X-coordinates
                    if ech_ord == 1 or ech_ord == 2:
                        ord_idx_x = 1
                    if ech_ord == 0 or ech_ord == 3:
                        ord_idx_x = 0
                    attr_dict.update(
                        {'''"ROI_COORDINATE_X_{}"'''.format(ech_ord + 1):
                             int(x_ordinate_lst[ord_idx_x])})
                    attr_dict.update(
                        {'''"ROI_COORDINATE_Y_{}"'''.format(ech_ord + 1):
                             int(y_ordinate_lst[ord_idx_y])})
        return attr_dict

    def generate_tan_lt5g(self):
        """"
        Generate a new TAN which has the LT5G string in it
        """

        from datetime import datetime
        curr_time = datetime.now()
        date_obj = datetime.strptime(str(curr_time), '%Y-%m-%d %H:%M:%S.%f')
        dt_conv = date_obj.strftime('%Y%m%d%H%M%S')
        lt5_tag = '0000LT5G'
        user_id = str(self.mks_uid)
        random_num = '0000010'
        tan = dt_conv + lt5_tag + user_id + random_num + self.ticket_id
        return tan

    def compute_centroid_pmd(self, ldroi_attrib, ordinate_attrib, track_id):
        """

        :param ldroi_attrib: dict; "attribute" field in the labeled data under LDROI
        :param ordinate_attrib: dict; ROIs having all coordinates
        :return: dict; computing the centroid, bayline and connecting track ID
        """

        # Compute Centroid
        connecting_crnr_attrib_lst =['''"PMD_BAYLINE_L1_CONNECTING_CORNER"''', '''"PMD_BAYLINE_L2_CONNECTING_CORNER"''',
                                     '''"PMD_BAYLINE_L3_CONNECTING_CORNER"''', '''"PMD_BAYLINE_L4_CONNECTING_CORNER"''']
        conn_trckid_lst = []
        pmd_specific_dict = {}
        if self.prj_fn in ['LSM501_PMDRT', 'LSM501_PMDLT']:
            # print "ordinate_attrib >> ", ordinate_attrib
            ordinate_dict = dict(ordinate_attrib)
            centroid_x = (ordinate_dict['''"ROI_COORDINATE_X_1"'''] + ordinate_dict['''"ROI_COORDINATE_X_2"'''])/2
            centroid_y = (ordinate_dict['''"ROI_COORDINATE_Y_1"'''] + ordinate_dict['''"ROI_COORDINATE_Y_3"''']) / 2
            pmd_specific_dict.update({'''"PMD_ROI_BOX_CENTER"''': 'C' + str(track_id) +' (' +
                                                              str(centroid_x) + ' ' +str(centroid_y) + ')'})
            # Add Bayline to each
            for ech_conn_attrib in connecting_crnr_attrib_lst:
                bayline_colmn = re.sub(r'\_CONNECTING_CORNER', '', ech_conn_attrib)
                # ldroi_attrib[ech_conn_attrib] != "None" or
                if ldroi_attrib[ech_conn_attrib] is not None:
                    conn_trckid_lst.append(ldroi_attrib[ech_conn_attrib])
                    # print "bayline_colmn :: ", bayline_colmn
                    pmd_specific_dict.update({bayline_colmn: 'L' +' (C' + str(track_id) +
                                                             ' C' +ldroi_attrib[ech_conn_attrib] + ')'})
                else:
                    pmd_specific_dict.update({bayline_colmn: None})
            # Add connecting trackids based on schema
            pmd_specific_dict.update({'''"PMD_CONNECTING_TRACKID"''': ",".join(conn_trckid_lst)})
            # print "ordinate_dict >> : ", ordinate_dict
            # exit(0)
            return pmd_specific_dict
        else:
            return {}


    def build_dict_qry_lst_ldroi(self):

        # print "label_data ::> ", label_data
        parsed_data_dict = self.parse_json_data_ldroi(self.label_data)
        # print "parsed_data_dict :: ", parsed_data_dict
        # self.tan = '201711161128490000LT5G010080000020201710051148123738'
        self.tan = self.generate_tan_lt5g()
        print "self.prj_fn, r_name, tan :: ", self.prj_fn, self.r_name, self.tan
        #updating parsed_data_dict to have recording name as taken from database not from JSON file,
        #commenting the below line will enable fetching the name from JSON
        parsed_data_dict['''"RecIdFileName"'''] = self.r_name
        parsed_data_dict['''"TAN"'''] = self.tan
        print "parsed_data_dict :: ", parsed_data_dict

        colun_names = get_clmn_names_for_tables(self.prj_fn, LDROI_LABEL)
        #Get column names of the tables
        colun_names = ['''"{}"'''.format(each_colmn) for each_colmn in
                       colun_names]
        # print 'colun_names :>> ', colun_names
        #get the rest of the columns from attributes
        try:
            ldroi_atttr_lst = self.label_data['Sequence'][0]["DeviceData"][0]["ChannelData"][0][
            "AnnotatedElements"]
        except KeyError, e:
            print "No annotated elemnts", str(e)
            return 1
        # print "ldroi_atttr_lst >> ", ldroi_atttr_lst
        lst_ldroi_data_insert = []
        for each_attr in ldroi_atttr_lst:
            time_stamp = each_attr["TimeStamp"]
            from collections import OrderedDict
            attr_dict = OrderedDict()

            attr_dict.update({'''"FrameMtLDROITimeStamp"''': time_stamp})
            for index, ech_frm_anno in enumerate(each_attr["FrameAnnoElements"]):
                # print "index :: ", index
                track_id = ech_frm_anno['Trackid']
                attr_dict2 = OrderedDict()
                attr_dict2.update(attr_dict)
                attr_dict2.update({'''"ROI_TRACK_ID"''': track_id})
                attr_dict2.update({'''"LDROI_CYCLE_ID"''': 60,
                                  '''"LDROI_CYCLE_COUNTER"''': 106,
                                  '''"LDROI_DATATYPE"''': "AAP///////8="})
                othr_attr = ech_frm_anno['attributes']
                othr_attr = {k: str(v) for k,v in othr_attr.iteritems()}
                # If the attributes have 'None' value then set it to None
                othr_attr = {k: None if v == 'None' else v for k, v in othr_attr.iteritems()}
                othr_attr = {'''"{}"'''.format(k): v for k, v in
                             othr_attr.iteritems()}
                print "othr_attr :: ", othr_attr
                attr_dict2.update(othr_attr)
            #Block from here is to add the ordinates data along with ROI quantity
                ordinate_data = each_attr["FrameAnnoElements"][index]["shape"]
                # print "track_id ,, ordinate_data :: ", track_id, ordinate_data
                attr_dict3 = self.gen_ordinates_on_shptyp(ordinate_data, attr_dict, colun_names)
                attr_dict2.update(attr_dict3)
                # Compute centroid, Bayline, connecting track ID for PMD function
                attr_dict4 = self.compute_centroid_pmd(othr_attr, attr_dict3, track_id)
                attr_dict2.update(attr_dict4)
            #Block ends here for adding the ROI coordinates
                # print "attr_dict before2 :: ", attr_dict2
            # Sanity check for the columns
                attr_dict2 = {k: v for k, v in attr_dict2.iteritems() if k in colun_names}
                attr_dict2.update(parsed_data_dict)
                # print "attr_dict2 after :: ", attr_dict2
                lst_ldroi_data_insert.append(attr_dict2)
                # print "\nlst_ldroi_data_insert ::> ", lst_ldroi_data_insert
        return lst_ldroi_data_insert

    def divide_file_in_portions(self, total_data_lst, portion):

        for i in range(0, len(total_data_lst), portion):
            yield total_data_lst[i:i + portion]

    def split_lst_for_insertion(self, total_data_lst):
        data_chunk_lst = list(
            self.divide_file_in_portions(total_data_lst, PORTION))
        return data_chunk_lst

    def order_keys_chunk(self, each_chnk):

        all_keys = each_chnk[0].keys()
        l = []
        for ech_row in each_chnk:
            nd = {}
            for ech_key in all_keys:
                nd[ech_key] = ech_row[ech_key]
            l.append(nd)

        return l

    def insert_data_T_tables_ldroi(self):
        print ">> Inserting data into LDROI table <<"
        # prj_fn = self.label_data['Sequence'][0]["DeviceData"][0]["ChannelData"][0][
        #     "AnnotatedElements"][0]["FrameAnnoElements"][0]["category"]
        data_lst = self.build_dict_qry_lst_ldroi()
        # print "data_lst ::> ", data_lst
        if data_lst is None:
            return None
        elif data_lst ==1:
            return 1
        chnk_data_lst = self.split_lst_for_insertion(data_lst)
        for each_chnk in chnk_data_lst:
            print "\n"
            # prj_fn = "MFC400_SR"
            each_chnk = self.order_keys_chunk(each_chnk)
            # print "each_chnk :: ", each_chnk
            add_generic_data_prepared_lt5(records=each_chnk, table_name="{}_{}_T".format(self.prj_fn, LDROI_LABEL))
            # self.orc_dbhandle.commit()
            print "Data inserted in {0}_{1}_T --------======== of batch size {2}\n".format(self.prj_fn, LDROI_LABEL, PORTION)


# ============================================================================================================================

    def build_dict_qry_lst_ldss(self):

        # print "label_data ::> ", label_data
        parsed_data_dict = self.parse_json_data_ldroi(self.label_data)
        # print "parsed_data_dict :: ", parsed_data_dict
        print "self.prj_fn, r_name, tan :: ", self.prj_fn, self.r_name, self.tan
        # updating parsed_data_dict to have recording name as taken from database not from JSON file,
        # commenting the below line will enable fetching the name from JSON
        parsed_data_dict['''"RecIdFileName"'''] = self.r_name
        parsed_data_dict['''"TAN"'''] = self.tan
        print "parsed_data_dict :: ", parsed_data_dict

        colun_names = get_clmn_names_for_tables(self.prj_fn, LDSS_LABEL)
        # colun_names = get_clmn_names_for_tables('MFC400_LD', LDSS_LABEL)
        # Get column names of the tables
        # colun_names = ['''"{}"'''.format(each_colmn) for each_colmn in
        #                colun_names]
        # print 'colun_names :>> ', colun_names
        # get the rest of the columns from attributes
        try:
            # get the rest of the columns from attributes
            lst_ldss_data_insert = []
            ldss_atttr_data_dict = \
            self.label_data['Sequence'][0]["DeviceData"][0][
                "InformationElements"]
        except KeyError, e:
            print "No information elemnts", str(e)
            return 1
        # print "ldss_atttr_data_dict :: ", ldss_atttr_data_dict
        ref_row_dict = {each_rw : None for each_rw in colun_names}
        # print "\nref_row_dict :: ", ref_row_dict
        # Get start and end time from all attributes and make it unique
        main_dt_lst = []
        data_lst = []
        # Convert all keys to upper for validation and data insertion
        # ldss_atttr_data_dict = {key.upper(): values for key,values in ldss_atttr_data_dict.iteritems()}
        for ech_attr, val_st_end_tm_lst in ldss_atttr_data_dict.iteritems():
            # Retain the column name as it is from the database as the names need not necessarily match with names from JSON files
            clmn_nm_attr = [each_clmn_nm for each_clmn_nm in colun_names if
                            each_clmn_nm.upper() == ech_attr.upper()][0]
            print "------------------------------------------>>", ech_attr, clmn_nm_attr
            for ech_st_end_tm in val_st_end_tm_lst:
                start_tm = int(ech_st_end_tm['start'])
                end_tm = int(ech_st_end_tm['end'])
                value = ech_st_end_tm['value']
                # data_lst = []
                if data_lst:
                    for ech_row in data_lst:
                        # if ech_row[ech_attr] is not None and \ FRAMEMTLDSSTIMESTAMPSTOP FRAMEMTLDSSTIMESTAMPSTART
                         print "compare :: ", ech_row['FrameMtLDSSTimeStampStop'] , " <> ", end_tm, "{}", ech_row['FrameMtLDSSTimeStampStart'], " <> ", start_tm
                         if (ech_row['FrameMtLDSSTimeStampStart'] != start_tm or ech_row['FrameMtLDSSTimeStampStop'] != end_tm):
                             exists_flag = False
                         else:
                             exists_flag = True
                             print "updating existing row :: ", clmn_nm_attr, value
                             ech_row[clmn_nm_attr] = value
                             break
                                 # and ech_row[ech_attr] != value:
                         print "exists_flag :: ", exists_flag
                    if not exists_flag:
                        print "value :: ", value
                        # ech_row[ech_attr] = value
                        # data_lst.append(ref_row_dict)
                        ref_row_dict_new = dict(ref_row_dict)
                        ref_row_dict_new['FrameMtLDSSTimeStampStart'] = start_tm
                        ref_row_dict_new['FrameMtLDSSTimeStampStop'] = end_tm
                        ref_row_dict_new[clmn_nm_attr] = value
                        data_lst.append(ref_row_dict_new)
                    # print "\ndata_lst brf :: ", data_lst
                else:
                    ref_row_dict_new = dict(ref_row_dict)
                    ref_row_dict_new['FrameMtLDSSTimeStampStart'] = start_tm
                    ref_row_dict_new['FrameMtLDSSTimeStampStop'] = end_tm
                    ref_row_dict_new[clmn_nm_attr] = value
                    data_lst.append(ref_row_dict_new)
                        # data_lst.extend(data_lst)
                # else:
                #         print "main_dt_lst :: ", main_dt_lst

            # print "\ndata_lst :: ", data_lst
        #
        return data_lst, parsed_data_dict


    def insert_data_T_tables_ldss(self):
        print ">> Inserting data into LDSS table <<"
        # prj_fn = self.label_data['Sequence'][0]["DeviceData"][0]["ChannelData"][0][
        #     "AnnotatedElements"][0]["FrameAnnoElements"][0]["category"]
        data_lst, parsed_data_dict = self.build_dict_qry_lst_ldss()
        data_lst_nw = []
        # surround the column names with double quotes for each row passed
        for evry_rw in data_lst:
            # Get the TAN and rec name from database
            # print "parsed_data_dict >> ", parsed_data_dict
            evry_rw['TAN'] = parsed_data_dict['''"TAN"''']
            # evry_rw['TAN'] = '20171025114012LUV3012M011090000050000000000000000001'
            evry_rw['RecIdFileName'] = parsed_data_dict['''"RecIdFileName"''']
            # Add a few other column defaults
            evry_rw['LDSS_START_DATATYPE'] = "AAP///////8="
            evry_rw['LDSS_STOP_DATATYPE'] = "AAP///////8="
            evry_rw['LDSS_START_CYCLE_ID'] = 60
            evry_rw['LDSS_STOP_CYCLE_ID'] = 60
            evry_rw['LDSS_START_CYCLE_COUNTER'] = 68
            evry_rw['LDSS_STOP_CYCLE_COUNTER'] = 112
            print "\n evry_rw :: ", evry_rw
            evry_rw = {'''"{}"'''.format(evry_clmn_nm): value for evry_clmn_nm,
                                                                  value in evry_rw.iteritems()}
            data_lst_nw.append(evry_rw)
        chnk_data_lst = self.split_lst_for_insertion(data_lst_nw)
        for each_chnk in chnk_data_lst:
            print "\n"
            # print "each_chnk :: ", each_chnk
            add_generic_data_prepared_lt5(records=each_chnk, table_name="{}_{}_T".format(self.prj_fn, LDSS_LABEL))
            # self.orc_dbhandle.commit()
            print "Data inserted in {0}_{1}_T --------======== of batch size {2}\n".format(self.prj_fn, LDSS_LABEL, PORTION)


# if __name__ == '__main__':
    # lt5g_obj = LT5GReadJSONUpdate(r'D:\LabelData_201706150816141838.json')
    # lt5g_obj = LT5GReadJSONUpdate()

if __name__ == '__main__':
    app = QApplication(sys.argv)
    window = LT5GReadJSONUpdate()
    window.show()
    sys.exit(app.exec_())



#
# For LDSS
# 1. column names are case sensitive - includes all caps and camel case names
# 	JSON file -  attribute names have all caps and camel case names
# 	Both neither have the same types for the same column name, so no matter what the names are in JSON file we should retrieve the column names as it is.
# 2. Read the entire info elements - all the time stamp.
# row insertion only happens when data for a time stamp(start and end) is gathered and a row made
# 3. Column names are always in double quotes.
# 4. If update required for a row check the start and end timestamp uniqueness and execute an update query(currently not implemented)
# 5. LDSS start and stop cycle counter not added - Logic yet to be known on how it is decided


# pyinstaller push_data_T_tables.py --add-data config.json;. -w





