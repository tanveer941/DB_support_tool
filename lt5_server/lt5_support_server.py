from flask import request
import requests
import re
import sys, os
from flask import jsonify, Flask, Response
from flask_restful import Resource, Api, reqparse
from json import loads
import json
import random
import getpass
from cx_Oracle import DatabaseError
from lnk_cat import OracleRecCatalogDB
from lnk_db_sql import SQLBinaryExpr, OP_EQ
from werkzeug.local import LocalProxy
from werkzeug.utils import secure_filename
from datetime import datetime
from flask_dashed.admin import Admin

UID = 'ALGO_DB_USER'
PWD = 'read'
# UID = 'ADMSADMIN'
# PWD = 'admin4_adms'
TBL_PRFX = 'ADMSADMIN'

PORTION = 1200

CD_LABEL = 'CD'
LDROI_LABEL = 'LDROI'
LDSS_LABEL = 'LDSS'

COMMIT = True

TOOL_CONF = {
  "Shape": { "SR":"Box",
            "SOD":"Box",
            "HLA":"Box",
            "LD":"Line",
            "TL":"Box",
             "TID": "Box",
             "OD": "Box",
            "PMD": "Box",
            "PMDLT": "Box",
            "PMDRT": "Box",
             "ODRR": "Box",
            "PMDRR": "Box"
  },
  "Category": {
              "SR":"Signs",
              "SOD":"Objects",
              "HLA":"Head light",
              "LD":"Lane",
              "TL":"Traffic Light",
              "TID": "Traffic Indicator Detection",
              "OD": "Object detection",
              "PMD": "Parking marker detection",
              "PMDLT": "Parking marker detection",
              "PMDRT": "Parking marker detection",
              "PMDRR": "Parking marker detection",
              "ODRR": "Object Detection"

  }
}

application = Flask(__name__)
api = Api(application)
admin = Admin(application)

UPLOAD_FOLDER = r'D:\Work\2018\code\Github_repo\Support_tool\LT5G_DB_support_tool\static'
application.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

def create_random_directory(path):
    rand = str(random.randint(0,9999999))
    folder_path = ''
    while True:
        if os.path.isdir(os.path.join(path,rand)):
            rand = str(random.randint(0,9999999))
        else:
            folder_path = os.path.join(path,rand)
            os.mkdir(folder_path)
            break
    return folder_path

def connect_to_database():

    db_handler = OracleRecCatalogDB("uid={0};""pwd={1}".format(UID, PWD),
                                    table_prefix="{}".format(TBL_PRFX))
    # print "connected _______"
    return db_handler

fd = {'_database': None}
def get_db():
    # print "context ============================"
    # print "g >>> ", g, dir(g), g.__dict__
    db = fd['_database']
    # print "-----------", isinstance(db, OracleRecCatalogDB)
    if not isinstance(db, OracleRecCatalogDB):
        # print "OracleRecCatalogDB instance ___________________"
        fd['_database'] = connect_to_database()
        # print "fd['_database'] :: ", fd['_database']
        db = fd['_database']
    return db

with application.app_context():
    #Get DB connection from application's context
    db = LocalProxy(lambda: get_db())

class BuildLabelConfig(Resource):
    def get_ldss_data(self, conf_id):
        """
        http://luv4evrg:9090/API/public/labels/getLabelsConfiguration/CONF_ID568/LDSS
        :param conf_id:
        :return:
        """
        # resp = requests.get(
        #     url=r"http://luv4evrg:9090/API/public/labels/getLabelsConfiguration/{}/LDSS".format(
        #         conf_id))
        resp = requests.get(
            url=r"http://statusportal.adas.conti.de/php/API/public/labels/getLabelsConfiguration/{}/LDSS ".format(
                conf_id))
        # print "resp :: ", loads(resp.content)
        return loads(resp.content)

    def get_ldroi_data(self, conf_id):
        # resp = requests.get(
        #     url=r"http://luv4evrg:9090/API/public/labels/getLabelsConfiguration/{}/LDROI".format(
        #         conf_id))
        resp = requests.get(
            url=r"http://statusportal.adas.conti.de/php/API/public/labels/getLabelsConfiguration/{}/LDROI".format(
                conf_id))
        return loads(resp.content)

    def gen_info_elmnts_ldss(self, conf_id):
        dt_ldss = self.get_ldss_data(conf_id)[conf_id]
        if not dt_ldss:
            return None
        info_elemnts_lst = []
        for key, value in dt_ldss.iteritems():
            if len(value) == 1 and value[0] == 'null':
                pass
            else:
                value = [str(each_elmnt) for each_elmnt in value]
                each_attr_data = {'Type': 'ComboBox', 'Optional': 'No',
                                  'DataType': "value",
                                  'Values': value, 'Default': ["None"]}
                info_elemnts_lst.append({key.upper(): each_attr_data})

        return info_elemnts_lst

    def gen_anno_elmnts_ldroi(self, conf_id):

        # get the LDSS data and parse it / remove keys with null
        dt_ldroi = self.get_ldroi_data(conf_id)[conf_id]
        # print "dt_ldroi :: ", dt_ldroi
        anno_elemnts_lst = []
        for key, value in dt_ldroi.iteritems():
            # print "key, value :: ", key, value
            if len(value) == 1 and value[0] == 'null':
                # dt_ldroi.pop(key, None)
                pass
                # print "Attributes with null value in the list,,,"
            else:
                # if all(isinstance(x, int) for x in value):
                value = [str(each_elmnt) for each_elmnt in value]
                each_attr_data = {'Type': 'ComboBox', 'Optional': 'No',
                                  'Values': value, 'Default': ["None"]}
                anno_elemnts_lst.append({key.upper(): each_attr_data})

        # print "anno_elemnts_lst :: ", anno_elemnts_lst
        return anno_elemnts_lst

    def connect_to_oracle(self):
        try:
            orc_conn = OracleRecCatalogDB("uid={0};""pwd={1}".format(UID, PWD),
                                    table_prefix="{}".format(TBL_PRFX))
            # orc_conn = cx_connect("ALGO_DB_USER", "read", "racadmpe", False)
            # print "orc_conn :: ", orc_conn
        except DatabaseError, info:
            print "Logon Error:", info
            sys.exit()
        return orc_conn

    def get_fn_confid(self, conf_id):

        stmt = '''SELECT ADMSADMIN.split_part(alc.LABEL_DESC_VERS,'xxxx',2) AS FN
                FROM ADMSADMIN.adms_lbl_config alc
                WHERE alc.CONF_ID = '{}' '''.format(conf_id)
        # orc_conn = self.connect_to_oracle()
        orc_conn = db
        # print "stmt :: ", stmt
        orc_cursor = orc_conn.cursor()
        orc_cursor.execute(stmt)
        data = orc_cursor.fetchall()
        if data:
            colun_names = [column[0] for column in orc_cursor.description]
            data = [dict(zip(colun_names, rec)) for rec in data]
            # print "data :: ", data
            fn = re.sub('[#]', '', data[0]['FN'])
            return fn
        else:
            return None

    def build_label_config(self, conf_id):
        conf_id = conf_id.upper()
        info_elmnts = self.gen_info_elmnts_ldss(conf_id)
        # print "info_elmnts :: ", info_elmnts
        anno_elmnts = self.gen_anno_elmnts_ldroi(conf_id)
        # print "anno_elmnts :: ", anno_elmnts
        fn = self.get_fn_confid(conf_id)
        print "fn :: ", fn
        try:
            shape_typ = TOOL_CONF['Shape'][fn]
            category_typ = TOOL_CONF['Category'][fn]
        except KeyError:
            # return {"message": "Configuration does not have the function " + fn}
            shape_typ = "Box"
            category_typ = fn

        config_json = {"LabelConfig": {"Shapes": [
            {
                "Line":
                    {
                        "Orientation": ["Horizontal"],
                        "Color": ["Yellow"],
                        "Thickness": ["2"],
                        "Type": ["Dash"]
                    }
            },
            {
                "Box":
                    {
                        "Corner": ["Normal"],
                        "Color": ["Blue"],
                        "Thickness": ["2"],
                        "Type": ["Dotted"]
                    }
            }
        ],
            "Algorithms":
                [
                    {
                        "EgoLaneChange":
                            {
                                "configParam1": ["configParam1DefaultValue"],
                                "configParam2": ["configParam2DefaultValue"]
                            },
                        "supportedShape": "Line",
                        "tip": "when to use algo"
                    },
                    {
                        "Algo2":
                            {
                                "configParam1": ["configParam1DefaultValue"],
                                "configParam2": ["configParam2DefaultValue"]
                            },
                        "supportedShape": "Box",
                        "tip": "when to use algo"
                    }
                ],
            "Projects":
                [
                    {
                        "Recording":
                            [
                                {
                                    "Recording1":
                                        {
                                            "Channel": "ImageViewer",
                                            "Completed": "Yes/No",
                                            "Type": "RRec"
                                        }
                                },
                                {
                                    "Recording1":
                                        {
                                            "Channel": "Signal",
                                            "Completed": "Yes/No",
                                            "Type": "RRec"
                                        }
                                }
                            ],

                        "SequenceLabels":
                            [
                                {
                                    "AnnoElements": [
                                        "Anno1",
                                        "Anno2"],
                                    "InfoElements": [
                                        "Info1",
                                        "Info2"]
                                }
                            ],
                        "FrameLabels":
                            [
                                {
                                    "AnnoElements": [
                                        "Anno1",
                                        "Anno2"],
                                    "InfoElements": [
                                        "Info1",
                                        "Info2"]
                                }
                            ]
                    }
                ],
            "Anno Elements": [{"category": category_typ,
                               "Attributes": anno_elmnts,
                               "Shapes": [
                                   {
                                       shape_typ:
                                           {
                                               "configParam2": "configParam2NewValue"
                                           },
                                       "Algos":
                                           [
                                               {
                                                   "Algo1":
                                                       {
                                                           "configParam2": [
                                                               "configParam2NewValue"]
                                                       }
                                               }
                                           ]
                                   }
                               ]
                               }],
            "InformationElements": [{"category": category_typ,
                                     "Attributes": info_elmnts}]}}
        return config_json


    def get(self):
        """
        http://127.0.0.1:5000/build_label_config?conf_id=conf_id369
        :return:
        """

        parser = reqparse.RequestParser()
        parser.add_argument('conf_id', type=str)
        # parser.add_argument('tool_conf', type=str)
        args = parser.parse_args()
        # Assign the arguments after parsing
        conf_id = args['conf_id']
        # tool_conf = args['tool_conf']
        # print "tool_conf :> ", tool_conf
        # data = 1
        data = self.build_label_config(conf_id)
        # print "data :: ", data
        return jsonify(data)

class GetConfIDFromTckID(Resource):
    def get(self):
        """
        http://127.0.0.1:5000/get_confid_frm_tckid?ticket_id=201709290649073737
        http://lu00093vmx.li.de.conti.de/label_tool/get_confid_frm_tckid?ticket_id=201709290649073737
        :return:
        """
        parser = reqparse.RequestParser()
        parser.add_argument('ticket_id', type=int)
        # parser.add_argument('tool_conf', type=str)
        args = parser.parse_args()
        # Assign the arguments after parsing
        ticket_id = args['ticket_id']
        try:
            # orc_conn = OracleRecCatalogDB("uid={0};""pwd={1}".format(UID, PWD),
            #                         table_prefix="{}".format(TBL_PRFX))
            orc_conn = db
            # orc_conn = cx_connect("ALGO_DB_USER", "read", "racadmpe", False)
            # print "orc_conn :: ", orc_conn
        except DatabaseError, info:
            print "Logon Error:", info
            sys.exit()
        orc_cursor = orc_conn.cursor()
        stmt = '''SELECT TICKET_ID,LABEL_DESC_VERS
                                FROM ADMSADMIN.ticket_master_table
                                WHERE TICKET_ID = '{}' '''.format(ticket_id)
        orc_cursor.execute(stmt)
        data = orc_cursor.fetchall()
        # print "orc_cursor.description :> ", orc_cursor.description
        colun_names = [column[0].upper() for column in
                       orc_cursor.description]
        data = [dict(zip(colun_names, rec)) for rec in data]
        return jsonify(data)

class LabelProperties(object):
    def __init__(self, **kwds):
        self.__dict__.update(kwds)

class GetLabeledOutputData(Resource):

    def __init__(self):

        self.orc_conn = db

    def get_prj_fn_for_ticketid(self, ticket_id):
        # stmt = '''SELECT tmt.TICKET_ID,
        #       REPLACE(CONCAT(ADMSADMIN.split_part(alc.LABEL_DESC_VERS,'xxxx',1),
        #     ADMSADMIN.split_part(alc.LABEL_DESC_VERS,'xxxx',2)),'#','_') as PRJ_FN,
        #     tmt."RecIdFileName"
        #     FROM ADMSADMIN.ticket_master_table tmt,
        #       ADMSADMIN.adms_lbl_config alc
        #     WHERE tmt.WORK_STATUS       = 'WORK'
        #     AND tmt.WORK_STATUS_INFO    = 'WORK'
        #     AND tmt.LABEL_DESC_VERS = alc.CONF_ID
        #     AND tmt.TICKET_ID = '{}' '''.format(ticket_id)

        stmt = '''SELECT tmt.TICKET_ID,tmt.LABEL_DESC_VERS,
                  REPLACE(CONCAT(ADMSADMIN.split_part(alc.LABEL_DESC_VERS,'xxxx',1),
                ADMSADMIN.split_part(alc.LABEL_DESC_VERS,'xxxx',2)),'#','_') as PRJ_FN,
                tmt."RecIdFileName"
                FROM ADMSADMIN.ticket_master_table tmt,
                  ADMSADMIN.adms_lbl_config alc
                WHERE 
                tmt.LABEL_DESC_VERS = alc.CONF_ID
                AND tmt.TICKET_ID = '{}' '''.format(ticket_id)
        # print "stmt :: ", stmt
        orc_cursor = self.orc_conn.cursor()
        orc_cursor.execute(stmt)
        data = orc_cursor.fetchall()
        colun_names = [column[0].upper() for column in
                       orc_cursor.description]
        data = [dict(zip(colun_names, rec)) for rec in data]
        # print "data ::> ", data
        if data:
            prj_fn = data[0]['PRJ_FN']
            rec_name = data[0]['RECIDFILENAME']
            conf_id = data[0]['LABEL_DESC_VERS']
            return prj_fn, rec_name, conf_id
        else:
            print "No configuration ID for the entered ticket ID."
            return [], [], []

    def gen_op_lbl_data(self, prj_fn, rec_name):

        stmt = '''SELECT *
            FROM ADMSADMIN.{0}_LDROI
            WHERE "RecIdFileName" = '{1}' ORDER BY ROI_TRACK_ID'''.format(prj_fn, rec_name)
        orc_cursor = self.orc_conn.cursor()
        orc_cursor.arraysize = 5000
        orc_cursor.execute(stmt)
        data = orc_cursor.fetchall()
        colun_names = [column[0] for column in
                       orc_cursor.description]
        data = [dict(zip(colun_names, rec)) for rec in data]
        return data

    def gen_op_lbl_ldss_data(self, prj_fn, rec_name):

        stmt = '''SELECT *
            FROM ADMSADMIN.{0}_LDSS
            WHERE "RecIdFileName" = '{1}' '''.format(prj_fn, rec_name)
        orc_cursor = self.orc_conn.cursor()
        orc_cursor.arraysize = 5000
        orc_cursor.execute(stmt)
        data = orc_cursor.fetchall()
        colun_names = [column[0] for column in
                       orc_cursor.description]
        data = [dict(zip(colun_names, rec)) for rec in data]
        return data

    def get_fn_confid(self, conf_id):

        stmt = '''SELECT ADMSADMIN.split_part(alc.LABEL_DESC_VERS,'xxxx',2) AS FN
                FROM ADMSADMIN.adms_lbl_config alc
                WHERE alc.CONF_ID = '{}' '''.format(conf_id)
        # print "stmt :: ", stmt
        orc_cursor = self.orc_conn.cursor()
        orc_cursor.execute(stmt)
        data = orc_cursor.fetchall()
        if data:
            colun_names = [column[0] for column in orc_cursor.description]
            data = [dict(zip(colun_names, rec)) for rec in data]
            # print "data :: ", data
            fn = re.sub('[#]', '', data[0]['FN'])
            return fn
        else:
            return None

    def get(self):
        """
        http://127.0.0.1:5000/get_lbld_op_data?ticket_id=201709290649073737&conf_id=CONF_ID568
        http://lu00093vmx.li.de.conti.de/label_tool/get_lbld_op_data?ticket_id=201709290649073737&conf_id=CONF_ID568
        :return:
        """
        parser = reqparse.RequestParser()
        parser.add_argument('ticket_id', type=int)
        # parser.add_argument('conf_id', type=str)
        # parser.add_argument('tool_conf', type=str)
        args = parser.parse_args()
        # Assign the arguments after parsing
        ticket_id = args['ticket_id']
        # conf_id = args['conf_id']
        prj_fn, rec_name, conf_id = self.get_prj_fn_for_ticketid(ticket_id)
        if prj_fn == [] or rec_name == []:
            return {'message': 'Could not retrieve project and function '
                                   'for the ticket ID', 'code': 12}
        sample_data = self.gen_op_lbl_data(prj_fn, rec_name)
        # print "sample_data >> ", sample_data
        ldss_data = self.gen_op_lbl_ldss_data(prj_fn, rec_name)
        # print "ldss_data >> ", ldss_data
        # Algo_Generated = "Algo Generated"
        # Manually_Corrected = "Manually Corrected"
        fn = self.get_fn_confid(conf_id)
        print "fn :: ", fn
        shape_typ = TOOL_CONF['Shape'][fn]
        if sample_data:
            rec_id_file_name = sample_data[0]['RecIdFileName']
            tan = sample_data[0]['TAN']
            ldroi_keys = sample_data[0].keys()
            ldroi_keys_not_needed = ['TAN', 'RecIdFileName', 'ROI_TRACK_ID', 'FrameMtLDROITimeStamp']
            ldroi_newkey_lst = [key for key in ldroi_keys if key not in ldroi_keys_not_needed]
        elif ldss_data:
            rec_id_file_name = ldss_data[0]['RecIdFileName']
            tan = ldss_data[0]['TAN']
        print "Data retrieved >>>>>>>>>>>>>>> "
        cmn_attr = LabelProperties()
        # print "rec_id_file_name :: ", rec_id_file_name

        cmn_attr.Name = rec_id_file_name
        # Put the default TAN in place instead the one from DB
        cmn_attr.TAN = "20161206120610OZD0578G009720000012016120105354598159"
        # cmn_attr.TAN = tan
        cmn_attr.FolderName = str(ticket_id)

        cmn_dt = LabelProperties()
        cmn_dt.DeviceCommonData = cmn_attr.__dict__

        dvc_dt = LabelProperties()
        chn_cmn_dt = LabelProperties()
        chn_cmn_dt.Type = "Image"
        dvc_dt.ChannelCommonData = chn_cmn_dt.__dict__

        chn_dt = LabelProperties()

        dvc_dt.DeviceName = "Camera"
        annotate_elmnts_lst = []
        if sample_data:

            # Loop begins here to add the frame annotated elements
            for each_row in sample_data:
                # print "each_row['ROI_TRACK_ID'] :: ", each_row['ROI_TRACK_ID']
                # print("annotate_elmnts_lst::", annotate_elmnts_lst)
                if each_row['ROI_COORDINATE_Y_3'] is None:
                    y = each_row['ROI_COORDINATE_Y_1']
                else:
                    y = each_row['ROI_COORDINATE_Y_3']
                ldroi_tmstamp_lst = [ech_anno_dict['TimeStamp'] for ech_anno_dict in annotate_elmnts_lst]
                if each_row['FrameMtLDROITimeStamp'] not in ldroi_tmstamp_lst:

                    # Add information under the annotated elements, this changes dynamically depending upon the timestamps
                    annotate_elmnts = LabelProperties()
                    annotate_elmnts.FrameNumber = 0
                    annotate_elmnts.TimeStamp = each_row['FrameMtLDROITimeStamp']

                    # If same timestamp with multiple track IDs then append the list
                    # FrameAnnoElements is for each track ID
                    frm_anno_elmnts = LabelProperties()
                    frm_anno_elmnts.Hierarchy = "0"
                    frm_anno_elmnts.Trackid = each_row['ROI_TRACK_ID']

                    # Get the attribute details from the Database and loop in with the number of rows
                    # Add attribute details - from the database

                    attr_db = LabelProperties()
                    for each_key in ldroi_newkey_lst:
                        # DB fetches value None, Convert to string "None" else it would be null in JSON
                        if each_row[each_key] is None:
                            row_data = "None"
                        else:
                            row_data = each_row[each_key]
                        attr_db.__dict__[each_key] = row_data

                    # attr_db.LDROI_CYCLE_COUNTER = each_row['LDROI_CYCLE_COUNTER']
                    # attr_db.LDROI_CYCLE_ID = each_row['LDROI_CYCLE_ID']
                    # attr_db.LDROI_DATATYPE = each_row['LDROI_DATATYPE']
                    # attr_db.ROI_COORDINATE_X_1 = each_row['ROI_COORDINATE_X_1']
                    # attr_db.ROI_COORDINATE_Y_1 = each_row['ROI_COORDINATE_Y_1']

                    # To add the attribute elements
                    frm_anno_elmnts.attributes = attr_db.__dict__

                    try:
                        category_typ = TOOL_CONF['Category'][fn]
                    except KeyError:
                        category_typ = fn
                    frm_anno_elmnts.category = category_typ
                    shp_dt = LabelProperties()
                    shp_dt.__dict__['Algo Generated'] = "YES"
                    shp_dt.__dict__['Manually Corrected'] = "YES"
                    shp_dt.type = shape_typ
                    if each_row['ROI_QUANTATY_OF_COORDINATES'] == 2:
                        # For Lane
                        x_lst = [each_row['ROI_COORDINATE_X_2'],
                                 each_row['ROI_COORDINATE_X_1']]
                        y_lst = [each_row['ROI_COORDINATE_Y_1'],
                                 y]
                        hght = y - each_row['ROI_COORDINATE_Y_1']
                        wdth = each_row['ROI_COORDINATE_X_2'] - each_row['ROI_COORDINATE_X_1']
                    elif each_row['ROI_QUANTATY_OF_COORDINATES'] == 4:
                        # For Box
                        x_lst = [each_row['ROI_COORDINATE_X_1'],
                                 each_row['ROI_COORDINATE_X_2'],
                                 each_row['ROI_COORDINATE_X_3'],
                                 each_row['ROI_COORDINATE_X_4']
                                 ]
                        y_lst = [each_row['ROI_COORDINATE_Y_1'],
                                 each_row['ROI_COORDINATE_Y_3'],
                                 each_row['ROI_COORDINATE_Y_2'],
                                 each_row['ROI_COORDINATE_Y_4']]
                        hght = each_row['ROI_COORDINATE_Y_3'] - each_row['ROI_COORDINATE_Y_1']
                        wdth = each_row['ROI_COORDINATE_X_2'] - each_row['ROI_COORDINATE_X_1']
                    elif each_row['ROI_QUANTATY_OF_COORDINATES'] == 6:
                        # For Box with edges
                        x_lst = [each_row['ROI_COORDINATE_X_1'],
                                 each_row['ROI_COORDINATE_X_3'],
                                 each_row['ROI_COORDINATE_X_4'],
                                 each_row['ROI_COORDINATE_X_6'],
                                 each_row['ROI_COORDINATE_X_2'],
                                 each_row['ROI_COORDINATE_X_5']]
                        y_lst = [each_row['ROI_COORDINATE_Y_1'],
                                 each_row['ROI_COORDINATE_Y_4'],
                                 each_row['ROI_COORDINATE_Y_2'],
                                 each_row['ROI_COORDINATE_Y_5'],
                                 each_row['ROI_COORDINATE_Y_3'],
                                 each_row['ROI_COORDINATE_Y_6']]
                        hght = each_row['ROI_COORDINATE_Y_4'] - each_row['ROI_COORDINATE_Y_1']
                        wdth = each_row['ROI_COORDINATE_X_3'] - each_row['ROI_COORDINATE_X_1']
                    else:
                        x_lst = [each_row['ROI_COORDINATE_X_2'],
                                 each_row['ROI_COORDINATE_X_1']]
                        y_lst = [each_row['ROI_COORDINATE_Y_1'],
                                 y]
                        hght = y - each_row['ROI_COORDINATE_Y_1']
                        wdth = each_row['ROI_COORDINATE_X_2'] - each_row['ROI_COORDINATE_X_1']
                    shp_dt.x = x_lst
                    shp_dt.y = y_lst

                    # Adding height and width after the Box rotation feature
                    try:
                        frm_anno_elmnts.width = wdth
                        frm_anno_elmnts.height = hght
                    except TypeError:
                        pass
                    frm_anno_elmnts.shape = shp_dt.__dict__
                    annotate_elmnts.FrameAnnoElements = [frm_anno_elmnts.__dict__]

                    annotate_elmnts_lst.append(annotate_elmnts.__dict__)

                    # End the loop here - Channel name remains same for now
                    # print "annotate_elmnts_lst :>> ", annotate_elmnts_lst
                    chn_dt.AnnotatedElements = annotate_elmnts_lst
                    chn_dt.ChannelName = "MFC4xxLongImageRight"
                else:
                    # print("In else....")
                    for ech_anno_dict in annotate_elmnts_lst:
                        # print("ldroi_tmstamp_lst::", ldroi_tmstamp_lst)
                        if each_row['FrameMtLDROITimeStamp'] == ech_anno_dict["TimeStamp"]:
                            # print "each_row['ROI_TRACK_ID']>> ", each_row['ROI_TRACK_ID']
                            # FrameAnnoElements is for each track ID
                            frm_anno_elmnts = LabelProperties()
                            frm_anno_elmnts.Hierarchy = "0"
                            frm_anno_elmnts.Trackid = each_row['ROI_TRACK_ID']

                            # Get the attribute details from the Database and loop in with the number of rows
                            # Add attribute details - from the database

                            attr_db = LabelProperties()
                            for each_key in ldroi_newkey_lst:
                                # DB fetches value None, Convert to string "None" else it would be null in JSON
                                if each_row[each_key] is None:
                                    row_data = "None"
                                else:
                                    row_data = each_row[each_key]
                                attr_db.__dict__[each_key] = row_data

                            # attr_db.LDROI_CYCLE_COUNTER = each_row['LDROI_CYCLE_COUNTER']
                            # attr_db.LDROI_CYCLE_ID = each_row['LDROI_CYCLE_ID']
                            # attr_db.LDROI_DATATYPE = each_row['LDROI_DATATYPE']
                            # attr_db.ROI_COORDINATE_X_1 = each_row['ROI_COORDINATE_X_1']
                            # attr_db.ROI_COORDINATE_Y_1 = each_row['ROI_COORDINATE_Y_1']

                            # To add the attribute elements
                            frm_anno_elmnts.attributes = attr_db.__dict__

                            try:
                                category_typ = TOOL_CONF['Category'][fn]
                            except KeyError:
                                category_typ = fn
                            frm_anno_elmnts.category = category_typ
                            shp_dt = LabelProperties()
                            shp_dt.__dict__['Algo Generated'] = "YES"
                            shp_dt.__dict__['Manually Corrected'] = "YES"
                            shp_dt.type = shape_typ
                            if each_row['ROI_QUANTATY_OF_COORDINATES'] == 2:
                                # For Lane
                                x_lst = [each_row['ROI_COORDINATE_X_2'],
                                        each_row['ROI_COORDINATE_X_1']]
                                y_lst = [each_row['ROI_COORDINATE_Y_1'],
                                        y]
                                hght = y - each_row['ROI_COORDINATE_Y_1']
                                wdth = each_row['ROI_COORDINATE_X_2'] - each_row['ROI_COORDINATE_X_1']
                            elif each_row['ROI_QUANTATY_OF_COORDINATES'] == 4:
                                # For Box
                                x_lst = [each_row['ROI_COORDINATE_X_1'],
                                         each_row['ROI_COORDINATE_X_2'],
                                         each_row['ROI_COORDINATE_X_3'],
                                         each_row['ROI_COORDINATE_X_4']
                                         ]
                                y_lst = [each_row['ROI_COORDINATE_Y_1'],
                                         each_row['ROI_COORDINATE_Y_3'],
                                         each_row['ROI_COORDINATE_Y_2'],
                                         each_row['ROI_COORDINATE_Y_4']]
                                hght = each_row['ROI_COORDINATE_Y_3'] - each_row['ROI_COORDINATE_Y_1']
                                wdth = each_row['ROI_COORDINATE_X_2'] - each_row['ROI_COORDINATE_X_1']
                            elif each_row['ROI_QUANTATY_OF_COORDINATES'] == 6:
                                # For Box with edges
                                x_lst = [each_row['ROI_COORDINATE_X_1'],
                                         each_row['ROI_COORDINATE_X_3'],
                                         each_row['ROI_COORDINATE_X_4'],
                                         each_row['ROI_COORDINATE_X_6'],
                                         each_row['ROI_COORDINATE_X_2'],
                                         each_row['ROI_COORDINATE_X_5']]
                                y_lst = [each_row['ROI_COORDINATE_Y_1'],
                                         each_row['ROI_COORDINATE_Y_4'],
                                         each_row['ROI_COORDINATE_Y_2'],
                                         each_row['ROI_COORDINATE_Y_5'],
                                         each_row['ROI_COORDINATE_Y_3'],
                                         each_row['ROI_COORDINATE_Y_6']]
                                hght = each_row['ROI_COORDINATE_Y_4'] - each_row['ROI_COORDINATE_Y_1']
                                wdth = each_row['ROI_COORDINATE_X_3'] - each_row['ROI_COORDINATE_X_1']
                            else:
                                x_lst = [each_row['ROI_COORDINATE_X_2'],
                                         each_row['ROI_COORDINATE_X_1']]
                                y_lst = [each_row['ROI_COORDINATE_Y_1'],
                                         y]
                                hght = y - each_row['ROI_COORDINATE_Y_1']
                                wdth = each_row['ROI_COORDINATE_X_2'] - each_row['ROI_COORDINATE_X_1']
                            shp_dt.x = x_lst
                            shp_dt.y = y_lst

                            # Adding height and width after the Box rotation feature
                            try:
                                frm_anno_elmnts.width = wdth
                                frm_anno_elmnts.height = hght
                            except TypeError:
                                pass
                            frm_anno_elmnts.shape = shp_dt.__dict__
                            ech_anno_dict["FrameAnnoElements"].append(frm_anno_elmnts.__dict__)
                            # print("Time stamp present", ech_anno_dict)

        info_elmnts_dict = {}
        if ldss_data:
            #Get the LDSS table keys
            ldss_keys = [ech_key.upper() for ech_key in ldss_data[0].keys()]
            # ldss_keys.remove('RecIdFileName')
            ldss_keys.remove('RECIDFILENAME')
            ldss_keys.remove('TAN')
            # print "ldss_keys :: ", ldss_keys

            for each_ldss_key in ldss_keys:
                info_elmnts_dict[str(each_ldss_key).upper()] = []
            # print "info_elmnts_dict :: ", info_elmnts_dict
            for info_elmnts_key, info_elmnts_value in info_elmnts_dict.iteritems():
                # print("info_elmnts_key::", info_elmnts_key)
                for each_ldss_dict in ldss_data:
                    each_ldss_dict = {k.upper(): v for k, v in each_ldss_dict.iteritems()}
                    # print("each_ldss_dict::", each_ldss_dict)
                    # print("\n")
                    if each_ldss_dict[info_elmnts_key] is not None:
                        info_elmnts_value.append({
                                    "end": str(each_ldss_dict["FRAMEMTLDSSTIMESTAMPSTOP"]),
                                    "start": str(each_ldss_dict["FRAMEMTLDSSTIMESTAMPSTART"]),
                                    "value": each_ldss_dict[info_elmnts_key]
                                })
        # print "After processing :: ", info_elmnts_dict
        dvc_dt.InformationElements = info_elmnts_dict

        dvc_dt.ChannelData = [chn_dt.__dict__]
        cmn_dt.DeviceData = [dvc_dt.__dict__]

        prop_lbl = LabelProperties()
        prop_lbl.Sequence = [cmn_dt.__dict__]
        # print "prop_lbl :: ", prop_lbl.__dict__

        data = prop_lbl.__dict__

        return jsonify(data)

class GetFncTanForTid(Resource):

    def get(self):
        """
        http://127.0.0.1:5000/get_prjfn_tan_rec_for_tid?ticket_id=201709290649073737
        http://lu00093vmx.li.de.conti.de/label_tool/get_prjfn_tan_rec_for_tid?ticket_id=201709290649073737
        :return:
        """
        parser = reqparse.RequestParser()
        parser.add_argument('ticket_id', type=int)
        # parser.add_argument('tool_conf', type=str)
        args = parser.parse_args()
        # Assign the arguments after parsing
        ticket_id = args['ticket_id']
        # print "ticket_id :> ", ticket_id
        try:
            orc_conn = db
            # orc_conn = OracleRecCatalogDB("uid={0};""pwd={1}".format(UID, PWD),
            #                         table_prefix="{}".format(TBL_PRFX))
            # orc_conn = cx_connect("ALGO_DB_USER", "read", "racadmpe", False)
            # print "orc_conn :: ", orc_conn
        except DatabaseError, info:
            print "Logon Error:", info
            sys.exit()

        # stmt = '''SELECT tmt.TICKET_ID,
        #               REPLACE(CONCAT(ADMSADMIN.split_part(alc.LABEL_DESC_VERS,'xxxx',1),
        #             ADMSADMIN.split_part(alc.LABEL_DESC_VERS,'xxxx',2)),'#','_') as PRJ_FN,
        #             tmt."RecIdFileName"
        #             FROM ADMSADMIN.ticket_master_table tmt,
        #               ADMSADMIN.adms_lbl_config alc
        #             WHERE tmt.WORK_STATUS       = 'WORK'
        #             AND tmt.WORK_STATUS_INFO    = 'WORK'
        #             AND tmt.LABEL_DESC_VERS = alc.CONF_ID
        #             AND tmt.TICKET_ID = {} '''.format(ticket_id)
        stmt = '''SELECT tmt.TICKET_ID,
                  REPLACE(CONCAT(ADMSADMIN.split_part(alc.LABEL_DESC_VERS,'xxxx',1),
                ADMSADMIN.split_part(alc.LABEL_DESC_VERS,'xxxx',2)),'#','_') as PRJ_FN,
                tmt."RecIdFileName"
                FROM ADMSADMIN.ticket_master_table tmt,
                  ADMSADMIN.adms_lbl_config alc
                WHERE 
                tmt.LABEL_DESC_VERS = alc.CONF_ID
                AND tmt.TICKET_ID = {} '''.format(ticket_id)
        orc_cursor = orc_conn.cursor()
        # print "stmt >> ", stmt
        orc_cursor.execute(stmt)
        data = orc_cursor.fetchall()
        # print ">>::{ ", data
        colun_names = [column[0].upper() for column in
                       orc_cursor.description]
        data = [dict(zip(colun_names, rec)) for rec in data]
        print "data ::> ", data
        if data:
            prj_fn = data[0]['PRJ_FN']
            rec_name = data[0]['RECIDFILENAME']
            # Get the TAN for the ticket ID from the CD table
            stmt_tan = ''' select DISTINCT(TAN) from ADMSADMIN.{0}_{1}_T where
                            "RecIdFileName" = '{2}' '''.format(prj_fn, 'CD',
                                                               data[0][
                                                                   'RECIDFILENAME'])
            print "stmt_tan ::> ", stmt_tan
            orc_cursor.execute(stmt_tan)
            data_tan = orc_cursor.fetchall()
            print "data_tan :: ", data_tan
            if data_tan:
                tan = data_tan[0][0]
            else:
                tan = None
            return jsonify(prj_fn, rec_name, tan)
            # return jsonify(None, None, None)
        else:
            return jsonify(None, None, None)

class GetClmnNamesTbl(Resource):

    def get(self):
        """
        http://127.0.0.1:5000/get_clmn_names_for_tables?prj_fn=MFC400_LD&label_typ=LDSS
        http://lu00093vmx.li.de.conti.de/label_tool/get_clmn_names_for_tables?prj_fn=MFC400_LD&label_typ=LDSS
        :return:
        """
        parser = reqparse.RequestParser()
        parser.add_argument('prj_fn', type=str)
        parser.add_argument('label_typ', type=str)
        args = parser.parse_args()
        # Assign the arguments after parsing
        prj_fn = args['prj_fn']
        label_typ = args['label_typ']
        # print "ticket_id :> ", ticket_id
        try:
            # orc_conn = OracleRecCatalogDB("uid={0};""pwd={1}".format(UID, PWD),
            #                         table_prefix="{}".format(TBL_PRFX))
            orc_conn = db
            # orc_conn = cx_connect("ALGO_DB_USER", "read", "racadmpe", False)
            # print "orc_conn :: ", orc_conn
        except DatabaseError, info:
            print "Logon Error:", info
            sys.exit()

        stmt = '''SELECT * FROM ADMSADMIN.{}_{} WHERE ROWNUM <1 '''.format(
            prj_fn, label_typ)
        orc_cursor = orc_conn.cursor()
        orc_cursor.execute(stmt)
        data = orc_cursor.fetchall()
        colun_names = [column[0] for column in
                       orc_cursor.description]
        return jsonify(colun_names)

class AddGenDataPrprd(Resource):
    """

    """
    def post(self):
        """
        Add data in bulk to a table.
        Can only be used if user has at least a collection user role
        :return: True if all data has been added appropriately
                False if an unknown error/exception occurs
        """
        input_para = loads(request.json)
        records = input_para["records"]
        table_name = str(input_para['table_name'])
        if type(records) is not list:
            return jsonify(
                data="The type of data to be given is a list with "
                     "dictionary in it.")
        try:
            db_handler = OracleRecCatalogDB(
                "uid={0};""pwd={1}".format(UID, PWD),
                table_prefix="{}".format(TBL_PRFX))
            db_handler.add_generic_data_prepared(records, table_name)
            db_handler.commit()
            # db_handler.close()
            return jsonify(data=True)
        except Exception:
            return jsonify(data=False)

class GetMTSUserDetails(Resource):

    def get(self):
        """
        uidj8936 SMR1EKO6PB
        http://127.0.0.1:5000/get_mts_usr_details?windows_uid=uidj8936
        http://lu00093vmx.li.de.conti.de/label_tool/get_mts_usr_details?windows_uid=uidj8936
        :return:
        """
        parser = reqparse.RequestParser()
        parser.add_argument('windows_uid', type=str)
        args = parser.parse_args()
        # Assign the arguments after parsing
        windows_uid = args['windows_uid']
        # print "ticket_id :> ", ticket_id
        try:
            orc_conn = OracleRecCatalogDB(
                "uid={0};""pwd={1}".format(UID, PWD),
                table_prefix="{}".format(TBL_PRFX))
            # orc_conn = cx_connect("ALGO_DB_USER", "read", "racadmpe", False)
            # print "orc_conn :: ", orc_conn
        except DatabaseError, info:
            print "Logon Error:", info
            sys.exit()

        stmt = '''SELECT WINDOWS_USERNAME, USERNAME, PW, PK
        FROM ADMSADMIN.USERS WHERE WINDOWS_USERNAME = '{}' '''.format(
            windows_uid)
        orc_cursor = orc_conn.cursor()
        orc_cursor.execute(stmt)
        usr_det = orc_cursor.fetchall()
        print usr_det
        colun_names = [column[0] for column in
                       orc_cursor.description]
        data = [dict(zip(colun_names, rec)) for rec in usr_det]
        if len(data) == 1:
            return jsonify(data[0])
        else:
            return jsonify({})

class UpdateTicketStatus(Resource):
    """

    """
    def post(self):
        """
        Update data in table
        :return: True if all data has been added appropriately
                False if an unknown error/exception occurs
        To verify the status of the ticket:
        select WORK_STATUS, WORK_STATUS_INFO
        from ADMSADMIN.TICKET_MASTER_TABLE
        where TICKET_ID = 2018102909282814543
        """
        input_para = loads(request.json)
        table_name = str(input_para['table_name'])
        ticket_id = int(input_para["ticket_id"])
        status = str(input_para['status'])
        try:
            db_handler = OracleRecCatalogDB(
                "uid={0};""pwd={1}".format(UID, PWD),
                table_prefix="{}".format(TBL_PRFX))
            cond = SQLBinaryExpr('TICKET_ID', OP_EQ, ticket_id)
            # record_lst = db_handler.select_generic_data(select_list=["*"], table_list=[table_name], where=cond)
            # record_dict = record_lst[0]
            record_dict1 = {}
            record_dict1["WORK_STATUS"] = status
            record_dict1["WORK_STATUS_INFO"] = status
            # print "record_dict::", record_dict
            db_handler.update_generic_data(record_dict1, table_name, where=cond)
            db_handler.commit()

            return jsonify(data=True)
        except Exception:
            return jsonify(data=False)


class SyncLabeledData(Resource):
    """
    Input parameters will be labeled data JSON file path
    Also the user ID for which the data needs to be synced

    """
    def post(self):
        # Read the JSON file
        # lbld_json_path = r'D:\2018121013475616511_Testin LT5G Tool_LabelData.json'
        try:
            userid = request.META['REMOTE_USER']
        except:
            print("Error in geting user")
            userid = getpass.getuser()
        userid = 'uib57489'
        print "userid ::", userid
        # try:
        if 'file' not in request.files:
            return Response({'message': "No File Found"}, status=400,
                            mimetype='application/json')
        file = request.files['file']
        # if user does not select file, browser also
        # submit an empty part without filename
        upload_path = create_random_directory(os.path.join(application.config['UPLOAD_FOLDER'], 'json'))
        if file:
            filename = secure_filename(file.filename)
            file.save(os.path.join(upload_path, filename))
            input_file = os.path.join(upload_path, filename)
            lbld_json_path = input_file
            print "lbld_json_path::", lbld_json_path

        with open(lbld_json_path) as data_file:
            self.lbld_json_data = json.load(data_file)
        # print ":::::"
        # ticket ID from JSON
        self.ticket_id = self.lbld_json_data['Sequence'][0]["DeviceCommonData"]["FolderName"]
        # print "ticket_id ::", self.ticket_id
        data = self.get_prjfn_tan_rec_for_tid(int(self.ticket_id))
        # print "data ::>>", data
        self.prj_fn, self.r_name, self.tan = data

        # Decide the table type based on the project
        if "HFL" in self.prj_fn or "HFL" in self.prj_fn or "HFL" in self.prj_fn:
            self.table_type = 'INTERFACE'
        else:
            # Intermediate table
            self.table_type = 'INTERFACE'

        # Get unique ID for the user from MTS database
        usr_data_dict = self.get_mts_usr_details(str(userid))
        if not bool(usr_data_dict):
            print "Username and password entered are not entered"
            # exit(0)
        # MTS unique user id will be used to generate TAN
        self.mks_uid = usr_data_dict['PK']
        # Insert data in the order CD, LDROI and LDSS
        self.insert_data_T_tables_cd()
        self.insert_data_T_tables_ldroi()
        self.insert_data_T_tables_ldss()
        # except Exception:
        #     return jsonify({'status': 'error'})
        return jsonify({'status': 'success'})

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
            if COMMIT:
                self.add_generic_data_prepared_lt5(records=each_chnk,
                                              table_name="{}_{}_{}".format(self.prj_fn, LDSS_LABEL, self.table_type))
                # self.orc_dbhandle.commit()
                print "Data inserted in {0}_{1}_{3} --------======== of batch size {2}\n".format(self.prj_fn, LDSS_LABEL,
                                                                                               PORTION, self.table_type)

    def build_dict_qry_lst_ldss(self):

        # print "label_data ::> ", label_data
        parsed_data_dict = self.parse_json_data_ldroi(self.lbld_json_data)
        print "self.prj_fn, r_name, tan :: ", self.prj_fn, self.r_name, self.tan
        # updating parsed_data_dict to have recording name as taken from database not from JSON file,
        # commenting the below line will enable fetching the name from JSON
        parsed_data_dict['''"RecIdFileName"'''] = self.r_name
        parsed_data_dict['''"TAN"'''] = self.tan
        print "parsed_data_dict :: ", parsed_data_dict

        colun_names = self.get_clmn_names_for_tables(self.prj_fn, LDSS_LABEL+'_'+self.table_type)
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
            self.lbld_json_data['Sequence'][0]["DeviceData"][0][
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
                            each_clmn_nm.upper() == ech_attr.upper()]
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
                             if clmn_nm_attr:
                                ech_row[clmn_nm_attr[0]] = value
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
                        if clmn_nm_attr:
                            ref_row_dict_new[clmn_nm_attr[0]] = value
                        data_lst.append(ref_row_dict_new)
                    # print "\ndata_lst brf :: ", data_lst
                else:
                    ref_row_dict_new = dict(ref_row_dict)
                    ref_row_dict_new['FrameMtLDSSTimeStampStart'] = start_tm
                    ref_row_dict_new['FrameMtLDSSTimeStampStop'] = end_tm
                    if clmn_nm_attr:
                        ref_row_dict_new[clmn_nm_attr[0]] = value
                    data_lst.append(ref_row_dict_new)
                        # data_lst.extend(data_lst)
                # else:
                #         print "main_dt_lst :: ", main_dt_lst

            # print "\ndata_lst :: ", data_lst
        #
        return data_lst, parsed_data_dict

    def get_clmn_names_for_tables(self, prj_fn, label_typ):

        try:
            # orc_conn = OracleRecCatalogDB("uid={0};""pwd={1}".format(UID, PWD),
            #                         table_prefix="{}".format(TBL_PRFX))
            orc_conn = db
            # orc_conn = cx_connect("ALGO_DB_USER", "read", "racadmpe", False)
            # print "orc_conn :: ", orc_conn
        except DatabaseError, info:
            print "Logon Error:", info
            sys.exit()

        stmt = '''SELECT * FROM ADMSADMIN.{}_{} WHERE ROWNUM <1 '''.format(
            prj_fn, label_typ)
        orc_cursor = orc_conn.cursor()
        orc_cursor.execute(stmt)
        data = orc_cursor.fetchall()
        colun_names = [column[0] for column in
                       orc_cursor.description]
        return colun_names

    def gen_ordinates_on_shptyp(self, ordinate_data, attr_dict, colun_names):

        if ordinate_data:
            x_ordinate_lst = ordinate_data["x"]
            y_ordinate_lst = ordinate_data["y"]
            # To fill ROI_QUANTATY_OF_COORDINATES column in the table
            ordinate_qntity_length = len(x_ordinate_lst)
            # print("ordinate_qntity_length::", ordinate_qntity_length)
            shape_typ = ordinate_data["type"]
            if shape_typ in ['Line']:
                ordinate_qntity = 2
            elif shape_typ in ['Box'] and ordinate_qntity_length == 4:
                ordinate_qntity = 4
            elif shape_typ in ['Box'] and ordinate_qntity_length == 6:
                ordinate_qntity = 6
            else:
                ordinate_qntity = 4
            if '''"ROI_QUANTATY_OF_COORDINATES"''' in colun_names:
                # print "enter this space ......."
                attr_dict.update(
                    {'''"ROI_QUANTATY_OF_COORDINATES"''': ordinate_qntity})
                if ordinate_qntity in [2, 4]:
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
                else:
                    for ech_ord in range(0, ordinate_qntity):
                        # x1 = x4 , x2 = x3, y1=y2, y3=y4, keep y1 as first coordinate and y3 as second coordinate
                        ord_idx_x = ech_ord
                        ord_idx_y = ech_ord
                        # Conditions for Y-coordinates
                        if ech_ord == 3 or ech_ord == 4 or ech_ord == 5:
                            ord_idx_y = 1
                        if ech_ord == 0 or ech_ord == 1 or ech_ord == 2:
                            ord_idx_y = 0
                        # Conditions for X-coordinates
                        if ech_ord == 2 or ech_ord == 3:
                            ord_idx_x = 2
                        if ech_ord == 1 or ech_ord == 4:
                            ord_idx_x = 4
                        if ech_ord == 0 or ech_ord == 5:
                            ord_idx_x = 0
                        attr_dict.update(
                            {'''"ROI_COORDINATE_X_{}"'''.format(ech_ord + 1):
                                 int(x_ordinate_lst[ord_idx_x])})
                        attr_dict.update(
                            {'''"ROI_COORDINATE_Y_{}"'''.format(ech_ord + 1):
                                 int(y_ordinate_lst[ord_idx_y])})
        return attr_dict

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
        # print "ldroi_attrib ::>", ldroi_attrib
        if self.prj_fn in ['LSM501_PMDRT', 'LSM501_PMDLT', 'LSM501_PMDFT', 'LSM501_PMDRR']:
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
                try:
                    if ldroi_attrib[ech_conn_attrib] is not None:
                        conn_trckid_lst.append(ldroi_attrib[ech_conn_attrib])
                        # print "bayline_colmn :: ", bayline_colmn
                        pmd_specific_dict.update({bayline_colmn: 'L' +' (C' + str(track_id) +
                                                                 ' C' +ldroi_attrib[ech_conn_attrib] + ')'})
                    else:
                        pmd_specific_dict.update({bayline_colmn: None})
                except KeyError:
                    pass
            # Add connecting trackids based on schema
            if '''"PMD_CONNECTING_TRACKID"''' in ldroi_attrib:
                connct_trkid = ldroi_attrib['''"PMD_CONNECTING_TRACKID"''']
                if connct_trkid is not None:
                    if connct_trkid != 'None' or connct_trkid != 'NA':
                        print "connct_trkid ::", connct_trkid
                        pmd_specific_dict.update({'''"PMD_BAYLINE_L1"''': 'L' + ' (C' + str(track_id) +
                                                                 ' C' + connct_trkid + ')'})
            # if '''"PMD_CONNECTING_TRACKID"''' in ldroi_attrib:
            #     pmd_specific_dict.update({'''"PMD_CONNECTING_TRACKID"''': ldroi_attrib['''"PMD_CONNECTING_TRACKID"''']})
            # else:
            #     pmd_specific_dict.update({'''"PMD_CONNECTING_TRACKID"''': ",".join(conn_trckid_lst)})
            # print "pmd_specific_dict >> : ", pmd_specific_dict
            # exit(0)
            return pmd_specific_dict
        else:
            return {}

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

    def build_dict_qry_lst_ldroi(self):

        # print "label_data ::> ", label_data
        parsed_data_dict = self.parse_json_data_ldroi(self.lbld_json_data)
        # print "parsed_data_dict :: ", parsed_data_dict
        # self.tan = "201809281417380000LT5G103900000102018092805342013428"
        print "self.prj_fn, r_name, tan :: ", self.prj_fn, self.r_name, self.tan
        #updating parsed_data_dict to have recording name as taken from database not from JSON file,
        #commenting the below line will enable fetching the name from JSON
        parsed_data_dict['''"RecIdFileName"'''] = self.r_name
        parsed_data_dict['''"TAN"'''] = self.tan

        colun_names = self.get_clmn_names_for_tables(self.prj_fn, LDROI_LABEL+'_'+self.table_type)
        #Get column names of the tables
        colun_names = ['''"{}"'''.format(each_colmn) for each_colmn in
                       colun_names]
        # print 'colun_names :>> ', colun_names
        #get the rest of the columns from attributes
        try:
            ldroi_atttr_lst = self.lbld_json_data['Sequence'][0]["DeviceData"][0]["ChannelData"][0][
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
                # print "othr_attr :: ", othr_attr
                attr_dict2.update(othr_attr)
            #Block from here is to add the ordinates data along with ROI quantity
                ordinate_data = each_attr["FrameAnnoElements"][index]["shape"]
                # print "track_id ,, ordinate_data :: ", track_id, ordinate_data
                attr_dict3 = self.gen_ordinates_on_shptyp(ordinate_data, attr_dict, colun_names)
                attr_dict2.update(attr_dict3)
                # Compute centroid, Bayline, connecting track ID for PMD function
                attr_dict4 = self.compute_centroid_pmd(othr_attr, attr_dict3, track_id)
                attr_dict2.update(attr_dict4)
                attr_dict2.update(parsed_data_dict)
            #Block ends here for adding the ROI coordinates
                # print "attr_dict before2 :: ", attr_dict2
                # print "colun_names::", colun_names
            # Sanity check for the columns
            #     attr_dict2 = {k: v for k, v in attr_dict2.iteritems() if k in colun_names}
                # Have the column names in place as in the DB so that data is pushed correctly
                # Camel case, lower/upper case
                attr_dict_dt = {}
                for k, v in attr_dict2.iteritems():
                    # print k
                    ky = [ech_clmn_name for ech_clmn_name in colun_names if k == ech_clmn_name.upper()]
                    # print "ky:::", ky
                    if ky:
                        # Delete the existing Key and retain the column name as in DB
                        # If column name does not match exactly then data will not be inserted
                        del attr_dict2[k]
                        attr_dict_dt.update({ky[0]: v})
                attr_dict2.update(attr_dict_dt)
                # print "attr_dict2 after :: ", attr_dict2
                lst_ldroi_data_insert.append(attr_dict2)
                # lst_ldroi_data_insert.append(attr_dict)
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

        # all_keys = each_chnk[0].keys()
        # New logic
        idx_dict_len_mapper = {}
        for idx, evry_dct in enumerate(each_chnk):
            d_len = len(evry_dct)
            idx_dict_len_mapper[idx] = d_len
        max_key_idx = max(idx_dict_len_mapper, key=idx_dict_len_mapper.get)
        all_keys = each_chnk[max_key_idx].keys()
        # print(">>", all_keys)
        l = []
        for ech_row in each_chnk:
            # print("<<", ech_row.keys())
            diff_column_names = list(set(all_keys) - set(ech_row.keys()))
            # print("diff_column_names::", diff_column_names)
            for ech_diff_column in diff_column_names:
                ech_row[ech_diff_column] = None
            nd = {}
            # try:
            for ech_key in all_keys:
                nd[ech_key] = ech_row[ech_key]
            # except KeyError:
            #     nd[ech_key] = None
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
            print "each_chnk :: ", each_chnk
            if COMMIT:
                self.add_generic_data_prepared_lt5(records=each_chnk, table_name="{}_{}_{}".format(self.prj_fn, LDROI_LABEL, self.table_type))
                import time
                # time.sleep(1)
                # self.orc_dbhandle.commit()
                print "Data inserted in {0}_{1}_{3} --------======== of batch size {2}\n".format(self.prj_fn, LDROI_LABEL, PORTION, self.table_type)

    def generate_tan_lt5g(self):
        """"
        Generate a new TAN which has the LT5G string in it
        """

        curr_time = datetime.now()
        date_obj = datetime.strptime(str(curr_time), '%Y-%m-%d %H:%M:%S.%f')
        dt_conv = date_obj.strftime('%Y%m%d%H%M%S')
        lt5_tag = '0000LT5G0'
        user_id = str(self.mks_uid)
        random_num = '000010'
        tan = dt_conv + lt5_tag + user_id + random_num + self.ticket_id
        return tan

    def insert_data_T_tables_cd(self):
        print ">> Inserting data into CD table <<"
        # self.tan = '201901281356310000LT5G010280000102019012507525121573'
        self.tan = self.generate_tan_lt5g()
        # Get the start and end timestamp from label data file
        try:
            # If channel data is an empty list then it will throw index error
            all_anno_elems_lst = self.lbld_json_data['Sequence'][0]["DeviceData"][0]["ChannelData"][0]["AnnotatedElements"]
            tmstamp_lst = [evry_frm_anno_dict['TimeStamp'] for evry_frm_anno_dict in all_anno_elems_lst]
            start_tm = tmstamp_lst[0]
            end_tm = tmstamp_lst[-1]
        except IndexError:
            # print "mmnnnbb", self.lbld_json_data['Sequence'][0]["DeviceData"][0]
            info_elem_dict = self.lbld_json_data['Sequence'][0]["DeviceData"][0]["InformationElements"]
            # Pick one LDSS attribute
            # one_attr = info_elem_dict.keys()[0]
            # Pick an attribute which has at least one entry
            one_attr_lst = [k for k, v in info_elem_dict.iteritems() if len(v) > 0]
            one_attr = one_attr_lst[0]
            print "info_elem_dict[one_attr] ::", one_attr, info_elem_dict[one_attr]
            start_tm = info_elem_dict[one_attr][0]["start"]
            end_tm = info_elem_dict[one_attr][0]["end"]
        cd_attrib_dict = {}
        cd_attrib_dict['''"TAN"'''] = self.tan
        cd_attrib_dict['''"RecIdFileName"'''] = self.r_name
        cd_attrib_dict['''"SequenzMtTimeStampStart"'''] = start_tm #tmstamp_lst[0]
        cd_attrib_dict['''"SequenzMtTimeStampStop"'''] = end_tm #tmstamp_lst[-1]
        cd_attrib_dict['''"CD_START_CYCLE_ID"'''] = 60
        cd_attrib_dict['''"CD_STOP_CYCLE_ID"'''] = 60
        cd_attrib_dict['''"CD_START_CYCLE_COUNTER"'''] = 7
        cd_attrib_dict['''"CD_STOP_CYCLE_COUNTER"'''] = 209
        cd_attrib_dict['''"CD_START_DATATYPE"'''] = 'AAP///////8='
        cd_attrib_dict['''"CD_STOP_DATATYPE"'''] = 'AAP///////8='
        print("cd_attrib_dict ::", cd_attrib_dict)
        if COMMIT:
            self.add_generic_data_prepared_lt5(records=[cd_attrib_dict], table_name="{}_{}_{}".format(self.prj_fn, CD_LABEL, self.table_type))

    def add_generic_data_prepared_lt5(self, records, table_name):

        if type(records) is not list:
            print "The type of data to be given is a list with dictionary in it."
        try:
            # db_handler = OracleRecCatalogDB(
            #     "uid={0};""pwd={1}".format(UID, PWD),
            #     table_prefix="{}".format(TBL_PRFX))
            db_handler = db
            db_handler.add_generic_data_prepared(records, table_name)
            db_handler.commit()
            # db_handler.close()
            return True
        except Exception:
            return False

    def get_mts_usr_details(self, windows_uid):
        try:
            # orc_conn = OracleRecCatalogDB(
            #     "uid={0};""pwd={1}".format(UID, PWD),
            #     table_prefix="{}".format(TBL_PRFX))
            orc_conn = db
            # orc_conn = cx_connect("ALGO_DB_USER", "read", "racadmpe", False)
            # print "orc_conn :: ", orc_conn
        except DatabaseError, info:
            print "Logon Error:", info
            sys.exit()

        stmt = '''SELECT WINDOWS_USERNAME, USERNAME, PW, PK
        FROM ADMSADMIN.USERS WHERE WINDOWS_USERNAME = '{}' '''.format(
            windows_uid)
        orc_cursor = orc_conn.cursor()
        orc_cursor.execute(stmt)
        usr_det = orc_cursor.fetchall()
        print usr_det
        colun_names = [column[0] for column in
                       orc_cursor.description]
        data = [dict(zip(colun_names, rec)) for rec in usr_det]
        if len(data) == 1:
            return data[0]
        else:
            return {}

    def get_prjfn_tan_rec_for_tid(self, ticket_id):
        try:
            orc_conn = db
        except DatabaseError, info:
            print "Logon Error:", info
            sys.exit()
        stmt = '''SELECT tmt.TICKET_ID,
                  REPLACE(CONCAT(ADMSADMIN.split_part(alc.LABEL_DESC_VERS,'xxxx',1),
                ADMSADMIN.split_part(alc.LABEL_DESC_VERS,'xxxx',2)),'#','_') as PRJ_FN,
                tmt."RecIdFileName"
                FROM ADMSADMIN.ticket_master_table tmt,
                  ADMSADMIN.adms_lbl_config alc
                WHERE 
                tmt.LABEL_DESC_VERS = alc.CONF_ID
                AND tmt.TICKET_ID = {} '''.format(ticket_id)
        orc_cursor = orc_conn.cursor()
        # print "stmt >> ", stmt
        orc_cursor.execute(stmt)
        data = orc_cursor.fetchall()
        # print ">>::{ ", data
        colun_names = [column[0].upper() for column in
                       orc_cursor.description]
        data = [dict(zip(colun_names, rec)) for rec in data]
        print "data ::> ", data
        if data:
            prj_fn = data[0]['PRJ_FN']
            rec_name = data[0]['RECIDFILENAME']
            # Get the TAN for the ticket ID from the CD table
            stmt_tan = ''' select DISTINCT(TAN) from ADMSADMIN.{0}_{1}_INTERFACE where
                            "RecIdFileName" = '{2}' '''.format(prj_fn, 'CD',
                                                               data[0][
                                                                   'RECIDFILENAME'])
            print "stmt_tan ::> ", stmt_tan
            orc_cursor.execute(stmt_tan)
            data_tan = orc_cursor.fetchall()
            print "data_tan :: ", data_tan
            if data_tan:
                tan = data_tan[0][0]
            else:
                tan = None
            return (prj_fn, rec_name, tan)
            # return jsonify(None, None, None)
        else:
            return (None, None, None)


#-----------------------------------------------------------------
# Label tool config and label output data
# http://127.0.0.1:5000/build_label_config?conf_id=conf_id369
api.add_resource(BuildLabelConfig, '/build_label_config')
api.add_resource(GetConfIDFromTckID, '/get_confid_frm_tckid')
api.add_resource(GetLabeledOutputData, '/get_lbld_op_data')
#-----------------------------------------------------------------
# Label tool data push to oracle
api.add_resource(GetFncTanForTid, '/get_prjfn_tan_rec_for_tid')
api.add_resource(GetClmnNamesTbl, '/get_clmn_names_for_tables')
api.add_resource(AddGenDataPrprd, '/add_generic_data_prepared_lt5')
#-----------------------------------------------------------------
# user validations
api.add_resource(GetMTSUserDetails, '/get_mts_usr_details')
#-----------------------------------------------------------------
# Update ticket status
api.add_resource(UpdateTicketStatus, '/update_tkt_status')
#-----------------------------------------------------------------
# Sync oracle
api.add_resource(SyncLabeledData, '/sync_lbld_data')


if __name__ == '__main__':
    application.run()
    # application.run(host='0.0.0.0', port=8055)



