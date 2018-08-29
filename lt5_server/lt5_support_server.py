from flask import request
import requests
import re
import sys
from flask import jsonify, Flask
from flask_restful import Resource, Api, reqparse
from json import loads
from cx_Oracle import DatabaseError
from lnk_cat import OracleRecCatalogDB
from werkzeug.local import LocalProxy

UID = ''
PWD = ''
TBL_PRFX = ''

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
             "ODRR": "Box"

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
              "ODRR": "Object Detection"

  }
}

application = Flask(__name__)
api = Api(application)

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

        :param conf_id:
        :return:
        """
        # resp = requests.get(
        #     url=r"http://luv4evrg:9090/API/public/labels/getLabelsConfiguration/{}/LDSS".format(
        #         conf_id))
        resp = requests.get(
            url=r" ".format(
                conf_id))
        # print "resp :: ", loads(resp.content)
        return loads(resp.content)

    def get_ldroi_data(self, conf_id):
        resp = requests.get(
            url=r"".format(
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
                info_elemnts_lst.append({key: each_attr_data})

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
                anno_elemnts_lst.append({key: each_attr_data})

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

        stmt = ''' '''.format(conf_id)
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
        stmt = ''' '''.format(ticket_id)
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

        stmt = ''' '''.format(ticket_id)
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
            return prj_fn, rec_name
        else:
            print "No configuration ID for the entered ticket ID."
            return [], []

    def gen_op_lbl_data(self, prj_fn, rec_name):

        # prj_fn, rec_name = self.get_prj_fn_for_ticketid(ticket_id)
        stmt = ''' '''.format(prj_fn, rec_name)
        orc_cursor = self.orc_conn.cursor()
        orc_cursor.arraysize = 5000
        orc_cursor.execute(stmt)
        data = orc_cursor.fetchall()
        colun_names = [column[0] for column in
                       orc_cursor.description]
        data = [dict(zip(colun_names, rec)) for rec in data]
        return data

    def gen_op_lbl_ldss_data(self, prj_fn, rec_name):

        # prj_fn, rec_name = self.get_prj_fn_for_ticketid(ticket_id)
        stmt = ''' '''.format(prj_fn, rec_name)
        orc_cursor = self.orc_conn.cursor()
        orc_cursor.arraysize = 5000
        orc_cursor.execute(stmt)
        data = orc_cursor.fetchall()
        colun_names = [column[0] for column in
                       orc_cursor.description]
        data = [dict(zip(colun_names, rec)) for rec in data]
        return data

    def get_fn_confid(self, conf_id):

        stmt = ''' '''.format(conf_id)
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
        :return:
        """
        parser = reqparse.RequestParser()
        parser.add_argument('ticket_id', type=int)
        parser.add_argument('conf_id', type=str)
        # parser.add_argument('tool_conf', type=str)
        args = parser.parse_args()
        # Assign the arguments after parsing
        ticket_id = args['ticket_id']
        conf_id = args['conf_id']
        prj_fn, rec_name = self.get_prj_fn_for_ticketid(ticket_id)
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
        cmn_attr.TAN = tan
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

                # Add information under the annotated elements, this changes dynamically depending upon the timestamps
                annotate_elmnts = LabelProperties()
                annotate_elmnts.FrameNumber = 0
                annotate_elmnts.TimeStamp = each_row['FrameMtLDROITimeStamp']

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

                # To add the attribute elements
                frm_anno_elmnts.attributes = attr_db.__dict__


                frm_anno_elmnts.category = prj_fn
                shp_dt = LabelProperties()
                shp_dt.__dict__['Algo Generated'] = "YES"
                shp_dt.__dict__['Manually Corrected'] = "YES"
                shp_dt.type = shape_typ
                shp_dt.x = [each_row['ROI_COORDINATE_X_1'],
                            each_row['ROI_COORDINATE_X_2']]
                shp_dt.y = [each_row['ROI_COORDINATE_Y_1'],
                            each_row['ROI_COORDINATE_Y_2']]
                frm_anno_elmnts.shape = shp_dt.__dict__
                annotate_elmnts.FrameAnnoElements = [frm_anno_elmnts.__dict__]

                annotate_elmnts_lst.append(annotate_elmnts.__dict__)

            # End the loop here - Channel name remains same for now
            # print "annotate_elmnts_lst :>> ", annotate_elmnts_lst
            chn_dt.AnnotatedElements = annotate_elmnts_lst
            chn_dt.ChannelName = "MFC4xxLongImageRight"

            info_dt = LabelProperties()

        info_elmnts_dict = {}
        if ldss_data:
            #Get the LDSS table keys
            ldss_keys = ldss_data[0].keys()
            ldss_keys.remove('RecIdFileName')
            ldss_keys.remove('TAN')
            # print "ldss_keys :: ", ldss_keys

            for each_ldss_key in ldss_keys:
                info_elmnts_dict[each_ldss_key] = []
            # print "info_elmnts_dict :: ", info_elmnts_dict
            for info_elmnts_key, info_elmnts_value in info_elmnts_dict.iteritems():
                for each_ldss_dict in ldss_data:
                    info_elmnts_value.append({
                                    "end": each_ldss_dict["FrameMtLDSSTimeStampStop"],
                                    "start": each_ldss_dict["FrameMtLDSSTimeStampStart"],
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
        stmt = ''' '''.format(ticket_id)
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
            stmt_tan = ''' select DISTINCT(TAN) from ADMSADMIN.{0}_{1}_t where
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
            orc_conn = db
        except DatabaseError, info:
            print "Logon Error:", info
            sys.exit()

        stmt = ''' '''.format(
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

        stmt = ''' '''.format(
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

if __name__ == '__main__':
    application.run()



