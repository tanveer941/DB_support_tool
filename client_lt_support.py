import json
import os

import requests
from json import loads, dumps

MAIN_URL = "http://127.0.0.1:5000"
# MAIN_URL = "http://lu00093vmx.li.de.conti.de/label_tool"

# CONFIG_PATH = os.getcwd() + r'\config.json'

# with open(CONFIG_PATH) as outfile:
#     TOOL_CONF = json.load(outfile)


def build_label_config(conf_id):
    print "into build ::"
    input_parameters = {'conf_id': conf_id}
    #parse tool conf on the server side
    response_data = requests.get(url=MAIN_URL + '/build_label_config',
                                 params=input_parameters)
    # print "response_data.text :: ", response_data.text
    conf_data = loads(response_data.text)
    # print "conf_data :>> ", conf_data
    return conf_data

def get_confid_frm_tckid(ticket_id):
    input_parameters = {'ticket_id': ticket_id}
    # parse tool conf on the server side
    response_data = requests.get(url=MAIN_URL + '/get_confid_frm_tckid',
                                 params=input_parameters)
    # print "response_data.text :: ", response_data.text
    tid_data = loads(response_data.text)
    # print "conf_data :>> ", conf_data
    return tid_data

def get_lbld_op_data(ticket_id, conf_id):
    input_parameters = {'ticket_id': ticket_id, 'conf_id': conf_id}
    # parse tool conf on the server side
    response_data = requests.get(url=MAIN_URL + '/get_lbld_op_data',
                                 params=input_parameters)
    # print "response_data.text :: ", response_data.text
    lbl_op_data = loads(response_data.text)
    # print "conf_data :>> ", conf_data
    return lbl_op_data

def get_prjfn_tan_rec_for_tid(ticket_id):
    input_parameters = {'ticket_id': ticket_id}
    # parse tool conf on the server side
    response_data = requests.get(url=MAIN_URL + '/get_prjfn_tan_rec_for_tid',
                                 params=input_parameters)
    # print "response_data.text :: ", response_data.text
    lbl_op_data = loads(response_data.text)
    print "lbl_op_data :>> ", lbl_op_data
    return lbl_op_data

def get_clmn_names_for_tables(prj_fn, label_typ):
    input_parameters = {'prj_fn': prj_fn, 'label_typ': label_typ}
    # parse tool conf on the server side
    response_data = requests.get(url=MAIN_URL + '/get_clmn_names_for_tables',
                                 params=input_parameters)
    # print "response_data.text :: ", response_data.text
    lbl_op_data = loads(response_data.text)
    # print "lbl_op_data :>> ", lbl_op_data
    return lbl_op_data

def add_generic_data_prepared_lt5(records, table_name):

    input_parameters = {'records': records, 'table_name': table_name}
    response_data = requests.post(url=MAIN_URL + "/add_generic_data_prepared_lt5",
                                  json=dumps(input_parameters),
                                  )
    data = loads(response_data.text)
    return data

def get_mts_usr_details(windows_uid):
    input_parameters = {'windows_uid': windows_uid}
    # parse tool conf on the server side
    response_data = requests.get(url=MAIN_URL + '/get_mts_usr_details',
                                 params=input_parameters)
    # print "response_data.text :: ", response_data.text
    cred_data = loads(response_data.text)
    # print "lbl_op_data :>> ", lbl_op_data
    return cred_data

if __name__ == '__main__':
    # with open(CONFIG_PATH) as outfile:
    #     tool_conf = json.load(outfile)
    build_label_config('conf_id369')