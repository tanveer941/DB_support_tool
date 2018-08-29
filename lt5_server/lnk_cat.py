from Queue import Queue
from datetime import datetime
from logging import warn, info
from os import path, environ
from time import time
from gen_func_lnk import splitunc


# - import STK modules ------------------------------------------------------------------------------------------------
from lnk_db_common import BaseDB, ERROR_TOLERANCE_LOW, AdasDBError, DB_FUNC_NAME_LOWER, PluginBaseDB,SQLite3BaseDB,\
    ERROR_TOLERANCE_NONE
from lnk_db_sql import GenericSQLSelect, SQLBinaryExpr, SQLFuncExpr, SQLJoinExpr, SQLLiteral, SQLIntegral, \
    SQLColumnExpr, SQLTableExpr, SQLNull, SQLUnaryExpr, SQLConcatExpr, SQLTernaryExpr, OP_NOP, OP_RETURNING, OP_AND, \
    OP_IS, OP_AS, OP_INNER_JOIN, OP_IN, OP_SUB, OP_ADD, OP_EQ, OP_LIKE, OP_MUL, OP_UNION_ALL, OP_USING, \
    GEN_RECUR_NAME, EXPR_WITH, EXPR_COUNT, EXPR_DISTINCT, OP_OR, OP_GT, OP_DIV, OP_LEQ, OP_GEQ,GenericSQLStatementFactory,\
    STMT_SELECT, EXPR_FROM, OP_LEFT_OUTER_JOIN, OP_MOD
# - defines -----------------------------------------------------------------------------------------------------------

COL_NAME_PROJECT_PID = "PID"
COL_NAME_PROJECT_NAME = "NAME"
COL_NAME_PROJECT_DESC = "DESCRIPTION"
TABLE_NAME_PROJECT = "GBL_PROJECT"


# Table base names:
TABLE_NAME_KWMAP = "CAT_KeywordMap"
TABLE_NAME_KW = "CAT_Keywords"
TABLE_NAME_COLLMAP = "CAT_CollectionMap"
TABLE_NAME_COLL = "CAT_Collections"
TABLE_NAME_FILES = "CAT_Files"
TABLE_NAME_PARTCFGS = "CAT_PartCfgs"
TABLE_NAME_PARTTYPES = "CAT_PartTypes"
TABLE_NAME_VEHICLECFGS = "CAT_VehicleCfgs"
TABLE_NAME_VEHICLECFGSTATES = "CAT_VehicleCfgStates"
TABLE_NAME_VEHICLES = "CAT_Vehicles"
TABLE_NAME_FILESTATES = "CAT_FileStates"
TABLE_NAME_DRIVERS = "CAT_Drivers"
TABLE_NAME_DATAINTTESTS = "CAT_DataIntTests"
TABLE_NAME_CAT_COLLECTION_LOG = "CAT_COLLECTION_LOG"
TABLE_NAME_CAT_COLLECTION_LOGDETAILS = "CAT_COLLECTION_LOGDETAILS"
TABLE_NAME_CAT_SHAREDCOLLECTIONMAP = "CAT_SHAREDCOLLECTIONMAP"
TABLE_NAME_OBJ_VIEW_RECTANGOBJ = "OBJ_VIEW_RECTANGULAROBJECT"
TABLE_NAME_OBJ_VIEW_ACCLANEREL = "OBJ_VIEW_ACCLANERELATION"
TABLE_NAME_OBJ_VIEW_TESTCASE = "OBJ_VIEW_TESTCASES"

COL_NAME_DRIVERS_DRIVERID = "DRIVERID"
COL_NAME_DRIVERS_NAME = "NAME"

COL_NAME_FILESTATES_FILESTATEID = "FILESTATEID"
COL_NAME_FILESTATES_NAME = "NAME"

COL_NAME_VEHICLES_VEHICLEID = "VEHICLEID"
COL_NAME_VEHICLES_NAME = "NAME"
COL_NAME_VEHICLES_MODEL = "MODEL"
COL_NAME_VEHICLES_LICNUMBER = "LICNUMBER"
COL_NAME_VEHICLES_VEHCOMMENT = "VEHCOMMENT"

COL_NAME_VEHICLECFGSTATES_VEHICLECFGSTATEID = "VEHCFGSTATEID"
COL_NAME_VEHICLECFGSTATES_NAME = "NAME"

COL_NAME_VEHICLECFGS_VEHCFGID = "VEHICLECFGID"
COL_NAME_VEHICLECFGS_VEHICLEID = "VEHICLEID"
COL_NAME_VEHICLECFGS_VEHICLECFGSTATEID = "VEHCFGSTATEID"
COL_NAME_VEHICLECFGS_NAME = "NAME"
COL_NAME_VEHICLECFGS_SETUPDATE = "SETUPDATE"
COL_NAME_VEHICLECFGS_SETUPBY = "SETUPBY"
COL_NAME_VEHICLECFGS_CFGCOMMENT = "CFGCOMMENT"

COL_NAME_PARTCFGS_PARTCFGID = "PARTCFGID"
COL_NAME_PARTCFGS_VEHCFGID = "VEHICLECFGID"
COL_NAME_PARTCFGS_NAME = "NAME"
COL_NAME_PARTCFGS_HWVERSION = "HWVERSION"
COL_NAME_PARTCFGS_SWVERSION = "SWVERSION"
COL_NAME_PARTCFGS_PARTCOMMENT = "PARTCOMMENT"
COL_NAME_PARTCFGS_PARTTYPEID = "PARTTYPEID"

COL_NAME_PARTTYPES_PARTTYPEID = "PARTTYPEID"
COL_NAME_PARTTYPES_NAME = "NAME"

COL_NAME_FILES_MEASID = "MEASID"
COL_NAME_FILES_RECFILEID = "RECFILEID"
COL_NAME_FILES_BEGINTIMESTAMP = "BEGINABSTS"
COL_NAME_FILES_ENDTIMESTAMP = "ENDABSTS"
COL_NAME_FILES_FILEPATH = "FILEPATH"
COL_NAME_FILES_RECTIME = "RECTIME"
COL_NAME_FILES_IMPORTDATE = "IMPORTDATE"
COL_NAME_FILES_IMPORTBY = "IMPORTBY"
COL_NAME_FILES_FILESTATEID = "FILESTATEID"
COL_NAME_FILES_VEHICLECFGID = "VEHICLECFGID"
COL_NAME_FILES_DRIVERID = "DRIVERID"
COL_NAME_FILES_CONTENTHASH = "CONTENTHASH"
COL_NAME_FILES_RECDRIVENDIST = "RECDRIVENDIST"
COL_NAME_FILES_RECODOSTARTDIST = "RECODOSTARTDIST"
COL_NAME_FILES_DATAINTTESTRESULT = "DATAINTTESTRESULT"
COL_NAME_FILES_DATAINTTESTSTATUS = "DATAINTTESTSTATUS"
COL_NAME_FILES_FILESIZE = "FILESIZE"
COL_NAME_FILES_ARCHIVED = "ARCHIVED"
COL_NAME_FILES_DELETED = "DELETED"
COL_NAME_FILES_GPSDRIVENDIST = "GPSDRIVENDIST"
COL_NAME_FILES_STATUS = "STATUS"
COL_NAME_FILES_PID = "PID"
COL_NAME_FILES_LOCATION = "LOC"

COL_NAME_KW_KWID = "KWID"
COL_NAME_KW_PARENTID = "PARENTID"
COL_NAME_KW_NAME = "NAME"
COL_NAME_KW_KWCOMMENT = "KWCOMMENT"

COL_NAME_KWMAP_KWMAPID = "KWMAPID"
COL_NAME_KWMAP_KWID = "KWID"
COL_NAME_KWMAP_MEASID = "MEASID"
COL_NAME_KWMAP_BEGINTIMESTAMP = "BEGINRELTS"
COL_NAME_KWMAP_ENDTIMESTAMP = "ENDRELTS"
COL_NAME_KWMAP_ASGNBY = "ASGNBY"
COL_NAME_KWMAP_ASGNDATE = "ASGNDATE"

COL_NAME_COLL_COLLID = "COLLID"
COL_NAME_COLL_PARENTID = "PARENTID"
COL_NAME_COLL_NAME = "NAME"
COL_NAME_COLL_COLLCOMMENT = "COLLCOMMENT"
COL_NAME_COLL_PRID = "PRID"
COL_NAME_COLL_IS_ACTIVE = "IS_ACTIVE"
COL_NAME_COLL_USERID = "USERID"
COL_NAME_COLL_CREATED = "CREATED"
COL_NAME_COLL_CP_LABEL = "CP_LABEL"

COL_NAME_COLLMAP_COLLMAPID = "COLLMAPID"
COL_NAME_COLLMAP_COLLID = "COLLID"
COL_NAME_COLLMAP_MEASID = "MEASID"
COL_NAME_COLLMAP_BEGINTIMESTAMP = "BEGINRELTS"
COL_NAME_COLLMAP_ENDTIMESTAMP = "ENDRELTS"
COL_NAME_COLLMAP_ASGNBY = "ASGNBY"
COL_NAME_COLLMAP_ASGNDATE = "ASGNDATE"
COL_NAME_COLLMAP_USERID = "USERID"
COL_NAME_COLLMAP_ASSIGNED = "ASSIGNED"

COL_NAME_DATAINTTESTS_TESTID = "TESTID"
COL_NAME_DATAINTTESTS_NAME = "NAME"
COL_NAME_DATAINTTESTS_DITCOMMENT = "DITCOMMENT"

COL_NAME_COLLOG_LOG_ID = "LOG_ID"
COL_NAME_COLLOG_COLL_NAME = "COLL_NAME"
COL_NAME_COLLOG_ACTION = "ACTION"
COL_NAME_COLLOG_ACTION_DATE = "ACTION_DATE"
COL_NAME_COLLOG_ACTIONBY = "ACTIONBY"
COL_NAME_COLLOG_COLLID = "COLLID"
COL_NAME_COLLOG_DETAILS = "DETAILS"

COL_NAME_COLLDET_DETAILID = "DETAILID"
COL_NAME_COLLDET_LOG_ID = "LOG_ID"
COL_NAME_COLLDET_MEASID = "MEASID"

COL_NANE_SHARECOLL_SAHREDMAPID = "SAHREDMAPID"
COL_NANE_SHARECOLL_PARENT_COLLID = "PARENT_COLLID"
COL_NANE_SHARECOLL_CHILD_COLLID = "CHILD_COLLID"
COL_NANE_SHARECOLL_PRID = "PRID"
PATH_SEPARATOR = "/"

COL_NAME_MEASID = "MEASID"
COL_NAME_RECTOBJ_ID = "RECTOBJID"
COL_NAME_CLASSNAME = "CLASSNAME"
COL_NAME_ASSOCTYPE = "ASSOCTYPE"
COL_NAME_CLSLBLSTATE = "CLSLBLSTATE"


DBCAT_SUB_SCHEME_TAG = "CAT"
CAT_PRIORIRTY_VERSION = 7
CAT_ACTIVE_VERSION = 8
CAT_SHARECOLL_VERSION = 10

# IDENT_STRING = DBCAT

LIFS010 = r"\\lifs010\prj"
LIFS010_FDQN = r"\\lifs010.cw01.contiwan.com\prj"
LIFS010S = r"\\lifs010s\prj"
LIFS010S_FDQN = r"\\lifs010s.cw01.contiwan.com\prj"
LUFS003X = r"\\lufs003x\legacy"
LUFS003X_FDQN = r"\\lufs003x.li.de.conti.de\legacy"

SHARED_MAP_ID = "SAHREDMAPID"
SHARED_COL_PARENT_ID = "PARENT_COLLID"
SHARED_COLLECTION_ID = "CHILD_COLLID"
SHARED_COL_PRI_ID = "PRID"

#Constants defined for these columns are different in the new STK
COL_NANE_SHARECOLL_SAHREDMAPID = SHARED_MAP_ID
COL_NANE_SHARECOLL_PARENT_COLLID = SHARED_COL_PARENT_ID
COL_NANE_SHARECOLL_CHILD_COLLID = SHARED_COLLECTION_ID
COL_NANE_SHARECOLL_PRID = SHARED_COL_PRI_ID

# virtual columns
SHARED_FLAG = "SHARED_FLAG"
# Flag defination from shared collections
# No shared collection
N0T_SHARED_COLL = 0
# The collection is shared
SHARED_COLL = 1

SHARED_CHILD_FLAG = "SHARED_CHILD_FLAG"
# Flag defination from shared child collections
# No shared child collection
N0T_SHARED_CHILD_COLL = 0
# The child collection is shared
SHARED_CHILD_COLL = 1

VAL_ADMIN_SCHEMA = "VAL_GLOBAL_ADMIN"
ARS4XX__ADMIN_SCHEMA = "DEV_ARS4XX_ADMIN"

TABLE_NAME_RECTANGULAR_OBJECT_VIEW = "OBJ_VIEW_RECTANGULAROBJECT"
COL_NAME_RECT_OBJ_MEASID = "MEASID"
COL_NAME_RECT_OBJ_VIEW_RECTOBJID = "RECTOBJID"

TABLE_NAME_OBJ_ACCLANERELATION_VIEW = "OBJ_VIEW_ACCLANERELATION"
TABLE_NAME_OBJ_TESTCASES_VIEW = "OBJ_VIEW_TESTCASES"

# - classes -----------------------------------------------------------------------------------------------------------
# Constraint DB Libary Base Implementation
class BaseRecCatalogDB(BaseDB):  # pylint: disable=R0904
    """Base implementation of the Rec File Database"""
    # ====================================================================
    # Constraint DB Libary Interface for public use
    # ====================================================================

    # ====================================================================
    # Handling of database
    # ====================================================================

    def __init__(self, *args, **kw):
        """
        Constructor to initialize BaseGblDB to represent CAT subschema

        :keyword db_connection: The database connection to be used
        :type db_connection: cx_oracle.Connection, pydodbc.Connection, sqlite3.Connection, sqlce.Connection
        :keyword table_prefix: The table name prefix which is usually master schema name
        :type table_prefix: str
        :keyword sql_factory: SQL Query building factory
        :type sql_factory: GenericSQLStatementFactory
        :keyword error_tolerance: Error tolerance level based on which some error are acceptable
        :type error_tolerance: int
        """
        # kw['strIdent'] = DBCAT
        BaseDB.__init__(self, *args, **kw)
        #  cache by project name as key and project Id as value
        self._gbl_projectid_cache = {}

    # ====================================================================
    # Handling of file data
    # ====================================================================

    def get_measurement(self, measid=None, file_name=None, file_path=None, select_list=["*"]):
        """
        Get measurement record based on criteria on optional argument. If no argument passed. It all the records
        from table will be return
        :param measid:
        :type measid:
        :param file_name:
        :param file_path:
        """

        cond = None
        sql_param = {}
        if measid is not None:
            sql_param[str(len(sql_param) + 1)] = measid
            cond = SQLBinaryExpr(COL_NAME_FILES_MEASID,
                                 OP_EQ, ":%d" % (len(sql_param)))

        cond_fpath = None
        if file_path is not None:
            file_path = file_path.lower()

            if file_name is None:
                file_name = path.basename(file_path)
            file_path, pid = self._convert_filepath(file_path)
            # print "file_path ::q ", file_path
            if pid is not None:
                sql_param[str(len(sql_param) + 1)] = file_path
                # make exact filepath lower case SQL condition
                cond_fpath = SQLBinaryExpr(SQLFuncExpr(self.db_func_map[DB_FUNC_NAME_LOWER],
                                                       COL_NAME_FILES_FILEPATH),
                                           OP_EQ, ":%d" % (len(sql_param)))
                sql_param[str(len(sql_param) + 1)] = pid
                cond_fpath = SQLBinaryExpr(cond_fpath, OP_AND, SQLBinaryExpr(COL_NAME_FILES_PID,
                                                                             OP_EQ, ":%d" % (len(sql_param))))
            else:
                if file_path[:2] == r"\\":
                    _, file_path = splitunc(file_path)

                else:
                    _, file_path = path.splitdrive(file_path)

                file_path = "%%%s" % file_path
                # print "file_path ::w ", file_path
                sql_param[str(len(sql_param) + 1)] = file_path
                cond_fpath = SQLBinaryExpr(SQLFuncExpr(self.db_func_map[DB_FUNC_NAME_LOWER],
                                                       COL_NAME_FILES_FILEPATH),
                                           OP_LIKE, ":%d" % (len(sql_param)))

            if cond is None:
                cond = cond_fpath

            else:
                cond = SQLBinaryExpr(cond, OP_AND, cond_fpath)
        if file_name is not None:
            file_name = file_name.lower()
            sql_param[str(len(sql_param) + 1)] = file_name
            if cond is None:
                cond = SQLBinaryExpr(COL_NAME_FILES_RECFILEID, OP_EQ, ":%d" % (len(sql_param)))
            else:
                cond = SQLBinaryExpr(cond, OP_AND, SQLBinaryExpr(COL_NAME_FILES_RECFILEID,
                                                                 OP_EQ, ":%d" % (len(sql_param))))
        return self.select_measurements(select_list, where=cond, sqlparams=sql_param)

    def select_measurements(self, select_list=["*"], where=None, group_by=None,  # pylint: disable=W0102,R0913
                            having=None, order_by=None, distinct_rows=False, sqlparams={}):
        """
        Get all measurements which fulfill some condition.

        :param select_list: List of selected table columns.
        :type select_list: list
        :param where: The additional condition that must hold for the scenarios to be returned.
        :type where: SQLBinaryExpression
        :param group_by: Expression defining the columns by which selected data is grouped.
        :type group_by: list
        :param having: Expression defining the conditions that determine groups
        :type having: SQLExpression
        :param order_by: Expression that defines the order of the returned records.
        :type order_by: list
        :return: Returns the list of scenarios.
        :rtype: list
        """
        return self.select_generic_data(select_list, [TABLE_NAME_FILES], where, group_by, having, order_by,
                                        distinct_rows, sqlparams)

    def add_measurement(self, measurement):
        """
        Add new file to database.

        :param measurement: The measurement record.
        :type measurement: dict
        :return: Returns the measurement ID. If a recording with the
                 same recfileid exists already an exception is raised.
        :rtype: int
        """
        # check for duplicate recfileid
        entries = self.get_measurement(file_path=measurement[COL_NAME_FILES_FILEPATH].lower())

        if len(entries) <= 0:
            measurement[COL_NAME_FILES_RECFILEID] = measurement[COL_NAME_FILES_RECFILEID].lower()
            if self.sub_scheme_version < CAT_ACTIVE_VERSION:
                measid = self._get_next_id(TABLE_NAME_FILES, COL_NAME_FILES_MEASID)
                measurement[COL_NAME_FILES_MEASID] = measid

                self.add_generic_data(measurement, TABLE_NAME_FILES)
            else:
                file_path, pid = self._convert_filepath(measurement[COL_NAME_FILES_FILEPATH])
                measurement[COL_NAME_FILES_FILEPATH] = file_path
                if pid is not None:
                    measurement[COL_NAME_FILES_PID] = pid
                self.add_generic_data(measurement, TABLE_NAME_FILES)
                entries = self.get_measurement(file_path=measurement[COL_NAME_FILES_FILEPATH].lower())
                measid = entries[0][COL_NAME_FILES_MEASID]
            return measid
        else:
            tmp = "Recording '%s' " % measurement[COL_NAME_FILES_FILEPATH]
            tmp += "exists already in the catalog."
            if self.error_tolerance < ERROR_TOLERANCE_LOW:
                raise AdasDBError(tmp)
            else:
                warn(tmp)
                if len(entries) == 1:
                    return entries[0][COL_NAME_FILES_MEASID]
                elif len(entries) > 1:
                    tmp = "File '%s' " % (measurement[COL_NAME_FILES_RECFILEID])
                    tmp += "cannot be resolved because it is ambiguous. (%s)" % entries
                    raise AdasDBError(tmp)

    def get_lbl_rectobj_coll(self, collid):
        """
        This will return the rectangular object details labelled for a recording
        Args:
            measid: Measurement ID/ Integer

        Returns: A list containing dictionary of the details labelled
        :author:        Mohammed Tanveer
        :date:          15/09/2016
        """

        sql_param = {"1": collid}


        recurs, _ = self._get_collections_tree_query(":%s" % (len(sql_param)))
        recur_sql = GenericSQLSelect([COL_NAME_COLL_COLLID], False, [GEN_RECUR_NAME])
        tree_sql = str(SQLConcatExpr(recurs, recur_sql))
        cond1 = self._get_union_shared_collection_cond(tree_sql, sql_param)
        cmap_tbl = str(GenericSQLSelect([COL_NAME_COLLMAP_MEASID], True, [TABLE_NAME_COLLMAP], cond1))
        cond = SQLBinaryExpr(COL_NAME_FILES_MEASID, OP_IN, "(%s)" % (cmap_tbl))
        collid_lst_qry = str(GenericSQLSelect([COL_NAME_FILES_MEASID], True, [TABLE_NAME_FILES], cond))
        # print "collid_lst_qry :: ", collid_lst_qry

        catfl_tbl_alias = "cf"
        objrect_tbl_alias = "rectobj"
        cat_coll_tbl = SQLTableExpr(SQLColumnExpr(VAL_ADMIN_SCHEMA, TABLE_NAME_FILES), catfl_tbl_alias)
        rect_obj_tbl = SQLTableExpr(SQLColumnExpr(ARS4XX__ADMIN_SCHEMA, TABLE_NAME_RECTANGULAR_OBJECT_VIEW), objrect_tbl_alias)
        rectobj_list = [SQLColumnExpr(objrect_tbl_alias, "*")]
        cond_rdr = SQLBinaryExpr(SQLColumnExpr(catfl_tbl_alias, COL_NAME_FILES_MEASID), OP_IN, "(%s)" % (collid_lst_qry))
        expr1 = SQLBinaryExpr(SQLColumnExpr(catfl_tbl_alias, COL_NAME_FILES_MEASID), OP_EQ,
                                              SQLColumnExpr(objrect_tbl_alias, COL_NAME_RECT_OBJ_MEASID))
        join = SQLJoinExpr(cat_coll_tbl, OP_INNER_JOIN, rect_obj_tbl, expr1)
        rect_obj_data = self.select_generic_data(select_list=rectobj_list, table_list=[join], where=cond_rdr, sqlparams=sql_param)
        sub_qry_stmt = str(GenericSQLSelect(select_list=[SQLColumnExpr(objrect_tbl_alias, COL_NAME_RECT_OBJ_VIEW_RECTOBJID)],
                                        distinct_rows=False, table_list=[join], where_condition=cond_rdr))

        return rect_obj_data, sub_qry_stmt

    def get_lbl_acclane_coll(self, collid, sub_qry):

        sql_param = {"1": collid}
        acclane_tbl_alias = "acclaneobj"
        acclane_obj_tbl = SQLTableExpr(SQLColumnExpr(ARS4XX__ADMIN_SCHEMA, TABLE_NAME_OBJ_ACCLANERELATION_VIEW), acclane_tbl_alias)

        cond = SQLBinaryExpr(SQLColumnExpr(acclane_tbl_alias, COL_NAME_RECT_OBJ_VIEW_RECTOBJID), OP_IN, "(%s)" % sub_qry)

        acc_lane_data = self.select_generic_data(select_list=[SQLColumnExpr(acclane_tbl_alias, "*")],
                                                  table_list=[acclane_obj_tbl], where=cond, sqlparams=sql_param)
        return acc_lane_data

    def get_lbl_testcases_coll(self, collid, sub_stmt):

        sql_param = {"1": collid}
        testobj_tbl_alias = "testobj"
        testobj_obj_tbl = SQLTableExpr(SQLColumnExpr(ARS4XX__ADMIN_SCHEMA, TABLE_NAME_OBJ_TESTCASES_VIEW), testobj_tbl_alias)

        cond = SQLBinaryExpr(SQLColumnExpr(testobj_tbl_alias, COL_NAME_RECT_OBJ_VIEW_RECTOBJID), OP_IN, "(%s)" % sub_stmt)

        testcase_data = self.select_generic_data(select_list=[SQLColumnExpr(testobj_tbl_alias, "*")],
                                                  table_list=[testobj_obj_tbl], where=cond, sqlparams=sql_param)
        return testcase_data

    def _convert_filepath(self, file_path):
        """
        Convert File path in to FQDN hostname for oracle only aligned with DMT

        :type file_path: str
        :type file_path: str
        :return: Dmt standard filepath, project Id
        :rtype: str, int

        """
        pid = None
        if file_path[:2] == r"\\":
            mount_point, rest = splitunc(file_path)
            # Is this lifs010 and oracle then convert to FQDN hostname
            if self.db_type[0] == -1 and mount_point.lower() in \
                    [LIFS010, LIFS010_FDQN, LIFS010S, LIFS010S_FDQN, LUFS003X, LUFS003X_FDQN]:
                # remove hostname to set FDQN hostname
                # print "....]]]]]"
                file_path = LIFS010_FDQN + rest
                if mount_point.lower() in [LUFS003X, LUFS003X_FDQN]:
                    file_path = LUFS003X_FDQN + rest
                project_name = rest.split("\\")[1]
                # print "project_name >> ", project_name
                # apply FDQN hostname
                if project_name in self._gbl_projectid_cache:
                    pid = self._gbl_projectid_cache[project_name]
                else:
                    records = pid = self.execute("SELECT %s from %s where lower(%s) = '%s'"
                                                 % (COL_NAME_PROJECT_PID, TABLE_NAME_PROJECT,
                                                    COL_NAME_PROJECT_NAME, project_name))
                    # print "records pid ", records
                    if len(records) == 1:
                        pid = records[0][0]
                    else:
                        pid = None
                    self._gbl_projectid_cache[project_name] = pid

        return file_path, pid

    def has_measurement(self, filepath):
        """
        Function to check if a file is in the database.

        :param filepath: The filepath.
        :type filepath: str
        :return: True or False.
        :rtype: bool
        """
        entries = self.get_measurement(file_path=filepath, select_list=[COL_NAME_FILES_MEASID])
        return len(entries) == 1

    def get_measurement_id(self, recfile):
        """
        Get a measurement ID of a file

        :param recfile: The file path name to be resolved.
        :type recfile: str
        :return: Returns the ID for the file path or None if file not exists.
        :rtype: int
        """
        entries = self.get_measurement(file_path=recfile, select_list=[COL_NAME_FILES_MEASID])
        if len(entries) == 1:
            measid = entries[0][COL_NAME_FILES_MEASID]
        elif len(entries) > 1:
            raise AdasDBError("File '%s' cannot be resolved because it is ambiguous. (%s)" % (recfile, entries))
        else:
            raise AdasDBError("No resolution of '%s'. (%s)" % (recfile, entries))
        return measid

    def get_measurement_content_hash(self, measid):
        """
        Get a content hash of a file

        :param measid: The measurement id.
        :type measid: int
        :return: Returns the content hash for the given measid or None if measid does not exist.
        :rtype: str
        """
        entries = self.get_measurement(measid=measid, select_list=[COL_NAME_FILES_CONTENTHASH])
        if len(entries) == 1:
            hash_content = entries[0][COL_NAME_FILES_CONTENTHASH]
        elif len(entries) > 1:
            raise AdasDBError("Meas ID '%s' cannot be resolved because it is ambiguous. (%s)" % (measid, entries))
        else:
            raise AdasDBError("No resolution of Meas ID '%s'. (%s)" % (measid, entries))
        return hash_content

    def get_measurement_with_sections(self, measid, collid=None):
        """
        Get all the data from a measurement, including a list of the sections.

        :param measid: The measurement id.
        :type measid: int
        :param collid: The collection id. If None, sections from all collections are retreived.
        :type collid: Ineger
        :return: Return two values measurement record, sections list.
        :rtype: dict, list
        """

        bpl_attr_name_start_time = "startTime"
        bpl_attr_name_end_time = "endTime"

        cond = SQLBinaryExpr(COL_NAME_FILES_MEASID, OP_EQ, measid)
        measurement_list = self.select_generic_data(select_list=["*"], table_list=[TABLE_NAME_FILES], where=cond)
        if len(measurement_list) != 1:
            raise AdasDBError("No resolution of measid: '%s'" % measid)

        measurement = measurement_list[0]

        # get sections of this measurement
        if collid is None:
            coll_cond = SQLBinaryExpr(COL_NAME_COLLMAP_MEASID, OP_EQ, measid)
        else:
            coll_cond = SQLBinaryExpr(SQLBinaryExpr(COL_NAME_COLLMAP_COLLID, OP_EQ, collid),
                                      OP_AND,
                                      SQLBinaryExpr(COL_NAME_COLLMAP_MEASID, OP_EQ, measid))

        order_expr = [SQLColumnExpr(SQLTableExpr(TABLE_NAME_COLLMAP), COL_NAME_COLLMAP_BEGINTIMESTAMP)]

        entries = self.select_generic_data(table_list=[TABLE_NAME_COLLMAP], where=coll_cond, order_by=order_expr)

        # create section list
        section_list = []
        for entry in entries:
            if((entry[COL_NAME_COLLMAP_BEGINTIMESTAMP] is not None) or
               (entry[COL_NAME_COLLMAP_ENDTIMESTAMP] is not None)):
                section = {bpl_attr_name_start_time: str("%d" % entry[COL_NAME_COLLMAP_BEGINTIMESTAMP])}

                if entry[COL_NAME_COLLMAP_ENDTIMESTAMP] is not None:
                    section[bpl_attr_name_end_time] = str("%d" % entry[COL_NAME_COLLMAP_ENDTIMESTAMP])

                section_list.append(section)

        return measurement, section_list

    def update_measurements(self, measurement, where=None):
        """
        Update existing file records.

        :param measurement: Dictionary of record with new or modified values
        :type measurement: dict
        :param where: The condition to be fulfilled by the files to the updated.
        :type where: SQLBinaryExpression
        :return: Returns the number of affected files.
        :rtype: int
        """
        rowcount = 0
        if measurement is not None:
            if where is None:
                where = SQLBinaryExpr(COL_NAME_FILES_MEASID, OP_EQ, measurement[COL_NAME_FILES_MEASID])
            self.update_generic_data(measurement, TABLE_NAME_FILES, where)
        # done
        return rowcount

    # ====================================================================
    # Handling of collection data
    # ====================================================================

    def add_collection(self, collection):
        """
        Add collection state to database.

        :param collection: The collection record.
        :type collection: dict
        :return: Returns the collection ID.
        :rtype: int
        """
        collid = None

        entries = []
        # if db type is Sqlite keep the collname unique
        if self._db_type == 0:
            cond = SQLBinaryExpr(SQLFuncExpr(self.db_func_map[DB_FUNC_NAME_LOWER], COL_NAME_COLL_NAME),
                                 OP_EQ, SQLLiteral(collection[COL_NAME_COLL_NAME].lower()))
            entries = self.select_generic_data(select_list=[COL_NAME_COLL_COLLID],
                                               table_list=[TABLE_NAME_COLL], where=cond)
            if len(entries) > 0:
                tmp = "Collection '%s' " % collection[COL_NAME_COLL_NAME]
                tmp += "exists already in the catalog for this parent."
                if self.error_tolerance < ERROR_TOLERANCE_LOW:
                    raise AdasDBError(tmp)
                else:
                    warn(tmp)
                    if len(entries) == 1:
                        return entries[0][COL_NAME_COLL_COLLID]
                    elif len(entries) > 1:
                        tmp = "Collection '%s' " % (collection[COL_NAME_COLL_NAME])
                        tmp += "cannot be resolved because it is ambiguous. (%s)" % entries
                        raise AdasDBError(tmp)
        if len(entries) == 0:
            collid = self.add_generic_data(collection, TABLE_NAME_COLL,
                                           SQLUnaryExpr(OP_RETURNING, COL_NAME_COLL_COLLID))
        return collid

    def get_basecoll_from_tree(self, collid):
            """
            Function to get all the rows in shared map within the entire collection tree
            Args:
                collid: Collection ID/Integer

            Returns: List of shared map details
            :author:        Mohammed Tanveer
            :date:          27/07/2016
            """
            sql_param = {}
            sql_param[str(len(sql_param) + 1)] = collid
            recurs, _ = self._get_collections_tree_query(":%s" % (len(sql_param)))
            recur_sql = GenericSQLSelect([COL_NAME_COLL_COLLID], False, [GEN_RECUR_NAME])
            tree_sql = str(SQLConcatExpr(recurs, recur_sql))
            cond = SQLBinaryExpr(COL_NAME_COLL_COLLID, OP_IN, "(%s)" % (tree_sql))
            trees_sql = "(%s)" % str(GenericSQLSelect([COL_NAME_COLL_COLLID], False, [TABLE_NAME_COLL], cond))
            cond_shr = SQLBinaryExpr(COL_NANE_SHARECOLL_CHILD_COLLID, OP_IN, "(%s)" % (trees_sql))
            return self.select_generic_data(["*"], [TABLE_NAME_CAT_SHAREDCOLLECTIONMAP], cond_shr, sqlparams=sql_param)

    def update_collid(self, record, log_collid):

        """
        :author:        Mohammed Tanveer
        :date:          26/10/2015
        :param record: a dictionary having column as key and value as details
        :param log_collid: collection ID of each log
        :return:
        """

        cond = SQLBinaryExpr(COL_NAME_COLLOG_COLLID, OP_EQ, SQLLiteral(log_collid))
        self.update_generic_data(record, TABLE_NAME_CAT_COLLECTION_LOG, where=cond)

    def update_collection(self, collection, collid):
        """
        Update existing collection records.

        :param collection: The collection record update.
        :type collection: dict
        :param collid: collection Id
        :type collid: int
        :return: Returns the number of affected collections.
        :rtype: int
        """
        cond = SQLBinaryExpr(COL_NAME_COLL_COLLID, OP_EQ, collid)
        rowcount = 0
        if collection is not None:
            self.update_generic_data(collection, TABLE_NAME_COLL, cond)
        # done
        return rowcount

    def add_collection_map(self, collmap):
        """
        Add collection mapping to database.

        :param collmap: The collection mapping record.
        :type collmap: dict
        :return: Returns the collection map ID.
        :rtype: int
        """
        sql_param = {}
        sql_param[str(len(sql_param) + 1)] = collmap[COL_NAME_COLLMAP_COLLID]
        coll_cond = SQLBinaryExpr(COL_NAME_COLLMAP_COLLID, OP_EQ, ":%d" % (len(sql_param)))
        sql_param[str(len(sql_param) + 1)] = collmap[COL_NAME_COLLMAP_MEASID]
        coll_cond = SQLBinaryExpr(coll_cond, OP_AND,
                                  SQLBinaryExpr(COL_NAME_COLLMAP_MEASID, OP_EQ, ":%d" % (len(sql_param))))

        if COL_NAME_COLLMAP_BEGINTIMESTAMP in collmap and collmap[COL_NAME_COLLMAP_BEGINTIMESTAMP] is not None:
            sql_param[str(len(sql_param) + 1)] = collmap[COL_NAME_COLLMAP_BEGINTIMESTAMP]
            coll_cond = SQLBinaryExpr(SQLBinaryExpr(COL_NAME_COLLMAP_BEGINTIMESTAMP,
                                                    OP_EQ, ":%d" % (len(sql_param))), OP_AND, coll_cond)

        if COL_NAME_COLLMAP_ENDTIMESTAMP in collmap and collmap[COL_NAME_COLLMAP_ENDTIMESTAMP] is not None:
            sql_param[str(len(sql_param) + 1)] = collmap[COL_NAME_COLLMAP_ENDTIMESTAMP]
            coll_cond = SQLBinaryExpr(SQLBinaryExpr(COL_NAME_COLLMAP_ENDTIMESTAMP,
                                                    OP_EQ, ":%d" % (len(sql_param))), OP_AND, coll_cond)

        entries = self.select_generic_data(table_list=[TABLE_NAME_COLLMAP], where=coll_cond, sqlparams=sql_param)
        if len(entries) <= 0:
            collmapid = self.add_generic_data(collmap, TABLE_NAME_COLLMAP,
                                              SQLUnaryExpr(OP_RETURNING, COL_NAME_COLLMAP_COLLMAPID))

            return collmapid
        else:
            tmp = "File '%s' " % collmap[COL_NAME_COLLMAP_MEASID]
            tmp += "is already assigned to collection '%s'." % collmap[COL_NAME_COLLMAP_COLLID]
            if self.error_tolerance < ERROR_TOLERANCE_LOW:
                raise AdasDBError(tmp)
            else:
                warn(tmp)
                if len(entries) == 1:
                    return entries[0][COL_NAME_COLLMAP_COLLMAPID]
                elif len(entries) > 1:
                    tmp = "Collection mapping of file '%s' " % collmap[COL_NAME_COLLMAP_MEASID]
                    tmp += "cannot be resolved because it is ambiguous. (%s)" % entries
                    raise AdasDBError(tmp)

    def get_search_rec_data(self, collid, cf_columns, rec_search_name):
        """
        Function to build the query for data to be shown in recording pane based on searched rec.
        :param collid: The ID of the collection.
        :type collid: Integer
        :param cf_columns: List of all the columns names of CAT_FILES
        :type cf_columns: List
        :param rec_search_name: Name of record search
        :type rec_search_name: Char

        :author: Sumangala Amati
        :date:  16/06/2016
        """

        sql_param = {}
        sql_param[str(len(sql_param) + 1)] = collid
        recurs, _ = self._get_collections_tree_query(":%s" % (len(sql_param)))
        recur_sql = GenericSQLSelect([COL_NAME_COLL_COLLID], False, [GEN_RECUR_NAME])
        tree_sql = str(SQLConcatExpr(recurs, recur_sql))
        cond1 = self._get_union_shared_collection_cond(tree_sql, sql_param)
        cmap_tbl = str(GenericSQLSelect([COL_NAME_COLLMAP_MEASID], True, [TABLE_NAME_COLLMAP], cond1))
        cond1 = SQLBinaryExpr(COL_NAME_FILES_MEASID, OP_IN, "(%s)" % (cmap_tbl))
        cond2 = SQLBinaryExpr(SQLFuncExpr(self.db_func_map[DB_FUNC_NAME_LOWER],
                                                       COL_NAME_FILES_RECFILEID), OP_LIKE, "(%s)" % ("'"+OP_MOD+rec_search_name.lower()+OP_MOD+"'"))
        cond = SQLBinaryExpr(cond1, OP_AND, cond2)

        stmt = str(GenericSQLSelect(cf_columns, False, [TABLE_NAME_FILES], cond))
        return stmt

    def get_search_rec_collname(self, collid, cf_columns, measid):
        """
        Function to build the query for collection names to be shown in collection pane,
        based on selected rec on rec pane.
        :param collid: The ID of the collection.
        :type collid: Integer
        :param cf_columns: List of all the columns names of CAT_Collections
        :type cf_columns: List
        :param measid: MeasID of selected record.
        :type measid: Integer

        :author: Sumangala Amati
        :date:  17/06/2016
        """
        sql_param = {}
        sql_param[str(len(sql_param) + 1)] = collid
        recurs, _ = self._get_collections_tree_query(":%s" % (len(sql_param)))
        recur_sql = GenericSQLSelect([COL_NAME_COLL_COLLID], False, [GEN_RECUR_NAME])
        tree_sql = str(SQLConcatExpr(recurs, recur_sql))
        cond1 = self._get_union_shared_collection_cond(tree_sql, sql_param)
        cond2 = SQLBinaryExpr(COL_NAME_COLLMAP_MEASID, OP_EQ, measid)
        cond = SQLBinaryExpr(cond1, OP_AND, cond2)
        cmap_tbl = str(GenericSQLSelect([COL_NAME_COLLMAP_COLLID], True, [TABLE_NAME_COLLMAP], cond))
        cond3 = SQLBinaryExpr(COL_NAME_COLL_COLLID, OP_IN, "(%s)" % (cmap_tbl))
        stmt = str(GenericSQLSelect(cf_columns, False, [TABLE_NAME_COLL], cond3))
        return stmt

    def delete_collection_map(self, measid, collection):
        """
        Delete a collection map based on measid and collection name.

        :param measid: The measurement id.
        :type measid: int
        :param collection_name: The collection name.
        :type collection_name: str
        """
        # Find the collection id.
        if type(collection) == str:
            collection_id = self.get_collection_id(collection)
        if type(collection) == int:
            collection_id = collection
        if type(collection) == unicode:
            collection_id = self.get_collection_id(str(collection))

        pre_cond = SQLBinaryExpr(COL_NAME_COLLMAP_MEASID, OP_EQ, measid)
        cond = SQLBinaryExpr(SQLBinaryExpr(COL_NAME_COLLMAP_COLLID, OP_EQ, collection_id), OP_AND, pre_cond)

        self.delete_generic_data(TABLE_NAME_COLLMAP, where=cond)

    def delete_collection(self, collection):
        """
        Delete a collection based on the collection name.This function requires as prerequisite removal of all the
        collection map entries and collection should not be the parent of other collection.

        :param collection_name: The collection name.
        :type collection_name: str
        :return: Return boolean representing success or failure of delete, error string
        :rtype: bool, string
        """
        # Find the collection id.
        if type(collection) == str:
            collection_id = self.get_collection_id(collection)
        if type(collection) == int:
            collection_id = collection
        if type(collection) == unicode:
            collection_id = self.get_collection_id(str(collection))

        # Get the measurements. There should be none.
        meas_count = self.get_measurements_number(collection_id, recurse=False)
        if meas_count != 0:
            return False, "Delete measurements prior to deleting collection."

        # Find collections with the collection id as parent. There should be none.
        sub_collections = self.get_collections(collection_id)
        if len(sub_collections) != 0:
            return False, "Delete sub-collections prior to deleting collection."

        cond = SQLBinaryExpr(COL_NAME_COLL_COLLID, OP_EQ, collection_id)
        self.delete_generic_data(TABLE_NAME_COLL, where=cond)

        return True, ""

    def activate_collection(self, coll_name=None, collid=None):
        """
        Set collection active flag 1 to activate collection

        :param coll_name: collection name
        :type coll_name: String
        :param collid: Collection Id
        :type collid: Integer
        :return: return True if the execution was sucessfull otherwise return false on failure
        :rtype: Boolean
        """
        return self.update_collection_active_flag(1, coll_name, collid)

    def is_collection_active(self, coll_name=None, collid=None):
        """
        Get collection active status

        :param coll_name: collection name
        :type coll_name: String
        :param collid: Collection Id
        :type collid: Integer
        :return: return True if the collection is active otherwise return False if the collection is Inactive
        :rtype: Boolean
        """
        if self.sub_scheme_version >= CAT_ACTIVE_VERSION:
            cond = None
            sql_param = {}
            if coll_name is not None:
                sql_param[str(len(sql_param) + 1)] = coll_name.lower()

                cond = SQLBinaryExpr(SQLFuncExpr(self.db_func_map[DB_FUNC_NAME_LOWER],
                                                 COL_NAME_COLL_NAME), OP_EQ, ":%d" % (len(sql_param)))

            if collid is not None:
                sql_param[str(len(sql_param) + 1)] = collid
                if cond is None:
                    cond = SQLBinaryExpr(COL_NAME_COLL_COLLID, OP_EQ, ":%d" % (len(sql_param)))
                else:
                    cond = SQLBinaryExpr(cond, OP_AND,
                                         SQLBinaryExpr(COL_NAME_COLL_COLLID, OP_EQ, ":%d" % (len(sql_param))))
            entries = []
            if cond is not None:
                entries = self.select_generic_data([COL_NAME_COLL_IS_ACTIVE],
                                                   table_list=[TABLE_NAME_COLL], where=cond, sqlparams=sql_param)
            if len(entries) == 1:
                return entries[0][COL_NAME_COLL_IS_ACTIVE]
            else:
                raise AdasDBError("Invalid collection name %s or Collid %s" % (coll_name, str(collid)))

        # if the feature missing then all collection are treated as active
        return 1

    def deactivate_collection(self, coll_name=None, collid=None):
        """
        Set collection active flag 0 to deactivate collection

        :param coll_name: collection name
        :type coll_name: String
        :param collid: Collection Id
        :type collid: Integer
        :return: return True if the execution was sucessfull otherwise return false on failure
        :rtype: Boolean
        """
        return self.update_collection_active_flag(0, coll_name, collid)

    def update_collection_active_flag(self, is_active, coll_name=None, collid=None):
        """
        update collection is_active value in cat_collection table

        :param is_active: Flag value representing with allowed value 0 or 1
        :type is_active: Integer
        :param coll_name: collection name
        :type coll_name: String
        :param collid: Collection Id
        :type collid: Integer
        :return: return True if the execution was sucessfull otherwise return false on failure
        :rtype: Boolean
        """
        if self.sub_scheme_version < CAT_ACTIVE_VERSION:
            warn("Database schema is too old to support collection active inactive feature")

        elif is_active == 1 or is_active == 0:
            if collid is None and coll_name is not None:
                collid = self.get_collection_id(coll_name)

            if collid is not None:
                self.update_collection({COL_NAME_COLL_IS_ACTIVE: is_active}, collid)
                return True

        return False

    def get_collection(self, coll_id):
        """
        Return all the collection entry based on the collection id.

        :param coll_id: The collection id.
        :type coll_id: int
        :return: Collection record in dict format.
        :rtype: dict
        """
        cond = SQLBinaryExpr(COL_NAME_COLL_COLLID, OP_EQ, ":1")
        collection_list = self.select_generic_data(["*"], [TABLE_NAME_COLL], where=cond, sqlparams={"1": coll_id})
        if len(collection_list) == 0:
            return None
        return collection_list[0]

    def get_collection_id(self, coll_name):
        """
        Find a collection with a given name (absolute or basic).

        :param coll_name: The collection name.
        :type coll_name: str
        :return: Returns the collection ID or None if not exists.
        :rtype: int
        """
        collid = None
        coll_name = coll_name.rstrip(PATH_SEPARATOR).split(PATH_SEPARATOR)
        coll_name = coll_name[-1]
        cond = SQLBinaryExpr(SQLFuncExpr(self.db_func_map[DB_FUNC_NAME_LOWER], COL_NAME_COLL_NAME), OP_EQ, ":1")
        entries = self.select_generic_data([COL_NAME_COLL_COLLID], table_list=[TABLE_NAME_COLL], where=cond,
                                           sqlparams={"1": coll_name.lower()})
        if len(entries) == 1:
            collid = entries[0][COL_NAME_COLL_COLLID]
        elif len(entries) <= 0:
            raise AdasDBError("Collection '%s' doesn't exists in the catalog." % coll_name)

        return collid

    def get_collection_name(self, collid):
        """Get the name of a collection either basic or absolute.
        :param collid: The collection ID.
        :type collid: int
        :return: Returns the collection name or None if not exists.
        :rtype: str
        """
        coll_name = ""
        if collid is None:
            return coll_name

        else:
            record = self.get_collection(collid)
            if record is None:
                raise AdasDBError("Collection '%s' doesn't exists in the catalog." % collid)
            else:
                parent_coll_name = self.get_collection_name(record[COL_NAME_COLL_PARENTID])
                coll_name = parent_coll_name + PATH_SEPARATOR + record[COL_NAME_COLL_NAME]

        return coll_name

    def get_collections_details(self, collection, recurse=True):
        """gets all sub-collection details of a given collection

        :param collection: name of collection or it's ID
        :type collection: int | str
        :param recurse: set True, if sub-collections shall be searched recursively
        :type recurse: bool
        :return: returns sub-collection details
        :rtype: list[dict]
        """
        if type(collection) == str:
            collid = self.get_collection_id(collection)
        else:
            collid = collection

        col_list = [COL_NAME_COLL_COLLID, COL_NAME_COLL_NAME, COL_NAME_COLL_IS_ACTIVE, COL_NAME_COLL_PRID,
                    COL_NAME_COLL_COLLCOMMENT, COL_NAME_COLL_PARENTID]
        if recurse:
            rec_list, col_list = self.get_collection_tree(collid, incl_shared=True, col_list=col_list)
            # exclude the first record it was not expect as per previous implementation and convert to list of dict
            records = [dict(zip(col_list, rec)) for rec in rec_list[1:]]
        else:

            cond = SQLBinaryExpr(COL_NAME_COLL_PARENTID, OP_EQ, ":1")
            records = self.select_generic_data(col_list, table_list=[TABLE_NAME_COLL],
                                               where=cond, sqlparams={"1": collid})
        return records

    def _get_union_shared_collection_cond(self, tree_sql, sql_param):
        """
        Generate SQL condition for the given recursive query to include all the shared collection subtree
        :param tree_sql: recursive SQL query
        :type tree_sql: str
        :param sql_param: sql variable binding dictionary
        :type sql_param: dict
        :return: Return Sql condition
        :rtype: `SQLBinaryExpr`
        """
        trees_sql = "(%s)" % (str(tree_sql))
        # print "self.sub_scheme_version :: ", self.sub_scheme_version
        # if self.sub_scheme_version >= CAT_SHARECOLL_VERSION:
            # print "sql_param 1> : ", sql_param
        shared_coll_cond = SQLBinaryExpr(COL_NANE_SHARECOLL_PARENT_COLLID, OP_IN, (trees_sql))
        shared_coll_query = GenericSQLSelect([COL_NANE_SHARECOLL_CHILD_COLLID], True,
                                             [TABLE_NAME_CAT_SHAREDCOLLECTIONMAP], shared_coll_cond)
        shared_coll = [i[0] for i in self.execute(str(shared_coll_query), **sql_param)]
        # print "sql_param 2> : ", sql_param
        for i in range(len(shared_coll)):
            shared_coll = shared_coll + self.get_collections(shared_coll[i])
        shared_coll = list(set(shared_coll))
        shared_coll = [shared_coll[i:i + 999] for i in range(0, len(shared_coll), 999)]

        cond = SQLBinaryExpr(COL_NAME_COLL_COLLID, OP_IN, trees_sql)

        label_cond = SQLBinaryExpr(COL_NAME_COLL_COLLID, OP_EQ, ":%s" % (len(sql_param)))
        # label = self.select_generic_data([COL_NAME_COLL_CP_LABEL], [TABLE_NAME_COLL],
        #                                  label_cond, sqlparams=sql_param)[0][COL_NAME_COLL_CP_LABEL]
        label_qry = GenericSQLSelect([COL_NAME_COLL_CP_LABEL], False,
                                             [TABLE_NAME_COLL], label_cond)
        # print "str(label_qry) :", str(label_qry), sql_param
        label = self.execute(str(label_qry), **sql_param)[0][0]
        # print "label : ", label
        if label == None: label = 'NULL'
        label_sel = ''' AND NVL(%s,'NULL') = '%s' ''' % (COL_NAME_COLL_CP_LABEL, label)
        if self.db_type[0] == 0:
            label_sel = label_sel.replace('NVL', 'IFNULL')
            trees_sql = "(%s%s" % (str(GenericSQLSelect([COL_NAME_COLL_COLLID], False, [TABLE_NAME_COLL], cond)), label_sel)
        else:
            trees_sql = "(%s%s)" % (str(GenericSQLSelect([COL_NAME_COLL_COLLID], False, [TABLE_NAME_COLL], cond)), label_sel)
        for entry in shared_coll:
            #Modified for one entry - Tanveer
            if len(entry)==1:
                cond = SQLBinaryExpr(COL_NAME_COLL_COLLID, OP_EQ, entry[0])
            else:
                cond = SQLBinaryExpr(COL_NAME_COLL_COLLID, OP_IN, str(tuple(entry)))
            if self.db_type[0] == 0:
                trees_sql += " UNION %s" % (str(GenericSQLSelect([COL_NAME_COLL_COLLID],
                                                               False, [TABLE_NAME_COLL], cond)))
            else:
                trees_sql += " UNION (%s)" % (str(GenericSQLSelect([COL_NAME_COLL_COLLID],
                                                               False, [TABLE_NAME_COLL], cond)))
        #Add Paranthesis else it will consider collection IDs insted of MeasID. Informed Zaheer.
        if self.db_type[0] != 0:
            trees_sql = "(" + trees_sql + ")"
        cond = SQLBinaryExpr(COL_NAME_COLL_COLLID, OP_IN, trees_sql)

        return cond

    def get_collection_summary(self, collid, recurse=True, group_by=None):
        """
        Make summary of aggregated statistic of file_size, file_count for TDSM

        :param collid: collection Id
        :type collid: int
        :param recurse: boolean flag to include sub collection
        :type recurse: boolean
        :param group_by: aggregate the value in group_by column. group by column will included select stmt implicity
        :type group_by: list
        :return: list of dict on exected query
        :rtype: list
        """
        file_count = SQLBinaryExpr(SQLFuncExpr(EXPR_COUNT, COL_NAME_FILES_MEASID), OP_AS, "FILE_COUNT")

        file_size = SQLBinaryExpr(SQLFuncExpr("SUM", SQLBinaryExpr(COL_NAME_FILES_FILESIZE, OP_DIV,
                                                                   "(1073741824)")),  # 1024^3
                                  OP_AS, COL_NAME_FILES_FILESIZE)
        file_dur = SQLBinaryExpr(SQLBinaryExpr("NVL(%s, 0)" % (COL_NAME_FILES_ENDTIMESTAMP), OP_SUB,
                                               "NVL(%s, 0)" % (COL_NAME_FILES_BEGINTIMESTAMP)),
                                 OP_MUL, 0.000001)  # convert to second

        file_dur = SQLBinaryExpr(SQLFuncExpr("SUM", file_dur), OP_AS, "DURATION")
        return self._get_collection_stats(collid, [file_count, file_size, file_dur], recurse, group_by)

    def _get_collection_stats(self, collid, cf_columns, recurse=True, group_by=None):
        """
        Generic function get aggregated statistic over recordings inside collection

        :param collid: collection id
        :type collid: int
        :param cf_columns: list of column from cat_Files table
        :type cf_columns: list
        :param recurse: boolean flag to include sub collection
        :type recurse: boolean
        :param group_by: aggregate the value in group_by column
        :type group_by: list
        :return: return records as list of dict on exected query
        :rtype: list
        """
        sql_param = {}
        sql_param[str(len(sql_param) + 1)] = collid
        if recurse:
            recurs, _ = self._get_collections_tree_query(":%s" % (len(sql_param)))
            recur_sql = GenericSQLSelect([COL_NAME_COLL_COLLID], False, [GEN_RECUR_NAME])
            tree_sql = str(SQLConcatExpr(recurs, recur_sql))
            cond1 = self._get_union_shared_collection_cond(tree_sql, sql_param)
        else:
            cond1 = SQLBinaryExpr(COL_NAME_COLLMAP_COLLID, OP_EQ, ":%s" % (len(sql_param)))
        cmap_tbl = str(GenericSQLSelect([COL_NAME_COLLMAP_MEASID], True, [TABLE_NAME_COLLMAP], cond1))
        cond = SQLBinaryExpr(COL_NAME_FILES_MEASID, OP_IN, "(%s)" % (cmap_tbl))

        if group_by is not None:
            select_list = cf_columns + group_by
        else:
            select_list = cf_columns
        return self.select_generic_data(select_list, [TABLE_NAME_FILES], cond, group_by, sqlparams=sql_param)

    def get_measurements_number(self, collection, recurse=True, group_by=None):
        """retrieves the amount of measurements being inside a collection

        :param collection: name or id of collection which is considered as root of desire tree
        :type collection: str
        :param recurse: count also sub collections along with root collection
        :type recurse: bool
        :return: number of recordings as integer group_by is not provide, otherwise list of dict
        :rtype: int | list
        """
        if type(collection) == str:
            collid = self.get_collection_id(collection)
        else:
            collid = collection
        cf_columns = [SQLBinaryExpr(SQLFuncExpr(EXPR_COUNT, COL_NAME_FILES_MEASID), OP_AS, "FILE_COUNT")]

        entries = self._get_collection_stats(collid, cf_columns, recurse, group_by)
        return entries[0]["FILE_COUNT"] if group_by is None else entries

    @staticmethod
    def _build_collection_query(collection, recurse, startcol):
        """builds up recursive query for collection details

        :param collection: name or id of collection
        :type collection: int | str
        :param recurse: weather we need to follow recurse query or not
        :type recurse: bool
        :param startcol: column name to base query upon
        :type startcol: str
        :return: recursive and out query
        :rtype: SQLBinaryExpr, GenericSQLSelect
        """

        if type(collection) == str:
            cond = SQLBinaryExpr(COL_NAME_COLL_NAME, OP_LIKE, SQLLiteral(collection))
            sbe = GenericSQLSelect([COL_NAME_COLL_COLLID], False, [TABLE_NAME_COLL], cond)
            oper = OP_IN
        else:
            sbe = SQLIntegral(collection)
            oper = OP_EQ

        sbe = SQLBinaryExpr(startcol, oper, sbe)

        if not recurse:
            return sbe, None

        virtcoll = "coll"
        start = GenericSQLSelect([COL_NAME_COLL_COLLID], False, [TABLE_NAME_COLL], sbe)
        join = SQLJoinExpr(TABLE_NAME_COLL, OP_INNER_JOIN, SQLTableExpr(GEN_RECUR_NAME, "r"),
                           SQLBinaryExpr(COL_NAME_COLL_PARENTID, OP_EQ, SQLColumnExpr("r", virtcoll)))
        stop = GenericSQLSelect([COL_NAME_COLL_COLLID], False, [join])
        outer = GenericSQLSelect([virtcoll], False, [GEN_RECUR_NAME])
        wexp = str(SQLConcatExpr(EXPR_WITH, SQLFuncExpr(GEN_RECUR_NAME, virtcoll)))
        wexp = SQLBinaryExpr(wexp, OP_AS, SQLConcatExpr(start, OP_UNION_ALL, stop))

        return wexp, outer

    def get_collections(self, collid, recurse=True):
        """
        Get the sub-collections of a collection.

        :param collid: The ID of the collection.
        :type collid: int
        :param recurse: Set True, if sub-collections shall be searched recursively.
        :type recurse: bool
        :return: Returns the sub-collections of a collection. The list contains
        :rtype: list
        """
        if recurse:
            records, _ = self.get_collection_tree(collid, incl_shared=True, col_list=[COL_NAME_COLL_COLLID])
            # remove the first entry this function doesnt expect the entry of passed collid
            records = records[1:] if len(records) >= 1 else []
        else:
            cond = SQLBinaryExpr(COL_NAME_COLL_COLLID, OP_EQ, ":1")
            records = self.select_generic_data_compact([COL_NAME_COLL_COLLID], [TABLE_NAME_COLL],
                                                       where=cond, sqlparams={"1": collid})[1]
        return [rec[0] for rec in records]

    def get_all_collection_names(self):
        """Get the Names of all collections.
        :return: Returns all collections names list
        :rtype: list
        """
        select_list = [SQLBinaryExpr(SQLColumnExpr(SQLTableExpr(TABLE_NAME_COLL), COL_NAME_COLL_NAME),
                                     OP_AS, COL_NAME_COLL_NAME)]

        entries = self.select_generic_data(select_list=select_list, table_list=[TABLE_NAME_COLL])
        return [entrie[COL_NAME_COLL_NAME] for entrie in entries]

    def get_all_coll_local(self):

        return self.select_generic_data(["*"], [TABLE_NAME_COLL])

    def get_search_result_coll(self, coll_id):

        condition_to_query = SQLBinaryExpr(COL_NAME_COLL_COLLID, OP_EQ, coll_id)
        coll_list = self.get_child_collections(coll_id, recurse=True)

        coll_info = self.select_generic_data(["*"], [TABLE_NAME_COLL], where=condition_to_query)

        coll_list.extend(coll_info)
        # print "coll_list >>", coll_list
        return coll_list

    # def get_user_info(self, user_id):
    #
    #     cond = SQLBinaryExpr(COL_NAME_USER_ID, OP_EQ, user_id)
    #     return self.select_generic_data(table_list=[TABLE_NAME_USERS], where=cond)

    def get_collection_measurements(self, collid, recurse=True,  # pylint: disable=R0912,R0914
                                    recfile_paths=False, recfile_dict=False):
        """
        Get all meaurement for the given collection

        :param collid: The ID of the collection.
        :type collid: int
        :param recurse: if Set True, sub-collections shall be searched recursively.
        :type recurse: bool
        :param recfile_paths: if Set True then recfile paths shall be returned.
               Otherwise the measurement IDs are returned.
        :type recfile_paths: bool
        :param recfile_dict: if Set True then dictionary with keys as recfile path
               and values as measurement Id will be return
        :type recfile_dict: bool
        :return: Returns the list of recording as list or dictionary
        :rtype: dict | list
        """
        sql_param = {}
        cmap_tbl_alias = "cmap"
        cf_tbl_alias = "cf"
        cf_tbl = SQLTableExpr(TABLE_NAME_FILES, cf_tbl_alias)
        sql_param[str(len(sql_param) + 1)] = collid
        if recurse:
            recurs, _ = self._get_collections_tree_query(":%s" % (len(sql_param)))
            recur_sql = GenericSQLSelect([COL_NAME_COLL_COLLID], False, [GEN_RECUR_NAME])
            tree_sql = str(SQLConcatExpr(recurs, recur_sql))
            cond = self._get_union_shared_collection_cond(tree_sql, sql_param)
            if self.db_type[0] == 0:
                cmap_tbl = SQLTableExpr("(%s))" % GenericSQLSelect([COL_NAME_COLLMAP_MEASID], False,
                                                              [TABLE_NAME_COLLMAP], cond), cmap_tbl_alias)
            else:
                cmap_tbl = SQLTableExpr("(%s)" % GenericSQLSelect([COL_NAME_COLLMAP_MEASID], False,
                                                              [TABLE_NAME_COLLMAP], cond), cmap_tbl_alias)

            join = SQLJoinExpr(cf_tbl, OP_INNER_JOIN, cmap_tbl,
                               SQLBinaryExpr(SQLColumnExpr(cmap_tbl_alias, COL_NAME_COLLMAP_MEASID), OP_EQ,
                                             SQLColumnExpr(cf_tbl_alias, COL_NAME_FILES_MEASID)))
            cond = None
        else:
            cmap_tbl = SQLTableExpr(TABLE_NAME_COLLMAP, cmap_tbl_alias)
            join = SQLJoinExpr(cf_tbl, OP_INNER_JOIN, cmap_tbl,
                               SQLBinaryExpr(SQLColumnExpr(cmap_tbl_alias, COL_NAME_COLLMAP_MEASID), OP_EQ,
                                             SQLColumnExpr(cf_tbl_alias, COL_NAME_FILES_MEASID)))
            cond = SQLBinaryExpr(SQLColumnExpr(cmap_tbl_alias, COL_NAME_COLLMAP_COLLID), OP_EQ,
                                 ":%s" % (len(sql_param)))
        columns = [SQLColumnExpr(cf_tbl_alias, COL_NAME_FILES_MEASID)]
        if recfile_paths or recfile_dict:
            columns.append(SQLColumnExpr(cf_tbl_alias, COL_NAME_FILES_FILEPATH))
        sql = GenericSQLSelect(columns, True, [join], cond)
        records = self.execute(str(sql), **sql_param)
        if recfile_paths and recfile_dict:
            recfile_data_dict = {recfile[1]: recfile[0]
                                 for recfile in records}
        elif recfile_paths and not recfile_dict:
            recfile_list = [recfile[1] for recfile in records]

        elif not recfile_paths and recfile_dict:
            recfile_data_dict = {recfile[0]: recfile[1]
                                 for recfile in records}
        elif not recfile_paths and not recfile_dict:
            recfile_list = [recfile[0] for recfile in records]
        if recfile_dict:
            return recfile_data_dict
        else:
            return recfile_list

    def update_collection_priority(self, collid, prid):
        """
        Update/Assign priority to collection

        :param collid: collection Id
        :type collid: int
        :param prid: priority Id
        :type prid: int
        """
        # cond = SQLBinaryExpr(collid, OP_EQ, collid)
        cond = SQLBinaryExpr(COL_NAME_COLL_COLLID, OP_EQ, collid)
        self.update_generic_data({COL_NAME_COLL_PRID: prid}, TABLE_NAME_COLL, cond)

    def update_shared_collection_priority(self, parent_collid, child_collid, prid):
        """
        Update/Assign priority to collection in its shared location

        :param parent_collid: parent collection Id
        :type parent_collid: int
        :param child_collid: child collection id
        :type child_collid: int
        :param prid: priority id
        :type prid: int
        """
        cond = SQLBinaryExpr(COL_NANE_SHARECOLL_PARENT_COLLID, OP_EQ, parent_collid)
        cond = SQLBinaryExpr(cond, OP_AND, SQLBinaryExpr(COL_NANE_SHARECOLL_CHILD_COLLID, OP_EQ, child_collid))
        self.update_generic_data({COL_NANE_SHARECOLL_PRID: prid}, TABLE_NAME_CAT_SHAREDCOLLECTIONMAP, cond)

    def get_shared_collection_priority(self, parent_collid, child_collid):
        """
        get priority for the shared location

        :param parent_collid: parent collection Id
        :type parent_collid: int
        :param child_collid: child collection id
        :type child_collid: int
        :return: list of dictionary record contain prority Id
        :rtype: list[dict]
        """
        sql_param = {}
        sql_param[str(len(sql_param) + 1)] = parent_collid
        cond = SQLBinaryExpr(COL_NANE_SHARECOLL_PARENT_COLLID, OP_EQ, ":%s" % str(len(sql_param)))
        sql_param[str(len(sql_param) + 1)] = child_collid
        cond = SQLBinaryExpr(cond, OP_AND, SQLBinaryExpr(COL_NANE_SHARECOLL_CHILD_COLLID,
                                                         OP_EQ, ":%s" % str(len(sql_param))))
        return self.select_generic_data([COL_NANE_SHARECOLL_PRID], [TABLE_NAME_CAT_SHAREDCOLLECTIONMAP],
                                        cond, sqlparams=sql_param)

    def get_collection_priority(self, collid):
        """
        Get Priority Id for the given collection Id

        :param collid: Collection Id
        :type collid: int
        :return: Return priority of collection
        :rtype: int
        """
        # cond = SQLBinaryExpr(collid, OP_EQ, collid)
        cond = SQLBinaryExpr(COL_NAME_COLL_COLLID, OP_EQ, collid)
        tblcollections = TABLE_NAME_COLL

        entries = self.select_generic_data(select_list=[COL_NAME_COLL_PRID], table_list=[tblcollections], where=cond)
        if len(entries) == 1:
            return entries[0][COL_NAME_COLL_PRID]
        elif len(entries) > 1:
            raise StandardError("Cannot Resolve collid " + str(collid) + " because it is ambigious")
        return -1

    def fill_auto_completer(self):

        return self.select_generic_data(select_list=[COL_NAME_COLLOG_ACTIONBY],
                                        table_list=[TABLE_NAME_CAT_COLLECTION_LOG], distinct_rows=True), \
               self.select_generic_data(select_list=[COL_NAME_COLLOG_COLL_NAME],
                                        table_list=[TABLE_NAME_CAT_COLLECTION_LOG], distinct_rows=True)

    def get_collection_log_selection(self, coll_ids, start_date, end_date, actions, user, all, coll_name):
        """
        :author:        Mohammed Tanveer
        :date:          06/05/2015

        :param coll_id: ID of the collection
        :param start_date: Start date on selection from the logger table
        :param end_date: End date on selection from the logger table
        :param action: An option to selected from the list
        :param user: A string to be entered in logger table for user
        :param all: All the data populated in the logger
        :return: A list of the collections
        """
        condUser = SQLBinaryExpr(COL_NAME_COLLOG_ACTIONBY,OP_EQ,SQLLiteral(user))
        cond_start_end_date = SQLBinaryExpr(SQLBinaryExpr(COL_NAME_COLLOG_ACTION_DATE,OP_GT,SQLLiteral(start_date)),
                                            OP_AND ,SQLBinaryExpr(COL_NAME_COLLOG_ACTION_DATE,OP_LEQ,SQLLiteral(end_date)))
        cond_coll_name = SQLBinaryExpr(COL_NAME_COLLOG_COLL_NAME,OP_EQ,SQLLiteral(coll_name))
        if actions == 'All':
             if user == '':
                 cond4 = cond_start_end_date
             else:
                 cond4 = SQLBinaryExpr(cond_start_end_date,OP_AND,condUser)
        else:
            if len(actions) == 1:
                action = str(tuple(actions))
                actions_tup = action[:len(action)-2]+action[-1]
            else:
                actions_tup = str(tuple(actions))
            cond1 = SQLBinaryExpr(COL_NAME_COLLOG_ACTION,OP_IN,actions_tup)
            cond2 = cond_start_end_date
            if user == '':
                cond4 = SQLBinaryExpr(cond1,OP_AND,cond2)
            else:
                cond5 = SQLBinaryExpr(cond1,OP_AND,cond2)
                cond4 = SQLBinaryExpr(cond5,OP_AND,condUser)
        if all:
            cond6 = cond4
        else:
            if len(coll_ids) == 1:
                collid = str(tuple(coll_ids))
                collid_tup = collid[:len(collid)-2]+collid[-1]
            else:
                collid_tup = str(tuple(coll_ids))
            cond3 = SQLBinaryExpr(COL_NAME_COLLOG_COLLID, OP_IN, collid_tup)
            cond6 = SQLBinaryExpr(cond4, OP_AND, cond3)
        if coll_name == '':
            cond_fin = cond6
        else:
            cond_fin = SQLBinaryExpr(cond6, OP_AND, cond_coll_name)
        return cond_fin

    def get_measID_count(self, logID):
        condition_to_query = SQLBinaryExpr(COL_NAME_COLLOG_LOG_ID, OP_EQ, logID)
        measurementID_count = self.select_generic_data\
            (["COUNT(MEASID)"], [TABLE_NAME_CAT_COLLECTION_LOGDETAILS],
             where=condition_to_query)
        return measurementID_count

    def get_master_collections(self):

        col_list = [COL_NAME_COLL_PARENTID, COL_NAME_COLL_COLLID, COL_NAME_COLL_NAME,
                    COL_NAME_COLL_COLLCOMMENT, COL_NAME_COLL_PRID, COL_NAME_COLL_IS_ACTIVE,
                    COL_NAME_COLL_USERID, COL_NAME_COLL_CREATED, COL_NAME_COLL_CP_LABEL]
        order_by_list = [COL_NAME_COLL_PRID, COL_NAME_COLL_NAME]
        where_cond = SQLBinaryExpr(COL_NAME_COLL_PARENTID, OP_IS, SQLNull())
        # where_cond = SQLBinaryExpr(COL_NAME_COLL_COLLID, OP_EQ, 9118)
        return self.select_generic_data(
            select_list=col_list,
            table_list=[TABLE_NAME_COLL],
            where=where_cond, order_by=order_by_list)




    # def get_collection_tree_single(self, collid, is_shared_coll=0):
    #     """
    #     new custom / not part
    #     Args:
    #         collid: Collection ID
    #         is_shared_coll: 0 is not shared, 1 is shared
    #     Returns: Dictionay of all collection details
    #
    #     :author:        Mohammed Tanveer
    #     :date:          5/5/2016
    #     """
    #     sql_param = {"1": collid}
    #     col_list = [COL_NAME_COLL_PARENTID, COL_NAME_COLL_COLLID, COL_NAME_COLL_NAME, COL_NAME_COLL_COLLCOMMENT,
    #                 COL_NAME_COLL_PRID, COL_NAME_COLL_IS_ACTIVE, COL_NAME_COLL_CP_LABEL, COL_NAME_COLL_CREATED,
    #                 COL_NAME_COLL_USERID]
    #     catcoll_tbl_alias = "c"
    #     gbl_tbl_alias = "gbu"
    #     cat_coll_tbl = SQLTableExpr(TABLE_NAME_COLL, catcoll_tbl_alias)
    #     gbl_coll_tbl = SQLTableExpr(TABLE_NAME_USERS, gbl_tbl_alias)
    #     col_list_aliased = [SQLColumnExpr(catcoll_tbl_alias, COL_NAME_COLL_PARENTID),
    #                         SQLColumnExpr(catcoll_tbl_alias, COL_NAME_COLL_COLLID),
    #                         SQLColumnExpr(catcoll_tbl_alias, COL_NAME_COLL_NAME),
    #                         SQLColumnExpr(catcoll_tbl_alias, COL_NAME_COLL_COLLCOMMENT),
    #                         SQLColumnExpr(catcoll_tbl_alias, COL_NAME_COLL_PRID),
    #                         SQLColumnExpr(catcoll_tbl_alias, COL_NAME_COLL_IS_ACTIVE),
    #                         SQLColumnExpr(catcoll_tbl_alias, COL_NAME_COLL_CP_LABEL),
    #                         SQLColumnExpr(catcoll_tbl_alias, COL_NAME_COLL_CREATED),
    #                         SQLBinaryExpr(SQLColumnExpr(gbl_tbl_alias, COL_NAME_USER_NAME), OP_AS, COL_NAME_COLL_USERID)]
    #
    #     cond = SQLBinaryExpr(SQLColumnExpr(catcoll_tbl_alias, COL_NAME_COLL_PARENTID), OP_EQ, ":%s" % str(len(sql_param)))
    #     expr1 = SQLBinaryExpr(SQLColumnExpr(catcoll_tbl_alias, COL_NAME_COLL_USERID), OP_EQ,
    #                                           SQLColumnExpr(gbl_tbl_alias, COL_NAME_USER_ID))
    #     join = SQLJoinExpr(cat_coll_tbl, OP_LEFT_OUTER_JOIN, gbl_coll_tbl, expr1)
    #     normal_colls = self.select_generic_data(select_list=col_list_aliased, table_list=[join], where=cond, sqlparams=sql_param)
    #     if is_shared_coll=="0":
    #         for each_record in normal_colls:
    #             each_record[SHARED_FLAG] = N0T_SHARED_COLL
    #             each_record[SHARED_CHILD_FLAG] = N0T_SHARED_CHILD_COLL
    #     else:
    #         # print "enter shared"
    #         for each_record in normal_colls:
    #             each_record[SHARED_FLAG] = SHARED_COLL
    #             each_record[SHARED_CHILD_FLAG] = SHARED_CHILD_COLL
    #     shared_ids, prids = self.get_shared_collid_sqlite(parent_collid=collid)
    #     # print "shared_ids : ",shared_ids,prids
    #     if len(shared_ids)!= 0:
    #         if len(shared_ids)==1:
    #             cond = SQLBinaryExpr(COL_NAME_COLL_COLLID, OP_EQ, shared_ids[0])
    #         else:
    #             cond = SQLBinaryExpr(COL_NAME_COLL_COLLID, OP_IN, str(tuple(shared_ids)))
    #         # print "cond --> ",cond
    #         #Add the priority of the collection from shared map table
    #         # self.get_shared_collection_priority(collid,shared_ids[0])
    #         shared_colls = self.select_generic_data(select_list=col_list, table_list=[TABLE_NAME_COLL],
    #                                                    where=cond)
    #         # print "shared_colls : ",shared_colls
    #         for each_shared_record in shared_colls:
    #             for share_id in shared_ids:
    #                 # print  "1>"
    #                 for prid in prids:
    #                     # print "2>",type(share_id),prid
    #                     if each_shared_record[COL_NAME_COLL_COLLID] == share_id:
    #                         # print "3>",each_shared_record[COL_NAME_COLL_PRID]
    #                         each_shared_record[COL_NAME_COLL_PRID] = prid
    #             each_shared_record[SHARED_FLAG] = SHARED_COLL
    #             each_shared_record[SHARED_CHILD_FLAG] = N0T_SHARED_CHILD_COLL
    #             # print "each_shared_record : ",each_shared_record
    #         if is_shared_coll=="1":
    #             for each_shared_record in shared_colls:
    #                 each_shared_record[SHARED_FLAG] = SHARED_COLL
    #                 each_shared_record[SHARED_CHILD_FLAG] = SHARED_CHILD_COLL
    #         normal_colls.extend(shared_colls)
    #         return normal_colls
    #     else:
    #         return normal_colls

    def get_shared_collid_sqlite(self, parent_collid):
        """
        not part / modified in the last return statement and one column in the condition- Tanveer
        get list of all shared collection ids for given parent collection Id

        :param parent_collid: parent collection
        :type parent_collid: list or int
        :return: list of child collection Id
        :rtype: list

        :author:        Mohammed Tanveer
        :date:          05/04/2016
        """
        cond = SQLBinaryExpr(COL_NANE_SHARECOLL_PARENT_COLLID, OP_EQ, ":1")
        records = self.select_generic_data_compact([COL_NANE_SHARECOLL_CHILD_COLLID,COL_NANE_SHARECOLL_PRID],
                                                   [TABLE_NAME_CAT_SHAREDCOLLECTIONMAP],
                                                   where=cond, sqlparams={"1": parent_collid})[1]
        # print "records : ",records
        return [rec[0] for rec in records], [rec[1] for rec in records]

    def get_collection_details_sqlite(self, collid):
        """
        Function to get the child collection IDs including shared
        Args:
            collid: Collection ID

        Returns: List of child collection IDs

        :author:        Mohammed Tanveer
        :date:          05/04/2016
        """

        # print "collid : >> ",collid
        child_colls = self.get_child_collections(str(collid))
        coll_ids = [str(each_child[COL_NAME_COLL_COLLID]) for each_child in child_colls]
        coll_ids.append(collid)
        coll_ids = [str(x) for x in coll_ids]
        #Include collids of shared collections
        # print "coll_ids :> 1", coll_ids
        shared_ids = self.get_shared_collids(coll_ids)
        # print "shared_ids :: ", shared_ids
        shared_collids = list(set([str(each_child[COL_NANE_SHARECOLL_CHILD_COLLID]) for each_child in shared_ids]))
        coll_ids.extend(shared_collids)
        # print "shared_collids : ",shared_collids
        coll_ids = [str(x) for x in coll_ids]
        # print "coll_ids :> 2", coll_ids
        if len(shared_collids) != 0:
        #Get shared child collections
            for each_sharedid in shared_collids:
                shared_child_colls = self.get_child_collections(each_sharedid)
                shared_child_ids = [str(each_child[COL_NAME_COLL_COLLID]) for each_child in shared_child_colls]
            coll_ids.extend(str(shared_child_ids))
        coll_ids = [str(x) for x in coll_ids]
        # print "coll_ids >> 3", coll_ids
        return coll_ids

    def get_shared_collids(self, collids_list):

        if len(collids_list) == 1:
            collid = str(tuple(collids_list))
            collid_tup = collid[:len(collid) - 2] + collid[-1]
        else:
            collid_tup = str(tuple(collids_list))
        cond = SQLBinaryExpr(COL_NANE_SHARECOLL_PARENT_COLLID, OP_IN, collid_tup)
        subcolls = self.select_generic_data(select_list=[COL_NANE_SHARECOLL_CHILD_COLLID],
                                               table_list=[
                                                   self.GetQualifiedTableName(TABLE_NAME_CAT_SHAREDCOLLECTIONMAP)],
                                               where=cond)
        return subcolls

    def get_child_collections(self, collid, recurse=True):
        """
        Get the sub-collections of a collection.

        :param collid: The ID of the collection.
        :type collid: Integer
        :param recurse: Set True, if sub-collections shall be searched recursively.
        :type recurse: Bool
        :return: Returns the sub-collections of a collection. The list contains
        :rtype : list
        :author:        Mohammed Tanveer
        :date:          15/12/2015
        """
        coll_list = []

        collid_queue = Queue()
        collid_queue.put(collid)
        while not collid_queue.empty():
            collid = collid_queue.get()
            # select all collections which have the current collection as parent
            cond = SQLBinaryExpr(COL_NAME_COLL_PARENTID, OP_EQ, SQLLiteral(collid))
            subcolls = self.select_generic_data(table_list=[self.GetQualifiedTableName(TABLE_NAME_COLL)],
                                                   where=cond)
            for subcoll in subcolls:
                subcollid = subcoll[COL_NAME_COLL_COLLID]
                if recurse:
                    collid_queue.put(subcollid)
                coll_list.append(subcoll)

        return coll_list

    def get_coll_summary_sqlite(self, collids_list):
        """

        Args:
            collids_list: list of collection IDs

        Returns: File count, File size/ tuple

        :author:        Mohammed Tanveer
        :date:          05/04/2016
        """
        collids_list = [str(x) for x in collids_list]
        if len(collids_list) == 1:
            collid = str(tuple(collids_list))
            collid_tup = collid[:len(collid)-2]+collid[-1]
        else:
            collid_tup = str(tuple(collids_list))
        # print "collid_tup :: ", collid_tup
        stmt='''SELECT COUNT(MEASID) AS FILECOUNT,
                SUM(FILESIZE  / (1073741824)) AS FILESIZE,
                ARCHIVED
                FROM CAT_FILES WHERE MEASID IN
                (SELECT DISTINCT(MEASID) FROM CAT_COLLECTIONMAP WHERE COLLID IN {})
                GROUP BY ARCHIVED '''.format(collid_tup)
        records = self.sql(stmt)
        return records

    def get_coll_distance_sqlite(self,collids_list):
        """

        Args:
            collids_list: list of collection IDs

        Returns: Distance/ tuple
        :author:        Mohammed Tanveer
        :date:          05/04/2016
        """
        collids_list = [str(x) for x in collids_list]
        if len(collids_list) == 1:
            collid = str(tuple(collids_list))
            collid_tup = collid[:len(collid)-2]+collid[-1]
        else:
            collid_tup = str(tuple(collids_list))
        stmt='''SELECT SUM(RECDRIVENDIST) AS  RECDRIVENDIST,
                ARCHIVED
                FROM CAT_FILES WHERE MEASID IN
                (SELECT DISTINCT(MEASID) FROM CAT_COLLECTIONMAP WHERE COLLID IN {})
                AND RECDRIVENDIST > 0
                GROUP BY ARCHIVED '''.format(collid_tup)
        records = self.sql(stmt)
        return records

    def is_checkpoint_created(self, coll_name, chkpnt_lbl):

        cond = SQLBinaryExpr(SQLBinaryExpr(COL_NAME_COLL_NAME, OP_EQ, SQLLiteral(coll_name)), OP_AND,
                             SQLBinaryExpr(COL_NAME_COLL_CP_LABEL, OP_EQ, SQLLiteral(chkpnt_lbl)))
        stmt = GenericSQLSelect(table_list=[TABLE_NAME_COLL], where_condition=cond)
        collid = self.sql(str(stmt))
        return False if (len(collid)) == 0 else True

    # def add_collection_checkpoint(self, coll_name, label, desc=None):
    #     """
    #     adds a new collection checkpoint out of ``collection`` using new ``label``
    #     with changed ``description`` on same parent level(s)
    #
    #     Copy all collection entries to a new collection with same name and set the checkpoint label for it,
    #     do this also for all sub collections and all linked shared collections.
    #
    #     A checkpointed collection freezes the current state for simulation and validation for customer reports.
    #     It can not be updated anymore and deleted only with special user rights.
    #     Because it is frozen it doesn't make sense to create a checkpoint for a checkpointed collection (no difference),
    #     therefore the starting collection has to be one without checkpoint.
    #
    #     It is possible to have different checkpoints of the same starting collection,
    #     but please **check before creating a new checkpoint if there is really a difference to existing ones**
    #     as we always create a real complete copy of all entries to keep forever!
    #
    #     **The collection name together with the checkpoint label have to be unique in the database!**
    #
    #     :param coll_name: name of collection to create a checkpoint from
    #     :type coll_name: str
    #     :param label: checkpoint label
    #     :type label: str
    #     :param desc: changed description
    #     :type desc: None | str
    #     :return: collection identifier of new checkpointed collection, None if failed
    #     :rtype: int
    #     """
    #     src = Collection(self, name=coll_name)
    #     dst = Collection(self, name=src.parent)
    #     ret = self._copy_collection(src, dst, label, desc)
    #     self.commit()
    #     return ret

    # def _copy_collection(self, src, dst, label, desc):
    #     """recursively copy new collection
    #
    #     :param src: source collection
    #     :param dst: destination collection
    #     :param label: collection label
    #     :param desc: collection description
    #     :return: DB ID of collection
    #     """
    #     dst = dst.add_coll(name=src.name, label=label, desc=desc, prio=src.prio)
    #
    #     # iterate thought source collection
    #     for i in src:
    #         # if item is a (shared /) collection
    #         if i.type == CollManager.COLL:  # copy inside as well
    #             self._copy_collection(i, dst, label, desc)
    #         elif i.type == CollManager.SHARE:
    #             nid = self._copy_collection(i, Collection(self, name=Collection(self, name=i.parent).parent),
    #                                         label, desc)
    #             dst.add_coll(name=nid, type=CollManager.SHARE)
    #         else:  # copy recording itself to sqlite file
    #             dst.add_rec(name=i.id, beginrelts=i.beginrelts, endrelts=i.endrelts)
    #
    #     return dst.id

    def sort_latest_logs(self, unsorted_logs):
        """
        :author:        Mohammed Tanveer
        :date:          26/10/2015
        :param unsorted_logs: A list containing dictionary of each log
        :return: A list having dictionary of each log sorted with latest created date

        Arranges all the logs for the given collection in order of latest creation date
        """

        dates = [str(log[COL_NAME_COLLOG_ACTION_DATE])
                  for log in unsorted_logs]
        unsorted_dates = list(set(dates))

        try:
            if self.db_type[1] == 'sqlite3':
                unsorted_dates.sort(key=lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S.%f'), reverse=True)
            else:
                unsorted_dates.sort(key=lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S'), reverse=True)
        except ValueError, e:
            print str(e)
        sorted_logs = []
        for each_date in unsorted_dates:
            sorted_logs.extend([each_log for each_log in unsorted_logs if each_date == str(each_log[COL_NAME_COLLOG_ACTION_DATE])])
        return sorted_logs

    def get_log_create_details(self, collID):
        """
        :author:        Kushagra Goyal
        :date:
        """
        cond = SQLBinaryExpr(SQLBinaryExpr(COL_NAME_COLLOG_COLLID, OP_LIKE, SQLLiteral(collID)),
                              OP_AND,
                              SQLBinaryExpr(COL_NAME_COLLOG_ACTION, OP_LIKE, "'Created'"))
        list = self.sort_latest_logs(self.SelectGenericData(select_list=['*'], table_list=[self.GetQualifiedTableName(TABLE_NAME_CAT_COLLECTION_LOG)],
                                      where=cond))
        if len(list) == 0:
            list_log_create_details = ["Data Not Available", "Data Not Available"]
        else:
            list_log_create_details = [list[0]['ACTIONBY'], list[0]['ACTION_DATE']]
        return list_log_create_details

    def get_log_modify_details(self, collID):
        """
        :author:        Kushagra Goyal
        :date:
        """
        cond2 = SQLBinaryExpr(COL_NAME_COLLOG_COLLID, OP_EQ, SQLLiteral(collID))
        list2 = self.sort_latest_logs(self.SelectGenericData(select_list=['*'], table_list=
        [self.GetQualifiedTableName(TABLE_NAME_CAT_COLLECTION_LOG)],
                                      where=cond2))
        #print "list2", list2
        if len(list2) == 0:
            list_log_modify_details = ["Data Not Available", "Data Not Available"]
        else:
            list_log_modify_details = [list2[0]['ACTIONBY'], list2[0]['ACTION_DATE']]
        return list_log_modify_details

    def get_shared_collection_id(self, shared_col_id):

        cond = SQLBinaryExpr(SHARED_COLLECTION_ID, OP_EQ, shared_col_id)
        shared_collection_ids = self.select_generic_data(select_list=[SHARED_COL_PARENT_ID, SHARED_COLLECTION_ID],
                                                         table_list=[TABLE_NAME_CAT_SHAREDCOLLECTIONMAP], where=cond)
        if not shared_collection_ids:
            return False
        else:
            shared_collection_id_name = []
            for share_collection_id in shared_collection_ids:
                shared_collection_parent_child = []
                for col_id in share_collection_id.values():
                    coll_name = self.get_collection_name(col_id)
                    shared_collection_parent_child.append(coll_name)
                shared_collection_id_name.append(shared_collection_parent_child)
            return shared_collection_id_name

    def is_shared_link(self, parent_col_id, shared_coll_id):

        pre_cond = SQLBinaryExpr(SHARED_COL_PARENT_ID, OP_EQ, parent_col_id)
        cond = SQLBinaryExpr(SQLBinaryExpr(SHARED_COLLECTION_ID, OP_EQ, shared_coll_id), OP_AND, pre_cond)
        map_link = self.select_generic_data(table_list=[TABLE_NAME_CAT_SHAREDCOLLECTIONMAP], where=cond)
        if map_link:
            return True
        else:
            return False

    # def get_coll_two_hierarchy(self, collid_lst, is_shared_coll=0):
    #     """
    #     new custom / not part
    #     Args:
    #         collid: Collection ID
    #         is_shared_coll: 0 is not shared, 1 is shared
    #     Returns: Dictionay of all collection details
    #
    #     :author:        Mohammed Tanveer
    #     :date:          5/5/2016
    #     """
    #     # sql_param = {"1": str(tuple(collid_lst))}
    #     col_list = [COL_NAME_COLL_PARENTID, COL_NAME_COLL_COLLID, COL_NAME_COLL_NAME, COL_NAME_COLL_COLLCOMMENT,
    #                 COL_NAME_COLL_PRID, COL_NAME_COLL_IS_ACTIVE, COL_NAME_COLL_CP_LABEL, COL_NAME_COLL_CREATED,
    #                 COL_NAME_COLL_USERID]
    #     catcoll_tbl_alias = "c"
    #     gbl_tbl_alias = "gbu"
    #     cat_coll_tbl = SQLTableExpr(TABLE_NAME_COLL, catcoll_tbl_alias)
    #     gbl_coll_tbl = SQLTableExpr(TABLE_NAME_USERS, gbl_tbl_alias)
    #     col_list_aliased = [SQLColumnExpr(catcoll_tbl_alias, COL_NAME_COLL_PARENTID),
    #                         SQLColumnExpr(catcoll_tbl_alias, COL_NAME_COLL_COLLID),
    #                         SQLColumnExpr(catcoll_tbl_alias, COL_NAME_COLL_NAME),
    #                         SQLColumnExpr(catcoll_tbl_alias, COL_NAME_COLL_COLLCOMMENT),
    #                         SQLColumnExpr(catcoll_tbl_alias, COL_NAME_COLL_PRID),
    #                         SQLColumnExpr(catcoll_tbl_alias, COL_NAME_COLL_IS_ACTIVE),
    #                         SQLColumnExpr(catcoll_tbl_alias, COL_NAME_COLL_CP_LABEL),
    #                         SQLColumnExpr(catcoll_tbl_alias, COL_NAME_COLL_CREATED),
    #                         SQLBinaryExpr(SQLColumnExpr(gbl_tbl_alias, COL_NAME_USER_NAME), OP_AS, COL_NAME_COLL_USERID)]
    #     collid_list_aliased = [SQLColumnExpr(catcoll_tbl_alias, COL_NAME_COLL_COLLID)]
    #     if len(collid_lst) == 1:
    #         collid = str(tuple(collid_lst))
    #         collid_tup = collid[:len(collid)-2]+collid[-1]
    #     else:
    #         collid_tup = str(tuple(collid_lst))
    #     cond = SQLBinaryExpr(SQLColumnExpr(catcoll_tbl_alias, COL_NAME_COLL_PARENTID), OP_IN, collid_tup)
    #     # cond = SQLBinaryExpr(SQLColumnExpr(catcoll_tbl_alias, COL_NAME_COLL_PARENTID), OP_IS, SQLNull())
    #     expr1 = SQLBinaryExpr(SQLColumnExpr(catcoll_tbl_alias, COL_NAME_COLL_USERID), OP_EQ,
    #                                           SQLColumnExpr(gbl_tbl_alias, COL_NAME_USER_ID))
    #     join = SQLJoinExpr(cat_coll_tbl, OP_LEFT_OUTER_JOIN, gbl_coll_tbl, expr1)
    #     sql1_union = GenericSQLSelect(select_list=col_list_aliased, distinct_rows=False, table_list=[join], where_condition=cond)
    #     data_list = self.sql(str(sql1_union))
    #     # return [dict(zip(col_list, rec)) for rec in data_list]
    #     normal_colls = [dict(zip(col_list, rec)) for rec in data_list]
    #     if is_shared_coll == 0:
    #         for each_record in normal_colls:
    #             each_record[SHARED_FLAG] = N0T_SHARED_COLL
    #             each_record[SHARED_CHILD_FLAG] = N0T_SHARED_CHILD_COLL
    #     else:
    #         # print "enter shared"
    #         for each_record in normal_colls:
    #             each_record[SHARED_FLAG] = SHARED_COLL
    #             each_record[SHARED_CHILD_FLAG] = SHARED_CHILD_COLL
    #     shared_colls = self.get_shared_collid(collid_lst)
    #     if shared_colls:
    #         for each_shared_record in shared_colls:
    #             each_shared_record[SHARED_FLAG] = SHARED_COLL
    #             each_shared_record[SHARED_CHILD_FLAG] = N0T_SHARED_CHILD_COLL
    #             # print "each_shared_record : ",each_shared_record
    #         # print "shared_colls : ", shared_colls
    #         if is_shared_coll == 1:
    #             for each_shared_record in shared_colls:
    #                 each_shared_record[SHARED_FLAG] = SHARED_COLL
    #                 each_shared_record[SHARED_CHILD_FLAG] = SHARED_CHILD_COLL
    #         normal_colls.extend(shared_colls)
    #         return normal_colls
    #     else:
    #         return normal_colls

    def get_shared_collid(self, parent_collid_lst):
        """
        not part / modified in the last return statement and one column in the condition- Tanveer
        get list of all shared collection ids for given parent collection Id

        :param parent_collid: parent collection
        :type parent_collid: list or int
        :return: list of child collection Id
        :rtype: list

        :author:        Mohammed Tanveer
        :date:          05/04/2016
        """

        if len(parent_collid_lst) == 1:
            collid = str(tuple(parent_collid_lst))
            collid_tup = collid[:len(collid)-2]+collid[-1]
        else:
            collid_tup = str(tuple(parent_collid_lst))
        stmt = ''' SELECT cshmap.CHILD_COLLID AS COLLID,
          cshmap.PARENT_COLLID     AS PARENTID,
          c.NAME,
          c.COLLCOMMENT,
          c.PRID,
          c.IS_ACTIVE,
          gbu.NAME AS USERID,
          c.CREATED,
          c.CP_LABEL
        FROM CAT_COLLECTIONS c
        LEFT OUTER JOIN GBL_USERS gbu
        ON c.USERID = gbu.USERID
        LEFT OUTER JOIN CAT_SHAREDCOLLECTIONMAP cshmap
        ON c.COLLID                = cshmap.CHILD_COLLID
        WHERE cshmap.PARENT_COLLID IN {}'''.format(collid_tup)
        # print stmt
        data_list = self.execute(stmt)
        col_list = [COL_NAME_COLL_COLLID, COL_NAME_COLL_PARENTID, COL_NAME_COLL_NAME, COL_NAME_COLL_COLLCOMMENT,
                    COL_NAME_COLL_PRID, COL_NAME_COLL_IS_ACTIVE, COL_NAME_COLL_USERID, COL_NAME_COLL_CREATED, COL_NAME_COLL_CP_LABEL
                    ]
        return [dict(zip(col_list, rec)) for rec in data_list]

    def get_childcoll_count(self, collid):
        """
        not part
        Args:
            collid: Collection ID

        Returns: Count of number of collections /Integer

        :author:        Mohammed Tanveer
        :date:          05/04/2016
        """
        sql_param = {}
        sql_param[str(len(sql_param) + 1)] = collid
        cond_main = SQLBinaryExpr(SQLFuncExpr(EXPR_COUNT, COL_NAME_COLL_PARENTID), OP_AS, "FILE_COUNT")
        cond_coll = SQLBinaryExpr(COL_NAME_COLL_PARENTID, OP_EQ, ":%d" % (len(sql_param)))
        sql_coll = GenericSQLSelect(select_list=[SQLBinaryExpr(COL_NAME_COLL_PARENTID, OP_AS, COL_NAME_COLL_PARENTID)],
                                distinct_rows=False,table_list=[TABLE_NAME_COLL],where_condition=cond_coll)
        cond_map = SQLBinaryExpr(COL_NANE_SHARECOLL_PARENT_COLLID, OP_EQ, ":%d" % (len(sql_param)))
        sql_map = GenericSQLSelect(select_list=[SQLBinaryExpr(COL_NANE_SHARECOLL_PARENT_COLLID, OP_AS, COL_NAME_COLL_PARENTID)],
                                distinct_rows=False,table_list=[TABLE_NAME_CAT_SHAREDCOLLECTIONMAP],where_condition=cond_map)
        select_union =  SQLConcatExpr(sql_coll, OP_UNION_ALL, sql_map)
        sql_main = SQLConcatExpr(SQLConcatExpr(STMT_SELECT,cond_main),SQLBinaryExpr("",EXPR_FROM, (select_union)))
        # return self.raw_sql_excute(str(sql_main),sql_param)[0][0]
        return self.sql(str(sql_main), sql_param)[0][0]

    def get_recpane_data(self, coll_id):

        file_dur = SQLBinaryExpr(SQLBinaryExpr("NVL(%s, 0)" % (COL_NAME_FILES_ENDTIMESTAMP), OP_SUB,
                                                       "NVL(%s, 0)" % (COL_NAME_FILES_BEGINTIMESTAMP)), OP_MUL,
                                         0.000001)
        cf_columns = [COL_NAME_FILES_RECFILEID, COL_NAME_FILES_FILEPATH, COL_NAME_FILES_IMPORTBY,
                      COL_NAME_FILES_IMPORTDATE,
                      COL_NAME_FILES_MEASID, COL_NAME_FILES_RECDRIVENDIST, COL_NAME_FILES_FILESIZE,
                      COL_NAME_FILES_ARCHIVED,
                      COL_NAME_FILES_BEGINTIMESTAMP, COL_NAME_FILES_ENDTIMESTAMP,COL_NAME_FILES_LOCATION, file_dur]
        gen_stmt = self.get_rec_data(coll_id, cf_columns)
        return gen_stmt

    def get_compar_rec_data(self, coll_id):

        cf_columns = [COL_NAME_FILES_RECFILEID, COL_NAME_FILES_ARCHIVED, COL_NAME_FILES_FILEPATH]
        gen_stmt = self.get_rec_data(coll_id, cf_columns)
        return gen_stmt

    def get_rec_data(self, collid, cf_columns):
        """
        Function to build the query for data to be shown in recording pane
        new / not part
        :param collid: Collection ID
        :param columns: List of columns from CAT_Files table
        :return: Sql statement

        :author:        Mohammed Tanveer
        :date:          05/04/2016
        """

        sql_param = {}
        sql_param[str(len(sql_param) + 1)] = collid
        recurs, _ = self._get_collections_tree_query(":%s" % (len(sql_param)))
        recur_sql = GenericSQLSelect([COL_NAME_COLL_COLLID], False, [GEN_RECUR_NAME])
        tree_sql = str(SQLConcatExpr(recurs, recur_sql))
        cond1 = self._get_union_shared_collection_cond(tree_sql, sql_param)
        cmap_tbl = str(GenericSQLSelect([COL_NAME_COLLMAP_MEASID], True, [TABLE_NAME_COLLMAP], cond1))
        if self.db_type[0] == 0:
            cond = SQLBinaryExpr(COL_NAME_FILES_MEASID, OP_IN, "(%s))" % (cmap_tbl))
        else:
            cond = SQLBinaryExpr(COL_NAME_FILES_MEASID, OP_IN, "(%s)" % (cmap_tbl))
        stmt = str(GenericSQLSelect(cf_columns, False, [TABLE_NAME_FILES], cond))
        return stmt

    def linking_qry_sec(self, file_path, collid):
        """
        This function will help differentiate if the files are present in CAT_FILES table and also in collection
        Will return nothing if the file does not exist in database.
        Will return two or more results if it is already linked to the collection
        Args:
            file_path: File path from BPL/ string
            collid: Collection ID/ Integer

        Returns: measurement ID, begin and end timestamp, factors T1 and T2

        :author:        Mohammed Tanveer
        :date:          04/08/2016
        """

        FACTOR_T1 = ''''T1' FACTOR'''
        FACTOR_T2 = ''''T2' FACTOR'''
        cond = None
        sql_param = {}
        cond_fpath = None
        if file_path is not None:
            file_path = file_path.lower()
            file_name = path.basename(file_path)
            file_path, pid = self._convert_filepath(file_path)
            # print "file_path, pid > ", file_path, pid
            if pid is not None:
                sql_param[str(len(sql_param) + 1)] = file_path
                # make exact filepath lower case SQL condition
                cond_fpath = SQLBinaryExpr(SQLFuncExpr(self.db_func_map[DB_FUNC_NAME_LOWER],
                                                       COL_NAME_FILES_FILEPATH),
                                           OP_EQ, ":%d" % (len(sql_param)))
                sql_param[str(len(sql_param) + 1)] = pid
                cond_fpath = SQLBinaryExpr(cond_fpath, OP_AND, SQLBinaryExpr(COL_NAME_FILES_PID,
                                                                             OP_EQ, ":%d" % (len(sql_param))))
            else:
                if file_path[:2] == r"\\":
                    _, file_path = splitunc(file_path)

                else:
                    _, file_path = path.splitdrive(file_path)

                file_path = "%%%s" % file_path
                sql_param[str(len(sql_param) + 1)] = file_path
                cond_fpath = SQLBinaryExpr(SQLFuncExpr(self.db_func_map[DB_FUNC_NAME_LOWER],
                                                       COL_NAME_FILES_FILEPATH),
                                           OP_LIKE, ":%d" % (len(sql_param)))
            if cond is None:
                cond = cond_fpath
            else:
                cond = SQLBinaryExpr(cond, OP_AND, cond_fpath)
        if file_name is not None:
            file_name = file_name.lower()
            sql_param[str(len(sql_param) + 1)] = file_name
            if cond is None:
                cond = SQLBinaryExpr(COL_NAME_FILES_RECFILEID, OP_EQ, ":%d" % (len(sql_param)))
            else:
                cond = SQLBinaryExpr(cond, OP_AND, SQLBinaryExpr(COL_NAME_FILES_RECFILEID,
                                                                 OP_EQ, ":%d" % (len(sql_param))))

        sql1_union = GenericSQLSelect([COL_NAME_FILES_MEASID, FACTOR_T1, COL_NAME_FILES_BEGINTIMESTAMP,
                                       COL_NAME_FILES_ENDTIMESTAMP],
                                      False, [TABLE_NAME_FILES], cond)
        cmap_tbl_alias = "cmap"
        cf_tbl_alias = "cf"
        cf_tbl = SQLTableExpr(TABLE_NAME_FILES, cf_tbl_alias)
        cmap_tbl = SQLTableExpr(TABLE_NAME_COLLMAP, cmap_tbl_alias)

        join = SQLJoinExpr(cf_tbl, OP_INNER_JOIN, cmap_tbl,
                           SQLBinaryExpr(SQLColumnExpr(cmap_tbl_alias, COL_NAME_COLLMAP_MEASID), OP_EQ,
                                         SQLColumnExpr(cf_tbl_alias, COL_NAME_FILES_MEASID)))
        sql_param[str(len(sql_param) + 1)] = collid
        columns = [SQLColumnExpr(cmap_tbl_alias, COL_NAME_COLLMAP_MEASID)]
        columns.append(FACTOR_T2)
        columns.append(SQLColumnExpr(cmap_tbl_alias, COL_NAME_COLLMAP_BEGINTIMESTAMP))
        columns.append(SQLColumnExpr(cmap_tbl_alias, COL_NAME_COLLMAP_ENDTIMESTAMP))
        two_tbl_cond = SQLBinaryExpr(cond, OP_AND, SQLBinaryExpr(COL_NAME_COLLMAP_COLLID,
                                                                 OP_EQ, ":%d" % (len(sql_param))))
        sql2_union = GenericSQLSelect(columns, False, [join], two_tbl_cond)
        stmt = "%s UNION %s" % (sql1_union, sql2_union)
        # print "stmt >> ", stmt, sql_param
        return self.sql(stmt, sql_param)
        # return self.execute(stmt, sql_param)


    # def export_bpl_measurment(self, measid_list, output_path):
    #     """
    #     Export BPL file for the selected measurement Ids
    #
    #     :param measid_list: list of measurement Ids
    #     :type measid_list: list
    #     :param output_path: path to output BPL file
    #     :type output_path: string
    #     """
    #     bplfile = Bpl(output_path)
    #     for measid in measid_list:
    #         # print "measid : ",measid
    #         filepath = self.get_rec_file_name_for_measid(measid)
    #         bplfile.append(BplListEntry(filepath))
    #     bplfile.write()

    # def export_bpl_measurment(self,item_id, selected_measids_dict, output_path, inc_section=False, relativets=True):
    #     """
    #     Export BPL file for the selected measurement Ids
    #
    #     :param measid_list: list of measurement Ids
    #     :type measid_list: list
    #     :param output_path: path to output BPL file
    #     :type output_path: string
    #     :param inc_section: flag to include section information if avaiable, default: don't include
    #     :type inc_section: bool
    #     :param relativets: Boolean flag to specify whether section timestamps are relative or absolute
    #                        default is Relative. Set it to false if sections are based absolute timestamp
    #     :type relativets: bool
    #     """
    #     bplfile = Bpl(output_path)
    #     for measid in selected_measids_dict:
    #         # resdict = self.get_measurement(measid=measid, select_list=[COL_NAME_FILES_FILEPATH])
    #         bplentry = BplListEntry(selected_measids_dict[measid])
    #         bplfile.append(bplentry)
    #
    #         if inc_section:
    #             cat_meas, sec_data = self.get_measurement_with_sections(measid, item_id)
    #             for section in sec_data:
    #                 if relativets:
    #                     # add relative timestamps
    #                     bplentry.append(int(section['startTime']), int(section['endTime']), True)
    #                 else:
    #                     # calculate absolute timestamps
    #                     bplentry.append(cat_meas[COL_NAME_FILES_BEGINTIMESTAMP] + int(section['startTime']),
    #                                     cat_meas[COL_NAME_FILES_BEGINTIMESTAMP] + int(section['endTime']),
    #                                     False)
    #
    #     bplfile.write()

#     def export_bpl_for_collection(self, coll_name, output_path,  # pylint: disable=W0102,R0912,R0913,R0914,R0915
#                                   recurse=True, inc_section=False, relativets=True, orderby=[]):
#         """
#         Export BPL file for the given collection based on priority. If no priority is assigned
#         then default priority level NORMAL will be used. For backward compatibility to older subschema
#         all the collections have NORMAL priority
#
#         :param coll_name: Collection name
#         :type coll_name: string
#         :param output_path: path to output BPL FILE
#         :type output_path: string
#         :param recurse: Boolean flag to include child collection
#         :type recurse: bool
#         :param inc_section: Boolean flag to include Sections in BPL default is False i.e. dont include
#         :type inc_section: bool
#         :param relativets: Boolean flag to specify whether section timestamps are relative or absolute
#                            default is Relative. Set it to false if sections are based absolute timestamp
#         :type relativets: bool
#         :param orderby: give it an order, dude!
#         :type  orderby: list
#         """
#         bplfile = Bpl(output_path)
#         cmap_tbl_alias = "cmap"
#         cf_tbl_alias = "cf"
#         coll_tbl_alias = "coll"
#         cf_tbl = SQLTableExpr(TABLE_NAME_FILES, cf_tbl_alias)
#         coll_tbl = SQLTableExpr(TABLE_NAME_COLL, coll_tbl_alias)
#         # collid = self.get_collection_id(coll_name)
#         collid = coll_name
#         default_prid = GenericSQLSelect([COL_NAME_PRIORITIES_PRID], False, [TABLE_NAME_PRIORITIES],
#                                         SQLBinaryExpr(COL_NAME_PRIORITIES_NAME, OP_EQ, SQLLiteral(PRIORITY_NORMAL)))
#         default_prid = self.execute(str(default_prid))[0][0]
#         fpath_exports = {}
#         sql_param = {"1": collid}
#         select_list = []
#         if recurse and collid is not None:
#             recurs, _ = self._get_collections_tree_query(":%s" % (len(sql_param)))
#             recur_sql = GenericSQLSelect([COL_NAME_COLL_COLLID], False, [GEN_RECUR_NAME])
#             tree_sql = str(SQLConcatExpr(recurs, recur_sql))
#             cond = self._get_union_shared_collection_cond(tree_sql, sql_param)
#             col_list = [COL_NAME_COLLMAP_COLLID, COL_NAME_COLLMAP_MEASID]
#             if inc_section:
#                 col_list = col_list + [COL_NAME_COLLMAP_BEGINTIMESTAMP, COL_NAME_COLLMAP_ENDTIMESTAMP]
#             cmap_tbl = SQLTableExpr("(%s)" % str(GenericSQLSelect(col_list, False,
#                                                                   [TABLE_NAME_COLLMAP], cond)), cmap_tbl_alias)
#             join1 = SQLJoinExpr(coll_tbl, OP_INNER_JOIN, cmap_tbl,
#                                 SQLBinaryExpr(SQLColumnExpr(coll_tbl_alias, COL_NAME_COLL_COLLID), OP_EQ,
#                                               SQLColumnExpr(cmap_tbl_alias, COL_NAME_COLLMAP_COLLID)))
#             col_prid_collect = SQLColumnExpr(coll_tbl_alias, COL_NAME_COLL_PRID)
#             col_collid = SQLColumnExpr(coll_tbl_alias, COL_NAME_COLL_COLLID)
#             select_list.append(SQLBinaryExpr("NVL(%s, %d)" % (col_prid_collect, default_prid),
#                                              OP_AS, COL_NAME_COLL_PRID))
#             cond = None
#         else:
#             join1 = SQLTableExpr(TABLE_NAME_COLLMAP, cmap_tbl_alias)
#             col_collid = SQLColumnExpr(cmap_tbl_alias, COL_NAME_COLL_COLLID)
#             cond = SQLBinaryExpr(SQLColumnExpr(cmap_tbl_alias, COL_NAME_COLLMAP_COLLID),
#                                  OP_EQ, ":%d" % (len(sql_param)))
#
#         col_measid_catfiles = SQLColumnExpr(cf_tbl_alias, COL_NAME_FILES_MEASID)
#         col_filepath_catfiles = SQLColumnExpr(cf_tbl_alias, COL_NAME_FILES_FILEPATH)
#         col_sectbegin_collmap = SQLColumnExpr(cmap_tbl_alias, COL_NAME_COLLMAP_BEGINTIMESTAMP)
#         col_sectend_collmap = SQLColumnExpr(cmap_tbl_alias, COL_NAME_COLLMAP_ENDTIMESTAMP)
#         select_list.append(SQLBinaryExpr(col_filepath_catfiles, OP_AS, COL_NAME_FILES_FILEPATH))
#         select_list.append(SQLBinaryExpr(col_collid, OP_AS, COL_NAME_COLLMAP_COLLID))
#         select_list.append(SQLBinaryExpr(col_measid_catfiles, OP_AS, COL_NAME_COLLMAP_MEASID))
#
#         if inc_section:
#             if relativets:
#                 select_list.append(SQLBinaryExpr(col_sectbegin_collmap, OP_AS, COL_NAME_COLLMAP_BEGINTIMESTAMP))
#                 select_list.append(SQLBinaryExpr(col_sectend_collmap, OP_AS, COL_NAME_COLLMAP_ENDTIMESTAMP))
#             else:
#                 col_beginabsts_catfiles = SQLColumnExpr(cf_tbl_alias, COL_NAME_FILES_BEGINTIMESTAMP)
# #                 col_endabsts_catfiles = SQLColumnExpr(cf_tbl_alias, COL_NAME_FILES_ENDTIMESTAMP)
#                 col_sectbeginabsts_collmap = SQLBinaryExpr(col_beginabsts_catfiles, OP_ADD, col_sectbegin_collmap)
#                 col_sectendabsts_collmap = SQLBinaryExpr(col_beginabsts_catfiles, OP_ADD, col_sectend_collmap)
#                 select_list.append(SQLBinaryExpr(col_sectbeginabsts_collmap, OP_AS, COL_NAME_COLLMAP_BEGINTIMESTAMP))
#                 select_list.append(SQLBinaryExpr(col_sectendabsts_collmap, OP_AS, COL_NAME_COLLMAP_ENDTIMESTAMP))
#         if len(orderby) == 0 and recurse:
#             orderby.append(COL_NAME_COLL_PRID)
#
#         join2 = SQLJoinExpr(SQLTableExpr(join1), OP_INNER_JOIN, cf_tbl,
#                             SQLBinaryExpr(SQLColumnExpr(cmap_tbl_alias, COL_NAME_COLLMAP_MEASID), OP_EQ,
#                                           SQLColumnExpr(cf_tbl_alias, COL_NAME_FILES_MEASID)))
#         # print "export single -- ", sql_param
#         records = self.select_generic_data_compact(select_list=select_list, table_list=[join2],
#                                                    where=cond, order_by=orderby, sqlparams=sql_param)
#         col_list = records[0]
#         records = records[1]
#
#         for rec in records:
#             entry = dict(zip(col_list, rec))
#             if entry[COL_NAME_FILES_FILEPATH] not in fpath_exports:
#                 fpath_exports[entry[COL_NAME_FILES_FILEPATH]] = []
#             if inc_section:
#                 begints = entry[COL_NAME_COLLMAP_BEGINTIMESTAMP]
#                 endts = entry[COL_NAME_COLLMAP_ENDTIMESTAMP]
#                 if begints is not None and endts is not None:
#                     fpath_exports[entry[COL_NAME_FILES_FILEPATH]].append([begints, endts, relativets])
#         for recfile in fpath_exports:
#             bplentry = BplListEntry(recfile)
#             for section in fpath_exports[recfile]:
#                 bplentry.append(section[0], section[1], section[2])
#             bplfile.append(bplentry)
#         bplfile.write()
#         return bplfile
#
#     def get_file_details_for_export(self, coll_name,
#                                   recurse=True, inc_section=False, relativets=True, orderby=[]):
#         """
#         Get the files for the given collection based on priority. If no priority is assigned
#         then default priority level NORMAL will be used. For backward compatibility to older subschema
#         all the collections have NORMAL priority
#
#         :param coll_name: Collection name
#         :type coll_name: string
#         :param recurse: Boolean flag to include child collection
#         :type recurse: bool
#         :param inc_section: Boolean flag to include Sections in BPL default is False i.e. dont include
#         :type inc_section: bool
#         :param relativets: Boolean flag to specify whether section timestamps are relative or absolute
#                            default is Relative. Set it to false if sections are based absolute timestamp
#         :type relativets: bool
#         :param orderby: give it an order, dude!
#         :type  orderby: list
#
#         :author:        Mohammed Tanveer
#         :date:          11/05/2016
#         """
#
#         cmap_tbl_alias = "cmap"
#         cf_tbl_alias = "cf"
#         coll_tbl_alias = "coll"
#         cf_tbl = SQLTableExpr(TABLE_NAME_FILES, cf_tbl_alias)
#         coll_tbl = SQLTableExpr(TABLE_NAME_COLL, coll_tbl_alias)
#         # collid = self.get_collection_id(coll_name)
#         collid = coll_name
#         default_prid = GenericSQLSelect([COL_NAME_PRIORITIES_PRID], False, [TABLE_NAME_PRIORITIES],
#                                         SQLBinaryExpr(COL_NAME_PRIORITIES_NAME, OP_EQ, SQLLiteral(PRIORITY_NORMAL)))
#         default_prid = self.execute(str(default_prid))[0][0]
#         fpath_exports = {}
#         sql_param = {"1": collid}
#         # print "sql_param 1 : ", sql_param
#         select_list = []
#         if recurse and collid is not None:
#             recurs, _ = self._get_collections_tree_query(":%s" % (len(sql_param)))
#             # print "sql_param 1.1 : ", sql_param
#             recur_sql = GenericSQLSelect([COL_NAME_COLL_COLLID], False, [GEN_RECUR_NAME])
#             tree_sql = str(SQLConcatExpr(recurs, recur_sql))
#             cond = self._get_union_shared_collection_cond(tree_sql, sql_param)
#             # print "sql_param 1.2 : ", sql_param
#             col_list = [COL_NAME_COLLMAP_COLLID, COL_NAME_COLLMAP_MEASID]
#             if inc_section:
#                 col_list = col_list + [COL_NAME_COLLMAP_BEGINTIMESTAMP, COL_NAME_COLLMAP_ENDTIMESTAMP]
#             cmap_tbl = SQLTableExpr("(%s)" % str(GenericSQLSelect(col_list, False,
#                                                                   [TABLE_NAME_COLLMAP], cond)), cmap_tbl_alias)
#             join1 = SQLJoinExpr(coll_tbl, OP_INNER_JOIN, cmap_tbl,
#                                 SQLBinaryExpr(SQLColumnExpr(coll_tbl_alias, COL_NAME_COLL_COLLID), OP_EQ,
#                                               SQLColumnExpr(cmap_tbl_alias, COL_NAME_COLLMAP_COLLID)))
#             col_prid_collect = SQLColumnExpr(coll_tbl_alias, COL_NAME_COLL_PRID)
#             col_collid = SQLColumnExpr(coll_tbl_alias, COL_NAME_COLL_COLLID)
#             select_list.append(SQLBinaryExpr("NVL(%s, %d)" % (col_prid_collect, default_prid),
#                                              OP_AS, COL_NAME_COLL_PRID))
#             cond = None
#         else:
#             join1 = SQLTableExpr(TABLE_NAME_COLLMAP, cmap_tbl_alias)
#             col_collid = SQLColumnExpr(cmap_tbl_alias, COL_NAME_COLL_COLLID)
#             cond = SQLBinaryExpr(SQLColumnExpr(cmap_tbl_alias, COL_NAME_COLLMAP_COLLID),
#                                  OP_EQ, ":%d" % (len(sql_param)))
#         # print "sql_param 2 : ", sql_param
#         col_measid_catfiles = SQLColumnExpr(cf_tbl_alias, COL_NAME_FILES_MEASID)
#         col_filepath_catfiles = SQLColumnExpr(cf_tbl_alias, COL_NAME_FILES_FILEPATH)
#         col_sectbegin_collmap = SQLColumnExpr(cmap_tbl_alias, COL_NAME_COLLMAP_BEGINTIMESTAMP)
#         col_sectend_collmap = SQLColumnExpr(cmap_tbl_alias, COL_NAME_COLLMAP_ENDTIMESTAMP)
#         select_list.append(SQLBinaryExpr(col_filepath_catfiles, OP_AS, COL_NAME_FILES_FILEPATH))
#         select_list.append(SQLBinaryExpr(col_collid, OP_AS, COL_NAME_COLLMAP_COLLID))
#         select_list.append(SQLBinaryExpr(col_measid_catfiles, OP_AS, COL_NAME_COLLMAP_MEASID))
#
#         if inc_section:
#             if relativets:
#                 select_list.append(SQLBinaryExpr(col_sectbegin_collmap, OP_AS, COL_NAME_COLLMAP_BEGINTIMESTAMP))
#                 select_list.append(SQLBinaryExpr(col_sectend_collmap, OP_AS, COL_NAME_COLLMAP_ENDTIMESTAMP))
#             else:
#                 col_beginabsts_catfiles = SQLColumnExpr(cf_tbl_alias, COL_NAME_FILES_BEGINTIMESTAMP)
# #                 col_endabsts_catfiles = SQLColumnExpr(cf_tbl_alias, COL_NAME_FILES_ENDTIMESTAMP)
#                 col_sectbeginabsts_collmap = SQLBinaryExpr(col_beginabsts_catfiles, OP_ADD, col_sectbegin_collmap)
#                 col_sectendabsts_collmap = SQLBinaryExpr(col_beginabsts_catfiles, OP_ADD, col_sectend_collmap)
#                 select_list.append(SQLBinaryExpr(col_sectbeginabsts_collmap, OP_AS, COL_NAME_COLLMAP_BEGINTIMESTAMP))
#                 select_list.append(SQLBinaryExpr(col_sectendabsts_collmap, OP_AS, COL_NAME_COLLMAP_ENDTIMESTAMP))
#         if len(orderby) == 0 and recurse:
#             orderby.append(COL_NAME_COLL_PRID)
#
#         join2 = SQLJoinExpr(SQLTableExpr(join1), OP_INNER_JOIN, cf_tbl,
#                             SQLBinaryExpr(SQLColumnExpr(cmap_tbl_alias, COL_NAME_COLLMAP_MEASID), OP_EQ,
#                                           SQLColumnExpr(cf_tbl_alias, COL_NAME_FILES_MEASID)))
#         # print "export --> ", sql_param
#         records = self.select_generic_data_compact(select_list=select_list, table_list=[join2],
#                                                    where=cond, order_by=orderby, sqlparams=sql_param)
#         col_list = records[0]
#         records = records[1]
#
#         for rec in records:
#             entry = dict(zip(col_list, rec))
#             if entry[COL_NAME_FILES_FILEPATH] not in fpath_exports:
#                 fpath_exports[entry[COL_NAME_FILES_FILEPATH]] = []
#             if inc_section:
#                 begints = entry[COL_NAME_COLLMAP_BEGINTIMESTAMP]
#                 endts = entry[COL_NAME_COLLMAP_ENDTIMESTAMP]
#                 if begints is not None and endts is not None:
#                     fpath_exports[entry[COL_NAME_FILES_FILEPATH]].append([begints, endts, relativets])
#
#         return fpath_exports

    def get_filepath_of_collections(self,collids_list,recurse = True):
        """

        Args:
            collids_list: list of collection IDs
            recurse: Recursive to go deep in the tree

        Returns: list of file paths in a tuple

        :author:        Mohammed Tanveer
        :date:          05/04/2016
        """

        if recurse:
            if len(collids_list) == 1:
                collid = str(tuple(collids_list))
                collid_tup = collid[:len(collid)-2]+collid[-1]
            else:
                collid_tup = str(tuple(collids_list))
            stmt = '''SELECT FILEPATH
                    FROM CAT_FILES WHERE MEASID IN
                    (SELECT MEASID FROM CAT_COLLECTIONMAP WHERE COLLID IN {})'''.format(collid_tup)
            records = self.sql(stmt)
            return records

    # def import_bpl_to_collection(self, bplfilepath, coll_name, inc_section=False):
    #     """
    #     import BPL file into given Collection Name
    #
    #     :param bplfilepath: path/filename of bpl file to import
    #     :type  bplfilepath: str
    #     :param coll_name:   collection name to link recordings to
    #     :type  coll_name:   str
    #     :param inc_section: opt. include defined sections into collection entry
    #     :type inc_section:  bool
    #     """
    #     bpl_file = Bpl(bplfilepath)
    #     bpl_file.read()
    #     collid = self.get_collection_id(coll_name)
    #     for bpl_list_entry in bpl_file.get_bpl_list_entries():
    #         measid = str(int(self.get_measurement_id(str(bpl_list_entry))))
    #         collmap_record = {COL_NAME_COLLMAP_COLLID: collid, COL_NAME_COLLMAP_MEASID: measid,
    #                           COL_NAME_COLLMAP_ASGNBY: environ["USERNAME"],
    #                           COL_NAME_COLLMAP_ASGNDATE: self.timestamp_to_date(time())}
    #         if inc_section:
    #             file_record = self.get_measurement(measid=measid)
    #             for section in bpl_list_entry.get_sections():
    #                 start_ts, end_ts, rel = section.sect2list()
    #                 # if the timstamp are not relative then make it relative because db stores sections as relative
    #                 if not rel:
    #                     start_ts = start_ts - file_record[COL_NAME_FILES_BEGINTIMESTAMP]
    #                     end_ts = end_ts - file_record[COL_NAME_FILES_BEGINTIMESTAMP]
    #                 collmap_record[COL_NAME_COLLMAP_BEGINTIMESTAMP] = start_ts
    #                 collmap_record[COL_NAME_COLLMAP_ENDTIMESTAMP] = end_ts
    #                 self.add_collection_map(collmap_record)
    #         else:
    #             self.add_collection_map(collmap_record)
    #     return True

    def add_bplentry_to_collection(self, collid, bpl_list_entry, inc_section):
        """
        add a single bpl entry to a given collection

        :param collid: db internal collection id
        :type  collid: int
        :param bpl_list_entry: bpl entry with rec file name and sections
        :type  bpl_list_entry: BplListEntry
        :param inc_section:  opt. include defined sections into collection entry
        :type  inc_section:  bool
        """
        measid = str(int(self.get_measurement_id(str(bpl_list_entry))))
        collmap_record = {COL_NAME_COLLMAP_COLLID: collid, COL_NAME_COLLMAP_MEASID: measid,
                          COL_NAME_COLLMAP_ASGNBY: environ["USERNAME"],
                          COL_NAME_COLLMAP_ASGNDATE: self.timestamp_to_date(time())}
        if inc_section and bpl_list_entry.has_sections():
            # get absolute start time stamp, only first element of returned list is used
            file_record = self.get_measurement(measid=measid)
            for section in bpl_list_entry.get_sections():
                start_ts, end_ts, rel = section.sect2list()
                # if the timstamp are not relative then make it relative because db stores sections as relative
                if not rel:
                    start_ts = start_ts - file_record[0][COL_NAME_FILES_BEGINTIMESTAMP]
                    end_ts = end_ts - file_record[0][COL_NAME_FILES_BEGINTIMESTAMP]
                collmap_record[COL_NAME_COLLMAP_BEGINTIMESTAMP] = start_ts
                collmap_record[COL_NAME_COLLMAP_ENDTIMESTAMP] = end_ts
                self.add_collection_map(collmap_record)
        else:
            self.add_collection_map(collmap_record)

    def get_rec_file_name_for_measid(self, measid):
        """
        Returns the filepath for a measid

        :param measid: the measurement id
        :type measid: int
        :return: Returns the corresponding filepath
        :rtype: str
        """
        columns = [COL_NAME_FILES_FILEPATH]
        measid_cond = SQLBinaryExpr(COL_NAME_FILES_MEASID, OP_EQ, measid)
        return self.select_generic_data(select_list=columns,
                                        table_list=[TABLE_NAME_FILES],
                                        where=measid_cond)[0][COL_NAME_FILES_FILEPATH]

    def _get_measurement_collection_names(self, file_path):  # pylint: disable=C0103
        """
        Get the Collection Names of a measurement.

        :param file_path: The file_path of the measurement.
        :type file_path: str
        :return: Returns the Collection Names to which the measurement is
                assigned, if file is not assigned return empty list.
        :rtype: list
        """
        measid = self.get_measurement_id(file_path)
        collectionnames = []

        tblcoll = TABLE_NAME_COLL
        tblcollmap = TABLE_NAME_COLLMAP
        columns = [SQLBinaryExpr(SQLColumnExpr(SQLTableExpr(tblcoll), COL_NAME_COLL_NAME), OP_AS, COL_NAME_COLL_NAME)]

        collmapcolljoin = SQLJoinExpr(SQLTableExpr(TABLE_NAME_COLLMAP),
                                      OP_INNER_JOIN,
                                      SQLTableExpr(TABLE_NAME_COLL),
                                      SQLBinaryExpr(SQLColumnExpr(SQLTableExpr(tblcollmap), COL_NAME_COLLMAP_COLLID),
                                                    OP_EQ, SQLColumnExpr(SQLTableExpr(tblcoll), COL_NAME_COLL_COLLID)))

        cond = SQLBinaryExpr(SQLColumnExpr(SQLTableExpr(tblcollmap), COL_NAME_COLLMAP_MEASID), OP_EQ, ":1")
        entries = self.select_generic_data(columns, [collmapcolljoin], cond, sqlparams={"1": measid})

        if len(entries) >= 1:
            for ent in range(len(entries)):
                collectionnames.append(entries[ent][COL_NAME_COLL_NAME])
        else:
            info("Rec file '" + file_path + "' is not assigned to a collection")

        return collectionnames

    def get_measurement_collection_names(self, file_path, coll_name=None):  # pylint: disable=C0103
        """
        Get list of collection name to which the given rec file path belong
        in the given pre select collection

        :param file_path: The file_path of the measurement.
        :type file_path: str
        :param coll_name: list of collection Name or Parent Collection Name as filter criteria
        :type coll_name: list or str
        :return: List of Collection name to which the given measurement belongs
        :rtype: list
        """
        coll_names = []
        if coll_name is not None:

            if type(coll_name) is list:
                collids = []
                for c_name in coll_name:
                    collids.append(self.get_collection_id(c_name))
            else:
                parent_collid = self.get_collection_id(coll_name)
                collids = self.get_collections(parent_collid, recurse=True)
                collids.append(parent_collid)
            collids = list(set(collids))
            for collid in collids:
                if self.is_measurement_in_collection(collid, measid=file_path, file_path=None):
                    coll_names.append(self.get_collection(collid)[COL_NAME_COLL_NAME])
            return coll_names

        else:
            return self._get_measurement_collection_names(file_path)

    def is_measurement_in_collection(self, collid, measid=None, file_path=None):
        """
        Tells whether the given measurement is in the  collection.
        The function doesnt recursively for the child collections

        :param collid: Collection Id
        :type collid: int
        :param measid: Meaurement ID
        :type measid: int | None
        :param file_path: filepath of recording
        :type file_path: str | None
        :return: return True if meas belongs to collection othersie return False
        :rtype: bool
        """
        if measid is not None or file_path is not None:
            if measid is None:
                measid = self.get_measurement_id(file_path)
            sql_param = {"1": measid, "2": collid}
            cond1 = SQLBinaryExpr(COL_NAME_COLLMAP_MEASID, OP_EQ, ":1")
            cond2 = SQLBinaryExpr(COL_NAME_COLLMAP_COLLID, OP_EQ, ":2")
            cond = SQLBinaryExpr(cond1, OP_AND, cond2)
            entries = self.select_generic_data([SQLBinaryExpr(SQLFuncExpr(EXPR_COUNT, OP_MUL), OP_AS, "COUNT")],
                                               table_list=[TABLE_NAME_COLLMAP], where=cond, sqlparams=sql_param)
            if entries[0]["COUNT"] > 0:
                return True
        return False

    def get_collnames_for_measid_within_coll(self, collid, measid):
        """
        Function to get the collection names associated with the recording file within the same tree
        Args:
            collid: Collection ID, Integer
            measid: Measurement ID, Integer

        Returns: A list containing collection names

        :author:        Mohammed Tanveer
        :date:          08/06/2016
        """

        sql_param = {}
        if collid is not None:
            sql_param[str(len(sql_param) + 1)] = collid
        recurs, _ = self._get_collections_tree_query(":%s" % (len(sql_param)))
        recur_sql = GenericSQLSelect([COL_NAME_COLL_COLLID], False, [GEN_RECUR_NAME])
        tree_sql = str(SQLConcatExpr(recurs, recur_sql))
        collid_sql = GenericSQLSelect([COL_NAME_COLL_COLLID], False, [TABLE_NAME_COLL],
                                      SQLBinaryExpr(COL_NAME_COLL_COLLID, OP_IN, "(%s)"%tree_sql))
        if measid is not None:
            sql_param[str(len(sql_param) + 1)] = measid
        collmap_sql = GenericSQLSelect([COL_NAME_COLLMAP_COLLID], False, [TABLE_NAME_COLLMAP],
                                                         SQLBinaryExpr(COL_NAME_COLLMAP_MEASID, OP_EQ, ":%d"%(len(sql_param))))
        comp_sql = str(collmap_sql) + " INTERSECT " + str(collid_sql)
        data = self.select_generic_data([COL_NAME_COLL_NAME], [TABLE_NAME_COLL],
                                                    SQLBinaryExpr(COL_NAME_COLL_COLLID, OP_IN, "(%s)"%comp_sql), sqlparams=sql_param)
        data = [element[COL_NAME_COLL_NAME] for element in data]
        return data

    def get_collection_hours_kilometer_for_part_config_version(self, collection_name,  # pylint: disable=C0103,R0914
                                                               version_type):
        """
        Get hours for Part Configuration version.

        :param collection_name: The name of the collection,
        :type collection_name: str
        :param version_type: HW oder SW version
        :type version_type: str
        :return: return list of list with following format
                [[version string, time in us, number of files, kilometer],...]
        :rtype: list
        """
        collection_id = self.get_collection_id(collection_name)

        rec, sql = self._build_collection_query(collection_id, True, COL_NAME_COLL_COLLID)
        join = SQLTernaryExpr(TABLE_NAME_FILES, OP_INNER_JOIN, TABLE_NAME_COLLMAP, OP_NOP,
                              SQLFuncExpr(OP_USING, COL_NAME_FILES_MEASID))
        cond = SQLBinaryExpr(COL_NAME_COLL_COLLID, OP_IN, sql)
        sql = GenericSQLSelect([COL_NAME_COLLMAP_MEASID], True, [join], cond)
        cond = SQLBinaryExpr(SQLColumnExpr(SQLTableExpr(TABLE_NAME_FILES), COL_NAME_FILES_MEASID),
                             OP_IN, SQLConcatExpr(rec, sql))

        version_time_files_dict = {}
        time_diff = SQLBinaryExpr("NVL(%s, 0)" % (str(SQLColumnExpr(SQLTableExpr(TABLE_NAME_FILES),
                                                                    COL_NAME_FILES_ENDTIMESTAMP))),
                                  OP_SUB,
                                  "NVL(%s, 0)" % (str(SQLColumnExpr(SQLTableExpr(TABLE_NAME_FILES),
                                                                    COL_NAME_FILES_BEGINTIMESTAMP))))
        time_diff = SQLBinaryExpr(time_diff, OP_AS, "DURATION")

        dist_km = SQLColumnExpr(SQLTableExpr(TABLE_NAME_FILES), COL_NAME_FILES_RECDRIVENDIST)
        dist_km = "NVL(%s, 0)" % (str(dist_km))
        dist_km = SQLBinaryExpr(dist_km, OP_AS, COL_NAME_FILES_RECDRIVENDIST)
        select_list = [SQLColumnExpr(SQLTableExpr(TABLE_NAME_PARTCFGS), version_type), dist_km, time_diff]

        tables = []
        join_cond_col1 = SQLColumnExpr(SQLTableExpr(TABLE_NAME_FILES), COL_NAME_FILES_VEHICLECFGID)
        join_cond_col2 = SQLColumnExpr(SQLTableExpr(TABLE_NAME_PARTCFGS), COL_NAME_PARTCFGS_VEHCFGID)
        join_cond = SQLBinaryExpr(join_cond_col1, OP_EQ, join_cond_col2)

        first_join = SQLJoinExpr(SQLTableExpr(TABLE_NAME_FILES), OP_INNER_JOIN,
                                 SQLTableExpr(TABLE_NAME_PARTCFGS), join_cond)
        tables.append(first_join)
        entries = self.select_generic_data(select_list, tables, where=cond)
        for entry in entries:
            if entry[version_type] not in version_time_files_dict:
                version_time_files_dict[entry[version_type]] = [0, 0, 0]

            recdist = entry[COL_NAME_FILES_RECDRIVENDIST]
            version_time_files_dict[entry[version_type]][0] += entry["DURATION"]
            version_time_files_dict[entry[version_type]][1] += 1
            version_time_files_dict[entry[version_type]][2] += recdist if recdist >= 0 else 0

        version_time_files_list = [[ver_type, values[0], values[1], values[2]]
                                   for ver_type, values in version_time_files_dict.iteritems()]
        return version_time_files_list

    def get_collection_hours_kilometer_per_recdate(self, collection_name):  # pylint: disable=C0103,R0914
        """
        Get hours and kilometer per recording date.

        :param collection_name: The collection name.
        :type collection_name: str
        :return: list of tuples where each tuple has forllowing format
                (recording_date, hours, files, kilometer).
        :rtype: list
        """
        recdate_hours_files_list = []
        raw_recdate_hours_files = []

        # Getting the collection ID from the collection name
        collection_id = self.get_collection_id(collection_name)

        # Getting the MEASID's for collection with subcollections
        coll_measid_list = self.get_collection_measurements(collection_id, True, False)
        if len(coll_measid_list) == 0:
            return recdate_hours_files_list

        recdate_list = []

        for measid in coll_measid_list:
            condition = SQLBinaryExpr(COL_NAME_FILES_MEASID, OP_EQ, measid)
            select_list = [COL_NAME_FILES_MEASID, COL_NAME_FILES_RECTIME, COL_NAME_FILES_BEGINTIMESTAMP,
                           COL_NAME_FILES_ENDTIMESTAMP, COL_NAME_FILES_RECDRIVENDIST]
            order_by_list = [COL_NAME_FILES_RECTIME]
            recdate_list.append(self.select_generic_data(select_list, [TABLE_NAME_FILES],
                                                         where=condition, order_by=order_by_list))

        for first_lst in recdate_list:
            for recdate_idx in first_lst:
                recdate = {COL_NAME_FILES_MEASID: recdate_idx[COL_NAME_FILES_MEASID],
                           COL_NAME_FILES_RECTIME: recdate_idx[COL_NAME_FILES_RECTIME],
                           "Timestamp": (recdate_idx[COL_NAME_FILES_ENDTIMESTAMP] -
                                         recdate_idx[COL_NAME_FILES_BEGINTIMESTAMP]),
                           COL_NAME_FILES_RECDRIVENDIST: recdate_idx[COL_NAME_FILES_RECDRIVENDIST]}
                raw_recdate_hours_files.append(recdate)

        for idx in raw_recdate_hours_files:
            new_entry = True
            for recdate_hours_files in recdate_hours_files_list:
                if recdate_hours_files[0] == datetime.date(idx[COL_NAME_FILES_RECTIME]):
                    recdate_hours_files[1] += idx["Timestamp"]
                    recdate_hours_files[2] += 1
                    if idx[COL_NAME_FILES_RECDRIVENDIST] is not None:
                        recdate_hours_files[3] += idx[COL_NAME_FILES_RECDRIVENDIST]
                    new_entry = False
            if new_entry:
                recdate_hours_files_list.append([datetime.date(idx[COL_NAME_FILES_RECTIME]), idx["Timestamp"],
                                                 1, idx[COL_NAME_FILES_RECDRIVENDIST]])

        return recdate_hours_files_list

    def get_collection_hours_kilometer_per_driver(self, collection_name):  # pylint: disable=C0103,R0914
        """
        Get the Driver hours and kilometer for the collection

        :param collection_name: The name of the collection.
        :type collection_name: str
        :return: return list of list with following info [[driver_id, time in us, number of files, kilometer], .., ..]
        :rtype: list
        """
        raw_driver_hours_files = []
        driver_hours_files_list = []
        # Getting the collection ID from the collection name
        collection_id = self.get_collection_id(collection_name)

        # Getting the MEASID's for collection with subcollections
        coll_measid_list = self.get_collection_measurements(collection_id, True, False)
        if len(coll_measid_list) == 0:
            return driver_hours_files_list

        driver_list = []

        for measid in coll_measid_list:
            condition = SQLBinaryExpr(COL_NAME_FILES_MEASID, OP_EQ, measid)
            select_list = [COL_NAME_FILES_MEASID,
                           COL_NAME_FILES_DRIVERID,
                           COL_NAME_FILES_BEGINTIMESTAMP,
                           COL_NAME_FILES_ENDTIMESTAMP,
                           COL_NAME_FILES_RECDRIVENDIST]
            order_by_list = [COL_NAME_FILES_DRIVERID]
            driver_list.append(self.select_generic_data(select_list, [TABLE_NAME_FILES],
                                                        where=condition, order_by=order_by_list))

        for first_lst in driver_list:
            for driver_idx in first_lst:
                driver = {COL_NAME_FILES_MEASID: driver_idx[COL_NAME_FILES_MEASID],
                          COL_NAME_FILES_DRIVERID: driver_idx[COL_NAME_FILES_DRIVERID],
                          "Timestamp": (driver_idx[COL_NAME_FILES_ENDTIMESTAMP] -
                                        driver_idx[COL_NAME_FILES_BEGINTIMESTAMP]),
                          COL_NAME_FILES_RECDRIVENDIST: driver_idx[COL_NAME_FILES_RECDRIVENDIST]}
                raw_driver_hours_files.append(driver)

        for idx in raw_driver_hours_files:
            new_entry = True
            for driver_hours_files in driver_hours_files_list:
                if driver_hours_files[0] == idx[COL_NAME_FILES_DRIVERID]:
                    driver_hours_files[1] += idx["Timestamp"]
                    driver_hours_files[2] += 1
                    if idx[COL_NAME_FILES_RECDRIVENDIST] is not None:
                        driver_hours_files[3] += idx[COL_NAME_FILES_RECDRIVENDIST]
                    new_entry = False
            if new_entry:
                driver_hours_files_list.append([idx[COL_NAME_FILES_DRIVERID], idx["Timestamp"], 1,
                                               idx[COL_NAME_FILES_RECDRIVENDIST]])

        # replace driverid with driver name in driver_hours_files
        for idx in driver_hours_files_list:
            if idx[0] is not None:
                idx[0] = self.get_driver_name(idx[0])

        return driver_hours_files_list

    def get_collection_hours_kilometer_per_vehicle(self, collection_name):  # pylint: disable=C0103,R0914
        """
        Get hours and kilometer per vehicle.

        :param collection_name: The name of the collection.
        :type collection_name: str
        :return:  return list of list with following info
                [[vehicle string, time in us,, number of files, kilometer][,..].[..]]
        :rtype: list
        """
        vehicle_time_files_list = []

        collection_id = self.get_collection_id(collection_name)

        select_list = [SQLColumnExpr(SQLTableExpr(TABLE_NAME_FILES), COL_NAME_FILES_MEASID),
                       SQLColumnExpr(SQLTableExpr(TABLE_NAME_VEHICLES), COL_NAME_VEHICLES_NAME),
                       SQLColumnExpr(SQLTableExpr(TABLE_NAME_FILES), COL_NAME_FILES_BEGINTIMESTAMP),
                       SQLColumnExpr(SQLTableExpr(TABLE_NAME_FILES), COL_NAME_FILES_ENDTIMESTAMP),
                       SQLColumnExpr(SQLTableExpr(TABLE_NAME_FILES), COL_NAME_FILES_RECDRIVENDIST)]

        tables = []
        join_cond_col1 = SQLColumnExpr(SQLTableExpr(TABLE_NAME_FILES), COL_NAME_FILES_VEHICLECFGID)
        join_cond_col2 = SQLColumnExpr(SQLTableExpr(TABLE_NAME_VEHICLECFGS), COL_NAME_VEHICLECFGS_VEHCFGID)
        join_cond = SQLBinaryExpr(join_cond_col1, OP_EQ, join_cond_col2)

        first_join = SQLJoinExpr(SQLTableExpr(TABLE_NAME_FILES), OP_INNER_JOIN,
                                 SQLTableExpr(TABLE_NAME_VEHICLECFGS), join_cond)

        join_cond_col1_3 = SQLColumnExpr(SQLTableExpr(TABLE_NAME_VEHICLECFGS), COL_NAME_VEHICLECFGS_VEHICLEID)
        join_cond_col2_3 = SQLColumnExpr(SQLTableExpr(TABLE_NAME_VEHICLES), COL_NAME_VEHICLES_VEHICLEID)
        join_cond_3 = SQLBinaryExpr(join_cond_col1_3, OP_EQ, join_cond_col2_3)

        tables.append(SQLJoinExpr(SQLTableExpr(first_join), OP_INNER_JOIN,
                                  SQLTableExpr(TABLE_NAME_VEHICLES), join_cond_3))

        # Get the measid's for the collection (with subcollections)
        coll_measid_list = self.get_collection_measurements(collection_id, True, False)
        if len(coll_measid_list) == 0:
            return vehicle_time_files_list

        query = ','.join(str(n) for n in coll_measid_list)
        cond = SQLBinaryExpr(COL_NAME_FILES_MEASID, OP_IN, "%s" % "(" + query + ")")

        try:
            entries = self.select_generic_data(select_list, tables, where=cond)

            # Check for measurement duplicates.
            measurements = []

            for entry in entries:
                if entry[COL_NAME_FILES_MEASID] in measurements:
                    continue
                new_entry = True
                for vehicle_time_files in vehicle_time_files_list:
                    if vehicle_time_files[0] == entry[COL_NAME_VEHICLES_NAME]:
                        vehicle_time_files[1] += (entry[COL_NAME_FILES_ENDTIMESTAMP] -
                                                  entry[COL_NAME_FILES_BEGINTIMESTAMP])
                        vehicle_time_files[2] += 1
                        if entry[COL_NAME_FILES_RECDRIVENDIST] is not None:
                            vehicle_time_files[3] += entry[COL_NAME_FILES_RECDRIVENDIST]
                        measurements.append(entry[COL_NAME_FILES_MEASID])
                        new_entry = False
                if new_entry:
                    time_ = (entry[COL_NAME_FILES_ENDTIMESTAMP] - entry[COL_NAME_FILES_BEGINTIMESTAMP])
                    vehicle_time_files_list.append([entry[COL_NAME_VEHICLES_NAME], time_,
                                                    1, entry[COL_NAME_FILES_RECDRIVENDIST]])
                    measurements.append(entry[COL_NAME_FILES_MEASID])

        except:
            pass

        return vehicle_time_files_list

    def get_collection_filesize(self, collid, recurse=False, group_by=None):
        """
        Filesize in GB for recordings in collection

        :param collid: collection id. if not collection provided then whole catalog i.e. all files
        :type collid: int
        :param recursive: flag to recusrively include sub collection and shared collections
        :type recursive: boolean
        :param group_by: group by column list e.g. ARCHIVED, PID or any column in CAT_FILES
        :type group_by: list
        :return: if group_by is None return total Filesize(GB) for the collection otherwise list of dictionary
                contain Filesize (GB) for each value of the columns given in group_by
        :rtype: dict | list
        """
        col_sum_fsize = SQLBinaryExpr(SQLFuncExpr("SUM", SQLBinaryExpr(COL_NAME_FILES_FILESIZE, OP_DIV,
                                                                       "(1073741824)")),  # 1024^3
                                      OP_AS, COL_NAME_FILES_FILESIZE)

        entries = self._get_collection_stats(collid, [col_sum_fsize], recurse, group_by)
        return entries[0] if group_by is None else entries

    def get_collection_kilometer(self, collid=None, recurse=False, group_by=None):
        """
        Get driven distance in kilometers for the collection

        :param collection_name: The name of the collection, if None sum of driven distance for all files.
        :type collection_name: str
        :param recurse: flag to recusrively include sub collection and shared collections
        :type recurse: boolean
        :param group_by: driven distance statistic group by column list e.g. ARCHIVED, PID or any column in CAT_FILES
        :type group_by: list
        :return: if group_by is None return total km for the collection otherwise list of dictionary contain kilometer
                for each value of the columns given in group_by
        :rtype: dict dict | list
        """
        # if collection_name is not None:
        #     collid = self.get_collection_id(collection_name)
        # else:
        #     collid = None
        # print "coll det : ", collection_name, collid
        sql_param = {}
        cmap_tbl_alias = "cmap"
        if collid is not None:
            sql_param[str(len(sql_param) + 1)] = collid
        km_cond = SQLBinaryExpr("NVL(%s, 0)" % (COL_NAME_FILES_RECDRIVENDIST), OP_GT, 0)
        if recurse and collid is not None:
            recurs, _ = self._get_collections_tree_query(":%s" % (len(sql_param)))
            recur_sql = GenericSQLSelect([COL_NAME_COLL_COLLID], False, [GEN_RECUR_NAME])
            tree_sql = str(SQLConcatExpr(recurs, recur_sql))
            cond = self._get_union_shared_collection_cond(tree_sql, sql_param)

            cmap_tbl = "(%s)" % str(GenericSQLSelect([COL_NAME_COLLMAP_MEASID], True,
                                                     [TABLE_NAME_COLLMAP], cond))

            cond = SQLBinaryExpr(COL_NAME_FILES_MEASID, OP_IN, "(%s)" % (cmap_tbl))
            cond = SQLBinaryExpr(cond, OP_AND, km_cond)

        elif not recurse and collid is not None:
            cmap_tbl = SQLTableExpr(TABLE_NAME_COLLMAP, cmap_tbl_alias)

            cond = SQLBinaryExpr(SQLColumnExpr(cmap_tbl_alias, COL_NAME_COLLMAP_COLLID),
                                 OP_EQ, ":%s" % (len(sql_param)))
            meas_sql = GenericSQLSelect([SQLColumnExpr(cmap_tbl_alias, COL_NAME_COLLMAP_MEASID)],
                                        True, [cmap_tbl], cond)
            cond = SQLBinaryExpr(COL_NAME_FILES_MEASID, OP_IN, "(%s)" % (meas_sql))
            cond = SQLBinaryExpr(cond, OP_AND, km_cond)

        else:  # Get all sum of rec drivent dist for all collection
            cond = km_cond
        select_list = [SQLBinaryExpr("NVL(%s, 0)" % (SQLFuncExpr("SUM", "NVL(%s, 0)" %
                                                                 (COL_NAME_FILES_RECDRIVENDIST))),
                                     OP_AS, COL_NAME_FILES_RECDRIVENDIST)]
        if group_by is not None:
            select_list = group_by + select_list

        entries = self.select_generic_data(select_list, [TABLE_NAME_FILES], where=cond, group_by=group_by,
                                           sqlparams=sql_param)
        return entries[0] if group_by is None else entries

    @staticmethod
    def _get_collections_tree_query(sql_value, sql_oper=OP_EQ):
        """
        Get shared recursive query that to generatee collection tree

        :param sql_value: SQL variable name for binding
        :type sql_value: str
        :param sql_oper: SQL variable name for binding
        :type sql_oper: str
        :return: sql recursive query
        :rtype: `SQLBinaryExpr`,`GenericSQLSelect`
        """
        col_list = [COL_NAME_COLL_PARENTID, COL_NAME_COLL_COLLID, COL_NAME_COLL_NAME, COL_NAME_COLL_COLLCOMMENT,
                    COL_NAME_COLL_PRID, COL_NAME_COLL_IS_ACTIVE]
        catcoll_tbl_alias = "c"
        cat_coll_tbl = SQLTableExpr(TABLE_NAME_COLL, catcoll_tbl_alias)
        col_list_aliased = [SQLColumnExpr(catcoll_tbl_alias, COL_NAME_COLL_PARENTID),
                            SQLColumnExpr(catcoll_tbl_alias, COL_NAME_COLL_COLLID),
                            SQLColumnExpr(catcoll_tbl_alias, COL_NAME_COLL_NAME),
                            SQLColumnExpr(catcoll_tbl_alias, COL_NAME_COLL_COLLCOMMENT),
                            SQLColumnExpr(catcoll_tbl_alias, COL_NAME_COLL_PRID),
                            SQLColumnExpr(catcoll_tbl_alias, COL_NAME_COLL_IS_ACTIVE)]

        start = GenericSQLSelect(col_list, False, [cat_coll_tbl],
                                 SQLBinaryExpr(COL_NAME_COLL_COLLID, sql_oper, sql_value))
        join = SQLJoinExpr(cat_coll_tbl, OP_INNER_JOIN, SQLTableExpr(GEN_RECUR_NAME, "r"),
                           SQLBinaryExpr(SQLColumnExpr(catcoll_tbl_alias, COL_NAME_COLL_PARENTID), OP_EQ,
                                         SQLColumnExpr("r", COL_NAME_COLL_COLLID)))
        stop = GenericSQLSelect(col_list_aliased, False, [join])
        outer = GenericSQLSelect(col_list, False, [GEN_RECUR_NAME])
        wexp = str(SQLConcatExpr(EXPR_WITH, SQLFuncExpr(GEN_RECUR_NAME, str(col_list)[1:-1].replace("'", ""))))
        wexp = SQLBinaryExpr(wexp, OP_AS, SQLConcatExpr(start, OP_UNION_ALL, stop))
        return wexp, outer


    def add_shared_collection(self, parent_collid, child_collid, prid=None):
        """
        Add shared collection link. The oracle database may throw exception if the collection shared is creating
        cyclic loop

        :param parent_collid: parent collection id
        :type parent_collid: int
        :param child_collid: child collection id
        :type child_collid: int
        :param prid: priority Id
        :type prid: int
        :return: primary key value of sharedmapid for the newly inserted row
        :rtype: int
        """

        # if self.sub_scheme_version < CAT_SHARECOLL_VERSION:
        #     return []
        record_dict = {COL_NANE_SHARECOLL_PARENT_COLLID: parent_collid,
                       COL_NANE_SHARECOLL_CHILD_COLLID: child_collid,
                       COL_NANE_SHARECOLL_PRID: prid}
        sharedmapid = self.add_generic_data(record_dict, TABLE_NAME_CAT_SHAREDCOLLECTIONMAP,
                                            SQLUnaryExpr(OP_RETURNING, COL_NANE_SHARECOLL_SAHREDMAPID))
        return sharedmapid

    def delete_shared_collection(self, parent_collid, child_collid):
        """
        Delete shared link

        :param parent_collid: parent collection id
        :type parent_collid: int
        :param child_collid: child collection id
        :type child_collid: int
        """
        # if self.sub_scheme_version < CAT_SHARECOLL_VERSION:
        #     return
        cond = SQLBinaryExpr(COL_NANE_SHARECOLL_PARENT_COLLID, OP_EQ, parent_collid)
        cond = SQLBinaryExpr(SQLBinaryExpr(COL_NANE_SHARECOLL_CHILD_COLLID, OP_EQ, child_collid), OP_AND, cond)
        self.delete_generic_data(TABLE_NAME_CAT_SHAREDCOLLECTIONMAP, where=cond)

    def get_parent_collections(self, collid):
        """
        not part
        Get the parent collections of a collection.

        :author:        Mohammed Tanveer
        :date:          26/10/2015

        :param collid: The ID of the collection.
        :type collid: Integer
        :return: Returns the parent collections of a collection.
        :rtype : list
        """
        coll_list = []
        pid_cond = SQLBinaryExpr(COL_NAME_COLL_COLLID, OP_EQ, SQLLiteral(collid))
        parent_id = self.select_generic_data(select_list=[COL_NAME_COLL_PARENTID], table_list=[self.GetQualifiedTableName(TABLE_NAME_COLL)], where=pid_cond)
        parcollid = parent_id[0][COL_NAME_COLL_PARENTID]
        while parcollid is not None :
            cond = SQLBinaryExpr(COL_NAME_COLL_COLLID, OP_EQ, SQLLiteral(parcollid))
            par_coll = self.select_generic_data(table_list=[self.GetQualifiedTableName(TABLE_NAME_COLL)], where=cond)
            parcollid = par_coll[0][COL_NAME_COLL_PARENTID]
            coll_list.append(par_coll[0])
        return coll_list

    # def get_collection_tree(self, *args, **kwargs):
    #     """
    #     Get collection tree starting from the specified collid as root_collid. the list is sorted by hiarchichal level
    #     i.e. root collection corresponding to paramter root_collid will be the first entry in the list
    #
    #     :keyword root_collid: The collection as the starting collection as root of tree or sub-tree
    #     :type    root_collid: int
    #     :keyword share_depth: recursion depth. it is recomended to use the default value
    #     :type    share_depth:
    #     :keyword incl_shared: flag to include or exclude the shared collection in the tree. This will recursively
    #                           populate shared collection till the max depth limit reached or
    #                           all the shared collection tree are populated
    #     :type    incl_shared: int
    #     :return: list of tuple as all set of records. selected columns,
    #              the order of tuple will be (shared_flag, parent_id, collid, coll_name, comment, prid, is_active)
    #     :rtype:  list
    #     """
    #     opt = arg_trans(['root_collid', ['incl_shared', True], ['col_list', None],
    #                      ['depth_size', 100]], *args, **kwargs)
    #
    #     root_collid = opt[0]
    #     incl_shared = opt[1]
    #     col_list = opt[2]
    #     depth_size = opt[3]
    #
    #     # Disable include_shared collection if the feature not available in database
    #     if self.sub_scheme_version < CAT_SHARECOLL_VERSION:
    #         incl_shared = False
    #     max_depth = 100
    #     if depth_size > max_depth:
    #         raise AdasDBError("Leave the depth_size as default value %d" % (max_depth))
    #     if depth_size == -1:
    #         raise AdasDBError("Max recursion depth has been reacheded")
    #
    #     if col_list is None:
    #         col_list = [SHARED_FLAG, COL_NAME_COLL_PARENTID, COL_NAME_COLL_COLLID, COL_NAME_COLL_NAME,
    #                     COL_NAME_COLL_COLLCOMMENT, COL_NAME_COLL_PRID, COL_NAME_COLL_IS_ACTIVE]
    #
    #     shared_flag_idx = None
    #     if depth_size < max_depth:
    #         if SHARED_FLAG in col_list:
    #             shared_flag_idx = col_list.index(SHARED_FLAG)
    #             col_list[shared_flag_idx] = SQLBinaryExpr(SHARED_COLL, OP_AS, SHARED_FLAG)
    #
    #         master_sql = GenericSQLSelect(col_list, False, [GEN_RECUR_NAME])
    #     else:
    #         if SHARED_FLAG in col_list:
    #             shared_flag_idx = col_list.index(SHARED_FLAG)
    #             col_list[shared_flag_idx] = SQLBinaryExpr(N0T_SHARED_COLL, OP_AS, SHARED_FLAG)
    #         master_sql = GenericSQLSelect(col_list, False, [GEN_RECUR_NAME])
    #     recurs, _ = self._get_collections_tree_query(":1")
    #
    #     sql_param = {"1": root_collid}
    #     try:
    #         master_coll_tree = self.execute(str(SQLConcatExpr(recurs, master_sql)), **sql_param)
    #     except Exception as exp:
    #         raise AdasDBError(str(exp) + "\nRecursive Query gone into infinite loopg for collection %s"
    #                           % str(sql_param))
    #     if incl_shared:
    #         master_sql = GenericSQLSelect([COL_NAME_COLL_COLLID], False, [GEN_RECUR_NAME])
    #         shared_coll_cond = SQLBinaryExpr(COL_NANE_SHARECOLL_PARENT_COLLID, OP_IN,
    #                                          "(%s)" % str(SQLConcatExpr(recurs, master_sql)))
    #
    #         sharecoll_sql = GenericSQLSelect([COL_NANE_SHARECOLL_PARENT_COLLID, COL_NANE_SHARECOLL_CHILD_COLLID],
    #                                          False, [TABLE_NAME_CAT_SHAREDCOLLECTIONMAP], shared_coll_cond)
    #         sharedcolls = self.execute(str(sharecoll_sql), **sql_param)
    #         for shared_coll in sharedcolls:
    #
    #             if shared_flag_idx is not None:
    #                 col_list[shared_flag_idx] = SHARED_FLAG
    #             shared_coll_tree, col_list = self.get_collection_tree(shared_coll[1], depth_size=depth_size - 1,
    #                                                                   incl_shared=incl_shared, col_list=col_list)
    #
    #             if len(shared_coll_tree) > 0:
    #                 first_rec = list(shared_coll_tree[0])
    #                 if COL_NAME_COLL_PARENTID in col_list:
    #                     first_rec[col_list.index(COL_NAME_COLL_PARENTID)] = shared_coll[0]
    #                 shared_coll_tree[0] = tuple(first_rec)
    #             master_coll_tree += shared_coll_tree
    #         if shared_flag_idx is not None:
    #             col_list[shared_flag_idx] = SHARED_FLAG
    #         return master_coll_tree, col_list
    #     else:
    #         if shared_flag_idx is not None:
    #             col_list[shared_flag_idx] = SHARED_FLAG
    #         return master_coll_tree, col_list

    def get_collection_time(self, collection_name=None):
        """
        Get collection Diving duration in Microsecond.

        :param collection_name: The name of the collection, if None sum of all files.
        :type collection_name: str
        :return: the total time over all the files
        :rtype: dict
        """
        record = {}
        cond = []
        if collection_name is not None:
            typejoin = SQLJoinExpr(SQLTableExpr(TABLE_NAME_FILES),
                                   OP_INNER_JOIN,
                                   SQLTableExpr(TABLE_NAME_COLLMAP),
                                   SQLBinaryExpr(SQLColumnExpr(SQLTableExpr(TABLE_NAME_COLLMAP),
                                                               COL_NAME_COLLMAP_MEASID),
                                                 OP_EQ,
                                                 SQLColumnExpr(SQLTableExpr(TABLE_NAME_FILES), COL_NAME_FILES_MEASID)))

            typejoin = SQLJoinExpr(SQLTableExpr(typejoin),
                                   OP_INNER_JOIN,
                                   SQLTableExpr(TABLE_NAME_COLL),
                                   SQLBinaryExpr(SQLColumnExpr(SQLTableExpr(TABLE_NAME_COLL), COL_NAME_COLL_COLLID),
                                                 OP_EQ,
                                                 SQLColumnExpr(SQLTableExpr(TABLE_NAME_COLLMAP),
                                                               COL_NAME_COLLMAP_COLLID)))

            cond = SQLBinaryExpr(SQLFuncExpr(self.db_func_map[DB_FUNC_NAME_LOWER],
                                             SQLColumnExpr(SQLTableExpr(TABLE_NAME_COLL),
                                                           COL_NAME_COLL_NAME)),
                                 OP_EQ,
                                 SQLLiteral(collection_name.lower()))

        delta = SQLBinaryExpr(SQLColumnExpr(SQLTableExpr(TABLE_NAME_FILES), COL_NAME_FILES_ENDTIMESTAMP),
                              OP_SUB,
                              SQLColumnExpr(SQLTableExpr(TABLE_NAME_FILES), COL_NAME_FILES_BEGINTIMESTAMP))
        select_list = [SQLBinaryExpr(SQLFuncExpr("SUM", delta), OP_AS, "TOTAL")]

        try:
            entries = self.select_generic_data(select_list, [typejoin], where=cond)
            record = entries[0]["TOTAL"]
        except:
            pass

        # done
        return record

    def add_collection_log(self, record):
        """
        To return the primary key
        Add collection log entry

        :param record: dictionary record
        :type record: dict
        :return: log_id value of the newly inserted record
        :rtype: Integer
        """
        return self.add_generic_data(record, TABLE_NAME_CAT_COLLECTION_LOG,
                                     SQLUnaryExpr(OP_RETURNING, COL_NAME_COLLOG_LOG_ID))

    def get_collection_log(self, coll_name, action=None, start_date=None, end_date=None,
                           action_by=None, order_by=None):
        """
        Get Collection log

        :param coll_name: collection name
        :type coll_name:
        :param action: action string e.g. Deleted, Renamed
        :type action: string
        :param start_date: start date time default none means that it is not included in sql condition
        :type start_date: datetime
        :param end_date: end date time default none means that it is not included in sql condition
        :type end_date: datetime
        :param action_by: windows loginname who performed action
        :type action_by: str
        :param order_by: sorted by columns default logid
        :type order_by: list
        """
        if order_by is None:
            order_by = [COL_NAME_COLLOG_LOG_ID]

        sql_param = {}
        sql_param[str(len(sql_param) + 1)] = coll_name
        cond = SQLBinaryExpr(COL_NAME_COLLOG_COLL_NAME, OP_EQ, ":%d" % (len(sql_param)))

        if action is not None:
            sql_param[str(len(sql_param) + 1)] = action
            cond = SQLBinaryExpr(cond, OP_AND,
                                 SQLBinaryExpr(COL_NAME_COLLOG_ACTION, OP_EQ, ":%d" % (len(sql_param))))

        if action_by is not None:
            sql_param[str(len(sql_param) + 1)] = action_by
            cond = SQLBinaryExpr(cond, OP_AND,
                                 SQLBinaryExpr(COL_NAME_COLLOG_ACTIONBY, OP_EQ, ":%d" % (len(sql_param))))

        if start_date is not None:
            sql_param[str(len(sql_param) + 1)] = start_date
            cond = SQLBinaryExpr(cond, OP_AND, SQLBinaryExpr(COL_NAME_COLLOG_ACTION_DATE, OP_GEQ,
                                                             ":%d" % (len(sql_param))))
        if end_date is not None:
            sql_param[str(len(sql_param) + 1)] = end_date
            cond = SQLBinaryExpr(cond, OP_AND, SQLBinaryExpr(COL_NAME_COLLOG_ACTION_DATE, OP_LEQ,
                                                             ":%d" % (len(sql_param))))

        return self.select_generic_data(table_list=[TABLE_NAME_CAT_COLLECTION_LOG],
                                        where=cond, order_by=order_by, sqlparams=sql_param)

    def add_collection_log_details(self, log_id, measids):
        """
        Add log details i.e. list of recording involved in the log activity

        :param log_id: log Id foreign key reference log_id from collection_log table
        :type  log_id: Integer
        :param measids: list of measurement Ids
        :type  measids: list
        :return: None
        :rtype:  None
        """
        col_names = [COL_NAME_COLLDET_LOG_ID, COL_NAME_COLLDET_MEASID]
        values = [tuple([log_id, measid]) for measid in measids]
        self.add_generic_compact_prepared(col_names, values, TABLE_NAME_CAT_COLLECTION_LOGDETAILS)

    def get_collection_log_details(self, log_id):
        """
        Get details of collection log activity

        :param log_id: Log id corresponding to action
        :type  log_id: integer
        :return: details containing filepath, filename and measurement id of the recording
        :rtype:  list
        """

        col_logdet_logid = SQLColumnExpr(SQLTableExpr(TABLE_NAME_CAT_COLLECTION_LOGDETAILS),
                                         COL_NAME_COLLDET_LOG_ID)
        col_logdet_measid = SQLColumnExpr(SQLTableExpr(TABLE_NAME_CAT_COLLECTION_LOGDETAILS),
                                          COL_NAME_COLLDET_MEASID)

        col_cf_measid = SQLColumnExpr(SQLTableExpr(TABLE_NAME_FILES), COL_NAME_FILES_MEASID)
        col_recfileid = SQLColumnExpr(SQLTableExpr(TABLE_NAME_FILES), COL_NAME_FILES_RECFILEID)
        col_filepath = SQLColumnExpr(SQLTableExpr(TABLE_NAME_FILES), COL_NAME_FILES_FILEPATH)

        join_1 = SQLJoinExpr(SQLTableExpr(TABLE_NAME_FILES),
                             OP_INNER_JOIN,
                             SQLTableExpr(TABLE_NAME_CAT_COLLECTION_LOGDETAILS),
                             SQLBinaryExpr(col_logdet_measid, OP_EQ, col_cf_measid))

        columns = [SQLBinaryExpr(col_logdet_logid, OP_AS, COL_NAME_COLLDET_LOG_ID),
                   SQLBinaryExpr(col_cf_measid, OP_AS, COL_NAME_FILES_MEASID),
                   SQLBinaryExpr(col_recfileid, OP_AS, COL_NAME_FILES_RECFILEID),
                   SQLBinaryExpr(col_filepath, OP_AS, COL_NAME_FILES_FILEPATH)]
        sql_param = {}
        sql_param[str(len(sql_param) + 1)] = log_id
        cond = SQLBinaryExpr(col_logdet_logid, OP_EQ, ":%d" % (len(sql_param)))
        return self.select_generic_data(columns, table_list=[join_1],
                                        where=cond, sqlparams=sql_param)
    # ====================================================================
    # Handling of keyword data
    # ====================================================================

    def add_keyword(self, keyword):
        """
        Add keyword state to database.

        :param keyword: The keyword record.
        :type  keyword: str
        :return: Returns the keyword ID.
        :rtype:  int
        """
        parentid_expr = SQLNull()
        parentid_op = OP_IS
        if keyword[COL_NAME_KW_PARENTID] is not None:
            parentid_expr = SQLLiteral(keyword[COL_NAME_KW_PARENTID])
            parentid_op = OP_EQ
        kw_cond = SQLBinaryExpr(SQLBinaryExpr(SQLFuncExpr(self.db_func_map[DB_FUNC_NAME_LOWER],
                                                          COL_NAME_KW_NAME), OP_EQ,
                                              SQLLiteral(keyword[COL_NAME_KW_NAME].lower())),
                                OP_AND, SQLBinaryExpr(COL_NAME_KW_PARENTID, parentid_op, parentid_expr))

        entries = self.select_generic_data(table_list=[TABLE_NAME_KW], where=kw_cond)
        if len(entries) <= 0:
            if self.sub_scheme_version < CAT_ACTIVE_VERSION:
                kwid = self._get_next_id(TABLE_NAME_KW, COL_NAME_KW_KWID)
                keyword[COL_NAME_KW_KWID] = kwid
                self.add_generic_data(keyword, TABLE_NAME_KW)
            else:
                entries = self.select_generic_data(table_list=[TABLE_NAME_KW], where=kw_cond)
                kwid = entries[0][COL_NAME_KW_KWID]
                self.add_generic_data(keyword, TABLE_NAME_KW)
            return kwid
        else:
            tmp = "Keyword '%s' " % keyword[COL_NAME_KW_NAME]
            tmp += "exists already in the catalog for this parent."
            if self.error_tolerance < ERROR_TOLERANCE_LOW:
                raise AdasDBError(tmp)
            else:
                warn(tmp)
                if len(entries) == 1:
                    return entries[0][COL_NAME_KW_KWID]
                elif len(entries) > 1:
                    tmp = "Keyword name '%s' " % (keyword[COL_NAME_KW_NAME])
                    tmp += "cannot be resolved because it is ambiguous. (%s)" % entries
                    raise AdasDBError(tmp)

    def update_keywords(self, keyword, where=None):
        """
        Update existing keyword records.

        :param keyword: dictionary of record with update values.
        :type  keyword: dict
        :param where: The condition to be fulfilled by the keywords to the updated.
        :type  where: SQLBinaryExpression
        :return: Returns the number of affected keywords.
        :rtype: int
        """
        rowcount = 0
        if keyword is not None:
            self.update_generic_data(keyword, TABLE_NAME_KW, where)
        # done
        return rowcount

    def add_keyword_map(self, kwmap):
        """
        Add keyword mapping to database.

        :param kwmap: The keyword mapping record.
        :type kwmap: dict
        :return: Returns the keyword map ID.
        :rtype: int
        """
        kw_cond = SQLBinaryExpr(SQLBinaryExpr(COL_NAME_KWMAP_KWID, OP_EQ, kwmap[COL_NAME_KWMAP_KWID]),
                                OP_AND,
                                SQLBinaryExpr(COL_NAME_KWMAP_MEASID, OP_EQ, kwmap[COL_NAME_KWMAP_MEASID]))
        entries = self.select_generic_data(table_list=[TABLE_NAME_KWMAP], where=kw_cond)
        if len(entries) <= 0:
            if self.sub_scheme_version < CAT_ACTIVE_VERSION:
                kwmapid = self._get_next_id(TABLE_NAME_KWMAP, COL_NAME_KWMAP_KWMAPID)
                kwmap[COL_NAME_KWMAP_KWMAPID] = kwmapid
                self.add_generic_data(kwmap, TABLE_NAME_KWMAP)
            else:
                self.add_generic_data(kwmap, TABLE_NAME_KWMAP)
                entries = self.select_generic_data(table_list=[TABLE_NAME_KWMAP], where=kw_cond)
                kwmapid = entries[0][COL_NAME_KWMAP_KWMAPID]
            return kwmapid
        else:
            tmp = "Keyword '%s' " % (kwmap[COL_NAME_KWMAP_KWID])
            tmp += "is already assigned to file "
            tmp += "'%s'." % (kwmap[COL_NAME_KWMAP_MEASID])
            if self.error_tolerance < ERROR_TOLERANCE_LOW:
                raise AdasDBError(tmp)
            else:
                warn(tmp)
                if len(entries) == 1:
                    return entries[0][COL_NAME_KWMAP_KWMAPID]
                elif len(entries) > 1:
                    tmp = "Keyword mapping of file '%s' " % (kwmap[COL_NAME_KWMAP_MEASID])
                    tmp += "cannot be resolved because it is ambiguous. (%s)" % entries
                    raise AdasDBError(tmp)

    def get_keyword_id(self, kw_name):
        """
        Find a keyword Id with a given name.

        :param kw_name: The keyword name.
        :type kw_name: str
        :return: Returns the keyword ID or None if not exists.
        :rtype: int
        """

        kwid = None
        cond = SQLBinaryExpr(SQLFuncExpr(self.db_func_map[DB_FUNC_NAME_LOWER], COL_NAME_KW_NAME),
                             OP_EQ, SQLLiteral(kw_name.lower()))
        entries = self.select_generic_data(table_list=[TABLE_NAME_KW], where=cond)
        if len(entries) == 1:
            kwid = entries[0][COL_NAME_KW_KWID]

        # done
        return kwid

    def get_keyword_name(self, kwid):
        """
        Get the name of a keyword for given.

        :param kwid: The keyword ID.
        :type kwid: int
        :return: Returns the keyword name or None if not exists.
        :rtype: str
        """
        kw_name = None
        if kwid is not None:
            cond = SQLBinaryExpr(COL_NAME_KW_KWID, OP_EQ, kwid)
            entries = self.select_generic_data(select_list=[COL_NAME_KW_NAME],
                                               table_list=[TABLE_NAME_KW], where=cond)
            if len(entries) == 1:
                kw_name = entries[0][COL_NAME_KW_NAME]

        return kw_name

    # ====================================================================
    # Handling of location data
    # ====================================================================
    #
    # @staticmethod
    # @deprecated()
    # def add_country(country):  # pylint: disable=W0613
    #     """deprecated
    #     """
    #     raise AdasDBError("tables dropped")
    #
    # @staticmethod
    # @deprecated()
    # def update_countries(country, where=None):  # pylint: disable=W0613
    #     """deprecated
    #     """
    #     raise AdasDBError("tables dropped")
    #
    # @staticmethod
    # @deprecated()
    # def get_country_id(country):  # pylint: disable=W0613
    #     """deprecated
    #     """
    #     raise AdasDBError("tables dropped")
    #
    # @staticmethod
    # @deprecated()
    # def get_country_name(cntryid):  # pylint: disable=W0613
    #     """deprecated
    #     """
    #     raise AdasDBError("tables dropped")
    #
    # @staticmethod
    # @deprecated()
    # def add_location(location):  # pylint: disable=W0613
    #     """deprecated
    #     """
    #     raise AdasDBError("tables dropped")
    #
    # @staticmethod
    # @deprecated()
    # def get_location_id(location):  # pylint: disable=W0613
    #     """deprecated
    #     """
    #     raise AdasDBError("tables dropped")
    #
    # @staticmethod
    # @deprecated()
    # def update_locations(location, where=None):  # pylint: disable=W0613
    #     """deprecated
    #     """
    #     raise AdasDBError("tables dropped")
    #
    # @staticmethod
    # @deprecated()
    # def add_location_info(location):  # pylint: disable=W0613
    #     """deprecated
    #     """
    #     raise AdasDBError("tables dropped")
    #
    # @staticmethod
    # @deprecated()
    # def get_location_info(locinfoid):  # pylint: disable=W0613
    #     """deprecated
    #     """
    #     raise AdasDBError("tables dropped")
    #
    # @staticmethod
    # @deprecated()
    # def get_location_info_ids(measid, location):  # pylint: disable=W0613
    #     """deprecated
    #     """
    #     raise AdasDBError("tables dropped")
    #
    # @staticmethod
    # @deprecated()
    # def get_location_info_idat_rel_time(measid, location, reltime):  # pylint: disable=W0613
    #     """deprecated
    #     """
    #     raise AdasDBError("tables dropped")
    #
    # @staticmethod
    # @deprecated()
    # def update_locations_info(location, where=None):  # pylint: disable=W0613
    #     """deprecated
    #     """
    #     raise AdasDBError("tables dropped")

    # ====================================================================
    # Handling of vehicle configuration data
    # ====================================================================

    def add_vehicle(self, vehicle):
        """
        Add new vehicle to database.

        :param vehicle: The vehicle record.
        :type vehicle: dict
        :return: Returns the vehicle ID.
        :rtype: int
        """
        veh_cond = SQLBinaryExpr(SQLFuncExpr(self.db_func_map[DB_FUNC_NAME_LOWER],
                                             COL_NAME_VEHICLES_NAME),
                                 OP_EQ, SQLLiteral(vehicle[COL_NAME_VEHICLES_NAME].lower()))
        entries = self.select_generic_data(table_list=[TABLE_NAME_VEHICLES], where=veh_cond)
        if len(entries) <= 0:
            if self.sub_scheme_version < CAT_ACTIVE_VERSION:
                vehid = self._get_next_id(TABLE_NAME_VEHICLES, COL_NAME_VEHICLES_VEHICLEID)
                vehicle[COL_NAME_VEHICLES_VEHICLEID] = vehid
                self.add_generic_data(vehicle, TABLE_NAME_VEHICLES)
            else:
                self.add_generic_data(vehicle, TABLE_NAME_VEHICLES)
                entries = self.select_generic_data(table_list=[TABLE_NAME_VEHICLES], where=veh_cond)
                vehid = entries[0][COL_NAME_VEHICLES_VEHICLEID]
            return vehid
        else:
            if self.error_tolerance < ERROR_TOLERANCE_LOW:
                raise AdasDBError("Vehicle '%s' exists already in the catalog." % vehicle[COL_NAME_VEHICLES_NAME])
            else:
                warn("Vehicle '" + vehicle[COL_NAME_VEHICLES_VEHICLEID] + "' already exists in the catalog.")
                if len(entries) == 1:
                    return entries[0][COL_NAME_VEHICLES_VEHICLEID]
                elif len(entries) > 1:
                    tmp = "Vehicle '%s' " % (vehicle[COL_NAME_VEHICLES_NAME])
                    tmp += "cannot be resolved because it is ambiguous. (%s)" % entries
                    raise AdasDBError(tmp)

    def update_vehicles(self, vehicle, where=None):
        """
        Update existing vehicle records.

        :param vehicle: The vehicle record update.
        :type vehicle: dict
        :param where: The condition to be fulfilled by the vehicles to the updated.
        :type where: SQLBinaryExpression
        :return: Returns the number of affected vehicles.
        :rtype: int
        """
        rowcount = 0
        if vehicle is not None:
            self.update_generic_data(vehicle, TABLE_NAME_VEHICLES, where)
        # done
        return rowcount

    def get_vehicle_id(self, veh_name):
        """
        Get a vehicle ID for a vehicle name

        :param veh_name: The vehicle name to be resolved.
        :type veh_name: str
        :return: Returns the ID for the vehicle or None if not exists.
        :rtype: int
        """
        cond = SQLBinaryExpr(SQLFuncExpr(self.db_func_map[DB_FUNC_NAME_LOWER],
                                         COL_NAME_VEHICLES_NAME),
                             OP_EQ, SQLLiteral(veh_name.lower()))
        entries = self.select_generic_data(select_list=[COL_NAME_VEHICLES_VEHICLEID],
                                           table_list=[TABLE_NAME_VEHICLES],
                                           where=cond)
        if len(entries) == 1:
            return entries[0][COL_NAME_VEHICLES_VEHICLEID]
        elif len(entries) > 1:
            raise AdasDBError("Vehicle '%s' cannot be resolved because it is ambiguous. (%s)" % (veh_name, entries))

        raise AdasDBError("No resolution of '%s'. (%s)" % (veh_name, entries))

    def get_vehicle_name(self, vehid):
        """
        Get a vehicle name for a ID

        :param vehid: The vehicle ID to be resolved.
        :type vehid: int
        :return: Returns the name for the vehicle or if not exists then raises AdasDBError
        :rtype: str
        """
        vehid_cond = SQLBinaryExpr(COL_NAME_VEHICLES_VEHICLEID, OP_EQ, SQLLiteral(vehid))
        entries = self.select_generic_data(select_list=[COL_NAME_VEHICLES_NAME],
                                           table_list=[TABLE_NAME_VEHICLES],
                                           where=vehid_cond)
        if len(entries) == 1:
            return entries[0][COL_NAME_VEHICLES_NAME]
        elif len(entries) > 1:
            raise AdasDBError("Vehicle ID '%s' cannot be resolved because it is ambiguous. (%s)" % (vehid, entries))

        raise AdasDBError("No resolution of '%s'. (%s)" % (vehid, entries))

    def add_vehicle_cfg(self, vehiclecfg):
        """
        Add new vehicle cfg to database.

        :param vehiclecfg: The vehicle cfg record.
        :return: Returns the vehicle cfg ID.
        :rtype: int
        """
        vehcfgid = self._get_next_id(TABLE_NAME_VEHICLECFGS, COL_NAME_VEHICLECFGS_VEHCFGID)
        vehiclecfg[COL_NAME_VEHICLECFGS_VEHCFGID] = vehcfgid
        self.add_generic_data(vehiclecfg, TABLE_NAME_VEHICLECFGS)
        return vehcfgid

    def update_vehicle_cfgs(self, vehiclecfg, where=None):
        """
        Update existing vehicle cfg records.

        :param vehiclecfg: The vehicle cfg record update.
        :type vehiclecfg: dict
        :param where: The condition to be fulfilled by the vehicle cfgs to the updated.
        :type where: SQLBinaryExpression
        :return: Returns the number of affected vehicle cfgs.
        :rtype: int
        """
        rowcount = 0
        if vehiclecfg is not None:
            self.update_generic_data(vehiclecfg, TABLE_NAME_VEHICLECFGS, where)
        # done
        return rowcount

    def get_vehicle_cfg_id(self, vehcfg_name):
        """
        Get a vehicle cfg ID for a vehicle cfg name

        :param vehcfg_name: The vehicle cfg name to be resolved.
        :type vehcfg_name: str
        :return: Returns the ID for the vehicle cfg or None if not exists.
        :rtype: Intege
        """
        is_rooted = False
        cfg_name = vehcfg_name.rstrip(PATH_SEPARATOR)
        if self.is_absolute_name(cfg_name):
            cfg_name = cfg_name.lstrip(PATH_SEPARATOR)
            is_rooted = True
        cfg_name_parts = cfg_name.split(PATH_SEPARATOR)
        # if absolute name, get vehicle ID
        vehid_cond = None
        if is_rooted:
            if len(cfg_name_parts) != 2:
                tmp = "Vehicle configuration name '%s' " % vehcfg_name
                tmp += "cannot be resolved. Name must have format '/vehicle/config'."
                raise AdasDBError(tmp)
            vehid = self.get_vehicle_id(cfg_name_parts[0])
            vehid_cond = SQLBinaryExpr(COL_NAME_VEHICLECFGS_VEHICLEID, OP_EQ, SQLLiteral(vehid))
            cfg_name = cfg_name_parts[1].strip()
        # get configuration id (for vehicle, if absolute)
        cfg_cond = SQLBinaryExpr(SQLFuncExpr(self.db_func_map[DB_FUNC_NAME_LOWER],
                                             COL_NAME_VEHICLECFGS_NAME),
                                 OP_EQ, SQLLiteral(cfg_name.lower()))
        if vehid_cond is not None:
            cfg_cond = SQLBinaryExpr(vehid_cond, OP_AND, cfg_cond)
        entries = self.select_generic_data(select_list=[COL_NAME_VEHICLECFGS_VEHCFGID],
                                           table_list=[TABLE_NAME_VEHICLECFGS],
                                           where=cfg_cond)
        if len(entries) == 1:
            return entries[0][COL_NAME_VEHICLECFGS_VEHCFGID]
        elif len(entries) > 1:
            tmp = "Vehicle configuration '%s' " % vehcfg_name
            tmp += "cannot be resolved because it is ambiguous. (%s)" % entries
            raise AdasDBError(tmp)

        raise AdasDBError("No resolution of '%s'. (%s)" % (vehcfg_name, entries))

    def get_vehicle_cfg_name(self, vehcfgid, absolute=True):
        """
        Get a vehicle configuration name for a ID

        :param vehcfgid: The vehicle configuration ID to be resolved.
        :type vehcfgid: int
        :param absolute: Set True to return absolute names
        :type absolute: bool
        :return: Returns the name for the vehicle configuration or if not exists raises AdasDBError
        :rtype: str
        """
        vehcfgid_cond = SQLBinaryExpr(COL_NAME_VEHICLECFGS_VEHCFGID, OP_EQ, SQLLiteral(vehcfgid))
        entries = self.select_generic_data(select_list=[COL_NAME_VEHICLECFGS_NAME, COL_NAME_VEHICLECFGS_VEHICLEID],
                                           table_list=[TABLE_NAME_VEHICLECFGS],
                                           where=vehcfgid_cond)
        if len(entries) == 1:
            if absolute:
                veh_name = self.get_vehicle_name(entries[0][COL_NAME_VEHICLECFGS_VEHICLEID])
                return "%s%s%s" % (veh_name, PATH_SEPARATOR, entries[0][COL_NAME_VEHICLECFGS_NAME])
            else:
                return entries[0][COL_NAME_VEHICLECFGS_NAME]
        elif len(entries) > 1:
            tmp = "Vehicle configuration ID '%s' " % vehcfgid
            tmp += "cannot be resolved because it is ambiguous. (%s)" % entries
            raise AdasDBError(tmp)

        raise AdasDBError("No resolution of '%s'. (%s)" % (vehcfgid, entries))

    def add_vehicle_cfg_state(self, vehiclecfgstate):
        """
        Add new vehicle cfg to database.

        :param vehiclecfgstate: The vehicle cfg state record.
        :type vehiclecfgstate: dict
        :return: Returns the vehicle cfg state ID.
        :rtype: int
        """
        vehcfgstateid = self._get_next_id(TABLE_NAME_VEHICLECFGSTATES, COL_NAME_VEHICLECFGSTATES_VEHICLECFGSTATEID)
        vehiclecfgstate[COL_NAME_VEHICLECFGSTATES_VEHICLECFGSTATEID] = vehcfgstateid
        self.add_generic_data(vehiclecfgstate, TABLE_NAME_VEHICLECFGSTATES)
        return vehcfgstateid

    def update_vehicle_cfg_states(self, vehiclecfgstate, where=None):
        """
        Update vehicle cfg in database.

        :param vehiclecfgstate: The vehicle cfg state record
        :type vehiclecfgstate: dict
        :param where: The condition to be fulfilled by the vehicle cfgs to the updated.
        :type where: SQLBinaryExpression
        :return: Returns the number of affected vehicle cfgs.
        :rtype: int
        """
        rowcount = 0
        if vehiclecfgstate is not None:
            self.update_generic_data(vehiclecfgstate, TABLE_NAME_VEHICLECFGSTATES, where)
        # done
        return rowcount

    def get_vehicle_cfg_state_id(self, vehcfgstate_name):
        """
        Get a vehicle cfg ID for a vehicle cfg name

        :param vehcfgstate_name: The vehicle cfg state name to be resolved.
        :type vehcfgstate_name: str
        :return: Returns the ID for the vehicle cfg. if not exist raises AdasDBError
        :rtype: int
        """
        # is_rooted = False
        cfg_name = vehcfgstate_name.rstrip(PATH_SEPARATOR)
        # get configuration id (for vehicle, if absolute)
        cfg_cond = SQLBinaryExpr(SQLFuncExpr(self.db_func_map[DB_FUNC_NAME_LOWER],
                                             COL_NAME_VEHICLECFGSTATES_NAME),
                                 OP_EQ, SQLLiteral(cfg_name.lower()))
        entries = self.select_generic_data(select_list=[COL_NAME_VEHICLECFGSTATES_VEHICLECFGSTATEID],
                                           table_list=[TABLE_NAME_VEHICLECFGSTATES],
                                           where=cfg_cond)
        if len(entries) == 1:
            return entries[0][COL_NAME_VEHICLECFGSTATES_VEHICLECFGSTATEID]
        elif len(entries) > 1:
            tmp = "Vehicle configuration state '%s' " % vehcfgstate_name
            tmp += "cannot be resolved because it is ambiguous. (%s)" % entries
            raise AdasDBError(tmp)

        raise AdasDBError("No resolution of vehicle configuration state '%s'. (%s)" % (vehcfgstate_name, entries))

    def get_vehicle_cfg_state_name(self, vehcfgstateid):
        """
        Get a vehicle configuration name for a ID

        :param vehcfgstateid: The vehicle configuration state ID to be resolved.
        :type vehcfgstateid: int
        :return: Returns the name for the vehicle configuration. if not exist raises AdasDBError
        :rtype: str
        """
        vehcfgid_cond = SQLBinaryExpr(COL_NAME_VEHICLECFGSTATES_VEHICLECFGSTATEID, OP_EQ, SQLLiteral(vehcfgstateid))
        entries = self.select_generic_data(select_list=[COL_NAME_VEHICLECFGSTATES_NAME],
                                           table_list=[TABLE_NAME_VEHICLECFGSTATES],
                                           where=vehcfgid_cond)
        if len(entries) == 1:
            return entries[0][COL_NAME_VEHICLECFGSTATES_NAME]
        elif len(entries) > 1:
            tmp = "Vehicle configuration state ID '%s' " % vehcfgstateid
            tmp += "cannot be resolved because it is ambiguous. (%s)" % entries
            raise AdasDBError(tmp)

        raise AdasDBError("No resolution of '%s'. (%s)" % (vehcfgstateid, entries))

    def add_vehicle_cfg_part(self, vehiclecfgpart):
        """
        Add new vehicle cfg part to database.

        :param vehiclecfgpart: The vehicle cfg part record.
        :type vehiclecfgpart: dict
        :return: Returns the vehicle cfg ID.
        :rtype: int
        """
        partid = self._get_next_id(TABLE_NAME_PARTCFGS, COL_NAME_PARTCFGS_PARTCFGID)
        vehiclecfgpart[COL_NAME_PARTCFGS_PARTCFGID] = partid
        self.add_generic_data(vehiclecfgpart, TABLE_NAME_PARTCFGS)
        # done
        return partid

    def update_vehicle_cfg_parts(self, vehiclecfgpart, where=None):
        """
        Update vehicle cfg parts in database.

        :param vehiclecfgpart: The vehicle cfg part record.
        :type vehiclecfgpart: dict
        :param where: The condition to be fulfilled by the vehicle cfg parts to the updated.
        :type where: SQLBinaryExpression
        :return: Returns the number of affected vehicle cfg parts.
        :rtype: int
        """
        rowcount = 0
        if vehiclecfgpart is not None:
            self.update_generic_data(vehiclecfgpart, TABLE_NAME_PARTCFGS, where)
        # done
        return rowcount

    def get_vehicle_part_cfg_id(self, partcfg_name):
        """
        Get a vehicle cfg ID for a vehicle cfg name

        :param partcfg_name: The vehicle cfg name to be resolved.
        :type partcfg_name: str
        :return: Returns the ID for the vehicle cfg. if not exist raises AdasDBError
        :rtype: int
        """
        is_rooted = False
        cfg_name = partcfg_name.rstrip(PATH_SEPARATOR)
        if self.is_absolute_name(cfg_name):
            cfg_name = cfg_name.lstrip(PATH_SEPARATOR)
            is_rooted = True
        cfg_name_parts = cfg_name.split(PATH_SEPARATOR)
        # if absolute name, get vehicle ID
        # partcfgid_cond = None
        if is_rooted:
            if len(cfg_name_parts) != 3:
                tmp = "Part configuration name '%s' " % partcfg_name
                tmp += "cannot be resolved. Name must have format "
                tmp += "'/vehicle/config/part'."
                raise AdasDBError(tmp)
            vehcfgid = self.get_vehicle_cfg_id("%s%s%s" % (cfg_name_parts[0], PATH_SEPARATOR, cfg_name_parts[1]))
            vehcfgid_cond = SQLBinaryExpr(COL_NAME_PARTCFGS_VEHCFGID, OP_EQ, SQLLiteral(vehcfgid))
            cfg_name = cfg_name_parts[2].strip()
        else:
            vehcfgid_cond = None
        # get configuration id (for vehicle configuration, if absolute)
        cfg_cond = SQLBinaryExpr(SQLFuncExpr(self.db_func_map[DB_FUNC_NAME_LOWER],
                                             COL_NAME_PARTCFGS_NAME),
                                 OP_EQ, SQLLiteral(cfg_name.lower()))
        if vehcfgid_cond is not None:
            cfg_cond = SQLBinaryExpr(vehcfgid_cond, OP_AND, cfg_cond)
        entries = self.select_generic_data(select_list=[COL_NAME_PARTCFGS_VEHCFGID],
                                           table_list=[TABLE_NAME_PARTCFGS],
                                           where=cfg_cond)
        if len(entries) == 1:
            return entries[0][COL_NAME_PARTCFGS_PARTCFGID]
        elif len(entries) > 1:
            tmp = "Part configuration '%s' " % partcfg_name
            tmp += "cannot be resolved because it is ambiguous. (%s)" % entries
            raise AdasDBError(tmp)

        raise AdasDBError("No resolution of '%s'. (%s)" % (partcfg_name, entries))

    def get_vehicle_cfg_part_name(self, partid, absolute=True):
        """
        Get a part name for a ID

        :param partid: The part ID to be resolved.
        :type partid: int
        :param absolute: Set True to return absolute name
        :type absolute: bool
        :return: Returns the name for the part or None if not exists.
        """
        partid_cond = SQLBinaryExpr(COL_NAME_PARTCFGS_PARTCFGID, OP_EQ, SQLLiteral(partid))
        entries = self.select_generic_data(select_list=[COL_NAME_PARTCFGS_VEHCFGID, COL_NAME_PARTCFGS_NAME],
                                           table_list=[TABLE_NAME_PARTCFGS],
                                           where=partid_cond)
        if len(entries) == 1:
            if absolute:
                vehcfg_name = self.get_vehicle_cfg_name(entries[0][COL_NAME_PARTCFGS_VEHCFGID])
                return "%s%s%s" % (entries[0][COL_NAME_PARTCFGS_NAME], PATH_SEPARATOR, vehcfg_name)
            else:
                return entries[0][COL_NAME_PARTCFGS_NAME]
        elif len(entries) > 1:
            tmp = "Part configuration ID '%s' " % partid
            tmp += "cannot be resolved because it is ambiguous. (%s)" % entries
            raise AdasDBError(tmp)

        raise AdasDBError("No resolution of '%s'. (%s)" % (partid, entries))

    def add_vehicle_cfg_part_type(self, parttype):
        """
        Add new part type to database.

        :param parttype: The part type record.
        :type parttype: dict
        :return: Returns the part type ID.
        """
        pt_cond = SQLBinaryExpr(SQLFuncExpr(self.db_func_map[DB_FUNC_NAME_LOWER],
                                            COL_NAME_PARTTYPES_NAME),
                                OP_EQ, SQLLiteral(parttype[COL_NAME_PARTTYPES_NAME].lower()))
        entries = self.select_generic_data(table_list=[TABLE_NAME_PARTTYPES], where=pt_cond)
        if len(entries) <= 0:
            ptid = self._get_next_id(TABLE_NAME_PARTTYPES, COL_NAME_PARTTYPES_PARTTYPEID)
            parttype[COL_NAME_PARTTYPES_PARTTYPEID] = ptid
            self.add_generic_data(parttype, TABLE_NAME_PARTTYPES)
            return ptid
        else:
            if self.error_tolerance < ERROR_TOLERANCE_LOW:
                raise AdasDBError("Part type '%s' exists already in the catalog." % parttype[COL_NAME_PARTTYPES_NAME])
            else:
                warn("Part type '" + parttype[COL_NAME_PARTTYPES_NAME] + "' already exists in the catalog.")
                if len(entries) == 1:
                    return entries[0][COL_NAME_PARTTYPES_PARTTYPEID]
                elif len(entries) > 1:
                    tmp = "Part type '%s' " % (parttype[COL_NAME_PARTTYPES_NAME])
                    tmp += "cannot be resolved because it is ambiguous. (%s)" % entries
                    raise AdasDBError(tmp)

    def update_vehicle_cfg_part_types(self, vehiclecfgparttype, where=None):
        """
        Update vehicle cfg parts in database.

        :param vehiclecfgparttype: The vehicle cfg part type record.
        :type vehiclecfgparttype: dict
        :param where: The condition to be fulfilled by the vehicle cfg part types to the updated.
        :type where: SQLBinaryExpression
        :return: Returns the number of affected vehicle cfg part types.
        :rtype: int
        """
        rowcount = 0
        if vehiclecfgparttype is not None:
            self.update_generic_data(vehiclecfgparttype, TABLE_NAME_PARTTYPES, where)
        # done
        return rowcount

    def get_part_type_id(self, parttype_name):
        """
        Get a parttype ID for a parttype name

        :param parttype_name: The parttype name to be resolved.
        :type parttype_name: str
        :return: Returns the ID for the parttype. if parttype not exists raises AdasDBError.
        :rtype: int
        """
        cond = SQLBinaryExpr(SQLFuncExpr(self.db_func_map[DB_FUNC_NAME_LOWER],
                                         COL_NAME_PARTTYPES_NAME),
                             OP_EQ, SQLLiteral(parttype_name.lower()))
        entries = self.select_generic_data(select_list=[COL_NAME_PARTTYPES_PARTTYPEID],
                                           table_list=[TABLE_NAME_PARTTYPES],
                                           where=cond)
        if len(entries) == 1:
            return entries[0][COL_NAME_PARTTYPES_PARTTYPEID]
        elif len(entries) > 1:
            tmp = "Part type '%s' " % parttype_name
            tmp += "cannot be resolved because it is ambiguous. (%s)" % entries
            raise AdasDBError(tmp)

        raise AdasDBError("No resolution of '%s'. (%s)" % (parttype_name, entries))

    def get_vehicle_cfg_part_type_name(self, parttypeid):
        """
        Get a part type name for a ID

        :param parttypeid: The parttype ID to be resolved.
        :type parttypeid:
        :return: Returns the name for the part type. if name not exist then raises AdasDBError.
        :rtype: int
        """
        partid_cond = SQLBinaryExpr(COL_NAME_PARTTYPES_PARTTYPEID, OP_EQ, SQLLiteral(parttypeid))
        entries = self.select_generic_data(select_list=[COL_NAME_PARTTYPES_NAME],
                                           table_list=[TABLE_NAME_PARTTYPES],
                                           where=partid_cond)
        if len(entries) == 1:
            return entries[0][COL_NAME_PARTTYPES_NAME]
        elif len(entries) > 1:
            tmp = "Part type ID '%s' cannot be resolved because it is ambiguous. (%s)" % (parttypeid, entries)
            raise AdasDBError(tmp)

        raise AdasDBError("No resolution of '%s'. (%s)" % (parttypeid, entries))

    # ====================================================================
    # Handling of driver data
    # ====================================================================

    def add_driver(self, driver):
        """
        Add new driver to database.

        :param driver: The driver record.
        :type driver: dict
        :return: Returns the driver ID.
        :rtype: int
        """
        drv_cond = SQLBinaryExpr(SQLFuncExpr(self.db_func_map[DB_FUNC_NAME_LOWER],
                                             COL_NAME_DRIVERS_NAME),
                                 OP_EQ, SQLLiteral(driver[COL_NAME_DRIVERS_NAME].lower()))
        entries = self.select_generic_data(table_list=[TABLE_NAME_DRIVERS], where=drv_cond)
        if len(entries) <= 0:
            drvid = self._get_next_id(TABLE_NAME_DRIVERS, COL_NAME_DRIVERS_DRIVERID)
            driver[COL_NAME_DRIVERS_DRIVERID] = drvid
            self.add_generic_data(driver, TABLE_NAME_DRIVERS)
            return drvid
        else:
            if self.error_tolerance < ERROR_TOLERANCE_LOW:
                raise AdasDBError("Driver '%s' exists already in the catalog." % driver[COL_NAME_DRIVERS_NAME])
            else:
                warn("Driver '" + entries[COL_NAME_DRIVERS_NAME] + "' already exists in the catalog.")
                if len(entries) == 1:
                    return entries[0][COL_NAME_DRIVERS_DRIVERID]
                elif len(entries) > 1:
                    tmp = "Driver'%s' " % (driver[COL_NAME_DRIVERS_NAME])
                    tmp += "cannot be resolved because it is ambiguous. (%s)" % entries
                    raise AdasDBError(tmp)

    def update_drivers(self, driver, where=None):
        """
        Update drivers in database.

        :param driver: The driver record.
        :type driver: dict
        :param where: The condition to be fulfilled by the drivers to the updated.
        :type where: SQLBinaryExpression
        :return: Returns the number of affected drivers.
        :rtype: int
        """
        rowcount = 0
        if driver is not None:
            self.update_generic_data(driver, TABLE_NAME_DRIVERS, where)
        # done
        return rowcount

    def get_driver_id(self, driver_name):
        """
        Get a driver ID for a driver name

        :param driver_name: The driver name to be resolved.
        :type driver_name: str
        :return: Returns the ID for the driver. if doesn't exist then raises AdasDBError.
        :rtype: int
        """
        cond = SQLBinaryExpr(SQLFuncExpr(self.db_func_map[DB_FUNC_NAME_LOWER],
                                         COL_NAME_DRIVERS_NAME),
                             OP_EQ, SQLLiteral(driver_name.lower()))
        entries = self.select_generic_data(select_list=[COL_NAME_DRIVERS_DRIVERID],
                                           table_list=[TABLE_NAME_DRIVERS],
                                           where=cond)
        if len(entries) == 1:
            return entries[0][COL_NAME_DRIVERS_DRIVERID]
        elif len(entries) > 1:
            raise AdasDBError("Driver '%s' cannot be resolved because it is ambiguous. (%s)" % (driver_name, entries))

        raise AdasDBError("No resolution of '%s'. (%s)" % (driver_name, entries))

    def get_driver_name(self, driverid):
        """
        Get a driver name for a ID

        :param driverid: The driver ID to be resolved.
        :type driverid: int
        :return: Returns the name for the driver. if doesn't exist then raises AdasDBError.
        :rtype: str
        """
        drv_cond = SQLBinaryExpr(COL_NAME_DRIVERS_DRIVERID, OP_EQ, SQLLiteral(driverid))
        entries = self.select_generic_data(select_list=[COL_NAME_DRIVERS_NAME],
                                           table_list=[TABLE_NAME_DRIVERS],
                                           where=drv_cond)
        if len(entries) == 1:
            return entries[0][COL_NAME_DRIVERS_NAME]
        elif len(entries) > 1:
            raise AdasDBError("Driver ID '%s' cannot be resolved because it is ambiguous. (%s)" % (driverid, entries))

        raise AdasDBError("No resolution of '%s'. (%s)" % (driverid, entries))

    # ====================================================================
    # Handling of auxiliary data
    # ====================================================================

    def add_file_state(self, filestate):
        """
        Add new file state to database.

        :param filestate: The file state record.
        :type filestate: dict
        :return: Returns the file state ID.
        :rtype: int
        """
        if filestate is not None:
            fs_cond = SQLBinaryExpr(SQLFuncExpr(self.db_func_map[DB_FUNC_NAME_LOWER],
                                                COL_NAME_FILESTATES_NAME),
                                    OP_EQ, SQLLiteral(filestate[COL_NAME_FILESTATES_NAME].lower()))
            entries = self.select_generic_data(table_list=[TABLE_NAME_FILESTATES], where=fs_cond)
            if len(entries) <= 0:

                if self.sub_scheme_version < CAT_ACTIVE_VERSION:
                    fsid = self._get_next_id(TABLE_NAME_FILESTATES, COL_NAME_FILESTATES_FILESTATEID)
                    filestate[COL_NAME_FILESTATES_FILESTATEID] = fsid
                else:
                    self.add_generic_data(filestate, TABLE_NAME_FILESTATES)
                    entries = self.select_generic_data(table_list=[TABLE_NAME_FILESTATES], where=fs_cond)
                    fsid = entries[0][COL_NAME_FILESTATES_FILESTATEID]

                return fsid
            else:
                if self.error_tolerance < ERROR_TOLERANCE_LOW:
                    tmp = "File state '%s' exists already in the catalog." % filestate[COL_NAME_FILESTATES_NAME]
                    raise AdasDBError(tmp)
                else:
                    warn("File state '" + filestate[COL_NAME_FILESTATES_NAME] + "' already exists in the catalog.")
                    if len(entries) == 1:
                        return entries[0][COL_NAME_FILESTATES_FILESTATEID]
                    elif len(entries) > 1:
                        tmp = "File state '%s' " % (filestate[COL_NAME_FILESTATES_NAME])
                        tmp += "cannot be resolved because it is ambiguous. (%s)" % entries
                        raise AdasDBError(tmp)
        return None

    def update_file_states(self, filestate, where=None):
        """
        Update file states in database.

        :param filestate: The file state record with new or modified values
        :type filestate: dict
        :param where: The condition to be fulfilled by the file states to the updated.
        :type where: SQLBinaryExpression
        :return: Returns the number of affected file states.
        :rtype: int
        """
        rowcount = 0
        if filestate is not None:
            self.update_generic_data(filestate, TABLE_NAME_FILESTATES, where)
        # done
        return rowcount

    def get_file_state_id(self, fstate_name):
        """
        Get a file state ID for a file state name

        :param fstate_name: The file state name to be resolved.
        :type fstate_name: str
        :return: Returns the ID for the file stat. if file state not exists then raise AdasDBError.
        :rtype: int
        """
        cond = SQLBinaryExpr(SQLFuncExpr(self.db_func_map[DB_FUNC_NAME_LOWER],
                                         COL_NAME_FILESTATES_NAME),
                             OP_EQ, SQLLiteral(fstate_name.lower()))
        entries = self.select_generic_data(select_list=[COL_NAME_FILESTATES_FILESTATEID],
                                           table_list=[TABLE_NAME_FILESTATES],
                                           where=cond)
        if len(entries) == 1:
            return entries[0][COL_NAME_FILESTATES_FILESTATEID]
        elif len(entries) > 1:
            tmp = "File state '%s' cannot be resolved because it is ambiguous. (%s)" % (fstate_name, entries)
            raise AdasDBError(tmp)

        raise AdasDBError("No resolution of '%s'. (%s)" % (fstate_name, entries))

    def get_file_state_name(self, fstateid):
        """
        Get a file state name for a file state ID

        :param fstateid: The file state ID to be resolved.
        :type fstateid: int
        :return: Returns the name for the file state or None. if file state not exists then raise AdasDBError.
        :rtype: str
        """
        cond = SQLBinaryExpr(COL_NAME_FILESTATES_FILESTATEID, OP_EQ, SQLLiteral(fstateid))
        entries = self.select_generic_data(select_list=[COL_NAME_FILESTATES_NAME],
                                           table_list=[TABLE_NAME_FILESTATES],
                                           where=cond)
        if len(entries) == 1:
            return entries[0][COL_NAME_FILESTATES_NAME]
        elif len(entries) > 1:
            raise AdasDBError("File state '%s' cannot be resolved because it is ambiguous. (%s)" % (fstateid, entries))

        raise AdasDBError("No resolution of '%s'. (%s)" % (fstateid, entries))

    # ====================================================================
    # Rec File Catalog Helper Functions
    # ====================================================================

    @staticmethod
    def is_absolute_name(name):
        """
        Check if a name is absolute.Absolute names start with path separator.

        :param name: The name to check.
        :type name: str
        :return: Returns true if the name is an absolute path name.
        :rtype: bool
        """
        if name is None:
            raise AdasDBError("Invalid name '%s'." % name)
        return name.startswith(PATH_SEPARATOR)

    @staticmethod
    def is_basic_name(name):
        """
        Check if a name is basic (doesnt contain path separator).

        :param name: The name to check.
        :type name: str
        :return: Returns true if the name is an basic name.
        :rtype: bool
        """
        if name is None:
            raise AdasDBError("Invalid name '%s'." % name)
        return name.find(PATH_SEPARATOR)
