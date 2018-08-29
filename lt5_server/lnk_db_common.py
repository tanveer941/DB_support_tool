"""

Common function database declarations and base class for specialized sub-scheme implementations.

**not for direct use**


"""
# pylint: disable=W0102,W0702,R0903,R0912,R0914,R0915
# - import Python modules ---------------------------------------------------------------------------------------------
from os import path, stat, chmod, environ
from sys import maxint
from stat import ST_MODE, S_IWRITE
from datetime import datetime
from re import compile as recomp, search, finditer
from platform import release
from logging import INFO
from cx_Oracle import connect as cxconnect
from pyodbc import connect as pyconnect
from adodbapi import connect as adoconnect, adUseServer
from sqlite3 import connect as sqconnect, register_adapter, register_converter
from types import StringTypes
from collections import OrderedDict

# - import STK modules ------------------------------------------------------------------------------------------------
from lnk_db_sql import GenericSQLStatementFactory, SQLDate, SQLFuncExpr, SQLListExpr, SCHEMA_PREFIX_SEPARATOR, \
    TABLE_PREFIX_SEPARATOR, SQL_TABLENAMES, SQL_DATETIME, SQL_DT_EXPR, SQL_COLUMNS, CX_VARS, SQ_VARS, DBTYPE, \
    IDENT_SPACE, CONN_STRING
# from tds_stk.error import StkError
# from tds_stk.util.logger import Logger
# from tds_stk.util.helper import deprecated, arg_trans

# - defines -----------------------------------------------------------------------------------------------------------
DEFAULT_MASTER_SCHEMA_PREFIX = None
DEFAULT_MASTER_DSN = None  # CLEO is not used for Oracle 11g
DEFAULT_MASTER_DBQ = "racadmpe"  # use sqlnet.ora and ldap.ora instead of "lidb003:1521/cleo"
DEFAULT_MASTER_DRV_XP = "Oracle in instantclient11_1"  # Released for WinXP
DEFAULT_MASTER_DRV_W7 = "Oracle in instantclient_11_2"  # Released for Win7
DEFAULT_SLAVE_DATA_PROVIDER_XP = "Microsoft.SQLSERVER.MOBILE.OLEDB.3.0"  # Released for WinXP
DEFAULT_SLAVE_DATA_PROVIDER_W7 = "Microsoft.SQLSERVER.CE.OLEDB.3.5"  # Released for Win7

if release() == 'XP':
    DEFAULT_MASTER_DRV = DEFAULT_MASTER_DRV_XP
    DEFAULT_SLAVE_DATA_PROVIDER = DEFAULT_SLAVE_DATA_PROVIDER_XP
else:
    DEFAULT_MASTER_DRV = DEFAULT_MASTER_DRV_W7
    DEFAULT_SLAVE_DATA_PROVIDER = DEFAULT_SLAVE_DATA_PROVIDER_W7

DEFAULT_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"

ERROR_TOLERANCE_NONE = 0
ERROR_TOLERANCE_LOW = 1
ERROR_TOLERANCE_MED = 2
ERROR_TOLERANCE_HIGH = 3

DB_FUNC_NAME_MIN = "DB_FUNC_NAME_MIN"
DB_FUNC_NAME_MAX = "DB_FUNC_NAME_MAX"
DB_FUNC_NAME_AVG = "DB_FUNC_NAME_AVG"
DB_FUNC_NAME_LOWER = "DB_FUNC_NAME_LOWER"
DB_FUNC_NAME_UPPER = "DB_FUNC_NAME_UPPER"
DB_FUNC_NAME_GETDATE = 'DB_FUNC_NAME_GETDATE'
DB_FUNC_NAME_SUBSTR = "DB_FUNC_NAME_SUBSTR"

# Common Tables
TABLE_NAME_VERSION = "Versions"

COL_NAME_SUBSCHEMEPREF = "SUBSCHEMEPREF"
COL_NAME_SUBSCHEMEVERSION = "SUBSCHEMEVERSION"

# Roles
ROLE_DB_ADMIN = "ADMIN"
ROLE_DB_USER = "USER"

LAST_ROWID = ";SELECT last_insert_rowid()"

SQLITE_FILE_EXT = (".db", ".db3", ".sqlite",)
SDF_FILE_EXT = (".sdf",)


class DefDict(OrderedDict):
    """I'm a default dict, but with my own missing method.

    .. python::
        from stk.util.helper import DefDict

        # set default to -1
        dct = DefDict(-1)
        # create key / value pairs: 'a'=0, 'b'=1, 'c'=-1, 'd'=-1
        dct.update((['a', 0], ['b', 1], 'c', 'd']))
        # prints 1
        print(dct['b'])
        # prints -1 (3rd index)
        print(dct[2])

    :param default: default value for missing key
    :type default: object
    """

    def __init__(self, default=None):
        OrderedDict.__init__(self)
        self._def = default

    def __getitem__(self, item):
        return self.values()[item] if type(item) == int else OrderedDict.__getitem__(self, item)

    def __missing__(self, _):
        return self._def


def arg_trans(mapping, *args, **kwargs):
    """argument transformation into dict with defaults

    :param mapping: list of argument names including their defaults
    :type mapping: list
    :param args: argument list
    :type args: list
    :param kwargs: named arguments with defaults
    :type kwargs: dict
    :return: dict with transfered arguments
    :rtype: dict
    """
    dflt = kwargs.pop('default', None)
    newmap = DefDict(dflt)
    k, l = 0, len(args)
    # update from mapping
    for i in mapping:
        key = i[0] if type(i) in (tuple, list) else i
        val = args[k] if l > k else (i[1] if type(i) in (tuple, list) else dflt)
        newmap[key] = val
        k += 1
    # update rest from args
    while k < l:
        newmap["arg%d" % k] = args[k]
        k += 1

    # update left over from kwargs
    newmap.update(kwargs)
    return newmap

# - classes -----------------------------------------------------------------------------------------------------------
class AdasDBError(Exception):
    """Base of all ConstraintDatabase errors"""
    pass


class AdasDBNotImplemented(AdasDBError):
    """Feature is not implemented"""
    pass


class BaseDB(object):
    """Base implementation of the Database Interface"""

    # ====================================================================
    # Handling of database
    # ====================================================================

    def __init__(self, *args, **kw):
        """base database class for underlying subpackages like cat, gbl, lbl, etc.
        This class can also be used directly.

        :param args: list of additional arguments: table_prefix, stmt_factory, error_tolerance

        :keyword db_connection: The database connection to be used
        :type db_connection: str, cx_oracle.Connection, pydodbc.Connection, sqlite3.Connection, sqlce.Connection
        :param args: list of additional arguments: table_prefix, stmt_factory, error_tolerance
        :keyword table_prefix: The table name prefix which is usually master schema name
        :type table_prefix: str
        :keyword stmt_factory: The SQL statement factory to be used
        :type stmt_factory: GenericSQLStatementFactory
        :keyword strIdent: string identifier used from subclass for identification
        :type strIdent: str
        :keyword error_tolerance: set log level for debugging purposes
        :type error_tolerance: int
        :keyword loglevel: being able to reduce logging, especially with scripts
        :type loglevel: Integer, see logging class for reference
        :keyword autocommit: boolean value to determine wether to commit after an insert or update automatically
        :type autocommit: bool
        :keyword foreign_keys: set to True when sqlite DB connection should take care of foreign key relations
        :type foreign_keys: bool
        :keyword arraysize: only for cxoracle: '...This read-write attribute specifies the number of rows to fetch
                            at a time internally...', default is 50, a good value should be about 500
        :type arraysize: int
        """
        opts = arg_trans(["db_connection", ("table_prefix", None), ("sql_factory", GenericSQLStatementFactory()),
                          ("error_tolerance", ERROR_TOLERANCE_NONE), ("strIdent", "")], *args, **kw)
        # self._log = Logger(self.__class__.__name__, kw.pop("loglevel", INFO))
        self._table_prefix = "" if opts["table_prefix"] is None else opts["table_prefix"].rstrip('.')
        self._sql_factory = opts["sql_factory"]
        self._ident = opts["strIdent"]
        self.error_tolerance = opts["error_tolerance"]
        self._enable_fks = opts.pop("foreign_keys", False)
        self._arraysize = None

        self._db_type = None
        self.db_func_map = {}
        self.role = None
        self.sync_slave_reload_all = False

        self._connstr = [None, self._table_prefix]
        self._func_params = recomp(r"(\(.*\))")
        self._auto_commit = opts.pop("autocommit", False)
        self._direct_conn = False

        self._db_connection = None

        # connect to DB and configure some DB specifics like role, date, time, etc.
        db_connection = opts["db_connection"]
        if type(db_connection) in StringTypes:
            self._connstr[0] = db_connection
            self._direct_conn = True
            cslower = db_connection.lower()
            if db_connection in CONN_STRING:
                self._table_prefix = CONN_STRING[db_connection][1]
                db_connection = CONN_STRING[db_connection][0]
                cslower = db_connection.lower()

            if any(cslower.endswith(ext) for ext in SQLITE_FILE_EXT) and path.isfile(cslower) or cslower == ":memory:":
                try:
                    if not stat(db_connection)[ST_MODE] & S_IWRITE:
                        chmod(db_connection, S_IWRITE)
                except:
                    pass

                self._db_connection = sqconnect(db_connection)
                self._db_connection.text_factory = str
                register_adapter(long, lambda l: str(l) if long(l) > maxint else l)
                register_converter("long", lambda l: long(l))

            elif any(cslower.endswith(ext) for ext in SDF_FILE_EXT):
                if cslower.startswith('data source='):
                    fname = db_connection[cslower.find("data source=") + 12:].partition(";")[0].strip()
                else:
                    fname = cslower
                    db_connection = 'data source=' + db_connection
                if not stat(fname)[ST_MODE] & S_IWRITE:
                    chmod(fname, S_IWRITE)
                if "provider=" not in cslower:
                    db_connection += (";Provider=%s" % DEFAULT_SLAVE_DATA_PROVIDER)
                self._db_connection = adoconnect(db_connection)
                self._db_connection.adoConn.CursorLocation = adUseServer
            else:
                # ex: DBQ=racadmpe;Uid=DEV_MFC31X_ADMIN;Pwd=MFC31X_ADMIN
                # init argument part, split up and use it for cxconnect

                # try:
                if cxconnect is not None and "uid" in cslower and "pwd" in cslower:
                    args = {}
                    for arg in db_connection.split(';'):
                        part = arg.split('=', 2)
                        args[part[0].strip().lower()] = part[1].strip()
                    self._db_connection = cxconnect(args['uid'], args['pwd'], args.pop('dbq', 'racadmpe'),
                                                    threaded=opts.pop('threaded', False))
                else:
                    self.db_connection = pyconnect(db_connection)
                # except:
                #     raise AdasDBError("couldn't open database")
                self._arraysize = opts.pop("arraysize", None)

            self._db_type = DBTYPE.index(str(self._db_connection)[1:].split('.')[0])
        else:
            self._db_connection = db_connection
            self._connstr[0] = str(db_connection)
            self._db_type = DBTYPE.index(str(self._db_connection)[1:].split('.')[0])

        self.db_func_map[DB_FUNC_NAME_MIN] = "MIN"
        self.db_func_map[DB_FUNC_NAME_MAX] = "MAX"
        self.db_func_map[DB_FUNC_NAME_LOWER] = "LOWER"
        self.db_func_map[DB_FUNC_NAME_UPPER] = "UPPER"
        self.db_func_map[DB_FUNC_NAME_GETDATE] = "GETDATE()"

        if self._db_type >= 2:
            self._db_type = -1
            if self._table_prefix != "":
                self.execute("ALTER SESSION SET CURRENT_SCHEMA = %s" % self._table_prefix)
            username = self._table_prefix if len(self._table_prefix) > 0 else "SELECT USER FROM DUAL"
            self._tablequery = SQL_TABLENAMES[-1].replace("$NM", username).replace("$TS", self._ident[2:])
            self._columnquery = SQL_COLUMNS[-1][1].replace("$NM", username)
            # username = self.execute("SELECT sys_context('USERENV', 'SESSION_USER') FROM dual")#[0][0]
            # username = 'VAL_GLOBAL_PWUSER_RES'
            # print "username --> ", username
            self.role = ROLE_DB_ADMIN if ROLE_DB_ADMIN in username.upper() else ROLE_DB_USER

            self.execute("ALTER SESSION SET NLS_COMP=LINGUISTIC")
            self.execute("ALTER SESSION SET NLS_SORT=BINARY_CI")
        else:
            self.role = ROLE_DB_ADMIN
            self._tablequery = SQL_TABLENAMES[self._db_type].replace("$TS", self._table_prefix)
            self._columnquery = SQL_COLUMNS[self._db_type][1]

        if self._enable_fks and self._db_type == 0:  # off per default to keep unittests running
            self.execute("PRAGMA FOREIGN_KEYS = ON")

        self.date_time_format = DEFAULT_DATETIME_FORMAT
        # retrieve the sub schema version
        try:
            self._sub_versions = {i[0]: i[1] for i in self.execute("SELECT SUBSCHEMEPREF, SUBSCHEMEVERSION "
                                                                   "FROM VERSIONS")}
        except:
            self._sub_versions = {}

        # retrieve the who is the current user gbl_user table
        try:
            self.current_gbluserid = self.execute("SELECT USERID FROM GBL_USERS WHERE LOGINNAME = $CU")[0][0]
        except:
            self.current_gbluserid = None

    def __del__(self):
        """if not Terminate(d) yet, we should do a bit here, I think
        """
        if self._direct_conn:
            self.close()

    def __str__(self):
        """return some self descriptive information
        """
        return ("connection: %s, DB prefix: %s, type: %s" %
                (str(self._db_connection), self._table_prefix, DBTYPE[self._db_type]))

    def __enter__(self):
        """being able to use with statement
        """
        return self

    def __exit__(self, *_):
        """close connection"""
        if self._direct_conn:
            self.close()

    @property
    def foreign_keys(self):
        """retrieve foreign_keys settings"""
        return self._enable_fks if self._db_type == 0 else True

    @foreign_keys.setter
    def foreign_keys(self, switch):
        """switch on or off forgeign key support for sqlite

        :param switch: set foreign_keys ON / OFF (True / False)
        """
        if self._db_type == 0:
            self.execute("PRAGMA FOREIGN_KEYS = " + ("ON" if switch else "OFF"))

    @property
    def connection(self):
        """internal connection string, returns [<connection string>, <table prefix>]"""
        return self._connstr

    @property
    def db_type(self):
        """return a string about what type of DB we have here."""
        return self._db_type, DBTYPE[self._db_type]

    @property
    def ident_str(self):
        """return my own ident
        """
        return self._ident

    def close(self):
        """Terminate the database proxy"""
        if self._db_connection is not None:
            self._db_connection.close()
        self._db_connection = None
        self._connstr[0] = None

    def commit(self):
        """Commit the pending transactions"""
        self._db_connection.commit()

    def rollback(self):
        """Rollback the pending transactions"""
        self._db_connection.rollback()

    def vacuum(self):
        """vacuums sqlite DB"""
        if self._db_type == 0 and self.execute("pragma auto_vacuum")[0][0] == 0:
            self.execute("vacuum")

    def cursor(self):
        """Get the current cursor of DB: only use it if you *really* know what you're doing!!!
        """
        return self._db_connection.cursor()

    def make_var(self, vartype):
        """
        Create a cx_Oracle variable.

        :param vartype: Create a variable associated with the cursor of the given type and
                        characteristics and return a variable object.
        :type vartype: string. Could be 'number' or 'blob'.
        :rtype: object

        :raises AdasDBError: on unknow connection (not oracle, nor sqlite)
        """
        if self._db_type == -1:
            return self._db_connection.cursor().var(CX_VARS[vartype.lower()])
        elif self._db_type == 0:
            return SQ_VARS[vartype.lower()]
        else:
            raise AdasDBError("connection doesn't support var creation!")

    @property
    def table_prefix(self):
        """returns tables prefix used for select queries"""
        return self._table_prefix

    # ====================================================================
    # Handling of generic data.
    # ====================================================================

    def select_generic_data(self, *args, **kwargs):
        """
        Select generic data from a table.

        :param args: list of arguments, covered by kwargs in following order
        :param kwargs: optional arguments
        :keyword select_list: list of selected table columns (list)
        :keyword table_list: list of table names from which to select data (list | None)
        :keyword where: additional condition that must hold for the scenarios to be returned (SQLBinaryExpression)
        :keyword group_by: expression defining the columns by which selected data is grouped (list | None)
        :keyword having: expression defining the conditions that determine groups (SQLExpression)
        :keyword order_by: expression that defines the order of the returned records (list | None)
        :keyword distinct_rows: set True or list of rows, if only distinct rows shall be returned (bool | list)
        :return: Returns the list of selected data.
        :rtype: list
        """
        opt = arg_trans([['select_list', ['*']], 'table_list', 'where', 'group_by', 'having', 'order_by',
                         ['distinct_rows', False], ['sqlparams', {}]], *args, **kwargs)
        sql_select_stmt = self._sql_factory.GetSelectBuilder()
        sql_select_stmt.select_list = opt[0]
        sql_select_stmt.table_list = opt[1]
        sql_select_stmt.where_condition = opt[2]
        sql_select_stmt.group_by_list = opt[3]
        sql_select_stmt.having_condition = opt[4]
        sql_select_stmt.order_by_list = opt[5]
        sql_select_stmt.distinct_rows = opt[6]
        sql_param = opt[7]
        sql_param['incDesc'] = True
        # print "str(sql_select_stmt) :: ", str(sql_select_stmt)
        # print "sql_param >> ", sql_param
        return self.execute(str(sql_select_stmt), **sql_param)

    def select_generic_data_compact(self, *args, **kwargs):
        """Select generic data from a table.

        :param args: list of arguments, covered by kwargs in following order
        :param kwargs: optional arguments
        :keyword select_list: List of selected table columns.
        :keyword table_list: List of table names from which to select data.
        :keyword where: The additional condition that must hold for the scenarios to be returned.
        :keyword group_by: Expression defining the columns by which selected data is grouped.
        :keyword having: Expression defining the conditions that determine groups.
        :keyword order_by: Expression that defines the order of the returned records.
        :keyword distinct_rows: Set True, if only distinct rows shall be returned.
        :return: Returns the list of selected data.
        """
        opt = arg_trans([['select_list', ['*']], 'table_list', 'where', 'group_by', 'having', 'order_by',
                         ['distinct_rows', False], ['sqlparams', {}]], *args, **kwargs)
        sql_select_stmt = self._sql_factory.GetSelectBuilder()
        sql_select_stmt.select_list = opt[0]
        sql_select_stmt.table_list = opt[1]
        sql_select_stmt.where_condition = opt[2]
        sql_select_stmt.group_by_list = opt[3]
        sql_select_stmt.having_condition = opt[4]
        sql_select_stmt.order_by_list = opt[5]
        sql_select_stmt.distinct_rows = opt[6]
        sql_param = opt[7]
        sql_param['iterate'] = True
        # print "str(sql_select_stmt) > ", str(sql_select_stmt), sql_param
        cursor = self.execute(str(sql_select_stmt), **sql_param)
        # print "cursor : ", cursor
        try:
            rows = cursor.fetchall()
            field_list = []
            for column in cursor.description:
                field_list.append(column[0].upper())
            # print "field_list : ", field_list
        except:
            raise
        finally:
            cursor.close()
        # If the select is just for one column, cursor produces bad results.
        if len(opt['select_list']) == 1 and opt['select_list'][0] != "*":
            row_list = [[row[0]] for row in rows]
        else:
            row_list = rows
        # done
        return [field_list, row_list]

    def add_generic_data(self, record, table_name, returning=None):
        """Add generic data to database.

        :param record: The data record to be added to the database.
        :param table_name: The name of the table into which to add the data.
        :param returning: column of autoincrement index of newly added record to get in return
        """
        # build statement
        sql_insert_stmt = self._sql_factory.GetInsertBuilder()
        sql_insert_stmt.table_name = table_name
        sql_insert_stmt.assign_items = record
        sql_insert_stmt.returning = returning
        return self.execute(str(sql_insert_stmt), incDesc=True)

    def add_generic_compact_prepared(self, col_names, values, table_name):
        """
        Add Generic data with prepared statement with compact data as input

        :param col_names: list of column names for values are specified
        :type col_names: list
        :param values: list of tuple contain tuple length and order must be align with col_names
        :type values: list
        :param table_name: Name of the table
        :type table_name: String
        """
        sql_insert_prep_stmt = self._sql_factory.GetPreparedInsertBuilder()
        sql_insert_prep_stmt.table_name = table_name
        sql_insert_prep_stmt.assign_items = col_names
        self.execute(str(sql_insert_prep_stmt), insertmany=values)

    def add_generic_data_prepared(self, records, table_name):
        """Add generic data to database.

        :param records: The data record to be added to the database.
        :type records: list of dict
        :param table_name: The name of the table into which to add the data.
        """

        if len(records) and type(records) is list:
            # build statement
            rowcount = 0
            sql_insert_prep_stmt = self._sql_factory.GetPreparedInsertBuilder()
            sql_insert_prep_stmt.table_name = table_name
            sql_insert_prep_stmt.assign_items = records[0].keys()
            stmt = str(sql_insert_prep_stmt)
            values = [tuple(record.values()) for record in records]
            # insert data
            cursor = self._db_connection.cursor()
            try:
                # self._log.debug(stmt)
                # print "stmt::", stmt, values
                cursor.executemany(stmt, values)
                # rowcount = cursor.rowcount
            except Exception, ex:
                self._log.error(stmt)
                self._log.exception(str(ex))
                raise
            finally:
                cursor.close()
            return rowcount

    def update_generic_data(self, record, table_name, where=None, sqlparams={}):
        """
        Update an existing record in the database.

        :param record: The data to be updated.
        :type record: dict
        :param table_name: The name of the table.
        :type table_name: str
        :param where: The condition to be fulfilled by the states to be updated.
        :type where: SQLBinaryExpression
        :return: Returns the number of affected rows.
        :rtype: Integer
        """
        sql_update_stmt = self._sql_factory.GetUpdateBuilder()
        sql_update_stmt.table_name = table_name
        sql_update_stmt.assign_items = record
        sql_update_stmt.where_condition = where
        sqlparams['incDesc'] = True
        try:
            return self.execute(str(sql_update_stmt), **sqlparams)
        except:
            self._log.exception(str(sql_update_stmt))
            raise
        return 0

    def delete_generic_data(self, table_name, where=None, sqlparams={}):
        """Delete records from a table.

        :param table_name: The name of the table to deleted the data from.
        :param where: The condition to be fulfilled by the states to be deleted.
        :return: Returns the number of affected rows.
        """
        sql_stmt = self._sql_factory.GetDeleteBuilder()
        sql_stmt.table_name = table_name
        sql_stmt.where_condition = where
        stmt = str(sql_stmt)
        # delete data
        rowcount = 0
        cursor = self._db_connection.cursor()
        try:
            self._log.debug(stmt)
            cursor.execute(stmt, sqlparams)
            # rowcount = cursor.rowcount      #was commented out, but e.g. used in cgeb_fctlabel -> ?
        except:
            self._log.exception(stmt)
            # raise AdasDBError(stmt)
        finally:
            cursor.close()
        # done
        return rowcount

    def _get_next_id(self, table_name, col_name):
        """Get the next ID of a column

        :param table_name: The name of the table.
        :param col_name: The name of the column to get the next ID. The column type must be integral
        :return: Returns the next available ID.
        """
        return self.execute("SELECT NVL(MAX(%s), -1) + 1 FROM %s" % (col_name, table_name))[0][0]

    @property
    def auto_commit(self):
        """states if after each insert / update / change a commit should be placed or not
        """
        return self._auto_commit

    @auto_commit.setter
    def auto_commit(self, val):
        """sets internal auto commit flag on executions

        :param val: do you want to commit at each statement, set it to True or False
        :type val: bool
        """
        self._auto_commit = val

    def execute(self, statement, *args, **kwargs):
        """
        Execute SQL statement(s). Multiple statements can be semicolon (;) separated.
        Below listed arguments are in use, others are directly passed through to the DB cursor

        :param statement: sql command to be executed
        :type statement: str
        :param args: Variable length argument list to execute method of cursor.
        :param kwargs: Arbitrary keyword arguments, whereas insertmany, incDesc, iterate and id are extracted
                       and rest flows into cursor's execute method.

        :keyword insertmany: supports insertion of many items which improves speed, here you need to provide a
                             list of tuples to be inserted, defaults to None, valid param type is bool.
        :keyword incDesc: include the description when doing a select as first row, defaults to False, type is bool
        :keywrod iterate: with it you can iterate over cursor(), but take care to close cursor afterwards

        :return: in case of an insert or update statement, column count (int) is returned. Within an insert statement,
                 you can specify a returning row id, e.g. *insert into table values (7, 'dummy') returning idx*
                 whereas idx would be primary key of table which is autoincreased and it's new value is returned.

                 in case of an select statement, all rows (list) are returned.
                 When ``incDesc`` is set to True, a list of dicts for each row is returned

        :raises AdasDBError: when statement cannot be executed
        """
        # ommit: insertmany, incDesc, iterate, id

        insertmany = kwargs.pop('insertmany', None)
        incdesc = kwargs.pop('incDesc', False)
        iterate = kwargs.pop('iterate', False)
        commit = kwargs.pop('commit', self._auto_commit)
        cursor = None
        # helper to return proper insertion ID
        pat = search(r"(?i)returning\s(\w*)$", statement)
        retlastid = False
        if pat is not None:
            if self._db_type == -1:
                statement = statement[:pat.regs[1][0]] + pat.groups()[0] + " INTO :id"
                rid = self.make_var('number')
                kwargs['id'] = rid
            elif self._db_type == 0:
                statement = statement[:pat.start() - 1]
                retlastid = True
        # we can also replace $DT as current date time, $CD as current date and $CT as current time
        if "$" in statement:
            statement = statement.strip().replace("$DT", "SYSDATE" if self._db_type == -1 else "CURRENT_TIMESTAMP")\
                .replace("$CD", "CURRENT_DATE")\
                .replace("$CT", "CURRENT_TIMESTAMP" if self._db_type == -1 else "CURRENT_TIME") \
                .replace(" None ", " null ") \
                .replace("$ST", "SYSTIMESTAMP" if self._db_type == -1 else "CURRENT_TIMESTAMP")
            if "$CU" in statement:
                kwargs['usr'] = environ['username']
                statement = statement.replace("$CU", ":usr")
            if "$UID" in statement:
                kwargs['usrid'] = self.current_gbluserid
                statement = statement.replace("$UID", ":usrid")

        if self._db_type == 0:
            statement = statement.replace('NVL', 'IFNULL')
            if len(kwargs):
                args = []
                for i in finditer(r':(\w+)\b', statement):
                    statement = statement.replace(":" + i.group(1), "?", 1)
                    args.append(kwargs[i.group(1)])
                args = tuple(args)
        try:
            stmt = ""
            records = []
            cursor = self._db_connection.cursor()
            if self._arraysize:
                cursor.arraysize = self._arraysize

            for stmt in ([statement.strip()] if search(r'(?i)^(begin|declare)\s', statement)
                         else statement.split(';')):
                if len(stmt) == 0:
                    continue

                stmt = stmt.strip()
                # self._log.debug(stmt)

                # remove keyword to get more ease in checking later on
                cmd = stmt.split(' ', 1)[0].lower()

                # exec
                if cmd == "insert" and insertmany is not None and all(type(i) == tuple for i in insertmany):
                    cursor.executemany(stmt, insertmany)
                else:
                    if self._db_type == 0:
                        cursor.execute(stmt, args)
                    else:
                        cursor.execute(stmt, **kwargs)
                if cmd in ("select", "pragma", "with", "declare"):
                    if type(records) == int:
                        records = int(cursor.fetchone()[0])
                    elif incdesc and cursor.description is not None:
                        desc = [d[0].upper() for d in cursor.description]
                        for rec in cursor:
                            records.append({desc[i]: rec[i] for i in xrange(len(desc))})
                    elif iterate:
                        records = cursor
                    else:
                        records.extend(cursor.fetchall())
                elif cmd == "execute":
                    # remove 'execute ', find & remove brackets, split args, strip quotes and recombine
                    params = [ii.strip(" '") for ii in self._func_params.findall(stmt[8:])[0].strip('()').split(',')]
                    records = cursor.callproc(stmt[0:stmt.find('(')].strip(), params)
                elif cmd == "func":
                    records = cursor.callfunc(stmt.split(' ')[1], *args, **kwargs)
                elif cmd == "proc":
                    records = cursor.callproc(stmt.split(' ')[1], *args, **kwargs)
                else:
                    records = cursor.rowcount
                    if commit:  # commit if not switched off
                        self._db_connection.commit()
                    if pat is not None and self._db_type == -1:  # grab the returning ID if oracle
                        records = int(rid.getvalue())
                    elif retlastid:
                        records = cursor.lastrowid

        except Exception as ex:
            iterate = False
            # raise AdasDBError(stmt + ": " + str(ex))
        finally:
            if not iterate and cursor:
                cursor.close()

        # done
        # print "records : ", records, "\n"
        return records

    def sql(self, statement, *args, **kw):
        """Execute SQL statement(s) more simpler / faster.
        Below listed arguments are in use, others are directly passed through to the DB cursor

        :param statement: sql command to be executed
        :type statement: str
        :param args: additional arguments to connection's execute
        :param kw: keyword arguments to connection's execute
        :return: list of records (tuples usually)
        """
        iterate = kw.pop('iterate', False)
        cursor = self._db_connection.cursor()
        try:
            # cursor.execute(statement, *args, **kw)
            # records = cursor.fetchall()
            # remove keyword to get more ease in checking later on
            cmd = statement.split(' ', 1)[0].lower()

            if cmd in ("select", "with"):
                cursor.execute(statement, *args, **kw)
                if iterate:
                    records = cursor
                else:
                    records = cursor.fetchall()
            elif cmd in ("alter", "insert", "update", "delete"):
                cursor.execute(statement, *args, **kw)
                records = cursor.rowcount
                if self._auto_commit:
                    self._db_connection.commit()
            elif cmd == "proc":
                records = cursor.callproc(statement.split(' ', 1)[1], *args, **kw)
            elif cmd == "func":
                records = cursor.callfunc(statement.split(' ', 1)[1], *args, **kw)
            else:
                raise AdasDBError("unknown command")
        except Exception as ex:
            raise AdasDBError("DB exception: " + str(ex))
        finally:
            if not iterate:
                cursor.close()

        # done
        return records

    # ====================================================================
    # Version methods
    # ====================================================================

    @property
    def sub_scheme_version(self):
        """Returns version number of the component as int.
        """
        return self._sub_versions.get(IDENT_SPACE[self._ident])

    def sub_scheme_versions(self):
        """Returns all the subschema version from validation database.
        """
        return [{COL_NAME_SUBSCHEMEVERSION: v, COL_NAME_SUBSCHEMEPREF: k} for k, v in self._sub_versions.iteritems()]

    # ====================================================================
    # aditive methods
    # ====================================================================

    def timestamp_to_date(self, timestamp):
        """ Convert a timestamp to a date-time of the database

        :param timestamp: The timestamp to convert
        :return: Returns the date-time expression
        """
        if self._db_type == 0:
            return SQLDate(datetime.fromtimestamp(timestamp).strftime(self.date_time_format))
        else:
            exp = SQLDate(datetime.fromtimestamp(timestamp).strftime(self.date_time_format))
            return SQLFuncExpr("TO_DATE", SQLListExpr([exp, "'YYYY-MM-DD HH24:MI:SS'"]))

    @property
    def table_names(self):
        """Returns a list of table names belonging to own table space, e.g. ``CL_``, ``CAT_``, ``GBL_``, etc.
        """
        return [str(i[0]).strip().upper() for i in self.execute(self._tablequery) if i[0] != u"sqlite_sequence"]

    def get_columns(self, table):
        """Gets a list of column names for a given table.

        :param table: name of table
        :return: list of columns
        """
        return [(str(i[SQL_COLUMNS[self._db_type][0][0]]), str(i[SQL_COLUMNS[self._db_type][0][1]]))
                for i in self.execute(self._columnquery.replace('$TBL', table).replace('$TP', self._table_prefix))]

    def curr_date_time(self):
        """Get the current date/time of the database.

        :return: Returns the current date time of the database
        """
        return str(self.execute(SQL_DATETIME[self._db_type])[0][0])

    def curr_datetime_expr(self):
        """Get expression that returns current date/time of database"""
        return SQL_DT_EXPR[self._db_type]

    # ====================================================================
    # deprecated methods and properties
    # ====================================================================

    @staticmethod
    def GetQualifiedTableName(table_base_name):  # pylint: disable=C0103
        """Deprecated and without warning as too many calls are used by now.
        Those would just pollute output
        """
        return table_base_name
    #
    # @deprecated('select_generic_data')
    # def SelectGenericData(self, *args, **kwargs):  # pylint: disable=C0103
    #     """deprecated"""
    #     return self.select_generic_data(*args, **kwargs)
    #
    # @deprecated('select_generic_data_compact')
    # def SelectGenericDataCompact(self, *args, **kwargs):  # pylint: disable=C0103
    #     """deprecated"""
    #     return self.select_generic_data_compact(*args, **kwargs)
    #
    # @deprecated('add_generic_data')
    # def AddGenericData(self, record, table_name):  # pylint: disable=C0103
    #     """deprecated"""
    #     return self.add_generic_data(record, table_name)
    #
    # @deprecated('add_generic_data_prepared')
    # def AddGenericPreparedData(self, records, table_name):  # pylint: disable=C0103
    #     """deprecated"""
    #     return self.add_generic_data_prepared(records, table_name)
    #
    # @deprecated('delete_generic_data')
    # def DeleteGenericData(self, table_name, where=None, sql_param={}):  # pylint: disable=C0103
    #     """deprecated"""
    #     return self.delete_generic_data(table_name, where, sql_param)
    #
    # @deprecated('update_generic_data')
    # def UpdateGenericData(self, record, table_name, where=None, sql_param={}):  # pylint: disable=C0103
    #     """deprecated"""
    #     return self.update_generic_data(record, table_name, where, sql_param)
    #
    # @deprecated()
    # def GetConnectString(self, *args, **kw):  # pylint: disable=C0103
    #     """deprecated, as BaseDB can be use by predefined values, e.g. 'ARS4XX', 'MFC4XX', 'algo' or direct
    #      filename in case of SDF or SQLITE extention.
    #
    #     Builds the connection string for the DB.
    #     The arguments must contain the values for the connection string as [DRIVER, DSN, DBQ, Uid, Pwd].
    #     All elements have type string in the list with the following details:
    #     * DRIVER: ODBC Driver Name to be provide when using pyodbc driver
    #     * DSN: Data source Name to be provided when using pydobc driver
    #     * DSN: str
    #     * DBQ: TNS service name of the Oracle database
    #     * DBQ: str
    #     * Uid: Username to login to the Oracle database
    #     * Uid: str
    #     * Pwd: Password to login to the Oracle database
    #     * Pwd: str
    #
    #     :keyword dbdriver: ODBC driver name to be provided when using pyodbc driver.
    #                        If not provided, default value will be used.
    #     :type dbdriver: str
    #     :return: Database connection string
    #     :rtype: str
    #     """
    #     if self._db_type == 0:
    #         return args[1]
    #     elif self._db_type == 1:
    #         return "Provider=%s;Data Source=%s;" % (args[0], args[1])
    #     else:  # dbdsn, dbdbq, dbuser, dbpassword, dbdriver=DEFAULT_MASTER_DRV:
    #         if args[0] is None:
    #             connect_str = "DRIVER={%s};" % (args[4] if len(args) > 4 else kw.pop("dbdriver", DEFAULT_MASTER_DRV))
    #         else:
    #             connect_str = "DSN=%s;" % args[0]
    #         if args[1] is not None:
    #             connect_str += "DBQ=%s;" % args[1]
    #         if args[2] is not None:
    #             connect_str += "Uid=%s;" % args[2]
    #         if args[3] is not None:
    #             connect_str += "Pwd=%s;" % args[3]
    #         return connect_str

    @property
    def db_connection(self):
        """:returns raw DB connection"""
        return self._db_connection

    # @property
    # @deprecated('ident_str')
    # def strIdent(self):  # pylint: disable=C0103
    #     """return my own ident
    #     """
    #     return self.ident_str
    #
    # @deprecated()
    # def initialize(self):
    #     """deprecated"""
    #     pass
    #
    # @deprecated()
    # def Initialize(self):  # pylint: disable=C0103
    #     """deprecated"""
    #     pass
    #
    # @deprecated('close')
    # def Terminate(self):  # pylint: disable=C0103
    #     """deprecated"""
    #     self.close()
    #
    # @deprecated('close')
    # def terminate(self):
    #     """deprecated"""
    #     self.close()
    #
    # @deprecated('commit')
    # def Commit(self):  # pylint: disable=C0103
    #     """deprecated"""
    #     self.commit()
    #
    # @deprecated('rollback')
    # def Rollback(self):  # pylint: disable=C0103
    #     """deprecated"""
    #     self.rollback()
    #
    # @deprecated('cursor')
    # def Cursor(self):  # pylint: disable=C0103
    #     """deprecated"""
    #     return self.cursor()
    #
    # @property
    # @deprecated('auto_commit')
    # def autoCommit(self):  # pylint: disable=C0103
    #     """deprecated"""
    #     return self.auto_commit
    #
    # @autoCommit.setter
    # @deprecated('auto_commit')
    # def autoCommit(self, val):  # pylint: disable=C0103
    #     """deprecated"""
    #     self.auto_commit = val
    #
    # @deprecated('table_names (property)')
    # def GetTableNames(self):  # pylint: disable=C0103
    #     """deprecated"""
    #     return self.table_names
    #
    # @deprecated('get_columns')
    # def GetColumns(self, tableName):  # pylint: disable=C0103
    #     """deprecated"""
    #     return self.get_columns(tableName)
    #
    # @property
    # @deprecated('db_type')
    # def dbType(self):  # pylint: disable=C0103
    #     """deprecated"""
    #     return self.db_type
    #
    # @deprecated('sub_scheme_version')
    # def GetSubSchemeVersion(self, SubSchemeTag):  # pylint: disable=C0103
    #     """deprecated"""
    #     return self.sub_scheme_version(SubSchemeTag)
    #
    # @deprecated('sub_scheme_versions')
    # def GetSubSchemeVersions(self):  # pylint: disable=C0103
    #     """deprecated"""
    #     return self.sub_scheme_versions()
    #
    # @deprecated('curr_date_time')
    # def GetCurrDateTime(self):  # pylint: disable=C0103
    #     """deprecated"""
    #     return self.curr_date_time()
    #
    # @deprecated('timestamp_to_date')
    # def ConvertTimestampToDate(self, timestamp):  # pylint: disable=C0103
    #     """deprecated"""
    #     return self.timestamp_to_date(timestamp)
    #
    # @deprecated('curr_datetime_expr')
    # def GetCurrDateTimeExpr(self):  # pylint: disable=C0103
    #     """deprecated"""
    #     return self.curr_datetime_expr()


# ====================================================================
# Interface helper classes
# ====================================================================
class PluginBaseDB(object):
    """used by plugin finder"""
    pass


class OracleBaseDB(object):
    """deprecated"""
    pass


class SQLCEBaseDB(object):
    """deprecated"""
    pass


class SQLite3BaseDB(object):
    """deprecated"""
    pass


# - functions ---------------------------------------------------------------------------------------------------------
# ====================================================================
# Connection string initialization
# ====================================================================
def GetOracleConnectString(dbdsn, dbdbq, dbuser, dbpassword, dbdriver=DEFAULT_MASTER_DRV):  # pylint: disable=C0103
    """
    Build the connection string for Oracle 11g.

    :param dbdsn: Data Source Name
    :type dbdsn: str
    :param dbdbq: TNS service name of the Oracle database to which you want to connect to
    :type dbdbq: str
    :param dbuser: Username
    :type dbuser: str
    :param dbpassword: Password
    :type dbpassword: str
    :param dbdriver: Name of the driver to connect to the database
    :type dbdriver: str
    :return: Returns the Oracle connection string.
    :rtype: str
    """
    if dbdsn is not None:
        connect_str = "DSN=%s;" % dbdsn
    else:
        connect_str = "DRIVER={%s};" % dbdriver
    if dbdbq is not None:
        connect_str += "DBQ=%s;" % dbdbq
    if dbuser is not None:
        connect_str += "Uid=%s;" % dbuser
    if dbpassword is not None:
        connect_str += "Pwd=%s;" % dbpassword
    return connect_str


def GetSQLCEConnectString(dbprovider, dbsource):  # pylint: disable=C0103
    """
    Build the connection string for SQL Server Compact

    Remark
      May be add SSCE: Default Lock Timeout=20000;
      May be add SSCE: Max Database Size=1024;

    :param dbsource: Path to the SQL Server Compact database file
    :type dbsource: str
    :param dbprovider:
    :type dbprovider: str
    :return: Returns the SQL Server Compact connection string
    :rtype: str
    """
    return "Provider=%s;Data Source=%s;" % (dbprovider, dbsource)


def GetSQLite3ConnectString(dbsource):  # pylint: disable=C0103
    """
    Build the connection string for SQL Lite.
    only file names are supported: http://www.connectionstrings.com/

    :param dbsource: Path to the SQLite3 database file
    :type dbsource: str
    :return: SQLite connection string
    :rtype: str
    """
    return dbsource


# ====================================================================
# Table prefix
# ====================================================================
def GetFullTablePrefix(schema_prefix, base_table_prefix):  # pylint: disable=C0103
    """ Determine the table prefix from a schema prefix and a base table prefix

    :param schema_prefix: The prefix of the database schema (may be None)
    :param base_table_prefix: The base prefix of the data base tables in the schema (may be None)
    """
    table_prefix = None
    if schema_prefix is not None:
        table_prefix = schema_prefix + SCHEMA_PREFIX_SEPARATOR
    if base_table_prefix is not None:
        if table_prefix is not None:
            table_prefix = "%s%s%s" % (table_prefix, base_table_prefix, TABLE_PREFIX_SEPARATOR)
        else:
            table_prefix = "%s%s" % (base_table_prefix, TABLE_PREFIX_SEPARATOR)
    return table_prefix

