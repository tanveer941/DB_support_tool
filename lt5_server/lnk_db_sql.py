"""


Python library to support the generation of SQL statements


"""

# pylint: disable=W0231,R0201

# Import Python Modules ----------------------------------------------------------------------------------------------
from copy import copy
from datetime import datetime
from time import timezone
from cx_Oracle import NUMBER, STRING, BLOB, CLOB
from sqlite3 import Binary
from warnings import warn

# Defines -------------------------------------------------------------------------------------------------------------
# SQL Statements ------------------------------------------------------------------------------------------------------

STMT_SELECT = "SELECT"  # SELECT
STMT_UPDATE = "UPDATE"  # UPDATE
STMT_INSERT = "INSERT INTO"  # INSERT
STMT_DELETE = "DELETE FROM"  # DELETE
STMT_DROP_TABLE = "DROP TABLE"  # DROP
EXPR_WHERE = "WHERE"  # WHERE
EXPR_SET = "SET"  # SET
EXPR_VALUES = "VALUES"  # VALUES
EXPR_DISTINCT = "DISTINCT"  # DISTINCT
EXPR_FROM = "FROM"  # FROM
EXPR_GROUP_BY = "GROUP BY"  # GROUP BY
EXPR_ORDER_BY = "ORDER BY"  # ORDER BY
EXPR_HAVING = "HAVING"  # HAVING

EXPR_ASC = "ASC"  # ASC
EXPR_DESC = "DESC"  # DESC

EXPR_TRUE = "TRUE"  # TRUE
EXPR_FALSE = "FALSE"  # FALSE

EXPR_WITH = "WITH"
EXPR_COUNT = "COUNT"

# following placeholders will be replace according DB connection ...
EXPR_DATE = "$CD"  # ... to current date
EXPR_TIME = "$CT"  # ... to current time
EXPR_DATETIME = "$DT"  # ... to current datetime
# SQL Operators -------------------------------------------------------------------------------------------------------

OP_NOP = ""  # no operation

OP_POS = "+"  # unary plus
OP_NEG = "-"  # unary minus

OP_ADD = "+"  # binary add
OP_MUL = "*"  # mult.
OP_SUB = "-"  # binary sub
OP_DIV = "/"  # div
OP_MOD = "%"  # modulo

OP_ASGN = "="  # assignment
OP_EQ = "="  # equal
OP_NEQ = "!="  # not equal
OP_LT = "<"  # less than
OP_LEQ = "<="  # less than or equal to
OP_GT = ">"  # greater than
OP_GEQ = ">="  # greater than or equal to

OP_AND = "AND"  # log. and
OP_NOT = "NOT"  # log. not
OP_OR = "OR"  # log. or

OP_LIKE = "LIKE"  # LIKE operator (pattern matching)
OP_IN = "IN"  # IN operator (sub-queries)

OP_BIT_AND = "&"  # bitwise and
OP_BIT_NOT = "~"  # bitwise not
OP_BIT_OR = "|"  # bitwise or
OP_BIT_XOR = "^"  # bitwise xor

OP_AS = "AS"  # Alias
OP_BETWEEN = "BETWEEN"  # Between operator
OP_EXISTS = "EXISTS"  # exists operator

OP_INNER_JOIN = "INNER JOIN"  # Inner join
OP_LEFT_OUTER_JOIN = "LEFT OUTER JOIN"  # Left outer join
OP_RIGHT_OUTER_JOIN = "RIGHT OUTER JOIN"  # Right outer join
OP_NATURAL_JOIN = "NATURAL JOIN"  # Natural join
OP_UNION_ALL = "UNION ALL"  # Union all
OP_RETURNING = "RETURNING"

OP_ON = "ON"  # On

OP_IS = "IS"  # On

OP_USING = "USING"  # using operator

# Miscellaneous settings ----------------------------------------------------------------------------------------------
GEN_RECUR_NAME = "RECUR"

# Separator for table prefix and table base name
TABLE_PREFIX_SEPARATOR = "_"
SCHEMA_PREFIX_SEPARATOR = "."

# ---specifics for supported DB's -------------------------------------------------------------------------------------
CONN_STRING = {"MFC4XX": ("uid=DEV_MFC4XX_ADMIN;pwd=MFC4XX_ADMIN", "DEV_MFC4XX_ADMIN"),
               "ARS4XX": ("uid=DEV_ARS4XX_ADMIN;pwd=ARS4XX_ADMIN", "DEV_ARS4XX_ADMIN"),
               "VGA": ("uid=VAL_GLOBAL_USER;pwd=PWD4VAL_GLBL", "VAL_GLOBAL_ADMIN",),
               "stk": ("uid=MT_STK_DEV_ADMIN;pwd=MT_STK_DEV_ADMIN_PW1", ""),
               "algo": ("uid=ALGO_DB_USER;pwd=read", "ADMSADMIN")}

# dbType --> 0: sqlite, 1: sqlCE, 2 / -1: oracle
DBTYPE = ["sqlite3", "adodbapi", "pyodbc", "cx_Oracle"]
SQL_TABLENAMES = ["SELECT name FROM sqlite_master WHERE type='table' AND NAME LIKE '$TS_%%' ORDER BY name",
                  "SELECT CAST(TABLE_NAME AS NCHAR(127)) FROM information_schema.tables "
                  "WHERE (TABLE_TYPE LIKE 'TABLE' AND TABLE_NAME LIKE '$TS_%%') ORDER BY TABLE_NAME",
                  "SELECT DISTINCT OBJECT_NAME FROM ALL_OBJECTS WHERE OBJECT_TYPE IN ('TABLE', 'VIEW') "
                  "AND OWNER = '$NM' AND LOWER(OBJECT_NAME) LIKE '$TS%'"]
#                  "SELECT TABLE_NAME AS NAME FROM ALL_TABLES WHERE TABLE_NAME LIKE '$TS_%%' ORDER BY TABLE_NAME"]
SQL_DT_EXPR = ["CURRENT_TIMESTAMP", "GETDATE()", "CURRENT_DATE"]
SQL_DATETIME = ["SELECT strftime('%Y-%m-%d %H:%M:%S','now')",
                "SELECT CONVERT(NVARCHAR(19), DATEADD(hour, %d, GETDATE()), 120)" % (timezone / 3600),
                "SELECT TO_CHAR(systimestamp at time zone 'utc', 'YYYY-MM-DD HH24:MI:SS') FROM SYS.dual"]
SQL_COLUMNS = [([1, 2], "PRAGMA table_info($TBL)"),
               ([0, 1], "SELECT CAST(column_name AS NTEXT), CAST(UPPER(data_type) AS NTEXT) "
                "FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '$TBL'"),
               ([0, 1], "SELECT column_name, data_type FROM all_tab_cols WHERE table_name = '$TBL' "
                "AND DATA_TYPE != 'RAW' AND OWNER = '$NM'")]

IDENT_SPACE = {"dbcat": "CAT", "dbcl": "CL", "dbgbl": "GBL", "dbobj": "OBJ",
               "dbpar": "PAR", "dbval": "VAL", "dbfct": "FCT"}

SQL_PARM = ['?', '?', ':1']

CX_VARS = {'number': NUMBER, 'string': STRING, 'blob': BLOB, 'clob': CLOB}
SQ_VARS = {'blob': Binary}

# Functions -----------------------------------------------------------------------------------------------------------

# SQL Statement Exceptions --------------------------------------------------------------------------------------------

# try:
#     from exceptions import StandardError as _BaseException
# except ImportError:
#     # py3k
#     _BaseException = Exception


class SQLStatementError(StandardError):
    """Base of all SQL statement errors"""
    pass


class SQLStatementGenerationError(SQLStatementError):
    """Error during the generation of a SQL statement"""
    pass

# Classes -------------------------------------------------------------------------------------------------------------


class BaseSQLStatement(object):
    """Base SQL statement"""

    TABLE_PREFIX_SET_METHOD_NAME = "SetTablePrefix"

    def __init__(self):
        pass

    @staticmethod
    def IsPrimitiveType(item):
        """Check if an item is of a primitive type

        :param item: name of the DB type to be checked
        :type item: db type
        :return: Returns True if the item type is primitive
        :rtype: bool
        """
        return item == str or item == unicode or item == datetime

    def ConvertConstantsToSQLLiterals(self, assign_items):
        """
        Copy given dictionary and convert all constant values to SQL literals

        :param assign_items: The assignment items whose values are to be converted.
        :type assign_items: dictionary
        :return: Returns a copy of the assignment items with all info converted.
        """
        assign_items_copy = copy(assign_items)
        for key in assign_items_copy:
            value = assign_items_copy[key]
            assign_items_copy[key] = self.GetExprForValue(value)
        # done
        return assign_items_copy

    @staticmethod
    def GetExprForValue(value):
        """
        Returns the appropriate SQL expression for a value.

        :param value: The value for which to return the expression.
        :return: Returns the expression representing the vaue.
        """
        if type(value) == SQLExpr or type(value) == BaseSQLStatement:
            return value
        elif type(value) == long or type(value) == int:
            return SQLIntegral(value)
        elif type(value) == float:
            return SQLFloat(value)
        elif type(value) == str or type(value) == unicode:
            return SQLString(value)
        elif type(value) == datetime:
            return SQLDate(value)
        elif value is None:
            return SQLNull()
        else:
            return value

    def SetTablePrefix(self, table_prefix, recurse=True):
        """Set the table prefix recursively in expressions.

        :param table_prefix: The table prefix.
        :type table_prefix: str
        :param recurse: Set False, if recursion to sub-expressions shall be allowed. True by default.
        :type recurse: bool
        """
        pass

    def _set_table_prefix(self, obj, table_prefix, recurse=True):
        """
        Set the table prefix recursively in sub-expressions.

        :param obj: The object to set the prefix for, if supported.
        :type obj: Object
        :param table_prefix: The table prefix.
        :type table_prefix: str
        :param recurse: Set False, if recursion to sub-expressions shall be allowed. True by default.
        :type recurse: bool
        """
        if obj is not None:
            try:
                set_prefix_func = getattr(obj, self.TABLE_PREFIX_SET_METHOD_NAME)
            except AttributeError:
                pass
            else:
                set_prefix_func(table_prefix, recurse)

# ====================================================================
# SQL SELECT Statement Support
# ====================================================================


class GenericSQLSelect(BaseSQLStatement):
    """SQL SELECT statement"""
    def __init__(self, select_list=["*"], distinct_rows=False, table_list=None, where_condition=None):
        """
        Initialize new statement

        :param select_list: List of selected items
        :type select_list:
        :param table_list: List of source tables
        :type table_list:
        """
        self.select_list = select_list
        self.table_list = table_list
        self.distinct_rows = distinct_rows
        self.where_condition = where_condition
        self.group_by_list = None
        self.having_condition = None
        self.order_by_list = None

    def __str__(self):
        """Generate SQL SELECT statement"""
        if not self.select_list or len(self.select_list) == 0:
            raise SQLStatementGenerationError("Invalid select list", str(self.select_list))
        stmt = STMT_SELECT
        # DISTINCT
        if self.distinct_rows:
            stmt = stmt + " " + EXPR_DISTINCT
        # Selected items
        stmt = stmt + " " + str(SQLListExpr(self.select_list))
        # Source tables
        if self.table_list and len(self.table_list) > 0:
            stmt = stmt + " " + EXPR_FROM + " " + str(SQLListExpr(self.table_list))
        # WHERE clause
        if self.where_condition:
            stmt = stmt + " " + EXPR_WHERE + " " + str(self.where_condition)
        # GROUP BY clause
        if self.group_by_list and len(self.group_by_list) > 0:
            stmt = stmt + " " + EXPR_GROUP_BY + " " + str(SQLListExpr(self.group_by_list))
        # HAVING clause
        if self.having_condition:
            stmt = stmt + " " + EXPR_HAVING + " " + str(self.having_condition)
        # ORDER BY clause
        if self.order_by_list and len(self.order_by_list) > 0:
            stmt = stmt + " " + EXPR_ORDER_BY + " " + str(SQLListExpr(self.order_by_list))
            # done
        return stmt

    def SetTablePrefix(self, table_prefix, recurse=True):
        """
        Set the table prefix recursively in sub-expressions.

        :param table_prefix: The table prefix.
        :type table_prefix: str
        :param recurse: Set False, if recursion to sub-expressions shall be allowed. True by default.
        :type recurse: bool
        """
        if recurse:
            self._set_table_prefix(self.select_list, table_prefix, recurse)
            self._set_table_prefix(self.table_list, table_prefix, recurse)
            self._set_table_prefix(self.group_by_list, table_prefix, recurse)
            self._set_table_prefix(self.having_condition, table_prefix, recurse)
            self._set_table_prefix(self.order_by_list, table_prefix, recurse)


class GenericSQLUpdate(BaseSQLStatement):
    """SQL UPDATE statement"""
    def __init__(self, table_name=None, assign_items=None, where_condition=None):
        """
        Initialize new statement

        :param table_name: The name of the updated table
        :type table_name: str
        :param assign_items: The dictionary of names and values to be set.
        :type assign_items: Dictionary
        :param where_condition: The condition hat must be fulfilled by the updated rows,
        :type where_condition: str
        """
        self.table_name = table_name
        self.assign_items = assign_items
        self.where_condition = where_condition

    def __str__(self):
        """Generate SQL UPDATE statement"""
        if not self.table_name or len(self.table_name) == 0:
            raise SQLStatementGenerationError("Invalid table name", str(self.table_name))
        if not self.assign_items or len(self.assign_items) == 0:
            raise SQLStatementGenerationError("Invalid set values", str(self.assign_items))
        stmt = STMT_UPDATE
        # Updated table
        stmt = stmt + " " + str(self.table_name)
        # Set values
        stmt = stmt + " " + EXPR_SET + " "
        stmt += str(SQLAssignListExpr(self.ConvertConstantsToSQLLiterals(self.assign_items)))
        # WHERE clause
        if self.where_condition:
            stmt = stmt + " " + EXPR_WHERE + " " + str(self.where_condition)
        # done
        return stmt


class GenericSQLInsert(BaseSQLStatement):
    """SQL INSERT statement"""
    def __init__(self, table_name=None, assign_items=None, returning=None):
        """
        Initialize new statement

        :param table_name: The name of the table ito which values are inserted
        :type table_name: str
        :param assign_items: Dictionary of columns and values to insert
        :type assign_items: dict
        :param returning: column name to get new auto incremented index
        """
        self.table_name = table_name
        self.assign_items = assign_items
        self.returning = returning

    def __str__(self):
        """Generate SQL INSERT statement"""
        if not self.assign_items or len(self.assign_items) == 0:
            raise SQLStatementGenerationError("Invalid set values", str(self.assign_items))
        stmt = STMT_INSERT
        # table
        stmt = stmt + " " + str(self.table_name)
        # columns
        assign_items_copy = self.ConvertConstantsToSQLLiterals(self.assign_items)
        stmt = stmt + " " + "(" + str(SQLListExpr(assign_items_copy.keys())) + ")"
        # values
        stmt = stmt + " " + EXPR_VALUES + "(" + str(SQLListExpr(assign_items_copy.values())) + ")"
        # returning
        if self.returning:
            stmt += " " + str(self.returning)
        # done
        return stmt


class GenericSQLPreparedInsert(BaseSQLStatement):
    """SQL INSERT statement"""
    def __init__(self, table_name=None, assign_items=None):
        """
        Initialize new statement

        :param table_name: The name of the table ito which values are inserted
        :type table_name: str
        :param assign_items: Dictionary of columns and values to insert
        :type assign_items: Dictionary
        """
        self.table_name = table_name
        self.assign_items = assign_items

    def __str__(self):
        """Generate SQL INSERT statement"""
        if not self.assign_items or len(self.assign_items) == 0:
            raise SQLStatementGenerationError("Invalid set values", str(self.assign_items))
        stmt = STMT_INSERT
        # table
        stmt = stmt + " " + str(self.table_name) + "("
        # column + values
        vals = " " + EXPR_VALUES + "("
        for i in range(len(self.assign_items)):
            stmt += " %s," % self.assign_items[i]
            vals += " :%s," % str(i + 1)
        stmt = stmt[:-1] + " )"
        vals = vals[:-1] + " )"
        stmt += vals

        return stmt


class GenericSQLDelete(BaseSQLStatement):
    """SQL DELETE statement"""
    def __init__(self, table_name=None, where_condition=None):
        """
        Initialize new statement

        :param table_name: The name of the table into which values are inserted
        :type table_name: str
        :param where_condition: The optional where condition
        :type where_condition: str
        """
        self.table_name = table_name
        self.where_condition = where_condition

    def __str__(self):
        """Generate SQL DELETE statement"""
        stmt = STMT_DELETE
        # table
        stmt = stmt + " " + str(self.table_name)
        # where clause
        if self.where_condition:
            stmt = stmt + " " + EXPR_WHERE + " " + str(self.where_condition)
        # done
        return stmt


class GenericSQLDropTable(BaseSQLStatement):
    """SQL DROP statement for tables"""
    def __init__(self, table_name=None, *args, **kwargs):
        """
        Initialize new statement

        :param table_name: The name of the table to drop
        :type table_name: str
        """
        self.table_name = table_name
        if len(args) or len(kwargs):
            warn("additional arguments to 'GenericSQLDropTable' are not supported!")

    def __str__(self):
        """Generate SQL DROP statement"""
        if not self.table_name or len(self.table_name) == 0:
            raise SQLStatementGenerationError("Invalid table name", str(self.table_name))
        stmt = STMT_DROP_TABLE
        # table
        stmt = stmt + " " + str(self.table_name)
        # done
        return stmt

# ====================================================================
# SQL Statement Factories
# ====================================================================


class BaseSQLStatementFactory(object):
    """Base factory to get database specific imlementations of SQL statements"""
    def __init__(self):
        pass


class GenericSQLStatementFactory(BaseSQLStatementFactory):
    """Generic SQL statements applicable to most DBMS"""
    def __init__(self):
        pass

    @staticmethod
    def GetSelectBuilder():
        """Returns a new builder for generic SQL Select statements"""
        return GenericSQLSelect()

    @staticmethod
    def GetUpdateBuilder():
        """Returns a new builder for generic SQL Update statements"""
        return GenericSQLUpdate()

    @staticmethod
    def GetInsertBuilder():
        """Returns a new builder for generic SQL Insert statements"""
        return GenericSQLInsert()

    @staticmethod
    def GetPreparedInsertBuilder():
        """Returns a new builder for generic SQL Insert statements"""
        return GenericSQLPreparedInsert()

    @staticmethod
    def GetDeleteBuilder():
        """Returns a new builder for generic SQL Delete statements"""
        return GenericSQLDelete()

    @staticmethod
    def GetDropTableBuilder():
        """Returns a new builder for generic SQL drop table statements"""
        return GenericSQLDropTable()

# ====================================================================
# SQL Expressions
# ====================================================================


class SQLExpr(object):
    """SQL Base expression"""

    TABLE_PREFIX_SET_METHOD_NAME = "SetTablePrefix"

    """SQL Base expression"""
    @staticmethod
    def IsPrimitiveType(tname):
        """
        Check if an item is of a primitive type

        :param tname: Name of the type to be checked
        :type: tname: type name
        :return: Returns True if the item type is primitive
        :rtype: bool
        """
        return tname in (float, long, int, str, unicode, datetime)

    def SetTablePrefix(self, table_prefix, recurse=True):
        """
        Set the table prefix recursively in sub-expressions.

        :param table_prefix: The table prefix.
        :type table_prefix: str
        :param recurse: Set False, if recursion to sub-expressions shall be allowed. True by default.
        :type recurse: bool
        """
        pass

    def _set_table_prefix(self, obj, table_prefix, recurse=True):
        """
        Set the table prefix recursively in sub-expressions.



        :param obj: The object to set the prefix for, if supported.
        :type obj: Object
        :param table_prefix: The table prefix.
        :type table_prefix: str
        :param recurse: Set False, if recursion to sub-expressions shall be allowed. True by default.
        :type recurse: bool
        """
        if obj is not None:
            try:
                set_prefix_func = getattr(obj, self.TABLE_PREFIX_SET_METHOD_NAME)
            except AttributeError:
                pass
            else:
                set_prefix_func(table_prefix, recurse)


class SQLLiteral(SQLExpr):
    """SQL Literal"""
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return "'" + str(self.value) + "'"


class SQLNull(SQLLiteral):
    """SQL NULL literal representation"""
    def __init__(self):
        pass

    def __str__(self):
        return "NULL"


class SQLDefault(SQLLiteral):
    """SQL DEFAULT literal representation"""
    def __init__(self):
        pass

    def __str__(self):
        return "DEFAULT"


class SQLString(SQLLiteral):
    """SQL string literal representation"""
    pass


class SQLIntegral(SQLLiteral):
    """SQL integer literal representation"""
    def __str__(self):
        return str(self.value)


class SQLFloat(SQLLiteral):
    """SQL float literal representation"""
    def __str__(self):
        return str(self.value)


class SQLDate(SQLLiteral):
    """SQL date literal representation"""
    pass


class SQLTime(SQLLiteral):
    """SQL time literal representation"""
    pass


class SQLTimestamp(SQLLiteral):
    """SQL timestamp literal representation"""
    pass


class SQLCurrency(SQLLiteral):
    """SQL currency literal representation"""
    pass


class SQLBoolean(SQLLiteral):
    """SQL Boolean literal representation"""
    def __init__(self, value):
        SQLLiteral.__init__(value)

    def __str__(self):
        if self.value:
            return str(EXPR_TRUE)
        else:
            return str(EXPR_FALSE)


class SQLBinary(SQLLiteral):
    """SQL binary literal representation"""
    def __init__(self, value):
        SQLLiteral.__init__(value)

    def __str__(self):
        return str(self.value)


class SQLBinaryExpr(SQLExpr):
    """SQL binary expression"""
    def __init__(self, left_expr, binary_operator, right_expr):
        self.left_expr = left_expr
        self.binary_operator = binary_operator
        self.right_expr = right_expr

    def __str__(self):
        left_str = str(self.left_expr)
        if not (isinstance(self.left_expr, SQLLiteral) or
                isinstance(self.left_expr, SQLColumnExpr) or
                isinstance(self.left_expr, SQLFuncExpr) or
                self.IsPrimitiveType(type(self.left_expr))):
            left_str = "(" + left_str + ")"
        right_str = str(self.right_expr)
        if not (isinstance(self.right_expr, SQLLiteral) or
                isinstance(self.right_expr, SQLColumnExpr) or
                isinstance(self.right_expr, SQLFuncExpr) or
                self.IsPrimitiveType(type(self.right_expr))):
            right_str = "(" + right_str + ")"
        return left_str + " " + str(self.binary_operator) + " " + right_str

    def SetTablePrefix(self, table_prefix, recurse=True):
        """
        Set the table prefix recursively in sub-expressions.

        :param table_prefix: The table prefix.
        :type table_prefix: str
        :param recurse: Set False, if recursion to sub-expressions shall be allowed. True by default.
        :type recurse: bool
        """
        if recurse:
            self._set_table_prefix(self.left_expr, table_prefix, recurse)
            self._set_table_prefix(self.right_expr, table_prefix, recurse)


class SQLConcatExpr(SQLExpr):
    """SQL concatenation expression which just catenates all expressions with space delimiter"""
    def __init__(self, *args):
        self.list = args

    def __str__(self):
        return " ".join([str(i) for i in self.list])


class SQLTernaryExpr(SQLExpr):
    """SQL ternary expression"""
    def __init__(self, left_expr, first_operator, middle_expr, second_operator, right_expr):
        self.left_expr = left_expr
        self.first_operator = first_operator
        self.second_operator = second_operator
        self.middle_expr = middle_expr
        self.right_expr = right_expr

    def __str__(self):
        left_str = str(self.left_expr)
        if not (isinstance(self.left_expr, SQLLiteral) or
                isinstance(self.left_expr, SQLTableExpr) or
                isinstance(self.left_expr, SQLColumnExpr) or
                isinstance(self.left_expr, SQLFuncExpr) or
                self.IsPrimitiveType(type(self.left_expr))):
            left_str = "(" + left_str + ")"
        middle_str = str(self.middle_expr)
        if not (isinstance(self.middle_expr, SQLLiteral) or
                isinstance(self.middle_expr, SQLTableExpr) or
                isinstance(self.middle_expr, SQLColumnExpr) or
                isinstance(self.middle_expr, SQLFuncExpr) or
                self.IsPrimitiveType(type(self.middle_expr))):
            middle_str = "(" + middle_str + ")"
        right_str = str(self.right_expr)
        if not (isinstance(self.right_expr, SQLLiteral) or
                isinstance(self.right_expr, SQLTableExpr) or
                isinstance(self.right_expr, SQLColumnExpr) or
                isinstance(self.right_expr, SQLFuncExpr) or
                self.IsPrimitiveType(type(self.right_expr))):
            right_str = "(" + right_str + ")"

        tmp = left_str + " " + str(self.first_operator) + " " + middle_str
        tmp += " " + str(self.second_operator) + " " + right_str

        return tmp

    def SetTablePrefix(self, table_prefix, recurse=True):
        """
        Set the table prefix recursively in sub-expressions.

        :param table_prefix: The table prefix.
        :type table_prefix: str
        :param recurse: Set False, if recursion to sub-expressions shall be allowed. True by default.
        :type recurse: bool
        """
        if recurse:
            self._set_table_prefix(self.left_expr, table_prefix, recurse)
            self._set_table_prefix(self.middle_expr, table_prefix, recurse)
            self._set_table_prefix(self.right_expr, table_prefix, recurse)


class SQLJoinExpr(SQLTernaryExpr):
    """
    SQL join expression

    :param left_table_expr: The expression representing the left table source.
    :param join_operator: Operant of left and right expression.
    :param right_table_expr:  The expression representing the right table source.
    """
    def __init__(self, left_table_expr, join_operator, right_table_expr, join_cond_expr=None):
        SQLTernaryExpr.__init__(self, left_table_expr, join_operator, right_table_expr, OP_ON, join_cond_expr)

    def __str__(self):
        left_str = str(self.left_expr)
        if not (isinstance(self.left_expr, SQLLiteral) or
                isinstance(self.left_expr, SQLTableExpr) or
                isinstance(self.left_expr, SQLColumnExpr) or
                isinstance(self.left_expr, SQLFuncExpr) or
                self.IsPrimitiveType(type(self.left_expr))):
            left_str = "(" + left_str + ")"
        middle_str = str(self.middle_expr)
        if not (isinstance(self.middle_expr, SQLLiteral) or
                isinstance(self.middle_expr, SQLTableExpr) or
                isinstance(self.left_expr, SQLColumnExpr) or
                isinstance(self.left_expr, SQLFuncExpr) or
                self.IsPrimitiveType(type(self.middle_expr))):
            middle_str = "(" + middle_str + ")"
        expr_str = left_str + " " + str(self.first_operator) + " " + middle_str
        if self.right_expr is not None:
            right_str = str(self.right_expr)
            if not (isinstance(self.right_expr, SQLLiteral) or
                    isinstance(self.right_expr, SQLTableExpr) or
                    isinstance(self.right_expr, SQLColumnExpr) or
                    isinstance(self.right_expr, SQLFuncExpr) or
                    self.IsPrimitiveType(type(self.right_expr))):
                right_str = "(" + right_str + ")"
            expr_str = expr_str + " " + str(self.second_operator) + " " + right_str
        return expr_str


class SQLTableExpr(SQLExpr):
    """Expression representing a table prefix, base name and alias"""
    def __init__(self, table_base_name, table_alias=None, table_prefix=None):
        """
        Expression representing a table prefix, base name and alias

        :param table_base_name: The base name of the table
        :type table_base_name: str
        :param table_alias: Optional alias (default=None)
        :type table_alias: str | None
        :param table_prefix: Optional table prefix. If not given, the table base name is used.
        :type table_prefix: str | None
        """

        self.table_base_name = table_base_name
        self.table_alias = table_alias
        self.table_prefix = table_prefix

    def __str__(self):
        table_name = str(self.table_base_name)
        if self.table_prefix is not None:
            self.table_prefix = self.table_prefix.strip()
            if len(self.table_prefix) > 0:
                table_name = str(self.table_prefix) + table_name
        if self.table_alias is not None:
            return table_name + " " + str(self.table_alias)
        return table_name

    def SetTablePrefix(self, table_prefix, recurse=True):
        """
        Set the table prefix recursively in sub-expressions.

        :param table_prefix: The table prefix.
        :type table_prefix: str
        :param recurse: Set False, if recursion to sub-expressions shall be allowed. True by default.
        :type recurse: bool
        """
        self.table_prefix = table_prefix


class SQLColumnExpr(SQLExpr):
    """SQL binary expression"""
    def __init__(self, table_expr, column_expr, use_column_quotes=False):
        self.table_expr = table_expr
        self.column_expr = column_expr
        self.use_column_quotes = use_column_quotes

    def __str__(self):
        column_str = str(self.column_expr)
        if self.use_column_quotes is True:
            column_str = '"' + column_str + '"'
        if self.table_expr is not None:
            column_str = str(self.table_expr) + "." + column_str
        return column_str

    def SetTablePrefix(self, table_prefix, recurse=True):
        """
        Set the table prefix recursively in sub-expressions.

        :param table_prefix: The table prefix.
        :type table_prefix: str
        :param recurse: Set False, if recursion to sub-expressions shall be allowed. True by default.
        :type recurse: bool
        """
        if recurse:
            self._set_table_prefix(self.table_expr, table_prefix, recurse)
            self._set_table_prefix(self.column_expr, table_prefix, recurse)


class SQLUnaryExpr(SQLExpr):
    """SQL unary expression"""
    def __init__(self, unary_operator, right_expr):
        self.unary_operator = unary_operator
        self.right_expr = right_expr

    def __str__(self):
        right_str = str(self.right_expr)
        if not (isinstance(self.right_expr, SQLLiteral) or
                isinstance(self.right_expr, SQLColumnExpr) or
                isinstance(self.right_expr, SQLFuncExpr) or
                self.IsPrimitiveType(type(self.right_expr))):
            right_str = "(" + right_str + ")"
        return str(self.unary_operator) + " " + right_str

    def SetTablePrefix(self, table_prefix, recurse=True):
        """
        Set the table prefix recursively in sub-expressions.

        :param table_prefix: The table prefix.
        :type table_prefix: str
        :param recurse: Set False, if recursion to sub-expressions shall be allowed. True by default.
        :type recurse: bool
        """
        if recurse:
            self._set_table_prefix(self.right_expr, table_prefix, recurse)


class SQLFuncExpr(SQLExpr):
    """SQL function expression"""
    def __init__(self, func_name, arg_expr=None):
        self.func_name = func_name
        self.arg_expr = arg_expr

    def __str__(self):
        arg_stmt = ""
        if self.arg_expr:
            arg_stmt = str(self.arg_expr)
        return str(self.func_name) + "(" + arg_stmt + ")"

    def SetTablePrefix(self, table_prefix, recurse=True):
        """
        Set the table prefix recursively in sub-expressions.

        :param table_prefix: The table prefix.
        :type table_prefix: str
        :param recurse: Set False, if recursion to sub-expressions shall be allowed. True by default.
        :type recurse: bool
        """
        if recurse:
            self._set_table_prefix(self.arg_expr, table_prefix, recurse)


class SQLListExpr(SQLExpr):
    """SQL list expression, i.e. comma separated list of expressions"""
    def __init__(self, expr_list, separator=", "):
        self.expr_list = expr_list
        self.separator = separator

    def __str__(self):
        stmt = ""
        num_exprs = len(self.expr_list)
        num_expr = 0
        for expr in self.expr_list:
            stmt += str(expr)
            if (num_expr + 1) < num_exprs:
                stmt += self.separator
            num_expr += 1
        return stmt

    def SetTablePrefix(self, table_prefix, recurse=True):
        """
        Set the table prefix recursively in sub-expressions.

        :param table_prefix: The table prefix.
        :type table_prefix: str
        :param recurse: Set False, if recursion to sub-expressions shall be allowed. True by default.
        :type recurse: bool
        """
        if recurse:
            for expr in self.expr_list:
                self._set_table_prefix(expr, table_prefix, recurse)


class SQLAssignListExpr(SQLExpr):
    """SQL assignment list expression"""
    def __init__(self, assign_items, separator=", "):
        self.assign_items = assign_items
        self.separator = separator

    def __str__(self):
        stmt = ""
        num_items = len(self.assign_items)
        num_item = 0
        for key in self.assign_items:
            stmt = stmt + str(key) + OP_ASGN + str(self.assign_items[key])
            if (num_item + 1) < num_items:
                stmt += self.separator
            num_item += 1
        return stmt

    def SetTablePrefix(self, table_prefix, recurse=True):
        """
        Set the table prefix recursively in sub-expressions.

        :param table_prefix: The table prefix.
        :type table_prefix: str
        :param recurse: Set False, if recursion to sub-expressions shall be allowed. True by default.
        :type recurse: bool
        """
        if recurse:
            for key in self.assign_items:
                self._set_table_prefix(key, table_prefix, recurse)
                self._set_table_prefix(self.assign_items[key], table_prefix, recurse)

