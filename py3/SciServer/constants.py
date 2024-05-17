"""Constants used in the code"""


def _join_str(*args, sep: str = " ") -> str:
    """
    Join a lists of strings

    :param args: Strings to join.
    :param sep: What to use to separate the strings.
    :return: A joined string.
    """
    return sep.join(args)


class DCONST:  # pylint: disable=too-few-public-methods
    """general data constants"""

    JOB_ID = "<job_id>"
    TABLENAME_ID = "<tablename>"
    HTTP_CODE_ID = "<code>"
    CONTEXT_ID = "<context>"
    TASKNAME_ID = "<taskname>"
    #


class EXCEPT:  # pylint: disable=too-few-public-methods
    """exception strings"""

    LOGIN_ERROR = "User token is not defined. First log into SciServer."
    SCHEMA_ERROR = "Error when getting schema name."
    GET_TABLE_ERROR = _join_str(
        "Error when getting table description",
        f"from database context {DCONST.CONTEXT_ID}.\n",
    )
    ILLEGAL_FORMAT_ERROR = _join_str(
        "Error when executing query. Illegal format parameter specification: "
    )
    EXECUTE_QUERY_ERROR = "Error when executing query."
    SUMBIT_JOB_ERROR = "Error when submitting a job."
    GET_JOB_STATUS_ERROR = _join_str(
        f"Error when getting the status of job {DCONST.JOB_ID}.\n"
    )
    CANCEL_JOB_ERROR = f"Error when canceling job {DCONST.JOB_ID}.\n"
    UPLOAD_CSV_ERROR = _join_str(
        "Error when uploading CSV data into",
        f"CasJobs table {DCONST.TABLENAME_ID}.\n",
    )
    HTTP_ERROR = _join_str(
        "Http Response from CasJobs API",
        f"returned status code {DCONST.HTTP_CODE_ID}:\n",
    )


class TASKNAMES:  # pylint: disable=too-few-public-methods
    """casjobs utility strings"""

    GETTABLES = "getTables"
    GETSCHEMANAME = "getSchemaName"
    EXECUTEQUERY = "executeQuery"
    SUBMITJOB = "submitJob"
    GETJOBSTATUS = "getJobStatus"
    CANCELJOB = "cancelJob"
    WRITEFITSFILEFROMQUERY = "writeFitsFileFromQuery"
    GETPANDASDATAFRAMEFROMQUERY = "getPandasDataFrameFromQuery"
    GETNUMPYARRAYFROMQUERY = "getNumpyArrayFromQuery"
    UPLOADPANDASDATAFRAMETOTABLE = "uploadPandasDataFrameToTable"
    UPLOADCSVDATATOTABLE = "uploadCSVDataToTable"


class CASJOBSCONST:  # pylint: disable=too-few-public-methods
    """casjobs utility strings"""

    SCISCRIPT_PYTHON = "SciScript-Python.CasJobs."
    SCISCRIPT_PYTHON_COMPUTE = f"Compute.{SCISCRIPT_PYTHON}"
    #
    X_AUTH_TOKEN = "X-Auth-Token"
    CONTENT_TYPE = "Content-Type"
    ACCEPT = "Accept"
    #
    CONTENT_JSON = "application/json"
    #
    FORMAT_PANDAS = "pandas"
    FORMAT_JSON = "json"
    FORMAT_DICT = "dict"
    #
    WEBSERVICESID = "WebServicesId"
    KEYSTONE_USER_ID = "<keystone_user_id>"
    MYDB = "MyDB"
    #
    QUERY = "Query"
    TASKNAME = "TaskName"


class URLCONST:  # pylint: disable=too-few-public-methods
    """URL utility strings"""

    CONTEXTS = "contexts"
    TABLES = "Tables"
    TASKNAME = CASJOBSCONST.TASKNAME
    USERS = "users"
    QUERY = "query"
    JOBS = "jobs"
    #
    EQ = "="
    Q = "?"
