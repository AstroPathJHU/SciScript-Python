# pylint: disable=invalid-name
# pylint: disable=missing-timeout
"""
Functions to interact with the CasJobsRestAPI
"""

import asyncio
import time

import requests
import pandas
from IPython import get_ipython
import nest_asyncio

from . import Authentication, Config

from .utils import __token
from .utils import __get_taskname
from .utils import __get_headers
from .utils import __validate_response_status
from .utils import __response_get
from .utils import __parse_post
from .utils import __format_data
from .utils import __async_post_request
from .utils import __taskname_url
from .utils import URL_DICT

from .constants import TASKNAMES
from .constants import CASJOBSCONST
from .constants import EXCEPT
from .constants import DCONST


class Task:  # pylint: disable=too-few-public-methods
    """
    The class TaskName stores the name of the task that executes the API call.
    """

    name = None


task = Task()


def getSchemaName():
    """
    Returns the WebServiceID that identifies the schema for a user
    in MyScratch database with CasJobs.

    :return: WebServiceID of the user (string).
    :raises: Throws an exception if the user is not logged into SciServer
        (use Authentication.login for that purpose). Throws an exception if the
        HTTP request to the CasJobs API returns an error.
    :example: wsid = CasJobs.getSchemaName()

    .. seealso:: CasJobs.getTables.
    """
    token = __token()
    usersUrl = URL_DICT[TASKNAMES.GETSCHEMANAME].replace(
        CASJOBSCONST.KEYSTONE_USER_ID,
        Authentication.getKeystoneUserWithToken(token).id,
    )

    return "wsid_" + str(
        __response_get(usersUrl, token, EXCEPT.SCHEMA_ERROR)["WebServicesId"]
    )


def getTables(context: str = CASJOBSCONST.MYDB):
    """
    Gets the names, size and creation date of all tables in a database context
    that the user has access to.

    :param context:	database context (string)
    :return: The result is a json object with format
        [{"Date":seconds,"Name":"TableName","Rows":int,"Size",int},..]
    :raises: Throws an exception if the user is not logged into SciServer
        (use Authentication.login for that purpose). Throws an exception if
        the HTTP request to the CasJobs API returns an error.
    :example: tables = CasJobs.getTables("MyDB")

    .. seealso:: CasJobs.getSchemaName
    """

    return __response_get(
        URL_DICT[TASKNAMES.GETTABLES].replace(DCONST.CONTEXT_ID, context),
        __token(),
        EXCEPT.GET_TABLE_ERROR.replace(DCONST.CONTEXT_ID, str(context)),
    )


def executeQuery(
    sql,
    context: str = CASJOBSCONST.MYDB,
    format="pandas",  # pylint: disable=redefined-builtin
):
    """
    Executes a synchronous SQL query in a CasJobs database context.

    :param sql: sql query (string)
    :param context: database context (string)
    :param format: parameter (string) that specifies the return type:\n
    \t\t'pandas': pandas.DataFrame.\n
    \t\t'json': a JSON string containing the query results. \n
    \t\t'dict': a dictionary created from the JSON string containing the query
    results.\n
    \t\t'csv': a csv string.\n
    \t\t'readable': an object of type io.StringIO, which has the .read() method
    and wraps a csv string that can be passed into pandas.read_csv for
    example.\n
    \t\t'StringIO': an object of type io.StringIO, which has the .read() method
    and wraps a csv string that can be passed into pandas.read_csv for
    example.\n
    \t\t'fits': an object of type io.BytesIO, which has the .read() method and
    wraps the result in fits format.\n
    \t\t'BytesIO': an object of type io.BytesIO, which has the .read() method
    and wraps the result in fits format.\n
    :return: the query result table, in a format defined by the 'format' input
    parameter.
    :raises: Throws an exception if the user is not logged into SciServer (use
        Authentication.login for that purpose). Throws an exception if the HTTP
        request to the CasJobs API returns an error. Throws an exception if
        parameter 'format' is not correctly specified.
    :example: table = CasJobs.executeQuery(sql="select 1 as foo, 2 as bar",
        format="pandas", context="MyDB")

    .. seealso:: CasJobs.submitJob, CasJobs.getTables, SkyServer.sqlSearch
    """
    if task.name is not None:
        taskName = task.name
        task.name = None
    else:
        taskName = __get_taskname(TASKNAMES.EXECUTEQUERY)
    #
    postResponse = requests.post(
        URL_DICT[TASKNAMES.EXECUTEQUERY]
        .replace(DCONST.CONTEXT_ID, context)
        .replace(DCONST.TASKNAME_ID, __taskname_url(taskName)),
        data=__format_data(sql, taskName),
        headers=__get_headers(__token(), format),
        stream=True,
    )
    __validate_response_status(postResponse, EXCEPT.EXECUTE_QUERY_ERROR)
    return __parse_post(postResponse, format)


def executeQueryAsync(
    sql,
    context: str = CASJOBSCONST.MYDB,
    format=None,  # pylint: disable=redefined-builtin
):
    """
    Executes synchronous SQL queries in a CasJobs database context.

    :param sql: sql query (string)
    :param context: database context (string)
    :raises: Throws an exception if the user is not logged into SciServer (use
        Authentication.login for that purpose). Throws an exception if the HTTP
        request to the CasJobs API returns an error. Throws an exception if
        parameter 'format' is not correctly specified.
    :example: table = CasJobs.executeQuery(sql="select 1 as foo, 2 as bar",
        format="pandas", context="MyDB")

    .. seealso:: CasJobs.submitJob, CasJobs.getTables, SkyServer.sqlSearch
    """
    #
    if isinstance(sql, str):
        sql = [sql]
    #
    if get_ipython() and hasattr(get_ipython(), "kernel"):
        nest_asyncio.apply()
    #
    responses = asyncio.run(
        __async_post_request(
            url=Config.CasJobsRESTUri + "/contexts/" + context + "/query",
            datas=sql,
            headers=__get_headers(__token(), accept_format="json"),
            task_name=__get_taskname(TASKNAMES.EXECUTEQUERY),
        )
    )
    #
    if format is not None:
        r = []
        for response in responses:
            r += response["Result"][0]["Data"]
        return pandas.DataFrame(r, columns=responses[0]["Result"][0]["Columns"])
    return responses


def submitJob(sql, context: str = CASJOBSCONST.MYDB):
    """
    Submits an asynchronous SQL query to the CasJobs queue.

    :param sql: sql query (string)
    :param context:	database context (string)
    :return: Returns the CasJobs jobID (integer).
    :raises: Throws an exception if the user is not logged into SciServer (use
        Authentication.login for that purpose). Throws an exception if the HTTP
        request to the CasJobs API returns an error.
    :example: jobid = CasJobs.submitJob("select 1 as foo","MyDB")

    .. seealso:: CasJobs.executeQuery, CasJobs.getJobStatus, CasJobs.waitForJob,
        CasJobs.cancelJob.
    """

    putResponse = requests.put(
        URL_DICT[TASKNAMES.SUBMITJOB].replace(DCONST.CONTEXT_ID, context),
        data=__format_data(sql, __get_taskname(TASKNAMES.SUBMITJOB)),
        headers=__get_headers(__token(), "readable"),
    )
    __validate_response_status(putResponse, EXCEPT.SUMBIT_JOB_ERROR)
    return int(putResponse.content.decode())


def getJobStatus(jobId):
    """
    Shows the status of a job submitted to CasJobs.

    :param jobId: id of job (integer)
    :return: Returns a dictionary object containing the job status and related
        metadata. The "Status" field can be equal to 0 (Ready), 1 (Started),
        2 (Canceling), 3(Canceled), 4 (Failed) or 5 (Finished). If jobId is
        the empty string, then returns a list with the statuses of all previous
        jobs.
    :raises: Throws an exception if the user is not logged into SciServer (use
        Authentication.login for that purpose). Throws an exception if the HTTP
        request to the CasJobs API returns an error.
    :example: status = CasJobs.getJobStatus(CasJobs.submitJob("select 1"))

    .. seealso:: CasJobs.submitJob, CasJobs.waitForJob, CasJobs.cancelJob.
    """
    jobId = str(jobId)
    return __response_get(
        URL_DICT[TASKNAMES.GETJOBSTATUS].replace(DCONST.JOB_ID, jobId),
        __token(),
        EXCEPT.GET_JOB_STATUS_ERROR.replace(DCONST.JOB_ID, jobId),
    )


def cancelJob(jobId):
    """
    Cancels a job already submitted.

    :param jobId: id of job (integer)
    :return: Returns True if the job was canceled successfully.
    :raises: Throws an exception if the user is not logged into SciServer (use
        Authentication.login for that purpose). Throws an exception if the HTTP
        request to the CasJobs API returns an error.
    :example: response = CasJobs.cancelJob(CasJobs.submitJob("select 1"))

    .. seealso:: CasJobs.submitJob, CasJobs.waitForJob.
    """
    jobId = str(jobId)
    response = requests.delete(
        URL_DICT[TASKNAMES.CANCELJOB].replace(DCONST.JOB_ID, jobId),
        headers=__get_headers(__token()),
    )
    __validate_response_status(
        response, EXCEPT.CANCEL_JOB_ERROR.replace(DCONST.JOB_ID, jobId)
    )

    return True


def waitForJob(jobId, verbose=False, pollTime=5):
    """
    Queries regularly the job status and waits until the job is completed.

    :param jobId: id of job (integer)
    :param verbose: if True, will print "wait" messages on the screen while the
        job is still running. If False, will suppress the printing of messages
        on the screen.
    :param pollTime: idle time interval (integer, in seconds) before querying
        again for the job status. Minimum value allowed is 5 seconds.
    :return: After the job is finished, returns a dictionary object containing
        the job status and related metadata. The "Status" field can be equal to
        0 (Ready), 1 (Started), 2 (Canceling), 3(Canceled), 4 (Failed) or 5
        (Finished).
    :raises: Throws an exception if the user is not logged into SciServer (use
        Authentication.login for that purpose). Throws an exception if the HTTP
        request to the CasJobs API returns an error.
    :example: CasJobs.waitForJob(CasJobs.submitJob("select 1"))

    .. seealso:: CasJobs.submitJob, CasJobs.getJobStatus, CasJobs.cancelJob.
    """

    try:
        minPollTime = 5  # in seconds
        complete = False

        waitingStr = "Waiting..."
        # back = "\b" * len(waitingStr)
        if verbose:
            print(waitingStr, end="")

        while not complete:
            if verbose:
                # print(back, end="")
                print(waitingStr, end="")
            jobDesc = getJobStatus(jobId)
            jobStatus = int(jobDesc["Status"])
            if jobStatus in (3, 4, 5):
                complete = True
                if verbose:
                    # print(back, end="")
                    print("Done!")
            else:
                time.sleep(max(minPollTime, pollTime))

        return jobDesc
    except Exception as e:
        raise e


def writeFitsFileFromQuery(
    fileName,
    queryString,
    context: str = CASJOBSCONST.MYDB,
):
    """
    Executes a quick CasJobs query and writes the result to a local Fits file
    (http://www.stsci.edu/institute/software_hardware/pyfits).

    :param fileName: path to the local Fits file to be created (string)
    :param queryString: sql query (string)
    :param context: database context (string)
    :return: Returns True if the fits file was created successfully.
    :raises: Throws an exception if the user is not logged into SciServer (use
        Authentication.login for that purpose). Throws an exception if the HTTP
        request to the CasJobs API returns an error.
    :example: CasJobs.writeFitsFileFromQuery("/home/user/myFile.fits","select 1
        as foo")

    .. seealso:: CasJobs.submitJob, CasJobs.getJobStatus, CasJobs.executeQuery,
        CasJobs.getPandasDataFrameFromQuery, CasJobs.getNumpyArrayFromQuery
    """
    try:
        task.name = __get_taskname(TASKNAMES.WRITEFITSFILEFROMQUERY)
        bytesio = executeQuery(queryString, context=context, format="fits")
        with open(fileName, "w+b") as theFile:
            theFile.write(bytesio.read())
        return True
    except Exception as e:
        raise e


# no explicit index column by default
def getPandasDataFrameFromQuery(queryString, context: str = CASJOBSCONST.MYDB):
    """
    Executes a casjobs quick query and returns the result as a pandas dataframe
    object with an index (http://pandas.pydata.org/pandas-docs/stable/).

    :param queryString: sql query (string)
    :param context: database context (string)
    :return: Returns a Pandas dataframe containing the results table.
    :raises: Throws an exception if the user is not logged into SciServer (use
        Authentication.login for that purpose). Throws an exception if the HTTP
        request to the CasJobs API returns an error.
    :example: df = CasJobs.getPandasDataFrameFromQuery("select 1 as foo",
        context="MyDB")

    .. seealso:: CasJobs.submitJob, CasJobs.getJobStatus, CasJobs.executeQuery,
        CasJobs.writeFitsFileFromQuery, CasJobs.getNumpyArrayFromQuery
    """
    try:
        task.name = __get_taskname(TASKNAMES.GETPANDASDATAFRAMEFROMQUERY)
        cvsResponse = executeQuery(
            queryString,
            context=context,
            format="readable",
        )

        # if the index column is not specified then it will add it's own column
        # which causes problems when uploading the transformed data
        dataFrame = pandas.read_csv(cvsResponse, index_col=None)

        return dataFrame

    except Exception as e:
        raise e


def getNumpyArrayFromQuery(queryString, context: str = CASJOBSCONST.MYDB):
    """
    Executes a casjobs query and returns the results table as a Numpy array
    (http://docs.scipy.org/doc/numpy/).

    :param queryString: sql query (string)
    :param context: database context (string)
    :return: Returns a Numpy array storing the results table.
    :raises: Throws an exception if the user is not logged into SciServer (use
        Authentication.login for that purpose). Throws an exception if the HTTP
        request to the CasJobs API returns an error.
    :example: array = CasJobs.getNumpyArrayFromQuery("select 1 as foo",
        context="MyDB")

    .. seealso:: CasJobs.submitJob, CasJobs.getJobStatus, CasJobs.executeQuery,
        CasJobs.writeFitsFileFromQuery, CasJobs.getPandasDataFrameFromQuery

    """
    try:
        task.name = __get_taskname(TASKNAMES.GETNUMPYARRAYFROMQUERY)
        dataFrame = getPandasDataFrameFromQuery(queryString, context)

        return dataFrame.as_matrix()

    except Exception as e:
        raise e


# require pandas for now but be able to take a string in the future
def uploadPandasDataFrameToTable(
    dataFrame, tableName, context: str = CASJOBSCONST.MYDB
):
    """
    Uploads a pandas dataframe object into a CasJobs table. If the dataframe
    contains a named index, then the index will be uploaded as a column as well.

    :param dataFrame: Pandas data frame containg the data
        (pandas.core.frame.DataFrame)
    :param tableName: name of CasJobs table to be created.
    :param context: database context (string)
    :return: Returns True if the dataframe was uploaded successfully.
    :raises: Throws an exception if the user is not logged into SciServer
        (use Authentication.login for that purpose). Throws an exception if the
        HTTP request to the CasJobs API returns an error.
    :example: response = CasJobs.uploadPandasDataFrameToTable(
        CasJobs.getPandasDataFrameFromQuery("select 1 as foo", context="MyDB"),
        "NewTableFromDataFrame")

    .. seealso:: CasJobs.uploadCSVDataToTable
    """
    try:
        task.name = __get_taskname(TASKNAMES.UPLOADPANDASDATAFRAMETOTABLE)

        if dataFrame.index.name is not None and dataFrame.index.name != "":
            sio = dataFrame.to_csv().encode("utf8")
        else:
            sio = dataFrame.to_csv(
                index_label=False,
                index=False,
            ).encode("utf8")

        return uploadCSVDataToTable(sio, tableName, context)

    except Exception as e:
        raise e


def uploadCSVDataToTable(csvData, tableName, context: str = CASJOBSCONST.MYDB):
    """
    Uploads CSV data into a CasJobs table.

    :param csvData: a CSV table in string format.
    :param tableName: name of CasJobs table to be created.
    :param context: database context (string)
    :return: Returns True if the csv data was uploaded successfully.
    :raises: Throws an exception if the user is not logged into SciServer (use
        Authentication.login for that purpose). Throws an exception if the HTTP
        request to the CasJobs API returns an error.
    :example: csv = CasJobs.getPandasDataFrameFromQuery("select 1 as foo",
        context="MyDB").to_csv().encode("utf8");
        response = CasJobs.uploadCSVDataToTable(csv, "NewTableFromDataFrame")

    .. seealso:: CasJobs.uploadPandasDataFrameToTable
    """
    if task.name is not None:
        taskName = task.name
        task.name = None
    else:
        taskName = __get_taskname(TASKNAMES.UPLOADCSVDATATOTABLE)
    #
    postResponse = requests.post(
        URL_DICT[TASKNAMES.EXECUTEQUERY]
        .replace(DCONST.CONTEXT_ID, context)
        .replace(DCONST.TABLENAME_ID, tableName)
        .replace(DCONST.TASKNAME_ID, __taskname_url(taskName)),
        data=csvData,
        headers={CASJOBSCONST.X_AUTH_TOKEN: __token()},
        stream=True,
    )
    __validate_response_status(
        postResponse,
        EXCEPT.UPLOAD_CSV_ERROR.replace(
            DCONST.TABLENAME_ID,
            tableName,
        ),
    )
    return True
