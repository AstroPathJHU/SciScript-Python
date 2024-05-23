"""utility functions"""

# pylint: disable=missing-timeout # timeout handled by SciServer

from io import StringIO, BytesIO
import asyncio

import aiohttp
import orjson
import requests
import pandas


from SciServer import Authentication, Config
from .constants import EXCEPT
from .constants import CASJOBSCONST
from .constants import DCONST
from .constants import URLCONST
from .constants import TASKNAMES
from .constants import _join_str


def __token():
    """
    Gets the token and validates that it is not empty.

    :return: The authentication token from Authentication.getToken
    :raises: Throws an exception if the user is not logged into SciServer (use
        Authentication.login for that purpose).
    """
    token = Authentication.getToken()
    if token is None or token == "":
        raise ValueError(EXCEPT.LOGIN_ERROR)
    return token


def __get_taskname(task_id=None):
    #
    if Config.isSciServerComputeEnvironment():
        return f"{CASJOBSCONST.SCISCRIPT_PYTHON_COMPUTE}{task_id}"
    return f"{CASJOBSCONST.SCISCRIPT_PYTHON}{task_id}"


def __taskname_url(taskname: str) -> str:
    return f"?{URLCONST.TASKNAME}={__get_taskname(taskname)}"


def __get_headers(token, accept_format: str = None) -> dict:
    """
    Creates the headers for the http requests.

    This create a dictionary with the keys 'X-Auth-Token'
    (value= ``token``), 'Content-Type' (value='application/json'),
    and if an ``accept_format`` is passed, will parse that format
    with utils.__format_to_accept_header and add to an 'Accept'
    key.

    :param token: Authentication.login token
    :param accept_format: An accept format to use for the header.
    :return: Returns headers for the query as a dictionary.
    :raises: Throws an exception if parameter 'format' is not correctly
        specified.
    """
    headers = {
        CASJOBSCONST.X_AUTH_TOKEN: token,
        CASJOBSCONST.CONTENT_TYPE: CASJOBSCONST.CONTENT_JSON,
    }
    if accept_format is not None:
        headers[CASJOBSCONST.ACCEPT] = __format_to_accept_header(accept_format)
    return headers


def __format_to_accept_header(accept_format) -> str:
    """
    Looks up the appropriate return header for an input ``format``.

    :param accept_format: An accept format to use for the header.
    :return: Returns the appropriate accept header string.
    :raises: Throws an exception if parameter 'format' is not correctly
        specified.
    """

    if accept_format in ("pandas", "json", "dict"):
        return "application/json+array"
    if accept_format in ("csv", "readable", "StringIO"):
        return "text/plain"
    if accept_format in ("fits", "BytesIO"):
        return "application/fits"  # defined later using specific serialization
    raise ValueError(EXCEPT.ILLEGAL_FORMAT_ERROR + str(accept_format))


def __format_data(sql, task_name):
    return orjson.dumps(
        {
            CASJOBSCONST.QUERY: sql,
            CASJOBSCONST.TASKNAME: task_name,
        }
    )


def __response_to_json(response):
    return orjson.loads(response.content.decode())


def __validate_response_status(response, on_error):
    if response.status_code != 200:
        raise ValueError(
            _join_str(
                on_error,
                EXCEPT.HTTP_ERROR.replace(
                    DCONST.HTTP_CODE_ID,
                    str(response.status_code),
                ),
                response.content.decode(),
            )
        )


def __response_get(url: str, token, on_error: str):
    """
    Runs a request get for the input params.

    Executes the a request get for the url with headers defined
    from utils.__get_headers (these are the x_auth and content type).
    Will also validate the response and return the response as a json,
    using the orjson library.

    :return: The result is a json object
    :raises: Throws an exception if the user is not logged into SciServer
        (use Authentication.login for that purpose). Throws an exception if
        the HTTP request to the CasJobs API returns an error.

    .. seealso:: CasJobs.getSchemaName, CasJobs.getTables
    """
    get_response = requests.get(url, headers=__get_headers(token))
    __validate_response_status(get_response, on_error)
    return __response_to_json(get_response)


def __parse_post(response, format):  # pylint: disable=redefined-builtin
    if format in ("readable", "StringIO"):
        return StringIO(response.content.decode())
    if format == "pandas":
        r = orjson.loads(response.content.decode())
        if len(r["Result"]) > 1:
            res = []
            for result in r["Result"]:
                res.append(
                    pandas.DataFrame(result["Data"], columns=result["Columns"]),
                )

            return res
        return pandas.DataFrame(
            r["Result"][0]["Data"], columns=r["Result"][0]["Columns"]
        )

    if format in ("csv", "json"):
        return response.content.decode()
    if format == "dict":
        return orjson.loads(response.content.decode())
    if format in ("fits", "BytesIO"):
        return BytesIO(response.content)

    raise ValueError(EXCEPT.ILLEGAL_FORMAT_ERROR + str(format))


async def __async_validate_response_status(response):
    if response.status != 200:
        raise ValueError(
            _join_str(
                EXCEPT.EXECUTE_QUERY_ERROR,
                EXCEPT.HTTP_ERROR.replace(
                    DCONST.HTTP_CODE_ID,
                    str(response.status),
                ),
                await response.text(),
            )
        )


async def __make_post_request(_session, url, data, task_name):
    async with _session.post(url, data=__format_data(data, task_name)) as resp:
        await __async_validate_response_status(resp)
        response_json = await resp.json()
        output = []
        for response in response_json["Result"]:
            output.append(
                {
                    "Columns": response["Columns"],
                    "Data": response["Data"],
                }
            )
        return output


async def __async_post_request(url, datas, headers, task_name):
    #
    # resolver = aiodns.DNSResolver()
    connector = aiohttp.TCPConnector(limit=0)
    #
    async with aiohttp.ClientSession(
        base_url=Config.SkyServerWSurl,
        headers=headers,
        connector=connector,
        json_serialize=orjson.loads,
    ) as _session:
        tasks = [
            __make_post_request(
                _session,
                url,
                data,
                task_name,
            )
            for data in datas
        ]
        responses = await asyncio.gather(*tasks)
    return responses


URL_DICT = {
    TASKNAMES.GETSCHEMANAME: _join_str(
        f"{Config.CasJobsRESTUri}/{URLCONST.USERS}/",
        f"{CASJOBSCONST.KEYSTONE_USER_ID}",
        f"{__taskname_url(TASKNAMES.GETSCHEMANAME)}",
        sep="",
    ),
    TASKNAMES.GETTABLES: _join_str(
        f"{Config.CasJobsRESTUri}/{URLCONST.CONTEXTS}/{DCONST.CONTEXT_ID}",
        f"/{URLCONST.TABLES}{__taskname_url(TASKNAMES.GETTABLES)}",
        sep="",
    ),
    TASKNAMES.EXECUTEQUERY: _join_str(
        f"{Config.CasJobsRESTUri}/{URLCONST.CONTEXTS}/{DCONST.CONTEXT_ID}",
        f"/{URLCONST.QUERY}{DCONST.TASKNAME_ID}",
        sep="",
    ),
    TASKNAMES.SUBMITJOB: _join_str(
        f"{Config.CasJobsRESTUri}/{URLCONST.CONTEXTS}/{DCONST.CONTEXT_ID}",
        f"/{URLCONST.JOBS}{__taskname_url(TASKNAMES.SUBMITJOB)}",
        sep="",
    ),
    TASKNAMES.GETJOBSTATUS: _join_str(
        f"{Config.CasJobsRESTUri}/{URLCONST.JOBS}",
        f"/{DCONST.JOB_ID}{__taskname_url(TASKNAMES.GETJOBSTATUS)}",
        sep="",
    ),
    TASKNAMES.CANCELJOB: _join_str(
        f"{Config.CasJobsRESTUri}/{URLCONST.JOBS}",
        f"/{DCONST.JOB_ID}{__taskname_url(TASKNAMES.CANCELJOB)}",
        sep="",
    ),
    TASKNAMES.UPLOADCSVDATATOTABLE: _join_str(
        f"{Config.CasJobsRESTUri}/{URLCONST.CONTEXTS}/{DCONST.CONTEXT_ID}",
        f"/{URLCONST.TABLES}/{DCONST.TABLENAME_ID}{DCONST.TASKNAME_ID}",
        sep="",
    ),
}
