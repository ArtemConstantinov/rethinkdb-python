from __future__ import annotations
import struct
from typing import (
    Any,
    cast,
)
from functools import (
    partial,
    partialmethod,
    cached_property,
)
try:
    from rethinkdb import ql2_pb2
except ImportError:
    raise ImportError("Please fetch <ql2_pb2.proto> and generate from it: <ql2_pb2.py>")

from rethinkdb.core import ast
from rethinkdb.core.converter import (
    ReqlEncoder,
    ReqlDecoder,

)
from rethinkdb.core import errors as errs
from rethinkdb.core.options import GlobalOptions
from .token import Token

pErrorType = ql2_pb2.Response.ErrorType
pResponseType = ql2_pb2.Response.ResponseType
pQueryType = ql2_pb2.Query.QueryType


class Query:
    """
    Query sent to the database.
    """

    __slot__ = (
        "query_type",
        "token",
        "term",
        "kwargs",
    )

    def __init__(self, type: int, token: Token, term_type: ast.ReqlQuery | None, kwargs: GlobalOptions | None = None) -> None:
        self.type = type
        self.token = token
        self.term_type = term_type
        self.kwargs = kwargs or {}

    def serialize(self, reql_encoder: ReqlEncoder = ReqlEncoder()) -> bytes:
        """
        Serialize Query using the Reql encoder.
        """
        message: list[Any] = [self.type]
        if self.term_type is not None:
            message.append(self.term_type)
        if self.kwargs is not None:
            message.append(ast.expr(self.kwargs))

        query_bytes = reql_encoder.encode(message).encode("utf-8")
        query_header = struct.pack("<QL", self.token.value, len(query_bytes))
        return query_header + query_bytes


Q_NoReplayWait = partial(Query, type=pQueryType.NOREPLY_WAIT)
Q_Server_Info = partial(Query, type=pQueryType.SERVER_INFO)
Q_Start = partial(Query, type=pQueryType.START)
Q_Continue = partial(Query, type=pQueryType.CONTINUE)
Q_Stop = partial(Query, type=pQueryType.STOP)



# def maybe_profile(value: "Cursor" | str, res: "Response") -> "Cursor" | str | dict[str, Any]:
#     """
#     If the profile is set for the response, return a dict composed of the
#     original value and profile.
#     """

#     if res.profile is not None:  # type: ignore
#         return {"value": value, "profile": res.profile}

#     return value


class Response:
    """
    Response received from the DB.
    """
    __slots__ = (
        "token",
        "type",
        "data",
        "backtrace",
        "profile",
        "error_type",
        "response_note",
        "__dict__",
    )

    def __init__(self, token: Token, json_response: bytes | bytearray, reql_decoder: ReqlDecoder = ReqlDecoder()) -> None:
        full_response: dict[str, Any] = reql_decoder.decode(json_response.decode("utf-8"))

        self.token = token
        self.type: int = full_response["t"]  # the ResponseType, as defined in ql2.proto
        self.data: list[str | dict[str, Any]] = full_response["r"]  # data from the result, as a JSON array
        self.backtrace: list[int] | None = full_response.get("b", None)  # a backtrace if t is an error type; this field will not be present otherwise
        self.profile = full_response.get("p", None)  # a profile if the global optarg profile: true was specified; this field will not be present otherwise
        self.error_type: int | None = full_response.get("e", None)
        self.response_note: list[int] | None = full_response.get("n", None)  # an optional array of ResponseNote values, as defined in ql2.proto

    def __is_type_of(self, expeced_type: int) -> bool:
        return self.type == expeced_type

    @cached_property
    def is_sequential(self) -> bool:
        return self.type in {
            pResponseType.SUCCESS_PARTIAL,
            pResponseType.SUCCESS_SEQUENCE,
        }

    is_success_atom = partialmethod(__is_type_of, expeced_type=pResponseType.SUCCESS_ATOM)
    is_success_partial = partialmethod(__is_type_of, expeced_type=pResponseType.SUCCESS_PARTIAL)
    is_success_sequence = partialmethod(__is_type_of, expeced_type=pResponseType.SUCCESS_SEQUENCE)
    is_server_info = partialmethod(__is_type_of, expeced_type=pResponseType.SERVER_INFO)
    is_wait_complete = partialmethod(__is_type_of, expeced_type=pResponseType.WAIT_COMPLETE)
    # errors
    is_client_error = partialmethod(__is_type_of, expeced_type=pResponseType.CLIENT_ERROR)
    is_compile_error = partialmethod(__is_type_of, expeced_type=pResponseType.COMPILE_ERROR)
    is_runtime_error = partialmethod(__is_type_of, expeced_type=pResponseType.RUNTIME_ERROR)


    def get_data(self, err_trace_query: Query):
        if self.is_success_atom():
            # SUCCESS_ATOM: The whole query has been returned and the result is in the first (and only) element of r
            return self.data[0]

        elif self.is_sequential:
            # SUCCESS_PARTIAL: The query has returned a stream, which may or may not be complete. To retrieve more results for the query, send a CONTINUE message (see below).
            # SUCCESS_SEQUENCE: Either the whole query has been returned in r, or the last section of a multi-response query has been returned.
            return self.data

        elif self.is_server_info():
            # SERVER_INFO: The response to a SERVER_INFO request. The data will be in the first (and only) element of r
            return self.data[0]

        elif self.is_wait_complete():
            # WAIT_COMPLETE: This ResponseType indicates all queries run in noreply mode have finished executing. r will be empty.
            return None

        raise self.make_error(err_trace_query)

    def make_error(self, query: Query) -> errs.ReqlError:
        """
        Compose an error response from the query and the response
        received, from the database. In case the response returned by
        the server is unknown to the client, a `ReqlDriverError` will
        return.
        """
        if self.is_client_error():
            return errs.ReqlDriverError(cast(str, self.data[0]), query.term_type, self.backtrace)
        elif self.is_compile_error():
            return errs.ReqlServerCompileError(cast(str, self.data[0]), query.term_type, self.backtrace)
        elif self.is_runtime_error():
            err_type = {
                pErrorType.INTERNAL: errs.ReqlInternalError,
                pErrorType.RESOURCE_LIMIT: errs.ReqlResourceLimitError,
                pErrorType.QUERY_LOGIC: errs.ReqlQueryLogicError,
                pErrorType.NON_EXISTENCE: errs.ReqlNonExistenceError,
                pErrorType.OP_FAILED: errs.ReqlOpFailedError,
                pErrorType.OP_INDETERMINATE: errs.ReqlOpIndeterminateError,
                pErrorType.USER: errs.ReqlUserError,
                pErrorType.PERMISSION_ERROR: errs.ReqlPermissionError,
            }.get(
                self.error_type,  # type: ignore [None type]
                errs.ReqlRuntimeError
            )
            return err_type(cast(str, self.data[0]), query.term_type, self.backtrace)
        return errs.ReqlDriverError(
            f"Unknown Response type {self.type} encountered in a response."
        )

DummyResponse = partial(Response, json_response=f"{{'t':{pResponseType.SUCCESS_SEQUENCE}, 'r': []}}".encode())
"""
Used by Cursor for raise an error
"""