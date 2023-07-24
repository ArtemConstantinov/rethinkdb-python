# Copyright 2018 RethinkDB
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# This file incorporates work covered by the following copyright:
# Copyright 2010-2016 RethinkDB, all rights reserved.
from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from .ast import ReqlQuery

from .q_printer import QueryPrinter


__all__ = [
    "ReqlAuthError",
    "ReqlOperationError",
    "ReqlDriverCompileError",
    "ReqlError",
    "ReqlInternalError",
    "ReqlNonExistenceError",
    "ReqlOpFailedError",
    "ReqlOpIndeterminateError",
    "ReqlPermissionError",
    "ReqlQueryLogicError",
    "ReqlResourceLimitError",
    "ReqlRuntimeError",
    "ReqlServerCompileError",
    "ReqlTimeoutError",
    "ReqlUserError",
    "ReqlCompileError",
    "ReqlCursorEmpty",
    "ReqlDriverError",
    "ReqlTimeoutError",
]


class ReqlCursorEmpty(Exception):
    def __init__(self) -> None:
        self.message = "Cursor is empty."
        super().__init__(self.message)


class ReqlError(Exception):
    """
    Base RethinkDB Query Language Error.
    """

    # NOTE: frames are the backtrace details
    def __init__(self, message: str, term: "ReqlQuery" | None = None, frames: list[int] | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.frames = frames
        self.__query_printer = QueryPrinter(term, self.frames) if not (term is None or frames is None) else None

    def __str__(self) -> str:
        """
        Return the string representation of the error
        """
        if self.__query_printer is None:
            return self.message
        message_ = self.message.rstrip('.')
        return f"{message_} in:\n{self.__query_printer.query}\n{self.__query_printer.carets}"

    def __repr__(self) -> str:
        """
        Return the representation of the error class.
        """
        return f"<{self.__class__.__name__} instance: {str(self)} >"


class ReqlCompileError(ReqlError):
    """
    Exception representing any kind of compilation error. A compilation error
    can be raised during parsing a Python primitive into a Reql primitive or even
    when the server cannot parse a Reql primitive, hence it returns an error.
    """


class ReqlDriverCompileError(ReqlCompileError):
    """
    Exception indicates that a Python primitive cannot be converted into a
    Reql primitive.
    """


class ReqlServerCompileError(ReqlCompileError):
    """
    Exception indicates that a Reql primitive cannot be parsed by the server, hence
    it returned an error.
    """


class ReqlRuntimeError(ReqlError):
    """
    Exception representing a runtime issue within the Python client. The runtime error
    is within the client and not the database.
    """


class ReqlQueryLogicError(ReqlRuntimeError):
    """
    Exception indicates that the query is syntactically correct, but not it has some
    logical errors.
    """


class ReqlNonExistenceError(ReqlQueryLogicError):
    """
    Exception indicates an error related to the absence of an expected value.
    """


class ReqlResourceLimitError(ReqlRuntimeError):
    """
    Exception indicates that the server exceeded a resource limit (e.g. the array size limit).
    """


class ReqlUserError(ReqlRuntimeError):
    """
    Exception indicates that en error caused by `r.error` with arguments.
    """


class ReqlInternalError(ReqlRuntimeError):
    """
    Exception indicates that some internal error happened on server side.
    """


class ReqlOperationError(ReqlRuntimeError):
    """
    Exception indicates that the error happened due to availability issues.
    """


class ReqlOpFailedError(ReqlOperationError):
    """
    Exception indicates that REQL operation failed.
    """


class ReqlOpIndeterminateError(ReqlOperationError):
    """
    Exception indicates that it is unknown whether an operation failed or not.
    """


class ReqlPermissionError(ReqlRuntimeError):
    """
    Exception indicates that the connected user has no permission to execute the query.
    """


class ReqlDriverError(ReqlError):
    """
    Exception representing the Python client related exceptions.
    """


class ReqlAuthError(ReqlDriverError):
    """
    The exception raised when the authentication was unsuccessful to the database
    server.
    """

    def __init__(self, message: str) -> None:
        message = f"Authentication failed, {message}"
        super().__init__(message)


class ReqlTimeoutError(ReqlDriverError, TimeoutError):
    """
    Exception indicates that the request towards the server is timed out.
    """

    def __init__(self, host: str | None = None, port: int | None = None) -> None:
        message = "Operation timed out."

        if host and port:
            message = f"Could not connect to {host}:{port}, {message}"
        elif host and port is None:
            raise ValueError("If host is set, you must set port as well")
        elif host is None and port:
            raise ValueError("If port is set, you must set host as well")

        super().__init__(message)


class InvalidHandshakeStateError(ReqlDriverError):
    """
    Exception raised when the client entered a not existing state during connection handshake.
    """
