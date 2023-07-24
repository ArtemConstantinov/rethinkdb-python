from __future__ import annotations
from typing import (
    Any,
    Generic,
    Protocol,
    TypeVar,
)
from typing_extensions import Unpack, Self
from rethinkdb.core import ast
from rethinkdb.core.net import repl
from rethinkdb.core import errors as errs
from rethinkdb.core.options import GlobalOptions
from . import msg
from .token import TokenGenerator


from . import cursor

T = TypeVar("T", bound="Transport")


class Transport(Protocol):

    def run_query(self, query: msg.Query, noreply: bool = False) -> cursor.Cursor | dict[str, Any] | None:
        ...

    def connect(self, timeout: int, inst: Connection) -> Connection:
        ...

    def close(self, noreply_wait_quey: msg.Query | None) -> None:
        ...

    @property
    def client_port(self) -> int | None:
        ...

    @property
    def client_address(self) -> str | None:
        ...

    @property
    def is_open(self) -> bool:
        ...


class Connection(Generic[T]):
    """
    Handle connection lifecycle, managing the connection instance, connect, reconnect,
    connection close and more.
    """
    __slots__ = ("db", "_timeout", "_transport", "_token", "_repl")

    def __init__(self, *, transport: T, db_name: str, timeout: int) -> None:
        self.db = db_name
        self._timeout = timeout
        self._transport = transport
        self._token = TokenGenerator()
        self._repl = repl.Repl()

    def client_port(self) -> int | None:
        """
        Return the port on which the connection instance is connected to the server.
        """

        if not self.is_open():
            return None

        return self._transport.client_port

    def client_address(self) -> str | None:
        """
        Return the address on which the connection instance is connected to the server.
        """
        if not self.is_open():
            return None

        return self._transport.client_address

    def reconnect(self, noreply_wait: bool = True, timeout: int | None = None) -> Self:
        """
        Reconnect to the server.
        """
        self.close(noreply_wait)

        return self._transport.connect(
            self._timeout if timeout is None else timeout,
            self
        )

    def repl(self) -> "Connection":
        """
        Sets this connection as global state that will be used by subsequence calls to
        `query.run`. Useful for trying out RethinkDB in a Python repl environment.
        """
        self._repl.set_connection(self)
        return self

    def use(self, db: str) -> None:
        """
        Set the encapsulated database to use.
        """
        self.db = db

    def is_open(self) -> bool:
        """
        Return if the connection instance is set and the connection is open.
        """
        return self._transport.is_open

    def __check_open(self) -> None:
        """
        Check if the connection is open, otherwise raise a connection closed error.
        """
        if not self._transport.is_open:
            raise errs.ReqlDriverError("Connection is closed.")

    def close(self, noreply_wait: bool = True) -> None:
        """
        Close the connection if connection instance is set.
        """
        if not self._transport.is_open:
            return None

        noreply_wait_quey = msg.Q_NoReplayWait(
            token=self._token.new(),
            term_type=None,
            kwargs=None
        ) if noreply_wait else None

        self._transport.close(noreply_wait_quey)
        self._token.reset()

    def noreply_wait(self):
        self.__check_open()
        query = msg.Q_NoReplayWait(token=self._token.new(), term_type=None, kwargs=None)
        return self._transport.run_query(query, False)

    def server(self):
        """
        Return the server we connected to.
        """
        self.__check_open()
        query = msg.Q_Server_Info(token=self._token.new(), term_type=None, kwargs=None)
        return self._transport.run_query(query, False)

    def start(self, term: ast.ReqlQuery, **kwargs: Unpack[GlobalOptions]):
        """
        Send a new query to the server.
        """
        self.__check_open()
        if not any(isinstance(i, ast.DB) for i in term._args):  # super slow think
            if "db" in kwargs or self.db is not None:
                kwargs["db"] = ast.DB(kwargs.get("db", self.db))
        query = msg.Q_Start(token=self._token.new(), term_type=term, kwargs=kwargs)
        return self._transport.run_query(query, kwargs.get("noreply", False))

    def resume(self, cursor: cursor.Cursor):
        """
        Send a CONTINUE query to the server if the connection is open.
        """
        self.__check_open()
        query = msg.Q_Continue(token=cursor.query.token, term_type=None, kwargs=None)
        return self._transport.run_query(query, True)

    def stop(self, cursor: cursor.Cursor):
        """
        Send a STOP query to the server if the connection is open.
        """
        self.__check_open()
        query = msg.Q_Stop(token=cursor.query.token, term_type=None, kwargs=None)
        return self._transport.run_query(query, True)
