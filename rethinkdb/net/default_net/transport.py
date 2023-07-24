from __future__ import annotations
from collections import UserDict
import time
from typing import TYPE_CHECKING, Any, Generator, Protocol, Type
from ._client import DefaultClient
from rethinkdb.core.net import msg
from rethinkdb.core.net.protocol import RdbProtocol
from rethinkdb.core.utilities import IterableGenerator
from rethinkdb.core.net.conn_config import SSLParams
from rethinkdb.core.errors import ReqlAuthError, ReqlDriverError

if TYPE_CHECKING:
    from rethinkdb.core.net.handshake import HandshakeV1_0


class SMapper(UserDict):
    def register_query(self, query: msg.Query) -> None:
        self.data[query.token] = (query, None)

    def set_response(self, response: msg.Response) -> None:
        self.data[response.token][1] = response

    def pop_response(self, query: msg.Query) -> msg.Response | None:
        return self.data.pop(query.token, None)


class RdbProtocol(Protocol):
    def new_handshake(self) -> IterableGenerator:
        ...

    def build_query(self, query: msg.Query, noreply: bool = False) -> bytes:
        ...

    def read_response(self) -> Generator[int, bytes | bytearray, None]:
        ...


class DefaultTransport:

    def __init__(
        self,
        host: str,
        port: int,
        ssl: SSLParams,
        rdb_protocol: RdbProtocol
    ) -> None:

        self.__client = DefaultClient(host, port, ssl)
        self._closing: bool = False
        self._mapper = SMapper()
        self._rdb_protocol = rdb_protocol

    @property
    def client_port(self) -> int | None:
        """
        Return the port on which the connection instance is connected to the server.
        """
        return self.__client.client_port()

    @property
    def client_address(self) -> str | None:
        """
        Return the address on which the connection instance is connected to the server.
        """
        return self.__client.client_address()

    @property
    def is_open(self) -> bool:
        """
        Return if the connection instance is set and the connection is open.
        """
        return self.__client.is_open()

    def connect(self, timeout: float | None, inst):
        """
        Open a new connection to the server with the given timeout.
        """

        self.__client.connect(timeout)
        deadline = time.time() + timeout if timeout is not None else None

        iter_gen = self._rdb_protocol.new_handshake()
        try:
            for request in iter_gen:
                if request != b"":
                    self.__client.sendall(request)
                resp = self.__client.readuntil(b'\0', deadline)
                iter_gen.send(resp)
        except ReqlAuthError as e:
            raise ReqlDriverError(f"Server {self.__client.host}:{self.__client.port}") from e
        self._rdb_protocol.set_ctx(inst)
        return inst

    def close(self, noreply_wait_quey: msg.Query | None) -> None:
        """
        Close the connection if connection instance is set.
        """
        self._closing = True

        # Cursors may remove themselves when errored, so copy a list of them
        # for cursor in list(self.cursor_cache.values()):
        #     cursor.raise_error("Connection is closed.")

        # self.reset_cursor_cache()
        if noreply_wait_quey:
            self.run_query(noreply_wait_quey, False)
        self.__client.close()

    def run_query(self, query: msg.Query, noreply: bool = False) -> dict[str, Any] | None:
        """
        Serialize and send the given query to the database.

        If noreply is set, the response won't be parsed and `run_query` returns
        immediately after sending the query.
        """
        protocol = self._rdb_protocol
        data = protocol.build_query(query, noreply)
        self.__client.sendall(data)

        if noreply:
            return None

        iter_gen = IterableGenerator(protocol.read_response())
        for lenght in iter_gen:
            row_bytes = self.__client.recvall(lenght)
            iter_gen.send(row_bytes)

        # return protocol.get_response(query.token)

        # response = self._mapper.pop_response(query)
        # val, err = response.get_reply(query, DefaultCursor)
        # if err is not None:
        #     raise err
        # return val
