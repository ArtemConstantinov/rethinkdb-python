from __future__ import annotations

import asyncio
from collections import UserDict
import time
from typing import (
    Type,
    TYPE_CHECKING,
)
from rethinkdb.core.utilities import IterableGenerator

from rethinkdb.core.net import msg
from rethinkdb.core.net.protocol import RdbProtocol
from rethinkdb.core.net.conn_config import SSLParams

from ._client import AsyncClient
if TYPE_CHECKING:
    from rethinkdb.core.net.handshake import HandshakeV1_0
    from rethinkdb.core.converter import (
        ReqlEncoder,
        ReqlDecoder,
    )

class SMapper(UserDict):
    def register_query(self, query: msg.Query) -> None:
        self.data[query.token] = (query, None)

    def set_response(self, response: msg.Response) -> None:
        self.data[response.token][1] = response

    def pop_response(self, query: msg.Query) -> msg.Response | None:
        return self.data.pop(query.token, None)


class AsyncTransport:

    def __init__(self,
        host: str,
        port: int,
        ssl: SSLParams,
        handshake: "HandshakeV1_0",
        t_json_encoder: Type[ReqlEncoder],
        t_json_decoder: Type[ReqlDecoder]
    ) -> None:
        self._closing = False
        self._handshake = handshake
        self.__client = AsyncClient(host, port, ssl)
        self._mapper = SMapper()
        self.__reql_protocol = RdbProtocol(
            self._mapper,
            decoder_type=t_json_decoder,
            encoder_type=t_json_encoder,
        )

        self._user_queries = {}
        self._cursor_cache = {}
        # self._ready = asyncio.Future()


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

    async def connect(self, timeout: float | None, inst):
        await self.__client.connect(timeout)
        deadline = time.time() + timeout if timeout is not None else None

        iter_gen = IterableGenerator(self._handshake.run())

        for request in iter_gen:
            if request != b"":
                self.__client.sendall(request)
            resp = await self.__client.readuntil(b'\0', deadline)
            iter_gen.send(resp)

        self._reader_task = asyncio.create_task(self._read_worker())
        return inst

    def close(self, noreply_wait: bool, token: int) -> None:
        self._closing = True
        # if exception is not None:
        #     err_message = "Connection is closed (%s)." % str(exception)
        # else:
        #     err_message = "Connection is closed."

        # # Cursors may remove themselves when errored, so copy a list of them
        # for cursor in list(self._cursor_cache.values()):
        #     cursor._error(err_message)

        # for query, future in iter(self._user_queries.values()):
        #     if not future.done():
        #         future.set_exception(ReqlDriverError(err_message))

        self._user_queries = {}
        self._cursor_cache = {}

        if noreply_wait:
            query = msg.Q_NoReplayWait(token=token, term_type=None, kwargs=None)
            self.run_query(query, False)

        self.__client.close()
        # We must not wait for the _reader_task if we got an exception, because that
        # means that we were called from it. Waiting would lead to a deadlock.
        # if self._reader_task and exception is None:
        #     # yield from self._reader_task
        #     await self._reader_task

        return None

    def run_query(self, query: msg.Query, noreply: bool = False) -> asyncio.Future | None:
        protocol = self.__reql_protocol
        data = protocol.build_query(query, noreply)
        self.__client.sendall(data)

        if noreply:
            return None

        response_future = asyncio.Future()
        self._user_queries[query.token] = (query, response_future)
        return response_future

    # The _reader coroutine runs in parallel, reading responses
    # off of the socket and forwarding them to the appropriate Future or Cursor.
    # This is shut down as a consequence of closing the stream, or an error in the
    # socket/protocol from the server.  Unexpected errors in this coroutine will
    # close the ConnectionInstance and be passed to any open Futures or Cursors.
    async def _read_worker(self) -> None:
        protocol = self.__reql_protocol
        while self.is_open: # need to use an queue
            iter_gen = IterableGenerator(protocol.read_response())
            for lenght in iter_gen:
                row_bytes = await self.__client.recvall(lenght)
                iter_gen.send(row_bytes)

            # response = protocol.get_response()

        # try:
        #     while True:
        #         # buf = yield from self._streamreader.readexactly(12)
        #         buf = await self._streamreader.readexactly(12)
        #         (token, length,) = struct.unpack("<qL", buf)
        #         # buf = yield from self._streamreader.readexactly(length)
        #         buf = await self._streamreader.readexactly(length)

        #         cursor = self._cursor_cache.get(token)
        #         if cursor is not None:
        #             cursor._extend(buf)
        #         elif token in self._user_queries:
        #             # Do not pop the query from the dict until later, so
        #             # we don't lose track of it in case of an exception
        #             query, future = self._user_queries[token]
        #             res = Response(token, buf, self._parent._get_json_decoder(query))
        #             if res.type == pResponse.SUCCESS_ATOM:
        #                 future.set_result(maybe_profile(res.data[0], res))
        #             elif res.type in (
        #                 pResponse.SUCCESS_SEQUENCE,
        #                 pResponse.SUCCESS_PARTIAL,
        #             ):
        #                 cursor = AsyncioCursor(self, query, res)
        #                 future.set_result(maybe_profile(cursor, res))
        #             elif res.type == pResponse.WAIT_COMPLETE:
        #                 future.set_result(None)
        #             elif res.type == pResponse.SERVER_INFO:
        #                 future.set_result(res.data[0])
        #             else:
        #                 future.set_exception(res.make_error(query))
        #             del self._user_queries[token]
        #         elif not self._closing:
        #             raise ReqlDriverError("Unexpected response received.")
        # except Exception as ex:
        #     if not self._closing:
        #         # yield from self.close(exception=ex)
        #         await self.close(exception=ex)