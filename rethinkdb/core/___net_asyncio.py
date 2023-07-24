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
import asyncio
import contextlib
import socket
import ssl
import struct
from typing import cast

from rethinkdb import ql2_pb2
from rethinkdb.core.errors import (
    ReqlAuthError,
    ReqlCursorEmpty,
    ReqlDriverError,
    ReqlTimeoutError,
    ReqlCursorEmpty,
)
from rethinkdb.net import Connection as ConnectionBase
from rethinkdb.net import (
    Cursor,
    Query,
    Response,
    maybe_profile
)

__all__ = ["Connection"]


pResponse = ql2_pb2.Response.ResponseType
pQuery = ql2_pb2.Query.QueryType





def reusable_waiter(loop: asyncio.AbstractEventLoop, timeout: float | None = None):
    """Wait for something, with a timeout from when the waiter was created.

    This can be used in loops::

        waiter = reusable_waiter(event_loop, 10.0)
        while some_condition:
            yield from waiter(some_future)
    """
    deadline = (loop.time() + timeout) if timeout is not None else None

    def wait(future):
        new_timeout = (
            max(deadline - loop.time(), 0)
            if deadline is not None
            else None
        )
        # return (yield from asyncio.wait_for(future, new_timeout))
        return asyncio.wait_for(future, new_timeout)

    return wait


@contextlib.contextmanager
def translate_timeout_errors():
    try:
        yield
    except asyncio.TimeoutError:
        raise ReqlTimeoutError()


# The asyncio implementation of the Cursor object:
# The `new_response` Future notifies any waiting coroutines that the can attempt
# to grab the next result.  In addition, the waiting coroutine will schedule a
# timeout at the given deadline (if provided), at which point the future will be
# errored.
class AsyncioCursor(Cursor):
    def __init__(self, *args, **kwargs):
        Cursor.__init__(self, *args, **kwargs)
        self.new_response = asyncio.Future()

    def __aiter__(self):
        return self

    # @asyncio.coroutine
    async def __anext__(self):
        try:
            # return (yield from self._get_next(None))
            return await self._get_next(None)
        except ReqlCursorEmpty:
            raise StopAsyncIteration

    # @asyncio.coroutine
    async def close(self) -> None:
        if self.error:
            return None
        self.error = self._empty_error()
        if self.conn.is_open():
            self.outstanding_requests += 1
            # yield from self.conn._parent._stop(self)
            await self.conn._parent._stop(self)

    def _extend(self, res_buf) -> None:
        Cursor._extend(self, res_buf)
        self.new_response.set_result(True)
        self.new_response = asyncio.Future()

    # Convenience function so users know when they've hit the end of the cursor
    # without having to catch an exception
    # @asyncio.coroutine
    async def fetch_next(self, wait: bool = True) -> bool:
        timeout = Cursor._wait_to_timeout(wait)
        waiter = reusable_waiter(self.conn._io_loop, timeout)
        while len(self.items) == 0 and self.error is None:
            self._maybe_fetch_batch()
            if self.error is not None:
                raise self.error
            with translate_timeout_errors():
                # yield from waiter(asyncio.shield(self.new_response))
                await waiter(asyncio.shield(self.new_response))
        # If there is a (non-empty) error to be received, we return True, so the
        # user will receive it on the next `next` call.
        return len(self.items) != 0 or not isinstance(self.error, ReqlCursorEmpty)

    def _empty_error(self) -> ReqlCursorEmpty:
        # We do not have RqlCursorEmpty inherit from StopIteration as that interferes
        # with mechanisms to return from a coroutine.
        return ReqlCursorEmpty()

    async def _get_next(self, timeout: float | None):
        waiter = reusable_waiter(self.conn._io_loop, timeout)
        while len(self.items) == 0:
            self._maybe_fetch_batch()
            if self.error is not None:
                raise self.error
            with translate_timeout_errors():
                await waiter(asyncio.shield(self.new_response))
        return self.items.popleft()

    def _maybe_fetch_batch(self):
        is_error = self.error is None
        in_treshhold = len(self.items) < self.threshold
        no_outstanding_requests = self.outstanding_requests == 0

        if is_error and in_treshhold and no_outstanding_requests:
            self.outstanding_requests += 1
            asyncio.ensure_future(self.conn._parent._continue(self))


class ConnectionInstance:
    _streamreader: asyncio.StreamReader | None = None
    _streamwriter: asyncio.StreamWriter | None = None
    _reader_task = None

    def __init__(self, parent, io_loop: asyncio.AbstractEventLoop | None = None) -> None:
        self._parent = parent
        self._closing = False
        self._user_queries = {}
        self._cursor_cache = {}
        self._ready = asyncio.Future()
        self._io_loop = io_loop
        if self._io_loop is None:
            self._io_loop = asyncio.get_running_loop()

    def client_port(self) -> int | None:
        if not self.is_open():
            return None
        return cast(
            asyncio.StreamWriter,
            self._streamwriter
        ).get_extra_info("sockname")[1]

    def client_address(self) -> str | None:
        if not self.is_open():
            return None
        return cast(
            asyncio.StreamWriter,
            self._streamwriter
        ).get_extra_info("sockname")[0]

    async def connect(self, timeout: float | None = None):
        try:
            ssl_context = None
            if len(self._parent.ssl) > 0:
                ssl_context = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
                if hasattr(ssl_context, "options"):
                    ssl_context.options |= getattr(ssl, "OP_NO_SSLv2", 0)  # type: ignore
                    ssl_context.options |= getattr(ssl, "OP_NO_SSLv3", 0)
                ssl_context.verify_mode = ssl.CERT_REQUIRED
                ssl_context.check_hostname = True  # redundant with match_hostname
                ssl_context.load_verify_locations(self._parent.ssl["ca_certs"])

            (
                self._streamreader,
                self._streamwriter,
            ) = await asyncio.open_connection(
                self._parent.host,
                self._parent.port,
                ssl=ssl_context,
            )
            self._streamwriter.get_extra_info("socket").setsockopt(
                socket.IPPROTO_TCP,
                socket.TCP_NODELAY,
                1
            )
            self._streamwriter.get_extra_info("socket").setsockopt(
                socket.SOL_SOCKET,
                socket.SO_KEEPALIVE,
                1
            )
        except Exception as err:
            raise ReqlDriverError(
                f"Could not connect to {self._parent.host}:{self._parent.port}. "
                f"Error: {err}"
            )
        try:
            self._parent.handshake.reset()
            response = None
            with translate_timeout_errors():
                while True:
                    request = self._parent.handshake.next_message(response)
                    if request is None:
                        break
                    # This may happen in the `V1_0` protocol where we send two requests as
                    # an optimization, then need to read each separately
                    # if request is not "": # SyntaxWarning: "is not" with a literal. Did you mean "!="?
                    if request != "":
                        self._streamwriter.write(request)

                    # response = yield from asyncio.wait_for(
                    response = await asyncio.wait_for(
                        self._streamreader.readuntil(b"\0"),
                        timeout,
                    )
                    response = response[:-1]
        except ReqlAuthError as e:
            # yield from self.close()
            await self.close()
            raise e
        except ReqlTimeoutError as err:
            # yield from self.close()
            await self.close()
            raise ReqlDriverError(
                "Connection interrupted during handshake with %s:%s. Error: %s"
                % (self._parent.host, self._parent.port, str(err))
            )
        except Exception as err:
            # yield from self.close()
            await self.close()
            raise ReqlDriverError(
                "Could not connect to %s:%s. Error: %s"
                % (self._parent.host, self._parent.port, str(err))
            )

        # Start a parallel function to perform reads
        #  store a reference to it so it doesn't get destroyed
        self._reader_task = asyncio.ensure_future(self._reader())
        return self._parent

    def is_open(self) -> bool:
        return not (self._closing or (self._streamreader and self._streamreader.at_eof()))

    async def close(self, noreply_wait=False, token=None, exception=None) -> None:
        self._closing = True
        if exception is not None:
            err_message = "Connection is closed (%s)." % str(exception)
        else:
            err_message = "Connection is closed."

        # Cursors may remove themselves when errored, so copy a list of them
        for cursor in list(self._cursor_cache.values()):
            cursor._error(err_message)

        for query, future in iter(self._user_queries.values()):
            if not future.done():
                future.set_exception(ReqlDriverError(err_message))

        self._user_queries = {}
        self._cursor_cache = {}

        if noreply_wait:
            noreply = Query(pQuery.NOREPLY_WAIT, token, None, None)
            # yield from self.run_query(noreply, False)
            await self.run_query(noreply, False)

        cast(asyncio.StreamWriter, self._streamwriter).close()
        # We must not wait for the _reader_task if we got an exception, because that
        # means that we were called from it. Waiting would lead to a deadlock.
        if self._reader_task and exception is None:
            # yield from self._reader_task
            await self._reader_task

        return None

    async def run_query(self, query, noreply: bool = False):
        self._streamwriter.write(query.serialize(self._parent._get_json_encoder(query)))
        if noreply:
            return None

        response_future = asyncio.Future()
        self._user_queries[query.token] = (query, response_future)
        return await response_future

    # The _reader coroutine runs in parallel, reading responses
    # off of the socket and forwarding them to the appropriate Future or Cursor.
    # This is shut down as a consequence of closing the stream, or an error in the
    # socket/protocol from the server.  Unexpected errors in this coroutine will
    # close the ConnectionInstance and be passed to any open Futures or Cursors.
    async def _reader(self):
        try:
            while True:
                # buf = yield from self._streamreader.readexactly(12)
                buf = await self._streamreader.readexactly(12)
                (token, length,) = struct.unpack("<qL", buf)
                # buf = yield from self._streamreader.readexactly(length)
                buf = await self._streamreader.readexactly(length)

                cursor = self._cursor_cache.get(token)
                if cursor is not None:
                    cursor._extend(buf)
                elif token in self._user_queries:
                    # Do not pop the query from the dict until later, so
                    # we don't lose track of it in case of an exception
                    query, future = self._user_queries[token]
                    res = Response(token, buf, self._parent._get_json_decoder(query))
                    if res.type == pResponse.SUCCESS_ATOM:
                        future.set_result(maybe_profile(res.data[0], res))
                    elif res.type in (
                        pResponse.SUCCESS_SEQUENCE,
                        pResponse.SUCCESS_PARTIAL,
                    ):
                        cursor = AsyncioCursor(self, query, res)
                        future.set_result(maybe_profile(cursor, res))
                    elif res.type == pResponse.WAIT_COMPLETE:
                        future.set_result(None)
                    elif res.type == pResponse.SERVER_INFO:
                        future.set_result(res.data[0])
                    else:
                        future.set_exception(res.make_error(query))
                    del self._user_queries[token]
                elif not self._closing:
                    raise ReqlDriverError("Unexpected response received.")
        except Exception as ex:
            if not self._closing:
                # yield from self.close(exception=ex)
                await self.close(exception=ex)


class Connection(ConnectionBase):
    async def __aenter__(self):
        return self

    async def __aexit__(self, exception_type, exception_val, traceback) -> None:
        await self.close(noreply_wait=False)

    async def _stop(self, cursor):
        self.check_open()
        q = Query(pQuery.STOP, cursor.query.token, None, None)
        return await self._instance.run_query(q, True)

    async def reconnect(self, noreply_wait=True, timeout: float | None = None):
        # We close before reconnect so reconnect doesn't try to close us
        # and then fail to return the Future (this is a little awkward).
        # yield from self.close(noreply_wait)
        await self.close(noreply_wait)
        self._instance = self._conn_type(self, **self._child_kwargs)
        return await self._instance.connect(timeout)

    async def close(self, noreply_wait: bool = True) -> None:
        if self._instance is None:
            return None
        if close := ConnectionBase.close(self, noreply_wait=noreply_wait):
            await close
        return None