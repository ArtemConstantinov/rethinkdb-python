from __future__ import annotations
import asyncio
import socket
import ssl
from typing import cast
from rethinkdb.core.utilities import ssl_ctx
from rethinkdb.core.net.conn_config import SSLParams



async def new_connection(host: str, port: int, ssl_context: ssl.SSLContext | None = None, timeout: float | None = None) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    (
        streamreader,
        streamwriter,
    ) = await asyncio.wait_for(
        asyncio.open_connection(host, port, ssl=ssl_context),
        timeout
    )
    socket_: socket.socket = streamwriter.get_extra_info("socket")
    socket_.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    socket_.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    return streamreader, streamwriter


class AsyncClient:
    __slots__ = (
        "host",
        "port",
        "ssl",
        "_streamreader",
        "_streamwriter",
    )

    def __init__(self, host_: str, port_: int, ssl_: SSLParams) -> None:
        self.host = host_
        self.port = port_
        self.ssl = ssl_
        self._streamreader = None
        self._streamwriter = None

    @property
    def r_stream(self) -> asyncio.StreamReader:
        if not self.is_open():
            raise RuntimeError("StreamReader is not initialized.")
        return cast(asyncio.StreamReader, self._streamreader)

    @property
    def w_stream(self) -> asyncio.StreamWriter:
        if not self.is_open():
            raise RuntimeError("StreamWriter is not initialized.")
        return cast(asyncio.StreamWriter, self._streamwriter)

    def client_port(self) -> int | None:
        if not self.is_open():
            return None
        return self.w_stream.get_extra_info("sockname")[1]

    def client_address(self) -> str | None:
        if not self.is_open():
            return None
        return self.w_stream.get_extra_info("sockname")[0]

    def is_open(self) -> bool:
        """
        Return if the connection is open.
        """
        w_stream, r_stream = self._streamwriter, self._streamreader
        is_open_ = not (w_stream is None or w_stream.is_closing())
        is_open_ |= not (r_stream is None or r_stream.at_eof())
        return is_open_

    async def connect(self, timeout: float | None) -> None:
        ssl_context = ssl_ctx(str(self.ssl["ca_certs"])) if (self.ssl and "ca_certs" in self.ssl) else None
        (
            self._streamreader,
            self._streamwriter
        ) = await new_connection(
            self.host,
            self.port,
            ssl_context,
            timeout
        )

    def close(self) -> None:
        """
        Close the connection.
        """
        if not self.is_open():
            return None

        self.w_stream.close()
        self.r_stream.feed_eof()
        self._streamwriter = None
        self._streamreader = None

    async def readuntil(self, separator: bytes, deadline: float | None = None) -> bytes:
        """
        Asynchronously read data from a stream until a separator byte is found or a deadline is reached.
        Args:
            separator: bytes: The separator to search for in the input stream.
            deadline: float | None: An optional time (in seconds) to limit the amount of time spent waiting for data.
                                    If None, the method will wait indefinitely.
        Returns:
            bytes: The data read from the stream, up to (but not including) the separator byte.
        """
        data = await asyncio.wait_for(
            self.r_stream.readuntil(separator),
            deadline
        )
        return data[:-1]  # Remove separator

    async def recvall(self, length: int, deadline: float | None = None) -> bytes:
        """
        Read data received through the socket.
        """
        return await asyncio.wait_for(
            self.r_stream.readexactly(length),
            deadline
        )


    def sendall(self, data: bytes) -> None:
        """
        Send all data to the server through the socket.
        """
        return self.w_stream.write(data)
