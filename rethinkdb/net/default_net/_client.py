from __future__ import annotations
from contextlib import contextmanager
import socket
import ssl
import logging
import errno
import time
from typing import (
    Generator,
    cast,
)
from rethinkdb.core.utilities import ssl_ctx
from rethinkdb.core.net.conn_config import SSLParams
from rethinkdb.core.errors import (
    ReqlAuthError,
    ReqlDriverError,
    ReqlError,
    ReqlTimeoutError,
)

logger = logging.getLogger(__name__)


class ErrProc:
    @staticmethod
    @contextmanager
    def except_socket_err() -> Generator[None, None, None]:
        try:
            yield
        except IOError as err:
            # self.socket.close()
            if "EOF occurred in violation of protocol" in str(err) or "sslv3 alert handshake failure" in str(err):
                # probably on an older version of OpenSSL

                # pylint: disable=line-too-long
                raise ReqlDriverError(
                    "SSL handshake failed, likely because Python is linked against an old version of OpenSSL "
                    "that does not support either TLSv1.2 or any of the allowed ciphers. This can be worked "
                    "around by lowering the security setting on the server with the options "
                    "`--tls-min-protocol TLSv1 --tls-ciphers "
                    "EECDH+AESGCM:EDH+AESGCM:AES256+EECDH:AES256+EDH:AES256-SHA` (see server log for more "
                    f"information): {err}"
                ) from err
            raise ReqlDriverError(f"SSL handshake failed (see server log for more information): {err}") from err

    @staticmethod
    @contextmanager
    def except_soc_err(host: str, port: int) -> Generator[None, None, None]:
        try:
            yield
        except socket.timeout as exc:
            raise ReqlTimeoutError(host, port) from exc
        except Exception as exc:
            raise ReqlDriverError( f"Could not connect to {host}:{port}. Error: {exc}") from exc

    @staticmethod
    @contextmanager
    def except_close_err() -> Generator[None, None, None]:
        try:
            yield
        except ReqlError as exc:
            logger.error(exc.message)
        except Exception as exc:  # pylint: disable=broad-except
            logger.error(exc)

    @staticmethod
    @contextmanager
    def except_recv_err(host: str, port: int) -> Generator[None, None, None]:
        try:
            yield
        except socket.timeout as exc:
            # self._read_buffer = res
            # self.socket.settimeout(None)
            raise ReqlTimeoutError(host, port) from exc
        except IOError as exc:
            if exc.errno == errno.ECONNRESET:
                # self.close()
                raise ReqlDriverError("Connection is closed.") from exc

            if exc.errno == errno.EWOULDBLOCK:
                # This should only happen with a timeout of 0
                raise ReqlTimeoutError(host, port) from exc

            if exc.errno != errno.EINTR:
                raise ReqlDriverError(
                    f"Connection interrupted receiving from {host}:{port} - {str(exc)}"
                ) from exc
        except Exception as exc:
            # self.close()
            raise ReqlDriverError(
                f"Error receiving from {host}:{port} - {exc}"
            ) from exc

    @staticmethod
    @contextmanager
    def except_send_err(host: str, port: int) -> Generator[None, None, None]:
        try:
            yield
        except IOError as exc:
            if exc.errno == errno.ECONNRESET:
                raise ReqlDriverError("Connection is closed.") from exc

            if exc.errno != errno.EINTR:
                raise ReqlDriverError(f"Connection interrupted sending to {host}:{port} - {str(exc)}") from exc

        except Exception as exc:
            raise ReqlDriverError(f"Error sending to {host}:{port} - {exc}") from exc
        finally:
            # self.close()
            ...


def new_socket(host: str, port: int, timeout: float | None = None) -> socket.socket:
    socket_ = socket.create_connection((host, port), timeout)
    socket_.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    socket_.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    return socket_


class DefaultClient:
    """
    Wrapper for socket connection handling
    """

    def __init__(self, host_: str, port_: int, ssl_: SSLParams) -> None:
        self.host = host_
        self.port = port_
        self.ssl = ssl_
        self.__socket: socket.socket | ssl.SSLSocket | None = None

    @property
    def sock(self) -> socket.socket | ssl.SSLSocket:
        if not self.is_open():
            raise RuntimeError("Socket is not initialized.")
        return cast("socket.socket | ssl.SSLSocket", self.__socket)

    def client_port(self) -> int | None:
        if not self.is_open():
            return None
        return self.sock.getsockname()[1]

    def client_address(self) -> str | None:
        if self.is_open():
            return None
        return self.sock.getsockname()[0]

    def is_open(self) -> bool:
        """
        Return if the connection is open.
        """
        return self.__socket is not None

    def connect(self, timeout: float | None = None) -> None:
        with ErrProc.except_soc_err(self.host, self.port):
            self.__socket = new_socket(self.host, self.port, timeout)

        if len(self.ssl) > 0 and "ca_certs" in self.ssl:
            with ErrProc.except_socket_err():
                ssl_context = ssl_ctx(str(self.ssl["ca_certs"]))
                self.__socket = ssl_context.wrap_socket(self.__socket, server_hostname=self.host)

    def close(self) -> None:
        """
        Close the connection.
        """
        if not self.is_open():
            return None

        with ErrProc.except_close_err():
            self.sock.shutdown(socket.SHUT_RDWR)
            self.sock.close()
        self.__socket = None

    def readuntil(self, separator: bytes, deadline: float | None = None) -> bytes:
        """
        The readuntil method reads data from a stream until a separator byte is found or a deadline is reached.

        Parameters:
        - separator: bytes - The byte to search for in the input stream.
        - deadline: float | None - An optional argument that specifies the amount of time to wait for the data (in seconds). If the deadline argument is set to None, the method will wait indefinitely.

        Returns:
        - bytes: A bytes object that includes the data read from the stream, up to (but not including) the separator byte.
        """
        response = bytearray()
        for chunk in iter(lambda: self.recvall(1, deadline), separator):
            response.extend(chunk)
        return bytes(response)

    def recvall(self, length: int, deadline: float | None = None) -> bytearray:
        """
        Read data received through the socket.
        """
        res: bytearray = bytearray()
        timeout: float | None = None if deadline is None else max(0.0, deadline - time.time())

        while len(res) < length:
            with ErrProc.except_recv_err(self.host, self.port):
                self.sock.settimeout(timeout)
                chunk = self.sock.recv(length - len(res))
                self.sock.settimeout(None)

            if len(chunk) == 0:
                self.close()
                raise ReqlDriverError("Connection is closed.")
            res.extend(chunk)
        return res

    def sendall(self, data: bytes | bytearray) -> None:
        """
        Send all data to the server through the socket.
        """
        sending_data = memoryview(data)
        offset = 0
        while offset < len(data):
            with ErrProc.except_send_err(self.host, self.port):
                offset += self.sock.send(sending_data[offset:])