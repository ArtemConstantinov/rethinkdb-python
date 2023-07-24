from __future__ import annotations
from functools import partial
from typing import (Literal, Union,)
from rethinkdb.core.net.connection import Connection

from rethinkdb.core.net.conn_config import SSLParams
from rethinkdb.core.net.conn_config import ConnectionConfig
from rethinkdb.core.net.connection import Connection
from rethinkdb.core.net import handshake
from rethinkdb.core.net import protocol


class DefaultConnection(Connection):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close(noreply_wait=False)


class AsyncConnection(Connection):
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close(noreply_wait=False)

    async def reconnect(self, noreply_wait: bool = True, timeout: int | None = None) -> None:
        return await super().reconnect(noreply_wait, timeout)






ConnectionTypes = Union[Literal["asyncio"], Literal["gevent"], Literal["tornado"], Literal["trio"], Literal["twisted"], None]


def __select_connection_type(library: ConnectionTypes = None):
    if library == "asyncio":
        from rethinkdb.net.asyncio_net import AsyncTransport
        return AsyncConnection

    # elif library == "gevent":
    #     from rethinkdb.net.gevent_net import net_gevent
    #     self.connection_type = net_gevent.Connection

    # elif library == "tornado":
    #     from rethinkdb.net.tornado_net import net_tornado
    #     self.connection_type = net_tornado.Connection

    # elif library == "trio":
    #     from rethinkdb.net.trio_net import net_trio
    #     self.connection_type = net_trio.Connection

    # elif library == "twisted":
    #     from rethinkdb.net.twisted_net import net_twisted
    #     self.connection_type = net_twisted.Connection

    elif library is None:
        from rethinkdb.net.default_net.transport import DefaultTransport
        return DefaultConnection
    raise ValueError("Unknown library type")

from rethinkdb.net.default_net.transport import DefaultTransport

def new_connection(
    host: str = "Localhost",
    port: int = 28015,
    db: str = "test",
    user: str = "admin",
    password: str = '',
    timeout: int = 20,
    ssl: SSLParams | None = None,
    url: str | None = None,
    connection_type: ConnectionTypes = None,
):
    _cfg = ConnectionConfig.new(
        host=host,
        port=port,
        db=db,
        user=user,
        password=password,
        timeout=timeout,
        ssl=ssl,
        url=url
    )

    _handshake = handshake.HandshakeV1_0(
        username=_cfg.user,
        password=_cfg.password
    )
    _rdb_protocol = protocol.RdbProtocol(
        handshake=_handshake,
        decoder_type=_cfg.json_decoder,
        encoder_type=_cfg.json_encoder
    )
    _transport = DefaultTransport(
        host=_cfg.host,
        port=_cfg.port,
        ssl=_cfg.ssl,
        rdb_protocol=_rdb_protocol,
    )
    conn_type = __select_connection_type(connection_type)
    conn = conn_type(
        transport=_transport,
        db_name=_cfg.db,
        timeout=_cfg.timeout
    )
    return conn.reconnect()


connect_async = partial(new_connection, connection_type="asyncio")
