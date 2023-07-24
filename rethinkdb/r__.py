# # Copyright 2018 RethinkDB
# #
# # Licensed under the Apache License, Version 2.0 (the 'License');
# # you may not use this file except in compliance with the License.
# # You may obtain a copy of the License at
# #
# #     http://www.apache.org/licenses/LICENSE-2.0
# #
# # Unless required by applicable law or agreed to in writing, software
# # distributed under the License is distributed on an 'AS IS' BASIS,
# # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# # See the License for the specific language governing permissions and
# # limitations under the License.

from __future__ import annotations
from functools import partial
from typing import (Literal, Union,)
from rethinkdb.core.net.connection import Connection

from rethinkdb.core.net.conn_config import SSLParams
from rethinkdb.core.net.conn_config import ConnectionConfig
# from rethinkdb.query import (
#     json,
#     js,
#     args,
#     http,
#     error,
#     random,
#     do,
#     table,
#     db,
#     db_create,
#     db_drop,
#     db_list,
#     db_config,
#     table_create,
#     table_drop,
#     table_list,
#     grant,
#     branch,
#     union,
#     map,
#     group,
#     reduce,
#     count,
#     sum,
#     avg,
#     min,
#     max,
#     distinct,
#     contains,
#     asc,
#     desc,
#     eq,
#     ne,
#     lt,
#     le,
#     gt,
#     ge,
#     add,
#     sub,
#     mul,
#     div,
#     mod,
#     bit_and,
#     bit_or,
#     bit_xor,
#     bit_not,
#     bit_sal,
#     bit_sar,
#     floor,
#     ceil,
#     round,
#     not_,
#     and_,
#     or_,
#     type_of,
#     info,
#     binary,
#     range,
#     make_timezone,
#     time,
#     iso8601,
#     epoch_time,
#     now,
#     literal,
#     object,
#     uuid,
#     geojson,
#     point,
#     line,
#     polygon,
#     distance,
#     intersects,
#     circle,
#     format,
#     row,
#     monday,
#     tuesday,
#     wednesday,
#     thursday,
#     friday,

# )
from rethinkdb import connections


ConnectionTypes = Union[Literal["asyncio"], Literal["gevent"], Literal["tornado"], Literal["trio"], Literal["twisted"], None]


# class RethinkDB:
#     __slots__ = ("__transport_type", "_loop_type")
#     _loop_type: ConnectionTypes

#     def __init__(self) -> None:

#         # Re-export internal modules for backward compatibility
#         # self.ast = ast
#         # self.errors = errors
#         # self.query = query.ReqlQueryMixin
#         self.set_loop_type(None)

def __select_connection_type(library: ConnectionTypes = None):
    if library == "asyncio":
        from rethinkdb.net.asyncio_net import AsyncTransport
        return AsyncTransport

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
        return DefaultTransport
    raise ValueError("Unknown library type")



def connect(
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
    conn_cfg = ConnectionConfig.new(
        host=host,
        port=port,
        db=db,
        user=user,
        password=password,
        timeout=timeout,
        ssl=ssl,
        url=url
    )

    conn = Connection(
        t_transport=self.__transport_type,
        config=conn_cfg,
    )
    return conn.reconnect()


connect_async = partial(connect, connection_type="asyncio")


__all__ = (
    "connect",
    "connect_async",
    "json",
    "js",

)