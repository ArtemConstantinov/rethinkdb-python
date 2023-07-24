import asyncio
from functools import partialmethod
from typing import Protocol, overload, Literal
from rethinkdb.core.net.conn_config import SSLParams
from rethinkdb import query
from rethinkdb import connections


class ConnectAsync(Protocol):
    def __call__(
        self,
        host: str = "Localhost",
        port: int = 28015,
        db: str = "test",
        user: str = "admin",
        password: str = '',
        timeout: int = 20,
        ssl: SSLParams | None = None,
        url: str | None = None,
    ) -> asyncio.Future[connections.AsyncConnection]:
        ...


@overload
def connect(
    host: str = "Localhost",
    port: int = 28015,
    db: str = "test",
    user: str = "admin",
    password: str = '',
    timeout: int = 20,
    ssl: SSLParams | None = None,
    url: str | None = None,
    connection_type: None = None
) -> connections.DefaultConnection:
    """Create an default Blocable connection"""


@overload
def connect(
    host: str = "Localhost",
    port: int = 28015,
    db: str = "test",
    user: str = "admin",
    password: str = '',
    timeout: int = 20,
    ssl: SSLParams | None = None,
    url: str | None = None,
    connection_type: Literal["asyncio"] = "asyncio",
) -> asyncio.Future[connections.AsyncConnection]:
    """Create asyncio based connection"""


connect_async: ConnectAsync
