from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import (
    Type,
    TypedDict,
    cast,
)
from typing_extensions import Unpack
from urllib.parse import (
    parse_qs,
    urlparse
)

from rethinkdb.core import converter


DEFAULT_HOST = "localhost"
DEFAULT_PORT = 28015
DEFAULT_USER = "admin"
DEFAULT_PWD = ""
DEFAULT_DB = "test"
DEFAULT_TIMEOUT = 20


class SSLParams(TypedDict, total=False):
    ca_certs: str | Path


class ExtraArgs(TypedDict, total=False):
    json_encoder: Type[json.JSONEncoder]
    json_decoder: Type[json.JSONDecoder]

@dataclass(frozen=True)
class ConnectionConfig:
    host: str
    port: int
    db: str
    user: str
    password: str = ""
    timeout: int = 0
    ssl: SSLParams = field(default_factory=cast(Type[SSLParams], dict))
    json_encoder: Type[json.JSONEncoder] = converter.ReqlEncoder
    json_decoder: Type[json.JSONDecoder] = converter.ReqlDecoder

    @classmethod
    def new(
        cls,
        host: str = DEFAULT_HOST,
        port: int = DEFAULT_PORT,
        db: str = DEFAULT_DB,
        user: str = DEFAULT_USER,
        password: str | None = None,
        timeout: int = DEFAULT_TIMEOUT,
        ssl: SSLParams | None = None,
        url: str | None = None,
        **kwargs: Unpack[ExtraArgs]
    ) -> ConnectionConfig:
        password = password or DEFAULT_PWD
        ssl = ssl or {}
        json_encoder = kwargs.pop("json_encoder", converter.ReqlEncoder)
        json_decoder = kwargs.pop("json_decoder", converter.ReqlDecoder)

        if url:
            connection_string = urlparse(url)
            query_string = parse_qs(connection_string.query)
            user = connection_string.username or user
            password = connection_string.password or password
            host = connection_string.hostname or host
            port = connection_string.port or port
            db = connection_string.path.replace("/", "") or DEFAULT_DB
            timeout = next(map(int, query_string.get("timeout", [])), DEFAULT_TIMEOUT)
        return cls(
            host=host,
            port=port,
            db=db,
            user=user,
            password=password,
            timeout=timeout,
            ssl=ssl,
            json_encoder=json_encoder,
            json_decoder=json_decoder
        )
