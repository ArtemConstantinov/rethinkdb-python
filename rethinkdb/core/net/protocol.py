from __future__ import annotations
import asyncio
from collections import UserDict
import json
from pprint import pprint
import struct
from typing import (
    TYPE_CHECKING,
    Generator,
    Protocol,
    Type,
    TypeVar,
    cast,
)
from rethinkdb.core.utilities import IterableGenerator
import weakref
from rethinkdb.core.net import msg
from .cursor import Cursor
from rethinkdb.core import converter
from .token import Token
from rethinkdb.core.q_printer import QueryPrinter



EncoderType = TypeVar("EncoderType", bound="json.JSONEncoder")
DecoderType = TypeVar("DecoderType", bound="json.JSONDecoder")


class RdbHandshake(Protocol):
    def set_decoder(self, decoder_type: Type[DecoderType]) -> None:
        ...

    def set_encoder(self, encoder_type: Type[EncoderType]) -> None:
        ...

    def reset(self) -> None:
        ...

    def run(self) -> Generator[bytes | bytearray, bytes | bytearray, None]:
        ...


class RdbProtocol:
    """
    Class define the process of reading Raw response bytes in to Response Obbject
    """

    __slots__ = (
        "_json_decoder",
        "_json_encoder",
        "_handshake",
        "_cursor_cache",
        "_waiting_respose",
        "ctx",
    )

    HEADER_SIZE = 12

    def __init__(self, decoder_type: Type["converter.ReqlDecoder"], encoder_type: Type["converter.ReqlEncoder"], handshake: RdbHandshake) -> None:
        self._json_decoder = decoder_type
        self._json_encoder = encoder_type

        handshake.set_decoder(decoder_type)
        handshake.set_encoder(encoder_type)
        self._handshake = handshake
        self._cursor_cache: weakref.WeakValueDictionary[Token, Cursor] = weakref.WeakValueDictionary()
        self._waiting_respose: dict[Token, msg.Query] = {}

    def set_ctx(self, ctx) -> None:
        self.ctx = ctx

    def new_handshake(self) -> IterableGenerator:
        return IterableGenerator(self._handshake.run())

    def build_query(self, query: msg.Query, noreply: bool = False) -> bytes:
        data = query.serialize(self._json_encoder())
        if not noreply:
            self._waiting_respose[query.token] = query
            print("Add", query.token, query)
        return data

    def read_response(self) -> Generator[int, bytes | bytearray, None]:
        """
        Process reading response values from the raw bytes sent by the server.
        """
        header_buff = yield self.HEADER_SIZE
        token_int, length = struct.unpack("<qL", header_buff)
        token = Token(token_int)
        body_buff = yield cast(int, length)

        processed_query = self._waiting_respose.pop(token)
        response = msg.Response(
            token,
            body_buff,
            self._json_decoder(**processed_query.kwargs)
        )

        if cursor := self._cursor_cache.get(token, None):
            # extending cursor
            cursor.extend(response)
            return
        elif response.is_sequential:
            user_data = Cursor(
                ctx=self.ctx,
                query=processed_query,
                first_response=response
            )
            self._cursor_cache[token] = user_data
            
        else:
            user_data = response.get_data(
                err_trace_query=processed_query
            )
        print(token, user_data)

        # if response.profile is not None:
        #     {
        #         "value": user_data,
        #         "profile": response.profile
        #     }
        # else:

        # response = msg.Response(
        #     token,
        #     body_buff,
        #     self._json_decoder(**getattr(q, "kwargs", {}))
        # )
        # if response.is_success_atom():
        #     return maybe_profile(response.data[0], self)

        # # elif response.type in (pResponseType.SUCCESS_PARTIAL, pResponseType.SUCCESS_SEQUENCE,):
        # #     return Reply(maybe_profile(DefaultCursor(conn, query, self), self))

        # elif response.is_server_info():
        #     return response.data[0]

        # elif response.is_wait_complete():
        #     return None

        # raise response.make_error(q)
        # pprint(converter.ReqlDecoder(**q.kwargs).decode(body_buff.decode()))

        # reself._mapper.pop(token)
        # self._mapper.set_response(
        #     Response(
        #         token,
        #         body_buff,
        #         self.get_json_decoder(query=)
        #     )
        # )
