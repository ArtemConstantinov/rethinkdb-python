# Copyright 2022 RethinkDB
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

"""
RethinkDB client drivers are responsible for serializing queries, sending them to the server
using the Reql wire protocol, and receiving responses from the server and returning them to
the calling application.

This module contains the supported handshakes which can be used to establish a new connection.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import struct
from abc import abstractmethod
from functools import partial
from random import SystemRandom
from typing import (
    Any,
    Dict,
    Generator,
    Type,
    TypeVar,
)

try:
    from rethinkdb import ql2_pb2
except ImportError:
    raise ImportError("Please fetch <ql2_pb2.proto> and generate from it: <ql2_pb2.py>")

from rethinkdb.core.errors import ReqlAuthError, ReqlDriverError
from rethinkdb.core.utilities import chain_to_bytes


class BaseHandshake:
    """
    :class:`BaseHandshake` is responsible for keeping the common functionality together, what
    handshake versions can reuse later.
    """

    def __init__(self, host: str, port: int) -> None:
        super().__init__()
        self.host = host
        self.port = port

    @property
    @abstractmethod
    def version(self) -> int:
        """
        Return the version number of the handshake.
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def protocol(self) -> int:
        """
        Return the protocol of the handshake.
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def protocol_version(self) -> int:
        """
        Return the version of the protocol.
        """
        raise NotImplementedError()


new_hash = partial(hmac.new, digestmod=hashlib.sha256)

EncoderType = TypeVar("EncoderType", bound="json.JSONEncoder")
DecoderType = TypeVar("DecoderType", bound="json.JSONDecoder")

class HandshakeV1_0:  # pylint: disable=invalid-name
    """
    The client sends the protocol version, authentication method, and authentication as a
    null-terminatedJSON response. RethinkDB currently supports only one authentication method,
    SCRAM-SHA-256, as specified in IETF RFC 7677 and RFC 5802. The RFC is followed with the
    exception of error handling (RethinkDB uses its own higher level error reporting rather than
    the e= field). RethinkDB does not support channel binding and clients should not request this.
    The value of "authentication" is the "client-first-message" specified in RFC 5802 (the channel
    binding flag, optional SASL authorization identity, username (n=), and random nonce (r=).

    More info: https://rethinkdb.com/docs/writing-drivers/
    """

    __slots__ = (
        "__username",
        "__password",
        "_random_nonce",
        "_first_client_message",
        "_server_signature",
        "json_encoder",
        "json_decoder"
    )

    def __init__(self, username: str, password: str) -> None:
        self.__username = username.replace("=", "=3D").replace(",", "=2C")
        self.__password = password

        self.reset()

        self.json_encoder = json.JSONEncoder()
        self.json_decoder = json.JSONDecoder()

    def set_decoder(self, decoder_type: Type[DecoderType]) -> None:
        self.json_decoder = decoder_type()

    def set_encoder(self, encoder_type: Type[EncoderType]) -> None:
        self.json_encoder = encoder_type()

    def reset(self) -> None:
        """
        Reset the handshake to its initial state.
        """
        self._random_nonce = bytes()
        self._first_client_message = bytes()
        self._server_signature = bytes()

    def run(self) -> Generator[bytes | bytearray, bytes | bytearray, None]:
        self.reset()
        init_response = yield self.__initialize_connection()
        auth_response1 = yield self.__read_response(init_response)
        auth_response2 = yield self.__prepare_auth_request(auth_response1)
        self.__read_auth_response(auth_response2)

    @property
    def version(self) -> int:
        return ql2_pb2.VersionDummy.Version.V1_0

    @property
    def protocol(self) -> int:
        return ql2_pb2.VersionDummy.Protocol.JSON

    @property
    def protocol_version(self) -> int:
        return 0

    @staticmethod
    def __get_authentication_message(response: Dict[str, str]) -> Dict[bytes, bytes]:
        """
        Get the first client message and the authentication related data from the
        response provided by RethinkDB.
        """

        message: Dict[bytes, bytes] = {}
        for auth in response["authentication"].encode("ascii").split(b","):
            key, value = auth.split(b"=", 1)
            message[key] = value

        return message

    def __decode_json_response(self, response: bytes | bytearray) -> Dict[str, Any]:
        """
        Get decoded json response from response.

        :raises: ReqlDriverError | ReqlAuthError
        """
        json_response: Dict[str, str] = self.json_decoder.decode(response.decode("utf-8"))

        if not json_response.get("success"):
            if 10 <= int(json_response["error_code"]) <= 20:
                raise ReqlAuthError(json_response["error"])

            raise ReqlDriverError(json_response["error"])

        return json_response

    def __initialize_connection(self) -> bytes:
        """
        Prepare initial connection message. We send the version as well as the initial
        JSON as an optimization.
        """

        self._random_nonce = base64.b64encode(
            bytearray(SystemRandom().getrandbits(8) for _ in range(18))
        )

        self._first_client_message = chain_to_bytes(
            "n=", self.__username,
            ",r=", self._random_nonce,
        )

        initial_message: bytes = chain_to_bytes(
            struct.pack("<L", self.version),
            self.json_encoder.encode(
                {
                    "protocol_version": self.protocol_version,
                    "authentication_method": "SCRAM-SHA-256",
                    "authentication": chain_to_bytes("n,,", self._first_client_message).decode("ascii"),
                }
            ).encode("utf-8"),
            b"\0",
        )

        return initial_message

    def __read_response(self, response: bytes | bytearray) -> bytes:
        """
        Read response of the server. Due to we've already sent the initial JSON, and only support
        a single protocol version at the moment thus we simply read the next response and return an
        empty string as a message.

        :raises: ReqlDriverError | ReqlAuthError
        """

        json_response: Dict[str, str] = self.__decode_json_response(response)
        min_protocol_version: int = int(json_response["min_protocol_version"])
        max_protocol_version: int = int(json_response["max_protocol_version"])

        if not min_protocol_version <= self.protocol_version <= max_protocol_version:
            raise ReqlDriverError(
                f"Unsupported protocol version {self.protocol_version}, expected between "
                f"{min_protocol_version} and {max_protocol_version}"
            )
        return b""

    def __prepare_auth_request(self, response: bytes | bytearray) -> bytes:
        """
        Put together the authentication request based on the response of the database.

        :raises: ReqlDriverError | ReqlAuthError
        """
        json_response: Dict[str, str] = self.__decode_json_response(response)
        first_client_message = json_response["authentication"].encode("ascii")
        authentication = self.__get_authentication_message(json_response)

        random_nonce: bytes = authentication[b"r"]

        if not random_nonce.startswith(self._random_nonce):
            raise ReqlAuthError("Invalid nonce from server")

        salted_password: bytes = hashlib.pbkdf2_hmac(
            "sha256",
            self.__password.encode("utf-8"),
            base64.standard_b64decode(authentication[b"s"]),
            int(authentication[b"i"]),
        )

        message_without_proof: bytes = chain_to_bytes("c=biws,r=", random_nonce)
        auth_message: bytes = b",".join(
            (self._first_client_message, first_client_message, message_without_proof)
        )

        self._server_signature = new_hash(
            new_hash(salted_password, b"Server Key").digest(),
            auth_message
        ).digest()
        client_key: bytes = new_hash(salted_password, b"Client Key").digest()
        client_signature: bytes = new_hash(hashlib.sha256(client_key).digest(), auth_message).digest()
        client_proof: bytes = struct.pack(
            "32B",
            *(
                left ^ random_nonce
                for left, random_nonce in zip(
                    struct.unpack("32B", client_key),
                    struct.unpack("32B", client_signature),
                )
            ),
        )

        authentication_request: bytes = chain_to_bytes(
            self.json_encoder.encode(
                {
                    "authentication": chain_to_bytes(
                        message_without_proof,
                        ",p=",
                        base64.standard_b64encode(client_proof),
                    ).decode("ascii")
                }
            ),
            b"\0",
        )

        return authentication_request

    def __read_auth_response(self, response: bytes | bytearray) -> None:
        """
        Read the authentication request's response sent by the database
        and validate the server signature which was returned.

        :raises: ReqlDriverError | ReqlAuthError
        """

        json_response: Dict[str, str] = self.__decode_json_response(response)
        authentication = self.__get_authentication_message(json_response)
        signature: bytes = base64.standard_b64decode(authentication[b"v"])

        if not hmac.compare_digest(signature, self._server_signature):
            raise ReqlAuthError("Invalid server signature")