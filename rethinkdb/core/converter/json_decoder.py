from __future__ import annotations
import base64
from datetime import datetime
import json
from typing import (
    Any,
    Callable,
    TYPE_CHECKING,
)
from typing_extensions import Unpack
if TYPE_CHECKING:
    from rethinkdb.core.options import GlobalOptions


from rethinkdb.core.ast import (
    ReqlBinary,
    ReqlTzinfo,
)
from rethinkdb.core.errors import ReqlDriverError

class ReqlDecoder(json.JSONDecoder):
    """
    Default JSONDecoder subclass to handle pseudo-type conversion.
    """

    def __init__(
        self,
        object_hook: Callable[[dict[str, Any]], Any] | None = None,
        parse_float: Callable[[str], Any] | None = None,
        parse_int: Callable[[str], Any] | None = None,
        parse_constant: Callable[[str], Any] | None = None,
        strict: bool = True,
        object_pairs_hook: Callable[[list[tuple[str, Any]]], Any] | None = None,
        **reql_format_opts: Unpack[GlobalOptions],
    ) -> None:
        custom_object_hook = object_hook or self.convert_pseudo_type

        super().__init__(
            object_hook=custom_object_hook,
            parse_float=parse_float,
            parse_int=parse_int,
            parse_constant=parse_constant,
            strict=strict,
            object_pairs_hook=object_pairs_hook,
        )

        self.reql_format_opts = reql_format_opts or {}

    @staticmethod
    def convert_time(obj: dict[str, Any]) -> datetime:
        """
        Convert pseudo-type TIME object to Python datetime object.

        :raises: ReqlDriverError
        """

        if "epoch_time" not in obj:
            raise ReqlDriverError(
                f"pseudo-type TIME object {json.dumps(obj)} does not "
                "have expected field \"epoch_time\"."
            )

        if "timezone" in obj:
            return datetime.fromtimestamp(
                obj["epoch_time"],
                ReqlTzinfo(obj["timezone"])
            )

        return datetime.utcfromtimestamp(obj["epoch_time"])

    @staticmethod
    def convert_grouped_data(obj: dict[str, Any]) -> dict:
        """
        Convert pseudo-type GROUPED_DATA object to Python dictionary.

        :raises: ReqlDriverError
        """

        if "data" not in obj:
            raise ReqlDriverError(
                f"pseudo-type GROUPED_DATA object {json.dumps(obj)} does not"
                'have the expected field "data".'
            )

        return {make_hashable(k): v for k, v in obj["data"]}

    @staticmethod
    def convert_binary(obj: dict[str, Any]) -> bytes:
        """
        Convert pseudo-type BINARY object to Python bytes object.

        :raises: ReqlDriverError
        """

        if "data" not in obj:
            raise ReqlDriverError(
                f"pseudo-type BINARY object {json.dumps(obj)} does not have "
                "the expected field \"data\"."
            )

        return ReqlBinary(base64.b64decode(obj["data"].encode("utf-8")))

    def __convert_pseudo_type(self, obj: dict[str, Any], format_name: str, converter: Callable) -> Any:
        """
        Convert pseudo-type objects using the given converter.

        :raises: ReqlDriverError
        """
        pseudo_type_format = self.reql_format_opts.get(format_name)
        if pseudo_type_format in (None, "native",):
            return converter(obj)
        elif pseudo_type_format == "raw":
            return obj

        raise ReqlDriverError(f"Unknown {format_name} run option \"{pseudo_type_format}\".")



    def convert_pseudo_type(self, obj: dict[str, Any]) -> Any:
        """
        Convert pseudo-type objects using the given converter.

        :raises: ReqlDriverError
        """

        reql_type = obj.get("$reql_type$")
        converter = {
            None: lambda x: x,
            "GEOMETRY": lambda x: x,
            "BINARY": lambda x: self.__convert_pseudo_type(x, "binary_format", self.convert_binary),
            "GROUPED_DATA": lambda x: self.__convert_pseudo_type(x, "group_format", self.convert_grouped_data),
            "TIME": lambda x: self.__convert_pseudo_type(x, "time_format", self.convert_time),
        }

        converted_type = converter.get(reql_type, lambda x: None)(obj)

        if converted_type is not None:
            return converted_type

        raise ReqlDriverError(f'Unknown pseudo-type "{reql_type}"')


def make_hashable(obj: dict[str, Any] | list[Any] | Any) -> tuple | frozenset:
    """
    Python only allows immutable built-in types to be hashed, such as for keys in
    a dict. This means we can't use lists or dicts as keys in grouped data objects,
    so we convert them to tuples and frozen sets, respectively. This may make it a
    little harder for users to work with converted grouped data, unless they do a
    simple iteration over the result.
    """

    if isinstance(obj, list):
        return tuple(make_hashable(i) for i in obj)

    if isinstance(obj, dict):
        return frozenset((k, make_hashable(v)) for k, v in obj.items())

    return obj
