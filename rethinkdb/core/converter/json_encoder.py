from __future__ import annotations
import json
from typing import (
    Any,
    Callable,
)
from rethinkdb.core.ast import ReqlQuery


class ReqlEncoder(json.JSONEncoder):
    """
    Default JSONEncoder subclass to handle query conversion.
    """

    def __init__(
        self,
        *,
        skipkeys: bool = False,
        ensure_ascii: bool = False,
        check_circular: bool = False,
        allow_nan: bool = False,
        sort_keys: bool = False,
        indent: int | None = None,
        separators: tuple[str, str] | None = (",", ":"),
        default: Callable[[Any], Any] | None = None,
    ) -> None:
        super().__init__(
            skipkeys=skipkeys,
            ensure_ascii=ensure_ascii,
            check_circular=check_circular,
            allow_nan=allow_nan,
            sort_keys=sort_keys,
            indent=indent,
            separators=separators,
            default=default,
        )

    def default(self, o: Any) -> Any:
        """
        Return a serializable object for ``o``.

        :raises: TypeError
        """

        if isinstance(o, ReqlQuery):
            return o.build()

        return super().default(o)
