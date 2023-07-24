
from __future__ import annotations
import ssl
from typing import (
    Any,
    Generator,
    Generic,
    Iterable,
    TypeVar,
)

__all__ = (
    "EnhancedTuple",
    "chain_to_bytes",
    "IterableGenerator",
    "ssl_ctx",
)


class EnhancedTuple:  # pylint: disable=too-few-public-methods
    """
    This 'enhanced' tuple recursively iterates over it's elements allowing us to
    construct nested hierarchies that insert subsequences into tree. It's used
    to construct the query representation used by the pretty printer.
    """

    def __init__(self, *sequence: Any, int_separator: Iterable[str] = "") -> None:
        self.sequence = sequence
        self.int_separator = int_separator

    def __iter__(self) -> Generator[str | Any, None, None]:
        iterator = iter(self.sequence)
        try:
            yield from next(iterator)
        except StopIteration:
            return
        for token in iterator:
            yield from self.int_separator
            yield from token



def chain_to_bytes(*strings: str | bytes) -> bytes:
    """
    Ensure the bytes and/or strings are chained as bytes.
    """
    return b"".join(
        string.encode("latin-1") if isinstance(string, str) else string
        for string in strings
    )


GTY = TypeVar("GTY")
GTS = TypeVar("GTS")


class IterableGenerator(Generic[GTY, GTS]):
    """
    The HS class is a generator that produces values using another generator.

    Args:
        generator: A generator function or expression that produces the values that the HS instance will iterate over.

    Attributes:
        __generator: The generator function or expression passed to the constructor.
        __out: The next value produced by the generator.
    """
    __slots__ = (
        "__generator",
        "__out"
    )
    def __init__(self, generator: Generator[GTY, GTS, None]) -> None:
        """
        Initializes the HS instance with a generator function or expression.

        Args:
            generator: A generator function or expression that produces the values that the HS instance will iterate over.
        """
        self.__generator = generator
        self.__out = None

    def __iter__(self):
        """
        Defines the iteration behavior of the HS instance.

        Sets the __out variable to the next value produced by the generator.
        Returns the HS instance itself (self).
        """
        self.__out = next(self.__generator)
        return self

    def __next__(self) -> GTY:
        """
        Defines the behavior of the HS instance when the next value is requested during iteration.

        Returns:
            The value of __out.

        Raises:
            StopIteration: When there are no more values to iterate over.
        """
        out = self.__out
        if out is None:
            raise StopIteration()
        return out

    def send(self, data: GTS) -> None:
        """
        Sends a value to the generator.

        Tries to assign the next value produced by the generator to the __out instance variable.
        If the generator raises a StopIteration exception, it sets __out to None.

        Args:
            data: The value to send to the generator.
        """
        try:
            self.__out = self.__generator.send(data)
        except StopIteration:
            self.__out = None


def ssl_ctx(cert_path: str) -> ssl.SSLContext:
    """
    Creates an SSL context with the given certificate path.

    Args:
        cert_path (str): The path to the certificate file.

    Returns:
        ssl.SSLContext: The SSL context with the certificate file loaded.

    Raises:
        FileNotFoundError: If the certificate file is not found.
    """
    ssl_context = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
    if hasattr(ssl_context, "options"):
        ssl_context.options |= getattr(ssl, "OP_NO_SSLv2", 0)  # type: ignore
        ssl_context.options |= getattr(ssl, "OP_NO_SSLv3", 0)
    ssl_context.verify_mode = ssl.CERT_REQUIRED
    ssl_context.check_hostname = True  # redundant with match_hostname
    ssl_context.load_verify_locations(cert_path)
    return ssl_context
    # return ssl_context.wrap_socket(socket_, server_hostname=host)
