import sys
import asyncio

__all__ = (
    "get_adapter",
)

class LegacyAsyncAdapter:
    """Adapter for python <= 3.10"""
    @staticmethod
    def open_connection(host, port, *, loop, ssl):
        return asyncio.open_connection(
            host,
            port,
            loop=loop,
            ssl=ssl
        )

    @staticmethod
    def wait_for(fut, timeout, loop):
        return asyncio.wait_for(
            fut,
            timeout,
            loop=loop,
        )

    @staticmethod
    def ensure_future(coro_or_future, *, loop=None):
        return asyncio.ensure_future(coro_or_future, loop=loop)


class AsyncAdapter:
    """Adapter for python >= 3.10"""
    @staticmethod
    def open_connection(host, port, loop, ssl):
        return asyncio.open_connection(
            host,
            port,
            ssl=ssl
        )

    @staticmethod
    def wait_for(fut, timeout, loop):
        return asyncio.wait_for(
            fut,
            timeout,
        )

    @staticmethod
    def ensure_future(coro_or_future, *, loop=None):
        return asyncio.ensure_future(coro_or_future, loop=loop)


def get_adapter():
    """
    Return an asyncio adapter for methods which
    not have loop argument in current python version.
    """
    return (
        AsyncAdapter
        if sys.version_info >= (3, 10)
        else LegacyAsyncAdapter
    )
