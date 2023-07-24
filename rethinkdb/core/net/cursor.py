from __future__ import annotations
from abc import abstractmethod
import pprint
from collections import deque
from typing import Protocol, Type
from typing_extensions import Unpack
from rethinkdb.core import ast
from rethinkdb.core.net import msg
from rethinkdb.core import errors as errs
from rethinkdb.core.options import GlobalOptions


"""
    This class encapsulates all shared behavior between cursor implementations.
    It provides iteration over the cursor using `iter`, as well as incremental
    iteration using `next`.
        query - the original query that resulted in the cursor, used for:
        query.term_type - the term to be used for pretty-printing backtraces
        query.token - the token to use for subsequent CONTINUE and STOP requests
        query.kwargs - dictate how to format results
    items - The current list of items obtained from the server, this is
        added to in `_extend`, which is called by the ConnectionInstance when a
        new response arrives for this cursor.
    outstanding_requests - The number of requests that are currently awaiting
        a response from the server.  This will typically be 0 or 1 unless the
        cursor is exhausted, but this can be higher if `close` is called.
    threshold - a CONTINUE request will be sent when the length of `items` goes
        below this number.
    error - indicates the current state of the cursor:
        None - there is more data available from the server and no errors have
            occurred yet
        Exception - an error has occurred in the cursor and should be raised
            to the user once all results in `items` have been returned.  This
            will be a ReqlCursorEmpty exception if the cursor completed successfully.
            TODO @gabor-boros: We should not set the `errors` to ReqlCursorEmpty, due
            to it is not an error but a success state.

    A class that derives from this should implement the following functions:
        def _get_next(self, timeout):
            where `timeout` is the maximum amount of time (in seconds) to wait for the
            next result in the cursor before raising a ReqlTimeoutError.
        def _empty_error(self):
            which returns the appropriate error to be raised when the cursor is empty
    """

def _fmt_cursor(obj: "Cursor") -> tuple[str, str, str]:
    val_str = pprint.pformat(
        [obj.items[x] for x in range(min(10, len(obj.items)))]
        + (["..."] if len(obj.items) > 10 else [])
    )
    if val_str.endswith("'...']"):
        val_str = val_str[: -len("'...']")] + "...]"
    spacer_str = "\n" if "\n" in val_str else ""
    if obj.error is None:
        status_str = "streaming"
    elif isinstance(obj.error, errs.ReqlCursorEmpty):
        status_str = "done streaming"
    else:
        status_str = f"error: {obj.error}"
    return status_str, spacer_str, val_str


class Context(Protocol):
    def start(self, term: ast.ReqlQuery, **kwargs: Unpack[GlobalOptions]):
        ...

    def resume(self, cursor: Cursor):
        ...

    def stop(self, cursor: Cursor):
        ...


class Cursor:
    def __init__(self, ctx: Context, query: msg.Query, first_response: msg.Response) -> None:
        self.ctx = ctx
        self.query = query
        self.items: deque[str] = deque()
        # self.outstanding_requests = 0
        # self.threshold = 1
        # self.error = None
        self.completed = False

        # self._maybe_fetch_batch()
        self._extend_internal(first_response)

    def __str__(self) -> str:
        status_str, spacer_str, val_str = _fmt_cursor(self)
        return (
            f"{self.__class__.__module__}.{self.__class__.__name__} ({status_str}):"
            f"{spacer_str}{val_str}"
        )

    def __repr__(self) -> str:
        status_str, spacer_str, val_str = _fmt_cursor(self)
        return (
            f"<{self.__class__.__module__}.{self.__class__.__name__} object at "
            f"{hex(id(self))} ({status_str}): {spacer_str}{val_str}>"
        )

    # def __iter__(self):
    #     return self

    # def __next__(self):
    #     return self._get_next()

    # def __aiter__(self):
    #     return self

    # def __anext__(self):
    #     return self._get_next()

    # def __enter__(self):
    #     return self

    # def __exit__(self, exc_type, exc_val, exc_tb):
    #     self.close()

    # @staticmethod
    # def _empty_error() -> Type[errs.ReqlCursorEmpty]:
    #     """
    #     Return the empty cursor exception's class.
    #     """
    #     return errs.ReqlCursorEmpty

    @abstractmethod
    def _get_next(self, timeout: float | None = None):
        """
        Return the next item through the cursor.

        `timeout` is the maximum amount of time (in seconds) to wait for the
        next result in the cursor before raising a ReqlTimeoutError.
        """
        raise NotImplementedError("implement _get_next before using it")

    def close(self):
        """
        Close the cursor.
        """
        # if self.error is None:
        #     self.error = self._empty_error()

        # if not self.conn.is_open():
        #     return None

        # self.outstanding_requests += 1
        self.ctx.stop(self)

    @staticmethod
    def _wait_to_timeout(wait: float | bool | None) -> float | None:
        if isinstance(wait, bool):
            return None if wait else 0

        if isinstance(wait, float) and wait >= 0:
            return wait

        raise errs.ReqlDriverError(f"Invalid wait timeout '{wait}'")

    def next(self, wait: bool | float = True):
        """
        Get the next item using the cursor.
        """
        return self._get_next(Cursor._wait_to_timeout(wait))

    def extend(self, res: msg.Response) -> None:
        self.outstanding_requests -= 1
        self._maybe_fetch_batch()

        # res = msg.Response(self.query.token, res_buf, self._json_decoder)
        self._extend_internal(res)

    def _extend_internal(self, res: msg.Response) -> None:
        self.threshold = len(res.data)

        if self.completed:
            return None

        if res.is_success_partial():
            self.items.extend(res.data)
        elif res.is_success_sequence():
            self.items.extend(res.data)
            self.completed = True
        else:
            self.error = res.make_error(self.query)

    def raise_error(self, message: str) -> None:
        """
        Set an error and extend with a dummy response to trigger any waiters
        """
        if self.error is None:
            self.error = errs.ReqlRuntimeError(message, self.query.term_type, [])
            self.extend(msg.DummyResponse(token=self.query.token))

    def _maybe_fetch_batch(self) -> None:
        if (
            self.error is None
            and len(self.items) < self.threshold
            and self.outstanding_requests == 0
        ):
            self.outstanding_requests += 1
            self.ctx.resume(self)


# class DefaultCursor(Cursor):
#     """
#     Default cursor used to get data.
#     """

#     def __iter__(self):
#         return self

#     def __next__(self):
#         self._get_next()
#         return self.items.popleft()
#     # @staticmethod
#     # def _empty_error():
#     #     return DefaultCursorEmpty()

#     def _get_next(self, timeout: float | None = None):
#         deadline = None if timeout is None else time.time() + timeout

#         while len(self.items) == 0:
#             self._maybe_fetch_batch()

#             if self.error is not None:
#                 raise self.error

#             self.ctx.read_response(self.query, deadline)

#         return self.items.popleft()


# class AsyncioCursor(Cursor):
#     # def __init__(self, *args, **kwargs):
#     #     Cursor.__init__(self, *args, **kwargs)
#     #     self.new_response = asyncio.Future()

#     def __aiter__(self):
#         return self

#     async def __anext__(self):

#         try:
#             await self._get_next(None)
#         except ReqlCursorEmpty:
#             await self.close()
#             raise StopAsyncIteration()
#         else:
#             return self.items.popleft()

#     async def close(self) -> None:
#         if self.error:
#             return None
#         self.error = self._empty_error()
#         if self.ctx.is_open():
#             self.outstanding_requests += 1
#             await self.ctx._stop(self)

#     def _extend(self, res_buf) -> None:
#         Cursor._extend(self, res_buf)
#         self.new_response.set_result(True)
#         self.new_response = asyncio.Future()

#     # Convenience function so users know when they've hit the end of the cursor
#     # without having to catch an exception
#     async def fetch_next(self, wait: bool = True) -> bool:
#         timeout = Cursor._wait_to_timeout(wait)
#         waiter = reusable_waiter(self.ctx._io_loop, timeout)
#         while len(self.items) == 0 and self.error is None:
#             self._maybe_fetch_batch()
#             if self.error is not None:
#                 raise self.error
#             with translate_timeout_errors():
#                 # yield from waiter(asyncio.shield(self.new_response))
#                 await waiter(asyncio.shield(self.new_response))
#         # If there is a (non-empty) error to be received, we return True, so the
#         # user will receive it on the next `next` call.
#         return len(self.items) != 0 or not isinstance(self.error, ReqlCursorEmpty)

#     def _empty_error(self) -> ReqlCursorEmpty:
#         # We do not have RqlCursorEmpty inherit from StopIteration as that interferes
#         # with mechanisms to return from a coroutine.
#         return ReqlCursorEmpty()

#     async def _get_next(self, timeout: float | None):
#         waiter = reusable_waiter(self.ctx._io_loop, timeout)
#         while len(self.items) == 0:
#             self._maybe_fetch_batch()
#             if self.error is not None:
#                 raise self.error
#             with translate_timeout_errors():
#                 await waiter(asyncio.shield(self.new_response))
#         # return self.items.popleft()

#     def _maybe_fetch_batch(self):
#         is_error = self.error is None
#         in_treshhold = len(self.items) < self.threshold
#         no_outstanding_requests = self.outstanding_requests == 0

#         if is_error and in_treshhold and no_outstanding_requests:
#             self.outstanding_requests += 1
#             asyncio.ensure_future(self.ctx.resume(self))
