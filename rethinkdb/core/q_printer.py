from __future__ import annotations
import typing as t
if t.TYPE_CHECKING:
    from .ast import ReqlQuery


class QueryPrinter:
    """
    Helper class to print Query failures in a formatted was using carets.
    """
    def __init__(self, root: "ReqlQuery", frames: list[int] | None = None):
        self.root = root
        self.frames = frames or []

    @property
    def query(self) -> str:
        """
        Return the composed query.
        """
        return "".join(self.__compose_term(self.root))

    @property
    def carets(self) -> str:
        """
        Return the carets indicating the location of the failure for the query.
        """
        return "".join(self.__compose_carets(self.root, self.frames))

    def __compose_term(self, term: "ReqlQuery") -> t.Iterable[str]:
        """
        Recursively compose the query term.
        """
        args: t.Generator[t.Iterable[str], None, None] = (
            self.__compose_term(arg) for arg in term._args
        )
        kwargs: dict[str, t.Iterable[str]] = {
            k: self.__compose_term(v) for k, v in term.kwargs.items()
        }
        return term.compose(*args, **kwargs)

    def __compose_carets(self, term: "ReqlQuery", frames: list[int]) -> t.Iterable[str]:
        """
        Generate the carets for the query term which caused the error.
        """

        # If the length of the frames is zero, it means that the current frame
        # is responsible for the error.
        if len(frames) == 0:
            return ("^" for _ in self.__compose_term(term))

        current_frame: int = frames.pop(0)
        args = (
            self.__compose_carets(arg, frames)
            if current_frame == i
            else self.__compose_term(arg)
            for i, arg in enumerate(term._args)
        )
        kwargs: dict[str | int, t.Iterable[str]] = {
            key:
            self.__compose_carets(value, frames) if current_frame == key else self.__compose_term(value)
            for key, value in term.kwargs.items()
        }
        return ("^" if i == "^" else " " for i in term.compose(args, kwargs))
