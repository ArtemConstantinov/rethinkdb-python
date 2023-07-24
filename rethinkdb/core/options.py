from __future__ import annotations
from typing import (
    Literal,
    TypedDict,
)

class GlobalOptions(TypedDict, total=False):
    read_mode: Literal["single"] | Literal["majority"] | Literal["outdated"]
    """
    default: "single"
    """
    time_format: Literal["native"] | Literal["raw"]
    """
    default "native"
    """
    profile: bool
    """
    default: False
    """
    durability: Literal["hard"] | Literal["soft"]
    """
    default: hard
    """
    group_format: Literal["native"] | Literal["raw"]
    """
    default "native"
    """
    noreply: bool
    """
    default: False
    """
    db: str
    """
    default: "test"
    """
    array_limit: int
    """
    default: 100_000
    """
    binary_format: Literal["native"] | Literal["raw"]
    """
    default "native"
    """
    min_batch_rows: int
    """
    default: 8
    """
    max_batch_rows: int | None
    """
    default: unlimited
    """
    max_batch_bytes: int
    """
    default: 1MB
    """
    max_batch_seconds: float
    """
    default: 0.5
    """
    first_batch_scaledown_factor: int
    """
    default: 4
    """