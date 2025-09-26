"""Compatibility layer for aiogram exceptions.

The aiogram 2.x series renamed several exceptions between minor releases.
This module resolves the available names at import time and exposes a
consistent API to the rest of the project.
"""

from __future__ import annotations

from typing import Tuple, Type

from aiogram.utils import exceptions as aio_exceptions

InvalidQueryID: Type[BaseException] | None = getattr(
    aio_exceptions, "InvalidQueryID", None
)
QueryIdInvalid: Type[BaseException] | None = getattr(
    aio_exceptions, "QueryIdInvalid", None
)

if InvalidQueryID is None and QueryIdInvalid is None:  # pragma: no cover - defensive
    raise ImportError("aiogram.utils.exceptions does not expose callback query errors")

if InvalidQueryID is None:
    InvalidQueryID = QueryIdInvalid  # type: ignore[assignment]
elif QueryIdInvalid is None:
    QueryIdInvalid = InvalidQueryID

# Ensure names are available at module level for static analyzers and runtime.
globals()["InvalidQueryID"] = InvalidQueryID
globals()["QueryIdInvalid"] = QueryIdInvalid

BadRequest: Type[BaseException] = aio_exceptions.BadRequest

callback_invalid_exc: Tuple[Type[BaseException], ...] = (
    InvalidQueryID,
    QueryIdInvalid,
    BadRequest,
)

callback_invalid_exc_names: Tuple[str, ...] = tuple(
    name
    for name, value in (
        ("InvalidQueryID", InvalidQueryID),
        ("QueryIdInvalid", QueryIdInvalid),
        ("BadRequest", BadRequest),
    )
    if value is not None
)

__all__ = [
    "BadRequest",
    "InvalidQueryID",
    "QueryIdInvalid",
    "callback_invalid_exc",
    "callback_invalid_exc_names",
]

