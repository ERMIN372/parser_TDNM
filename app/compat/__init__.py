"""Compatibility helpers for dealing with aiogram differences."""

from .aiogram_exceptions import (
    BadRequest,
    InvalidQueryID,
    QueryIdInvalid,
    callback_invalid_exc,
    callback_invalid_exc_names,
)

__all__ = [
    "BadRequest",
    "InvalidQueryID",
    "QueryIdInvalid",
    "callback_invalid_exc",
    "callback_invalid_exc_names",
]

