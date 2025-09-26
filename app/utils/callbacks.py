from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict

from aiogram import types

try:  # pragma: no cover - exercised only when compat is missing
    from app.compat import (
        BadRequest,
        InvalidQueryID,
        QueryIdInvalid,
        callback_invalid_exc,
    )
except Exception:  # pragma: no cover - defensive fallback for legacy deployments
    from aiogram.utils import exceptions as _aio_exceptions

    BadRequest = _aio_exceptions.BadRequest
    InvalidQueryID = getattr(_aio_exceptions, "InvalidQueryID", BadRequest)
    QueryIdInvalid = getattr(_aio_exceptions, "QueryIdInvalid", InvalidQueryID)
    callback_invalid_exc = tuple(
        exc for exc in {InvalidQueryID, QueryIdInvalid, BadRequest} if exc is not None
    )
from app.utils.logging import log_event

STALE_MARKERS = ("query is too old", "query_id_invalid")


@dataclass
class SafeAnswerResult:
    ok: bool
    detail: str

    def __bool__(self) -> bool:  # pragma: no cover - simple delegation
        return self.ok

    def as_tuple(self) -> tuple[bool, str]:
        return self.ok, self.detail

    def __iter__(self):  # pragma: no cover - convenience for unpacking
        yield from (self.ok, self.detail)


def _callback_log_context(callback: types.CallbackQuery) -> Dict[str, Any]:
    message = callback.message
    user = callback.from_user
    context: Dict[str, Any] = {
        "callback_id": getattr(callback, "id", None),
        "callback_data": (callback.data or "")[:256],
        "chat_id": message.chat.id if message and message.chat else None,
        "user_id": user.id if user else None,
    }
    if user:
        if user.username:
            context.setdefault("username", user.username)
        if user.full_name:
            context.setdefault("full_name", user.full_name)
    if message:
        context.setdefault("message_id", message.message_id)
    return context


def _calculate_age_seconds(callback: types.CallbackQuery) -> float | None:
    message = callback.message
    if not message:
        return None
    timestamp = getattr(message, "edit_date", None) or getattr(message, "date", None)
    if not timestamp:
        return None
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    now = datetime.now(timezone.utc)
    return (now - timestamp.astimezone(timezone.utc)).total_seconds()


def _is_stale_exception(exc: BaseException) -> bool:
    if isinstance(exc, (InvalidQueryID, QueryIdInvalid)):
        return True
    if isinstance(exc, BadRequest):
        message = str(exc).lower()
        return any(marker in message for marker in STALE_MARKERS)
    message = str(exc).lower()
    return any(marker in message for marker in STALE_MARKERS)


async def safe_answer(
    callback: types.CallbackQuery,
    text: str | None = None,
    show_alert: bool = False,
    cache_time: int = 0,
    *,
    expired_text: str | None = "Кнопка устарела. Пожалуйста, повторите действие.",
    notify_user_on_expired: bool = True,
    log_extra: Dict[str, Any] | None = None,
    **kwargs: Any,
) -> SafeAnswerResult:
    """Safely acknowledge callback queries and handle expired ones."""

    answer_kwargs: dict[str, Any] = dict(kwargs)
    answer_kwargs.setdefault("show_alert", show_alert)
    answer_kwargs.setdefault("cache_time", cache_time)

    log_context = _callback_log_context(callback)
    if log_extra:
        log_context.update(log_extra)

    try:
        await callback.answer(text, **answer_kwargs)
    except callback_invalid_exc as exc:
        expired = _is_stale_exception(exc)
        age_seconds = _calculate_age_seconds(callback)
        detail = "expired" if expired else f"error:{type(exc).__name__}"
        level = "WARN" if expired else "ERROR"
        log_payload = {
            **log_context,
            "age_seconds": age_seconds,
            "err": str(exc),
            "exception_type": type(exc).__name__,
        }
        event = "callback_expired" if expired else "callback_error"
        message_text = (
            "Callback query expired"
            if expired
            else "Failed to answer callback query"
        )
        log_event(event, level=level, message=message_text, **log_payload)

        if expired and notify_user_on_expired and expired_text and callback.message:
            try:
                await callback.message.answer(
                    expired_text,
                    reply_markup=callback.message.reply_markup,
                )
            except Exception as notify_exc:  # pragma: no cover - defensive logging
                log_event(
                    "callback_expired_notify_failed",
                    level="WARN",
                    message="Failed to notify user about expired callback",
                    err=str(notify_exc),
                    exception_type=type(notify_exc).__name__,
                    **log_context,
                )
        return SafeAnswerResult(False, detail)
    except Exception as exc:  # pragma: no cover - unexpected errors
        log_event(
            "callback_error",
            level="ERROR",
            message="Failed to answer callback query",
            err=str(exc),
            exception_type=type(exc).__name__,
            **log_context,
        )
        return SafeAnswerResult(False, f"error:{type(exc).__name__}")

    log_event(
        "callback_ack",
        level="DEBUG",
        message="Callback acknowledged",
        **log_context,
    )
    return SafeAnswerResult(True, "ack")
