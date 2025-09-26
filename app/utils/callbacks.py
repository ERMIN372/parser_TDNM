from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from aiogram import types
from aiogram.utils.exceptions import InvalidQueryID, QueryIdInvalid

from app.utils.logging import log_event


async def safe_answer(
    callback: types.CallbackQuery,
    text: str | None = None,
    *,
    show_alert: bool | None = None,
    cache_time: int | None = None,
    url: str | None = None,
    **kwargs: Any,
) -> bool:
    """Safely acknowledge callback queries and handle expired ones."""

    answer_kwargs: dict[str, Any] = {}
    if show_alert is not None:
        answer_kwargs["show_alert"] = show_alert
    if cache_time is not None:
        answer_kwargs["cache_time"] = cache_time
    if url is not None:
        answer_kwargs["url"] = url
    answer_kwargs.update(kwargs)

    try:
        await callback.answer(text, **answer_kwargs)
        return True
    except (InvalidQueryID, QueryIdInvalid) as exc:
        message = callback.message
        chat_id = message.chat.id if message else None
        data_preview = (callback.data or "")[:256]
        age_seconds: float | None = None
        if message and message.date:
            msg_dt = message.date
            if msg_dt.tzinfo is None:
                age_seconds = (datetime.utcnow() - msg_dt).total_seconds()
            else:
                age_seconds = (
                    datetime.now(timezone.utc) - msg_dt.astimezone(timezone.utc)
                ).total_seconds()
        log_event(
            "callback_expired",
            level="WARN",
            message="Callback query expired",
            callback_data=data_preview,
            age_seconds=age_seconds,
            chat_id=chat_id,
            err=str(exc),
        )
        try:
            if message:
                reply_markup = message.reply_markup
                await message.answer(
                    "Кнопка устарела, пожалуйста, повторите действие.",
                    reply_markup=reply_markup,
                )
        except Exception:  # pragma: no cover - we do not want to fail handlers here
            pass
        return False
