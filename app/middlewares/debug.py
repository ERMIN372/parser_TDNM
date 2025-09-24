from __future__ import annotations

import logging
import time
import uuid
from typing import Any, Tuple

from aiogram import types
from aiogram.dispatcher.middlewares import BaseMiddleware


logger = logging.getLogger("updates")


def _extract_update_details(update: types.Update) -> Tuple[str, types.User | None, str]:
    if update.message:
        payload = update.message.text or update.message.caption or ""
        return "message", update.message.from_user, payload
    if update.callback_query:
        payload = update.callback_query.data or ""
        return "callback", update.callback_query.from_user, payload
    if update.inline_query:
        payload = update.inline_query.query or ""
        return "inline", update.inline_query.from_user, payload
    if update.chosen_inline_result:
        payload = update.chosen_inline_result.query or ""
        return "chosen_inline_result", update.chosen_inline_result.from_user, payload
    if update.shipping_query:
        payload = update.shipping_query.invoice_payload or ""
        return "shipping", update.shipping_query.from_user, payload
    if update.pre_checkout_query:
        payload = update.pre_checkout_query.invoice_payload or ""
        return "pre_checkout", update.pre_checkout_query.from_user, payload
    if update.poll_answer:
        return "poll_answer", update.poll_answer.user, ""
    if update.poll:
        return "poll", None, update.poll.question or ""
    return update.event_type or "update", None, ""


def _format_user(user: types.User | None) -> str:
    if user is None:
        return "unknown"
    username = f" @{user.username}" if user.username else ""
    return f"{user.id}{username}"


def _preview(text: str, limit: int = 160) -> str:
    clean = " ".join(text.split())
    if len(clean) <= limit:
        return clean
    return clean[: limit - 1] + "…"


class DebugMiddleware(BaseMiddleware):
    """Log every update with timings for easier debugging."""

    TRACE_KEY = "_debug_trace_id"
    STARTED_KEY = "_debug_started_at"

    async def on_pre_process_update(self, update: types.Update, data: dict[str, Any]):
        trace_id = uuid.uuid4().hex[:8]
        data[self.TRACE_KEY] = trace_id
        data[self.STARTED_KEY] = time.perf_counter()

        update_type, user, payload = _extract_update_details(update)
        user_repr = _format_user(user)
        payload_preview = _preview(payload)

        logger.info("<= [%s] %s from %s: %s", trace_id, update_type, user_repr, payload_preview)

    async def on_post_process_update(self, update: types.Update, result: Any, data: dict[str, Any]):
        trace_id = data.get(self.TRACE_KEY)
        started_at = data.get(self.STARTED_KEY)
        if trace_id and started_at:
            duration = (time.perf_counter() - started_at) * 1000
            logger.info("=> [%s] handled in %.1f ms", trace_id, duration)

    async def on_pre_process_error(self, update: types.Update, error: Exception, data: dict[str, Any]):
        trace_id = data.get(self.TRACE_KEY) or uuid.uuid4().hex[:8]
        exc_info = (type(error), error, getattr(error, "__traceback__", None))
        logger.error("!! [%s] handler error: %s", trace_id, error, exc_info=exc_info)
